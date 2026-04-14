from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from collections.abc import Callable, Iterable
from typing import Any

from . import serializer
from .activity import ActivityContext, ActivityInfo, _set_context
from .client import Client
from .errors import ActivityCancelled, NonRetryableError
from .workflow import replay

log = logging.getLogger("durable_workflow.worker")


def _activity_name(fn: Callable[..., Any]) -> str:
    return getattr(fn, "__activity_name__", fn.__name__)


def _workflow_name(cls: type) -> str:
    return getattr(cls, "__workflow_name__", cls.__name__)


class Worker:
    def __init__(
        self,
        client: Client,
        *,
        task_queue: str,
        workflows: Iterable[type] = (),
        activities: Iterable[Callable[..., Any]] = (),
        worker_id: str | None = None,
        poll_timeout: float = 5.0,
        max_concurrent_workflow_tasks: int = 10,
        max_concurrent_activity_tasks: int = 10,
        shutdown_timeout: float = 30.0,
    ) -> None:
        self.client = client
        self.task_queue = task_queue
        self.workflows = {_workflow_name(w): w for w in workflows}
        self.activities = {_activity_name(a): a for a in activities}
        self.worker_id = worker_id or f"py-worker-{uuid.uuid4().hex[:8]}"
        self._poll_timeout = poll_timeout
        self._stop = asyncio.Event()
        self._wf_semaphore = asyncio.Semaphore(max_concurrent_workflow_tasks)
        self._act_semaphore = asyncio.Semaphore(max_concurrent_activity_tasks)
        self._shutdown_timeout = shutdown_timeout
        self._in_flight: set[asyncio.Task[Any]] = set()

    async def _register(self) -> None:
        await self.client.register_worker(
            worker_id=self.worker_id,
            task_queue=self.task_queue,
            supported_workflow_types=list(self.workflows),
            supported_activity_types=list(self.activities),
        )
        log.info("worker %s registered on %s", self.worker_id, self.task_queue)

    async def _run_workflow_task(self, task: dict[str, Any]) -> list[dict[str, Any]] | None:
        import json as _json

        log.debug("workflow task payload: %s", _json.dumps(task, default=str)[:2000])
        task_id: str = task["task_id"]
        attempt: int = task.get("workflow_task_attempt", 1)
        wf_type: str = task.get("workflow_type", "")
        history = task.get("history_events", [])

        start_input: list[Any] = []
        codec = task.get("payload_codec")
        raw_args = task.get("arguments")
        try:
            decoded = serializer.decode(raw_args, codec=codec)
            if decoded is not None:
                start_input = decoded if isinstance(decoded, list) else [decoded]
        except ValueError as e:
            log.warning("task %s start input unreadable (%s); falling back to [].", task_id, e)

        run_id: str = task.get("run_id", "")

        cls = self.workflows.get(wf_type)
        if cls is None:
            log.warning("no workflow registered for %s; failing task", wf_type)
            try:
                await self.client.fail_workflow_task(
                    task_id=task_id,
                    lease_owner=self.worker_id,
                    workflow_task_attempt=attempt,
                    message=f"no workflow registered for type {wf_type!r}",
                )
            except Exception as e:
                log.warning("failed to report unknown workflow type: %s", e)
            return None

        try:
            outcome = replay(cls, history, start_input, run_id=run_id)
        except Exception as e:
            log.exception("replay failed")
            try:
                await self.client.fail_workflow_task(
                    task_id=task_id,
                    lease_owner=self.worker_id,
                    workflow_task_attempt=attempt,
                    message=f"replay failed: {e}",
                    failure_type=type(e).__name__,
                    stack_trace=traceback.format_exc(),
                )
            except Exception as fe:
                log.warning("failed to report replay failure: %s", fe)
            return None

        commands = [c.to_server_command(self.task_queue) for c in outcome.commands]
        log.info(
            "completing workflow task %s with %d command(s): %s",
            task_id,
            len(commands),
            [c["type"] for c in commands],
        )
        try:
            await self.client.complete_workflow_task(
                task_id=task_id,
                lease_owner=self.worker_id,
                workflow_task_attempt=attempt,
                commands=commands,
            )
        except Exception as e:
            log.warning("failed to complete workflow task %s: %s", task_id, e)
        return commands

    async def _run_activity_task(self, task: dict[str, Any]) -> None:
        task_id: str = task["task_id"]
        attempt_id: str = task.get("activity_attempt_id") or task.get("attempt_id", "")
        activity_type: str = task.get("activity_type", "")
        attempt_number: int = task.get("attempt_number", 1)
        raw_args = task.get("arguments")
        args = serializer.decode(raw_args, codec=task.get("payload_codec")) or []
        if not isinstance(args, list):
            args = [args]

        fn = self.activities.get(activity_type)
        if fn is None:
            try:
                await self.client.fail_activity_task(
                    task_id=task_id,
                    activity_attempt_id=attempt_id,
                    lease_owner=self.worker_id,
                    message=f"no activity registered for {activity_type!r}",
                )
            except Exception as e:
                log.warning("failed to report unknown activity type: %s", e)
            return

        act_ctx = ActivityContext(
            info=ActivityInfo(
                task_id=task_id,
                activity_type=activity_type,
                activity_attempt_id=attempt_id,
                attempt_number=attempt_number,
                task_queue=self.task_queue,
                worker_id=self.worker_id,
            ),
            client=self.client,
        )
        _set_context(act_ctx)
        try:
            result = fn(*args) if not asyncio.iscoroutinefunction(fn) else await fn(*args)
        except ActivityCancelled:
            log.info("activity %s cancelled via heartbeat", task_id)
            try:
                await self.client.fail_activity_task(
                    task_id=task_id,
                    activity_attempt_id=attempt_id,
                    lease_owner=self.worker_id,
                    message="activity cancelled",
                    failure_type="ActivityCancelled",
                    non_retryable=True,
                )
            except Exception as fe:
                log.warning("failed to report activity cancellation: %s", fe)
            return
        except NonRetryableError as e:
            log.exception("activity failed (non-retryable)")
            try:
                await self.client.fail_activity_task(
                    task_id=task_id,
                    activity_attempt_id=attempt_id,
                    lease_owner=self.worker_id,
                    message=str(e),
                    failure_type=type(e).__name__,
                    stack_trace=traceback.format_exc(),
                    non_retryable=True,
                )
            except Exception as fe:
                log.warning("failed to report activity failure: %s", fe)
            return
        except Exception as e:
            log.exception("activity failed")
            try:
                await self.client.fail_activity_task(
                    task_id=task_id,
                    activity_attempt_id=attempt_id,
                    lease_owner=self.worker_id,
                    message=str(e),
                    failure_type=type(e).__name__,
                    stack_trace=traceback.format_exc(),
                )
            except Exception as fe:
                log.warning("failed to report activity failure: %s", fe)
            return
        finally:
            _set_context(None)

        log.info("completing activity task %s (%s) -> %r", task_id, activity_type, result)
        try:
            await self.client.complete_activity_task(
                task_id=task_id,
                activity_attempt_id=attempt_id,
                lease_owner=self.worker_id,
                result=result,
            )
        except Exception as e:
            log.warning("failed to complete activity task %s: %s", task_id, e)

    def _track(self, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)
        return task

    async def _poll_workflow_tasks(self) -> None:
        while not self._stop.is_set():
            await self._wf_semaphore.acquire()
            if self._stop.is_set():
                self._wf_semaphore.release()
                return
            try:
                task = await self.client.poll_workflow_task(
                    worker_id=self.worker_id,
                    task_queue=self.task_queue,
                    timeout=self._poll_timeout,
                )
            except Exception as e:
                self._wf_semaphore.release()
                log.warning("workflow poll error: %s", e)
                await asyncio.sleep(1.0)
                continue
            if task is None:
                self._wf_semaphore.release()
                continue
            self._track(self._dispatch_workflow_task(task))

    async def _dispatch_workflow_task(self, task: dict[str, Any]) -> None:
        try:
            await self._run_workflow_task(task)
        except Exception:
            log.exception("unhandled error in workflow task execution")
        finally:
            self._wf_semaphore.release()

    async def _poll_activity_tasks(self) -> None:
        while not self._stop.is_set():
            await self._act_semaphore.acquire()
            if self._stop.is_set():
                self._act_semaphore.release()
                return
            try:
                task = await self.client.poll_activity_task(
                    worker_id=self.worker_id,
                    task_queue=self.task_queue,
                    timeout=self._poll_timeout,
                )
            except Exception as e:
                self._act_semaphore.release()
                log.warning("activity poll error: %s", e)
                await asyncio.sleep(1.0)
                continue
            if task is None:
                self._act_semaphore.release()
                continue
            self._track(self._dispatch_activity_task(task))

    async def _dispatch_activity_task(self, task: dict[str, Any]) -> None:
        try:
            await self._run_activity_task(task)
        except Exception:
            log.exception("unhandled error in activity task execution")
        finally:
            self._act_semaphore.release()

    async def run(self) -> None:
        await self._register()
        wf_loop = asyncio.create_task(self._poll_workflow_tasks())
        act_loop = asyncio.create_task(self._poll_activity_tasks())
        try:
            await asyncio.gather(wf_loop, act_loop)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        self._stop.set()
        if self._in_flight:
            log.info("draining %d in-flight task(s)…", len(self._in_flight))
            done, pending = await asyncio.wait(
                self._in_flight, timeout=self._shutdown_timeout
            )
            for t in pending:
                t.cancel()
            if pending:
                log.warning("cancelled %d task(s) after shutdown timeout", len(pending))
