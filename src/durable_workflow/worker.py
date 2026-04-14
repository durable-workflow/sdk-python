from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from collections.abc import Callable, Iterable
from typing import Any

from . import serializer
from .client import Client
from .errors import ServerError
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
    ) -> None:
        self.client = client
        self.task_queue = task_queue
        self.workflows = {_workflow_name(w): w for w in workflows}
        self.activities = {_activity_name(a): a for a in activities}
        self.worker_id = worker_id or f"py-worker-{uuid.uuid4().hex[:8]}"
        self._stop = asyncio.Event()

    async def _register(self) -> None:
        await self.client.register_worker(
            worker_id=self.worker_id,
            task_queue=self.task_queue,
            supported_workflow_types=list(self.workflows),
            supported_activity_types=list(self.activities),
        )
        log.info("worker %s registered on %s", self.worker_id, self.task_queue)

    async def _run_workflow_task(self, task: dict[str, Any]) -> None:
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

        cls = self.workflows.get(wf_type)
        if cls is None:
            log.warning("no workflow registered for %s; failing task", wf_type)
            await self.client.fail_workflow_task(
                task_id=task_id,
                lease_owner=self.worker_id,
                workflow_task_attempt=attempt,
                message=f"no workflow registered for type {wf_type!r}",
            )
            return

        try:
            outcome = replay(cls, history, start_input)
        except Exception as e:
            log.exception("replay failed")
            await self.client.fail_workflow_task(
                task_id=task_id,
                lease_owner=self.worker_id,
                workflow_task_attempt=attempt,
                message=f"replay failed: {e}",
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
            )
            return

        commands = [c.to_server_command(self.task_queue) for c in outcome.commands]
        log.info(
            "completing workflow task %s with %d command(s): %s",
            task_id,
            len(commands),
            [c["type"] for c in commands],
        )
        await self.client.complete_workflow_task(
            task_id=task_id,
            lease_owner=self.worker_id,
            workflow_task_attempt=attempt,
            commands=commands,
        )

    async def _run_activity_task(self, task: dict[str, Any]) -> None:
        task_id: str = task["task_id"]
        attempt_id: str = task.get("activity_attempt_id") or task.get("attempt_id", "")
        activity_type: str = task.get("activity_type", "")
        raw_args = task.get("arguments")
        args = serializer.decode(raw_args, codec=task.get("payload_codec")) or []
        if not isinstance(args, list):
            args = [args]

        fn = self.activities.get(activity_type)
        if fn is None:
            await self.client.fail_activity_task(
                task_id=task_id,
                activity_attempt_id=attempt_id,
                lease_owner=self.worker_id,
                message=f"no activity registered for {activity_type!r}",
            )
            return

        try:
            result = fn(*args) if not asyncio.iscoroutinefunction(fn) else await fn(*args)
        except Exception as e:
            log.exception("activity failed")
            await self.client.fail_activity_task(
                task_id=task_id,
                activity_attempt_id=attempt_id,
                lease_owner=self.worker_id,
                message=str(e),
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
            )
            return

        log.info("completing activity task %s (%s) -> %r", task_id, activity_type, result)
        await self.client.complete_activity_task(
            task_id=task_id,
            activity_attempt_id=attempt_id,
            lease_owner=self.worker_id,
            result=result,
        )

    async def _workflow_loop(self) -> None:
        while not self._stop.is_set():
            try:
                task = await self.client.poll_workflow_task(
                    worker_id=self.worker_id, task_queue=self.task_queue
                )
            except ServerError as e:
                log.warning("workflow poll error: %s", e)
                await asyncio.sleep(1.0)
                continue
            if task is None:
                continue
            await self._run_workflow_task(task)

    async def _activity_loop(self) -> None:
        while not self._stop.is_set():
            try:
                task = await self.client.poll_activity_task(
                    worker_id=self.worker_id, task_queue=self.task_queue
                )
            except ServerError as e:
                log.warning("activity poll error: %s", e)
                await asyncio.sleep(1.0)
                continue
            if task is None:
                continue
            await self._run_activity_task(task)

    async def run(self) -> None:
        await self._register()
        await asyncio.gather(self._workflow_loop(), self._activity_loop())

    def stop(self) -> None:
        self._stop.set()
