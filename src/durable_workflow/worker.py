from __future__ import annotations

import asyncio
import contextlib
import logging
import traceback
import uuid
from collections.abc import Callable, Iterable
from typing import Any

from . import serializer
from .activity import ActivityContext, ActivityInfo, _set_context
from .client import (
    CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA,
    CONTROL_PLANE_REQUEST_CONTRACT_VERSION,
    CONTROL_PLANE_VERSION,
    PROTOCOL_VERSION,
    Client,
)
from .errors import ActivityCancelled, AvroNotInstalledError, NonRetryableError
from .workflow import replay

log = logging.getLogger("durable_workflow.worker")


def _activity_name(fn: Callable[..., Any]) -> str:
    return getattr(fn, "__activity_name__", fn.__name__)


def _workflow_name(cls: type) -> str:
    return getattr(cls, "__workflow_name__", cls.__name__)


def _manifest_version(manifest: Any) -> str:
    if isinstance(manifest, dict):
        value = manifest.get("version")
        if isinstance(value, str | int):
            return str(value)
    return "missing"


def _validate_server_compatibility(info: dict[str, Any]) -> None:
    control_plane = info.get("control_plane")
    if not isinstance(control_plane, dict):
        raise RuntimeError(
            "Server compatibility error: missing control_plane manifest; "
            f"sdk-python 0.2.x requires control_plane.version {CONTROL_PLANE_VERSION}."
        )

    control_plane_version = _manifest_version(control_plane)
    if control_plane_version != CONTROL_PLANE_VERSION:
        raise RuntimeError(
            "Server compatibility error: unsupported control_plane.version "
            f"{control_plane_version!r}; sdk-python 0.2.x requires {CONTROL_PLANE_VERSION!r}."
        )

    request_contract = control_plane.get("request_contract")
    if not isinstance(request_contract, dict):
        raise RuntimeError(
            "Server compatibility error: missing control_plane.request_contract; "
            f"expected {CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA} v{CONTROL_PLANE_REQUEST_CONTRACT_VERSION}."
        )

    request_schema = request_contract.get("schema")
    request_version = request_contract.get("version")
    if (
        request_schema != CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA
        or not _contract_version_matches(request_version, CONTROL_PLANE_REQUEST_CONTRACT_VERSION)
    ):
        raise RuntimeError(
            "Server compatibility error: unsupported control_plane.request_contract "
            f"{request_schema!r} v{request_version!r}; expected "
            f"{CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA} v{CONTROL_PLANE_REQUEST_CONTRACT_VERSION}."
        )

    worker_protocol = info.get("worker_protocol")
    if not isinstance(worker_protocol, dict):
        raise RuntimeError(
            "Server compatibility error: missing worker_protocol manifest; "
            f"sdk-python 0.2.x requires worker_protocol.version {PROTOCOL_VERSION}."
        )

    worker_protocol_version = _manifest_version(worker_protocol)
    if worker_protocol_version != PROTOCOL_VERSION:
        raise RuntimeError(
            "Server compatibility error: unsupported worker_protocol.version "
            f"{worker_protocol_version!r}; sdk-python 0.2.x requires {PROTOCOL_VERSION!r}."
        )


def _contract_version_matches(value: Any, expected: int) -> bool:
    if isinstance(value, int):
        return value == expected
    if isinstance(value, str) and value.isdigit():
        return int(value) == expected
    return False


class Worker:
    def __init__(
        self,
        client: Client,
        *,
        task_queue: str,
        workflows: Iterable[type] = (),
        activities: Iterable[Callable[..., Any]] = (),
        worker_id: str | None = None,
        poll_timeout: float = 35.0,
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
        try:
            info = await self.client.get_cluster_info()
        except Exception as e:
            raise RuntimeError(f"Server compatibility error: unable to read /api/cluster/info: {e}") from e

        _validate_server_compatibility(info)
        log.debug(
            "server compatibility accepted: app_version=%s control_plane=%s worker_protocol=%s",
            info.get("version", "unknown"),
            _manifest_version(info.get("control_plane")),
            _manifest_version(info.get("worker_protocol")),
        )

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

        # Paginate history if needed
        next_page_token = task.get("next_history_page_token")
        while next_page_token:
            try:
                page_data = await self.client.workflow_task_history(
                    task_id=task_id,
                    page_token=next_page_token,
                    lease_owner=self.worker_id,
                    workflow_task_attempt=attempt,
                )
                if page_data and page_data.get("history_events"):
                    history.extend(page_data["history_events"])
                next_page_token = page_data.get("next_history_page_token") if page_data else None
            except Exception as e:
                log.warning("failed to fetch history page for task %s: %s", task_id, e)
                break

        start_input: list[Any] = []
        codec = task.get("payload_codec")
        raw_args = task.get("arguments")
        try:
            decoded = serializer.decode_envelope(raw_args, codec=codec)
            if decoded is not None:
                start_input = decoded if isinstance(decoded, list) else [decoded]
        except AvroNotInstalledError as e:
            log.exception("task %s start input Avro decode failed (avro dependency unavailable)", task_id)
            try:
                await self.client.fail_workflow_task(
                    task_id=task_id,
                    lease_owner=self.worker_id,
                    workflow_task_attempt=attempt,
                    message=(
                        f"cannot decode workflow start input with codec 'avro': {e}. "
                        f"Reinstall durable-workflow with its runtime dependencies."
                    ),
                    failure_type=type(e).__name__,
                    stack_trace=traceback.format_exc(),
                )
            except Exception as fe:
                log.warning("failed to report Avro-missing start input failure: %s", fe)
            return None
        except (ValueError, TypeError) as e:
            log.exception("task %s start input decode failed (codec=%r)", task_id, codec)
            try:
                await self.client.fail_workflow_task(
                    task_id=task_id,
                    lease_owner=self.worker_id,
                    workflow_task_attempt=attempt,
                    message=(
                        f"cannot decode workflow start input with codec {codec!r}: {e}. "
                        f"Verify the start input bytes match the declared codec and writer schema."
                    ),
                    failure_type=type(e).__name__,
                    stack_trace=traceback.format_exc(),
                )
            except Exception as fe:
                log.warning("failed to report start input decode failure: %s", fe)
            return None

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
            outcome = replay(cls, history, start_input, run_id=run_id, payload_codec=codec)
        except AvroNotInstalledError as e:
            log.exception("replay failed: Avro dependency unavailable")
            try:
                await self.client.fail_workflow_task(
                    task_id=task_id,
                    lease_owner=self.worker_id,
                    workflow_task_attempt=attempt,
                    message=(
                        f"cannot replay workflow history with codec 'avro': {e}. "
                        f"Reinstall durable-workflow with its runtime dependencies."
                    ),
                    failure_type=type(e).__name__,
                    stack_trace=traceback.format_exc(),
                )
            except Exception as fe:
                log.warning("failed to report replay Avro-missing failure: %s", fe)
            return None
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
        inbound_codec = task.get("payload_codec") or serializer.JSON_CODEC
        result_codec = inbound_codec if inbound_codec in serializer.SUPPORTED_CODECS else serializer.AVRO_CODEC
        try:
            args = serializer.decode_envelope(raw_args, codec=inbound_codec) or []
        except AvroNotInstalledError as e:
            log.exception("activity %s arguments Avro decode failed (avro dependency unavailable)", task_id)
            try:
                await self.client.fail_activity_task(
                    task_id=task_id,
                    activity_attempt_id=attempt_id,
                    lease_owner=self.worker_id,
                    message=(
                        f"cannot decode activity arguments with codec 'avro': {e}. "
                        f"Reinstall durable-workflow with its runtime dependencies."
                    ),
                    failure_type=type(e).__name__,
                    stack_trace=traceback.format_exc(),
                    non_retryable=True,
                )
            except Exception as fe:
                log.warning("failed to report Avro-missing activity decode failure: %s", fe)
            return
        except (ValueError, TypeError) as e:
            log.exception("activity %s arguments decode failed (codec=%r)", task_id, inbound_codec)
            try:
                await self.client.fail_activity_task(
                    task_id=task_id,
                    activity_attempt_id=attempt_id,
                    lease_owner=self.worker_id,
                    message=(
                        f"cannot decode activity arguments with codec {inbound_codec!r}: {e}. "
                        f"Verify the argument bytes match the declared codec and writer schema."
                    ),
                    failure_type=type(e).__name__,
                    stack_trace=traceback.format_exc(),
                    non_retryable=True,
                )
            except Exception as fe:
                log.warning("failed to report activity decode failure: %s", fe)
            return
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
                codec=result_codec,
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
                await asyncio.sleep(0)
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
                await asyncio.sleep(0)
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
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(wf_loop, act_loop)

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
