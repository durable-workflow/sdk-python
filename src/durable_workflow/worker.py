"""Long-polling worker that runs workflow and activity tasks.

:class:`Worker` registers itself with the server for a given task queue, then
spawns poll loops for both workflow tasks and activity tasks. Each received
task is dispatched to the registered workflow class or activity function,
results are serialized, and success/failure commands are sent back to the
server. Workers drain in-flight tasks on shutdown up to a configurable
``shutdown_timeout``.

Most applications create one :class:`Worker` per task queue and pass it the
same :class:`~durable_workflow.Client` used for control-plane calls, plus
lists of workflow classes and activity callables registered via
:func:`durable_workflow.workflow.defn` and :func:`durable_workflow.activity.defn`.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import logging
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable, Iterable
from types import FunctionType
from typing import Any

from . import serializer
from .activity import ActivityContext, ActivityInfo, _set_context
from .auth_composition import (
    AUTH_COMPOSITION_CONTRACT_SCHEMA,
    AUTH_COMPOSITION_CONTRACT_VERSION,
    AuthCompositionContractError,
    parse_auth_composition_contract,
)
from .client import (
    CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA,
    CONTROL_PLANE_REQUEST_CONTRACT_VERSION,
    CONTROL_PLANE_VERSION,
    PROTOCOL_VERSION,
    Client,
    WorkflowExecution,
)
from .errors import ActivityCancelled, AvroNotInstalledError, NonRetryableError, QueryFailed
from .interceptors import (
    ActivityInterceptorContext,
    QueryTaskInterceptorContext,
    WorkerInterceptor,
    WorkflowTaskInterceptorContext,
)
from .metrics import (
    NOOP_METRICS,
    WORKER_POLL_DURATION_SECONDS,
    WORKER_POLLS,
    WORKER_TASK_DURATION_SECONDS,
    WORKER_TASKS,
    MetricsRecorder,
)
from .workflow import apply_update, commands_to_server_commands, query_state, replay

log = logging.getLogger("durable_workflow.worker")

_TERMINAL_WORKFLOW_STATUSES = {"completed", "failed", "terminated", "canceled", "cancelled"}
_WORKER_WORKFLOW_FINGERPRINTS: dict[tuple[str, str], str] = {}


def _command_payload_codec(codec: object) -> str:
    return codec if isinstance(codec, str) and codec in serializer.SUPPORTED_CODECS else serializer.AVRO_CODEC


def _activity_name(fn: Callable[..., Any]) -> str:
    return getattr(fn, "__activity_name__", fn.__name__)


def _workflow_name(cls: type) -> str:
    return getattr(cls, "__workflow_name__", cls.__name__)


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _callable_fingerprint_payload(value: object) -> str:
    if isinstance(value, staticmethod | classmethod):
        value = value.__func__
    if not isinstance(value, FunctionType):
        return repr(value)

    try:
        return inspect.getsource(value)
    except (OSError, TypeError):
        code = value.__code__
        return repr(
            (
                code.co_argcount,
                code.co_posonlyargcount,
                code.co_kwonlyargcount,
                code.co_code,
                code.co_consts,
                code.co_names,
                code.co_varnames,
            )
        )


def _workflow_definition_fingerprint(cls: type) -> str:
    h = hashlib.sha256()
    h.update(b"durable-workflow-python.workflow-definition.v1\0")
    h.update(f"{cls.__module__}.{cls.__qualname__}\0".encode())
    h.update(f"{_workflow_name(cls)}\0".encode())

    for registry_name in (
        "__workflow_signals__",
        "__workflow_queries__",
        "__workflow_updates__",
        "__workflow_update_validators__",
    ):
        registry = getattr(cls, registry_name, {}) or {}
        h.update(registry_name.encode())
        h.update(repr(sorted(registry.items())).encode())

    try:
        h.update(inspect.getsource(cls).encode())
    except (OSError, TypeError):
        for attr, value in sorted(cls.__dict__.items()):
            if attr.startswith("__") and attr.endswith("__"):
                continue
            h.update(attr.encode())
            h.update(_callable_fingerprint_payload(value).encode())

    return f"sha256:{h.hexdigest()}"


def _guard_worker_workflow_fingerprints(worker_id: str, fingerprints: dict[str, str]) -> None:
    for workflow_type, fingerprint in fingerprints.items():
        key = (worker_id, workflow_type)
        previous = _WORKER_WORKFLOW_FINGERPRINTS.get(key)
        if previous is not None and previous != fingerprint:
            raise RuntimeError(
                "Workflow definition changed for worker "
                f"{worker_id!r} and workflow type {workflow_type!r}; "
                "restart the worker process before re-registering this workflow type."
            )
        _WORKER_WORKFLOW_FINGERPRINTS[key] = fingerprint


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

    auth_composition = info.get("auth_composition_contract")
    if not isinstance(auth_composition, dict):
        raise RuntimeError(
            "Server compatibility error: missing auth_composition_contract; "
            f"expected {AUTH_COMPOSITION_CONTRACT_SCHEMA} v{AUTH_COMPOSITION_CONTRACT_VERSION}."
        )

    try:
        parse_auth_composition_contract(auth_composition)
    except AuthCompositionContractError as exc:
        raise RuntimeError(f"Server compatibility error: unsupported auth_composition_contract: {exc}") from exc


def _server_supports_query_tasks(info: dict[str, Any]) -> bool:
    worker_protocol = info.get("worker_protocol")
    if not isinstance(worker_protocol, dict):
        return False
    capabilities = worker_protocol.get("server_capabilities")
    return isinstance(capabilities, dict) and capabilities.get("query_tasks") is True


def _contract_version_matches(value: Any, expected: int) -> bool:
    if isinstance(value, int):
        return value == expected
    if isinstance(value, str) and value.isdigit():
        return int(value) == expected
    return False


class Worker:
    """Polls workflow and activity tasks and dispatches them to Python callables."""

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
        metrics: MetricsRecorder | None = None,
        interceptors: Iterable[WorkerInterceptor] = (),
    ) -> None:
        self.client = client
        self.task_queue = task_queue
        self.workflows = {_workflow_name(w): w for w in workflows}
        self.workflow_definition_fingerprints = {
            workflow_type: _workflow_definition_fingerprint(workflow_cls)
            for workflow_type, workflow_cls in self.workflows.items()
        }
        self.activities = {_activity_name(a): a for a in activities}
        self.worker_id = worker_id or f"py-worker-{uuid.uuid4().hex[:8]}"
        _guard_worker_workflow_fingerprints(self.worker_id, self.workflow_definition_fingerprints)
        if max_concurrent_workflow_tasks < 1:
            raise ValueError("max_concurrent_workflow_tasks must be at least 1")
        if max_concurrent_activity_tasks < 1:
            raise ValueError("max_concurrent_activity_tasks must be at least 1")

        self._poll_timeout = poll_timeout
        self.max_concurrent_workflow_tasks = max_concurrent_workflow_tasks
        self.max_concurrent_activity_tasks = max_concurrent_activity_tasks
        self._stop = asyncio.Event()
        self._wf_semaphore = asyncio.Semaphore(max_concurrent_workflow_tasks)
        self._act_semaphore = asyncio.Semaphore(max_concurrent_activity_tasks)
        self._shutdown_timeout = shutdown_timeout
        self._in_flight: set[asyncio.Task[Any]] = set()
        self._query_tasks_supported = False
        configured_metrics = metrics if metrics is not None else getattr(client, "metrics", NOOP_METRICS)
        self.metrics: MetricsRecorder = configured_metrics or NOOP_METRICS
        self.interceptors = tuple(interceptors)

    def _record_poll_metrics(self, task_kind: str, outcome: str, duration: float) -> None:
        tags = {"task_kind": task_kind, "task_queue": self.task_queue, "outcome": outcome}
        self.metrics.increment(WORKER_POLLS, tags=tags)
        self.metrics.record(WORKER_POLL_DURATION_SECONDS, duration, tags=tags)

    def _record_task_metrics(self, task_kind: str, outcome: str, duration: float) -> None:
        tags = {"task_kind": task_kind, "task_queue": self.task_queue, "outcome": outcome}
        self.metrics.increment(WORKER_TASKS, tags=tags)
        self.metrics.record(WORKER_TASK_DURATION_SECONDS, duration, tags=tags)

    def _payload_size_warning_config(self) -> serializer.PayloadSizeWarningConfig | None:
        config = getattr(self.client, "payload_size_warning_config", serializer.DEFAULT_PAYLOAD_SIZE_WARNING)
        if config is None or isinstance(config, serializer.PayloadSizeWarningConfig):
            return config
        return serializer.DEFAULT_PAYLOAD_SIZE_WARNING

    def _workflow_payload_warning_context(
        self,
        task: dict[str, Any],
        *,
        kind: str,
        update_name: str | None = None,
    ) -> serializer.PayloadSizeWarningContext:
        namespace = getattr(self.client, "namespace", None)
        return serializer.PayloadSizeWarningContext(
            kind=kind,
            workflow_id=_string_or_none(task.get("workflow_id")),
            run_id=_string_or_none(task.get("run_id")),
            update_name=update_name,
            task_queue=self.task_queue,
            namespace=namespace if isinstance(namespace, str) else None,
        )

    def _update_name_for_id(self, history: list[dict[str, Any]], update_id: str | None) -> str | None:
        if not update_id:
            return None
        for event in reversed(history):
            if event.get("event_type") not in {"UpdateAccepted", "UpdateApplied"}:
                continue
            payload = event.get("payload")
            if not isinstance(payload, dict) or payload.get("update_id") != update_id:
                continue
            update_name = payload.get("update_name")
            return update_name if isinstance(update_name, str) and update_name else None
        return None

    async def _register(self) -> None:
        try:
            info = await self.client.get_cluster_info()
        except Exception as e:
            raise RuntimeError(f"Server compatibility error: unable to read /api/cluster/info: {e}") from e

        _validate_server_compatibility(info)
        self._query_tasks_supported = _server_supports_query_tasks(info)
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
            workflow_definition_fingerprints=self.workflow_definition_fingerprints,
            supported_activity_types=list(self.activities),
            max_concurrent_workflow_tasks=self.max_concurrent_workflow_tasks,
            max_concurrent_activity_tasks=self.max_concurrent_activity_tasks,
        )
        log.info("worker %s registered on %s", self.worker_id, self.task_queue)

    async def _run_workflow_task(self, task: dict[str, Any]) -> list[dict[str, Any]] | None:
        context = WorkflowTaskInterceptorContext(
            worker_id=self.worker_id,
            task_queue=self.task_queue,
            task=task,
        )

        async def call_core(ctx: WorkflowTaskInterceptorContext) -> list[dict[str, Any]] | None:
            return await self._run_workflow_task_core(ctx.task)

        handler = call_core
        for interceptor in reversed(self.interceptors):
            next_handler = handler

            async def call_interceptor(
                ctx: WorkflowTaskInterceptorContext,
                *,
                interceptor: WorkerInterceptor = interceptor,
                next_handler: Callable[
                    [WorkflowTaskInterceptorContext],
                    Awaitable[list[dict[str, Any]] | None],
                ] = next_handler,
            ) -> list[dict[str, Any]] | None:
                return await interceptor.execute_workflow_task(ctx, next_handler)

            handler = call_interceptor

        return await handler(context)

    async def _run_workflow_task_core(self, task: dict[str, Any]) -> list[dict[str, Any]] | None:
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
                    next_history_page_token=next_page_token,
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
        command_codec = _command_payload_codec(codec)
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

        update_id = task.get("workflow_update_id")
        if isinstance(update_id, str) and update_id:
            update_command = apply_update(
                cls,
                history,
                start_input,
                update_id,
                workflow_id=task.get("workflow_id"),
                run_id=run_id,
                payload_codec=codec,
            )
            command = update_command.to_server_command(
                self.task_queue,
                payload_codec=command_codec,
                size_warning=self._payload_size_warning_config(),
                warning_context=self._workflow_payload_warning_context(
                    task,
                    kind="workflow_command",
                    update_name=self._update_name_for_id(history, update_id),
                ),
            )
            log.info(
                "completing workflow update task %s for update %s with %s",
                task_id,
                update_id,
                command["type"],
            )
            try:
                await self.client.complete_workflow_task(
                    task_id=task_id,
                    lease_owner=self.worker_id,
                    workflow_task_attempt=attempt,
                    commands=[command],
                )
            except Exception as e:
                log.warning("failed to complete workflow update task %s: %s", task_id, e)
            return [command]

        try:
            outcome = replay(
                cls,
                history,
                start_input,
                workflow_id=task.get("workflow_id"),
                run_id=run_id,
                payload_codec=codec,
            )
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

        commands = commands_to_server_commands(
            outcome.commands,
            self.task_queue,
            payload_codec=command_codec,
            size_warning=self._payload_size_warning_config(),
            warning_context=self._workflow_payload_warning_context(
                task,
                kind="workflow_command",
            ),
        )
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

    async def _run_activity_task(self, task: dict[str, Any]) -> str:
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
            return "decode_error"
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
            return "decode_error"
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
            return "no_handler"

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
            result = await self._execute_activity_callable(task, activity_type, tuple(args), fn)
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
            return "cancelled"
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
            return "failed_non_retryable"
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
            return "failed"
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
                activity_name=activity_type,
            )
        except Exception as e:
            log.warning("failed to complete activity task %s: %s", task_id, e)
            return "complete_error"
        return "completed"

    async def _execute_activity_callable(
        self,
        task: dict[str, Any],
        activity_type: str,
        args: tuple[Any, ...],
        fn: Callable[..., Any],
    ) -> Any:
        context = ActivityInterceptorContext(
            worker_id=self.worker_id,
            task_queue=self.task_queue,
            task=task,
            activity_type=activity_type,
            args=args,
        )

        async def call_activity(ctx: ActivityInterceptorContext) -> Any:
            result = fn(*ctx.args)
            if asyncio.iscoroutine(result):
                return await result
            return result

        handler = call_activity
        for interceptor in reversed(self.interceptors):
            next_handler = handler

            async def call_interceptor(
                ctx: ActivityInterceptorContext,
                *,
                interceptor: WorkerInterceptor = interceptor,
                next_handler: Callable[[ActivityInterceptorContext], Awaitable[Any]] = next_handler,
            ) -> Any:
                return await interceptor.execute_activity(ctx, next_handler)

            handler = call_interceptor

        return await handler(context)

    async def _run_query_task(self, task: dict[str, Any]) -> str:
        context = QueryTaskInterceptorContext(
            worker_id=self.worker_id,
            task_queue=self.task_queue,
            task=task,
        )

        async def call_core(ctx: QueryTaskInterceptorContext) -> str:
            return await self._run_query_task_core(ctx.task)

        handler = call_core
        for interceptor in reversed(self.interceptors):
            next_handler = handler

            async def call_interceptor(
                ctx: QueryTaskInterceptorContext,
                *,
                interceptor: WorkerInterceptor = interceptor,
                next_handler: Callable[[QueryTaskInterceptorContext], Awaitable[str]] = next_handler,
            ) -> str:
                return await interceptor.execute_query_task(ctx, next_handler)

            handler = call_interceptor

        return await handler(context)

    async def _run_query_task_core(self, task: dict[str, Any]) -> str:
        query_task_id: str = task["query_task_id"]
        attempt: int = task.get("query_task_attempt", 1)
        wf_type: str = task.get("workflow_type", "")
        query_name: str = task.get("query_name", "")
        codec = task.get("payload_codec")
        result_codec = _command_payload_codec(codec)
        history = task.get("history_events", [])

        cls = self.workflows.get(wf_type)
        if cls is None:
            await self._fail_query_task(
                query_task_id,
                attempt,
                f"no workflow registered for type {wf_type!r}",
                reason="query_workflow_type_not_registered",
                failure_type="WorkflowTypeNotRegistered",
            )
            return "failed"

        try:
            start_input = self._decode_list_payload(
                task.get("workflow_arguments"),
                codec=codec,
                context="workflow query start input",
            )
            query_args = self._decode_list_payload(
                task.get("query_arguments"),
                codec=codec,
                context="workflow query arguments",
            )
            result = query_state(
                cls,
                history,
                start_input,
                query_name,
                query_args,
                workflow_id=task.get("workflow_id"),
                run_id=task.get("run_id", ""),
                payload_codec=codec,
            )
        except AvroNotInstalledError as e:
            await self._fail_query_task(
                query_task_id,
                attempt,
                (
                    "cannot execute query with codec 'avro': "
                    f"{e}. Reinstall durable-workflow with its runtime dependencies."
                ),
                reason="query_payload_decode_failed",
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
            )
            return "failed"
        except QueryFailed as e:
            reason = "rejected_unknown_query" if "unknown query" in str(e) else "query_rejected"
            await self._fail_query_task(
                query_task_id,
                attempt,
                str(e),
                reason=reason,
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
            )
            return "failed"
        except Exception as e:
            await self._fail_query_task(
                query_task_id,
                attempt,
                str(e) or "query execution failed",
                reason="query_rejected",
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
            )
            return "failed"

        await self.client.complete_query_task(
            query_task_id=query_task_id,
            lease_owner=self.worker_id,
            query_task_attempt=attempt,
            result=result,
            codec=result_codec,
            workflow_id=task.get("workflow_id"),
            run_id=task.get("run_id"),
            query_name=query_name,
        )
        return "completed"

    def _decode_list_payload(self, raw: Any, *, codec: Any, context: str) -> list[Any]:
        try:
            decoded = serializer.decode_envelope(raw, codec=codec)
        except Exception:
            log.exception("%s decode failed", context)
            raise

        if decoded is None:
            return []
        return decoded if isinstance(decoded, list) else [decoded]

    async def _fail_query_task(
        self,
        query_task_id: str,
        attempt: int,
        message: str,
        *,
        reason: str,
        failure_type: str | None = None,
        stack_trace: str | None = None,
    ) -> None:
        try:
            await self.client.fail_query_task(
                query_task_id=query_task_id,
                lease_owner=self.worker_id,
                query_task_attempt=attempt,
                message=message,
                reason=reason,
                failure_type=failure_type,
                stack_trace=stack_trace,
            )
        except Exception as e:
            log.warning("failed to report query task failure %s: %s", query_task_id, e)

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
                poll_start = time.perf_counter()
                task = await self.client.poll_workflow_task(
                    worker_id=self.worker_id,
                    task_queue=self.task_queue,
                    timeout=self._poll_timeout,
                )
            except Exception as e:
                self._wf_semaphore.release()
                self._record_poll_metrics("workflow", "error", time.perf_counter() - poll_start)
                log.warning("workflow poll error: %s", e)
                await asyncio.sleep(1.0)
                continue
            if task is None:
                self._wf_semaphore.release()
                self._record_poll_metrics("workflow", "empty", time.perf_counter() - poll_start)
                await asyncio.sleep(0)
                continue
            self._record_poll_metrics("workflow", "task", time.perf_counter() - poll_start)
            self._track(self._dispatch_workflow_task(task))

    async def _dispatch_workflow_task(self, task: dict[str, Any]) -> None:
        task_start = time.perf_counter()
        outcome = "error"
        try:
            commands = await self._run_workflow_task(task)
            outcome = "completed" if commands is not None else "failed"
        except Exception:
            log.exception("unhandled error in workflow task execution")
        finally:
            self._record_task_metrics("workflow", outcome, time.perf_counter() - task_start)
            self._wf_semaphore.release()

    async def _poll_activity_tasks(self) -> None:
        while not self._stop.is_set():
            await self._act_semaphore.acquire()
            if self._stop.is_set():
                self._act_semaphore.release()
                return
            try:
                poll_start = time.perf_counter()
                task = await self.client.poll_activity_task(
                    worker_id=self.worker_id,
                    task_queue=self.task_queue,
                    timeout=self._poll_timeout,
                )
            except Exception as e:
                self._act_semaphore.release()
                self._record_poll_metrics("activity", "error", time.perf_counter() - poll_start)
                log.warning("activity poll error: %s", e)
                await asyncio.sleep(1.0)
                continue
            if task is None:
                self._act_semaphore.release()
                self._record_poll_metrics("activity", "empty", time.perf_counter() - poll_start)
                await asyncio.sleep(0)
                continue
            self._record_poll_metrics("activity", "task", time.perf_counter() - poll_start)
            self._track(self._dispatch_activity_task(task))

    async def _dispatch_activity_task(self, task: dict[str, Any]) -> None:
        task_start = time.perf_counter()
        outcome = "error"
        try:
            outcome = await self._run_activity_task(task)
        except Exception:
            log.exception("unhandled error in activity task execution")
        finally:
            self._record_task_metrics("activity", outcome, time.perf_counter() - task_start)
            self._act_semaphore.release()

    async def _poll_query_tasks(self) -> None:
        while not self._stop.is_set():
            try:
                poll_start = time.perf_counter()
                task = await self.client.poll_query_task(
                    worker_id=self.worker_id,
                    task_queue=self.task_queue,
                    timeout=self._poll_timeout,
                )
            except Exception as e:
                self._record_poll_metrics("query", "error", time.perf_counter() - poll_start)
                log.warning("query poll error: %s", e)
                await asyncio.sleep(1.0)
                continue
            if task is None:
                self._record_poll_metrics("query", "empty", time.perf_counter() - poll_start)
                await asyncio.sleep(0)
                continue
            self._record_poll_metrics("query", "task", time.perf_counter() - poll_start)
            self._track(self._dispatch_query_task(task))

    async def _dispatch_query_task(self, task: dict[str, Any]) -> None:
        task_start = time.perf_counter()
        outcome = "error"
        try:
            outcome = await self._run_query_task(task)
        except Exception:
            log.exception("unhandled error in query task execution")
        finally:
            self._record_task_metrics("query", outcome, time.perf_counter() - task_start)

    async def run(self) -> None:
        """Register the worker and poll until `stop()` is called or the task is cancelled."""
        await self._register()
        wf_loop = asyncio.create_task(self._poll_workflow_tasks())
        act_loop = asyncio.create_task(self._poll_activity_tasks())
        loops = [wf_loop, act_loop]
        if self._query_tasks_supported:
            loops.append(asyncio.create_task(self._poll_query_tasks()))
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*loops)

    async def run_until(
        self,
        *,
        workflow_id: str,
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> WorkflowExecution:
        """Run this worker until a workflow reaches a terminal state.

        This is intended for examples, smoke tests, and single-workflow scripts.
        Long-running workers should call :meth:`run` and coordinate shutdown from
        their process supervisor.
        """
        deadline = time.monotonic() + timeout
        next_task_kind = "workflow"

        try:
            await self._register()
            while True:
                desc = await self.client.describe_workflow(workflow_id)
                status = (desc.status or "").lower()
                if status in _TERMINAL_WORKFLOW_STATUSES:
                    return desc

                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"workflow {workflow_id} not terminal after {timeout}s (status={desc.status})"
                    )

                if next_task_kind == "workflow":
                    poll_start = time.perf_counter()
                    task = await self.client.poll_workflow_task(
                        worker_id=self.worker_id,
                        task_queue=self.task_queue,
                        timeout=self._poll_timeout,
                    )
                    self._record_poll_metrics(
                        "workflow",
                        "task" if task is not None else "empty",
                        time.perf_counter() - poll_start,
                    )
                    if task is None:
                        next_task_kind = "activity"
                        await asyncio.sleep(poll_interval)
                        continue

                    task_start = time.perf_counter()
                    outcome = "error"
                    try:
                        commands = await self._run_workflow_task(task)
                        outcome = "completed" if commands is not None else "failed"
                    finally:
                        self._record_task_metrics("workflow", outcome, time.perf_counter() - task_start)

                    if commands and any(command.get("type") == "schedule_activity" for command in commands):
                        next_task_kind = "activity"
                    else:
                        next_task_kind = "workflow"
                    continue

                poll_start = time.perf_counter()
                task = await self.client.poll_activity_task(
                    worker_id=self.worker_id,
                    task_queue=self.task_queue,
                    timeout=self._poll_timeout,
                )
                self._record_poll_metrics(
                    "activity",
                    "task" if task is not None else "empty",
                    time.perf_counter() - poll_start,
                )
                if task is None:
                    next_task_kind = "workflow"
                    await asyncio.sleep(poll_interval)
                    continue

                task_start = time.perf_counter()
                outcome = "error"
                try:
                    outcome = await self._run_activity_task(task)
                finally:
                    self._record_task_metrics("activity", outcome, time.perf_counter() - task_start)
                next_task_kind = "workflow"
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop polling and drain in-flight tasks up to the configured shutdown timeout."""
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
