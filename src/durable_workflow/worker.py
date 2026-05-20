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
import sys
import threading
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import datetime, timezone
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
from .errors import (
    ActivityCancelled,
    AvroNotInstalledError,
    DurableWorkflowError,
    InvalidArgument,
    NonRetryableError,
    QueryFailed,
    ServerError,
)
from .external_storage import ExternalPayloadCache, ExternalStorageDriver
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

QUERY_TASKS_CAPABILITY = "query_tasks"

_TERMINAL_WORKFLOW_STATUSES = {"completed", "failed", "terminated", "canceled", "cancelled"}
_QUERY_TASK_FINAL_REJECTION_REASONS = {
    "lease_expired",
    "query_task_not_found",
    "query_task_not_leased",
    "query_task_timed_out",
}
_WORKFLOW_TASK_COMPLETION_AMBIGUOUS_REJECTION_REASONS = {
    "lease_expired",
    "lease_owner_mismatch",
    "run_already_closed",
    "run_closed",
    "task_not_found",
    "task_not_leased",
    "workflow_task_attempt_mismatch",
}
_WORKFLOW_TASK_COMPLETION_MAX_ATTEMPTS = 3
_WORKFLOW_TASK_COMPLETION_RETRY_DELAYS = (0.05, 0.2)
_WORKER_WORKFLOW_FINGERPRINTS: dict[tuple[str, str], str] = {}


def _command_payload_codec(codec: object) -> str:
    return codec if isinstance(codec, str) and codec in serializer.SUPPORTED_CODECS else serializer.AVRO_CODEC


def _validate_payload_codec(codec: object) -> str | None:
    if codec is None:
        return None
    if isinstance(codec, str) and codec in serializer.SUPPORTED_CODECS:
        return codec
    raise ValueError(
        f"Unsupported payload codec {codec!r}; this SDK supports {serializer.SUPPORTED_CODECS!r}."
    )


def _activity_name(fn: Callable[..., Any]) -> str:
    return getattr(fn, "__activity_name__", fn.__name__)


def _workflow_name(cls: type) -> str:
    return getattr(cls, "__workflow_name__", cls.__name__)


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _is_final_query_task_rejection(error: BaseException) -> bool:
    return (
        isinstance(error, ServerError)
        and error.status in {404, 409}
        and error.reason() in _QUERY_TASK_FINAL_REJECTION_REASONS
    )


def _should_fail_workflow_task_after_completion_error(error: BaseException) -> bool:
    if isinstance(error, InvalidArgument):
        return True

    if isinstance(error, ServerError):
        if error.status >= 500 or error.status == 429:
            return False

        return error.reason() not in _WORKFLOW_TASK_COMPLETION_AMBIGUOUS_REJECTION_REASONS

    return isinstance(error, DurableWorkflowError)


def _should_retry_workflow_task_completion_error(error: BaseException) -> bool:
    if isinstance(error, ServerError):
        return error.status >= 500 or error.status == 429

    return not isinstance(error, DurableWorkflowError)


def _signal_arguments_envelope_from_export(
    signal: Mapping[str, Any],
    *,
    default_codec: str | None,
) -> dict[str, Any] | None:
    raw_arguments = signal.get("arguments")
    if raw_arguments is None:
        return None

    codec = signal.get("payload_codec")
    if not isinstance(codec, str) or codec == "":
        codec = default_codec or serializer.AVRO_CODEC

    if isinstance(raw_arguments, str):
        if raw_arguments == "":
            return None
        return {"codec": codec, "blob": raw_arguments}

    if not isinstance(raw_arguments, Mapping):
        return None

    envelope = dict(raw_arguments)
    if "blob" not in envelope and "external_storage" not in envelope:
        return None
    envelope.setdefault("codec", codec)
    return envelope


def _history_events_from_query_export(history: Any, history_export: Any) -> Any:
    if isinstance(history, list) and history:
        return history
    if not isinstance(history_export, Mapping):
        return history

    raw_events = history_export.get("history_events")
    if not isinstance(raw_events, list):
        return history

    events: list[Any] = []
    changed = False
    for raw_event in raw_events:
        if not isinstance(raw_event, Mapping):
            events.append(raw_event)
            continue

        event = dict(raw_event)
        if "event_type" not in event:
            export_type = event.get("type")
            if isinstance(export_type, str) and export_type:
                event["event_type"] = export_type
                changed = True
        events.append(event)

    if events:
        return events
    return history if isinstance(history, list) or not changed else events


def _activity_result_by_sequence_from_export(
    history_export: Mapping[str, Any],
) -> dict[int, Mapping[str, Any]]:
    raw_activities = history_export.get("activities")
    if not isinstance(raw_activities, list):
        return {}

    results: dict[int, Mapping[str, Any]] = {}
    for raw_activity in raw_activities:
        if not isinstance(raw_activity, Mapping):
            continue
        sequence = raw_activity.get("sequence")
        if isinstance(sequence, str) and sequence.isdigit():
            sequence = int(sequence)
        if not isinstance(sequence, int):
            continue
        if raw_activity.get("result") is None:
            continue
        results.setdefault(sequence, raw_activity)

    return results


def _query_history_with_export_signal_arguments(
    history: Any,
    history_export: Any,
    *,
    default_codec: str | None,
) -> Any:
    # Query-task history may be omitted or compact when the server routes a
    # completed/in-flight query to a fresh worker. The attached export remains
    # the durable source for replay payloads needed to rebuild workflow state.
    history = _history_events_from_query_export(history, history_export)
    if not isinstance(history, list) or not isinstance(history_export, Mapping):
        return history

    raw_signals = history_export.get("signals")
    if not isinstance(raw_signals, list):
        raw_signals = []

    export_payloads = history_export.get("payloads")
    export_codec = (
        export_payloads.get("codec")
        if isinstance(export_payloads, Mapping)
        else None
    )
    signal_default_codec = default_codec
    if signal_default_codec is None and isinstance(export_codec, str) and export_codec:
        signal_default_codec = export_codec

    signals_by_id: dict[str, Mapping[str, Any]] = {}
    signals_by_command_id: dict[str, Mapping[str, Any]] = {}
    signals_by_name: dict[str, list[Mapping[str, Any]]] = {}
    for raw_signal in raw_signals:
        if not isinstance(raw_signal, Mapping):
            continue
        envelope = _signal_arguments_envelope_from_export(raw_signal, default_codec=signal_default_codec)
        if envelope is None:
            continue
        signal_id = raw_signal.get("id")
        if isinstance(signal_id, str) and signal_id:
            signals_by_id[signal_id] = raw_signal
        command_id = raw_signal.get("command_id")
        if isinstance(command_id, str) and command_id:
            signals_by_command_id[command_id] = raw_signal
        name = raw_signal.get("name")
        if isinstance(name, str) and name:
            signals_by_name.setdefault(name, []).append(raw_signal)

    if not signals_by_id and not signals_by_command_id and not signals_by_name:
        signals_available = False
    else:
        signals_available = True

    activity_results_by_sequence = _activity_result_by_sequence_from_export(history_export)

    name_offsets: dict[str, int] = {}
    enriched: list[Any] = []
    changed = False
    for raw_event in history:
        if not isinstance(raw_event, Mapping):
            enriched.append(raw_event)
            continue
        event_type = raw_event.get("event_type") or raw_event.get("type")
        base_event = dict(raw_event)
        if "event_type" not in base_event and isinstance(event_type, str) and event_type:
            base_event["event_type"] = event_type
            changed = True
        raw_event = base_event
        raw_payload = raw_event.get("payload")
        if not isinstance(raw_payload, Mapping):
            enriched.append(raw_event)
            continue

        if event_type == "ActivityCompleted":
            payload = dict(raw_payload)
            sequence = payload.get("sequence") or payload.get("workflow_sequence")
            if isinstance(sequence, str) and sequence.isdigit():
                sequence = int(sequence)
            activity = activity_results_by_sequence.get(sequence) if isinstance(sequence, int) else None
            if activity is not None:
                payload_changed = False
                if "result" not in payload and activity.get("result") is not None:
                    payload["result"] = activity["result"]
                    payload_changed = True
                if "payload_codec" not in payload and isinstance(activity.get("payload_codec"), str):
                    payload["payload_codec"] = activity["payload_codec"]
                    payload_changed = True
                if "activity_type" not in payload and isinstance(activity.get("activity_type"), str):
                    payload["activity_type"] = activity["activity_type"]
                    payload_changed = True
                if payload_changed:
                    event = dict(raw_event)
                    event["payload"] = payload
                    enriched.append(event)
                    changed = True
                    continue
            enriched.append(raw_event)
            continue

        if event_type != "SignalReceived":
            enriched.append(raw_event)
            continue
        if not signals_available:
            enriched.append(raw_event)
            continue
        signal: Mapping[str, Any] | None = None
        signal_id = raw_payload.get("signal_id")
        if isinstance(signal_id, str) and signal_id:
            signal = signals_by_id.get(signal_id)
        if signal is None:
            command_id = raw_payload.get("workflow_command_id") or raw_event.get("workflow_command_id")
            if isinstance(command_id, str) and command_id:
                signal = signals_by_command_id.get(command_id)
        if signal is None:
            signal_name = raw_payload.get("signal_name")
            if isinstance(signal_name, str) and signal_name:
                candidates = signals_by_name.get(signal_name, [])
                offset = name_offsets.get(signal_name, 0)
                if offset < len(candidates):
                    signal = candidates[offset]
                    name_offsets[signal_name] = offset + 1
        if signal is None:
            enriched.append(dict(raw_event))
            continue

        payload = dict(raw_payload)
        payload_changed = False
        workflow_sequence = signal.get("workflow_sequence")
        if isinstance(workflow_sequence, int):
            payload.setdefault("workflow_sequence", workflow_sequence)
            payload_changed = True
        elif isinstance(workflow_sequence, str) and workflow_sequence.isdigit():
            payload.setdefault("workflow_sequence", int(workflow_sequence))
            payload_changed = True

        envelope = _signal_arguments_envelope_from_export(signal, default_codec=signal_default_codec)
        if envelope is not None:
            payload.setdefault("arguments", envelope)
            payload.setdefault("payload_codec", envelope.get("codec"))
            payload_changed = True

        if not payload_changed:
            enriched.append(raw_event)
            continue

        event = dict(raw_event)
        event["payload"] = payload
        enriched.append(event)
        changed = True

    return enriched if changed else history


def _query_history_events(
    history: Any,
    history_export: Any,
    *,
    default_codec: str | None,
) -> Any:
    if isinstance(history, list):
        events = history
    else:
        events = []

    if isinstance(history_export, Mapping):
        export_events = history_export.get("history_events")
        if isinstance(export_events, list) and len(export_events) > len(events):
            events = export_events

    return _query_history_with_export_signal_arguments(
        events,
        history_export,
        default_codec=default_codec,
    )


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


def _parse_protocol_version(value: str) -> tuple[int, int] | None:
    """Parse "MAJOR.MINOR" into a tuple. Returns None for malformed inputs."""
    if not isinstance(value, str):
        return None
    parts = value.split(".")
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None


def _server_protocol_compatible(server_version: str, sdk_version: str) -> bool:
    """A server's worker_protocol.version is compatible with this SDK when
    they share a major and the server's minor is at least the SDK's minor.

    Per workflow:v2's WorkerProtocolVersion contract, MINOR bumps are
    additive (new optional fields, new non-terminal command types) and
    therefore safe for older SDKs to talk to newer servers — they just
    will not exercise the new optional shapes. MAJOR bumps are breaking
    and must always be rejected.
    """
    server = _parse_protocol_version(server_version)
    sdk = _parse_protocol_version(sdk_version)
    if server is None or sdk is None:
        return server_version == sdk_version
    if server[0] != sdk[0]:
        return False
    return server[1] >= sdk[1]


def _validate_server_compatibility(info: dict[str, Any]) -> None:
    control_plane = info.get("control_plane")
    if not isinstance(control_plane, dict):
        raise RuntimeError(
            "Server compatibility error: missing control_plane manifest; "
            f"sdk-python requires control_plane.version {CONTROL_PLANE_VERSION}."
        )

    control_plane_version = _manifest_version(control_plane)
    if control_plane_version != CONTROL_PLANE_VERSION:
        raise RuntimeError(
            "Server compatibility error: unsupported control_plane.version "
            f"{control_plane_version!r}; sdk-python requires {CONTROL_PLANE_VERSION!r}."
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
            f"sdk-python requires worker_protocol.version {PROTOCOL_VERSION}."
        )

    worker_protocol_version = _manifest_version(worker_protocol)
    if not _server_protocol_compatible(worker_protocol_version, PROTOCOL_VERSION):
        raise RuntimeError(
            "Server compatibility error: incompatible worker_protocol.version "
            f"{worker_protocol_version!r}; sdk-python requires major-equal and "
            f"minor>={PROTOCOL_VERSION!r}."
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


def _server_long_poll_timeout(info: dict[str, Any]) -> float | None:
    worker_protocol = info.get("worker_protocol")
    if not isinstance(worker_protocol, dict):
        return None

    capabilities = worker_protocol.get("server_capabilities")
    if not isinstance(capabilities, dict):
        return None

    timeout = capabilities.get("long_poll_timeout")
    if isinstance(timeout, bool):
        return None
    if isinstance(timeout, (int, float)):
        return float(timeout) if timeout > 0 else None
    if isinstance(timeout, str):
        try:
            parsed = float(timeout)
        except ValueError:
            return None
        return parsed if parsed > 0 else None

    return None


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
        build_id: str | None = None,
        poll_timeout: float = 35.0,
        max_concurrent_workflow_tasks: int = 10,
        max_concurrent_activity_tasks: int = 10,
        shutdown_timeout: float = 30.0,
        heartbeat_interval: float = 60.0,
        metrics: MetricsRecorder | None = None,
        interceptors: Iterable[WorkerInterceptor] = (),
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
        external_storage_cache: ExternalPayloadCache | None = None,
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
        if build_id is not None:
            if not isinstance(build_id, str) or build_id.strip() == "":
                raise ValueError("build_id must be a non-empty string when provided")
            self.build_id: str | None = build_id
        else:
            self.build_id = None
        _guard_worker_workflow_fingerprints(self.worker_id, self.workflow_definition_fingerprints)
        if max_concurrent_workflow_tasks < 1:
            raise ValueError("max_concurrent_workflow_tasks must be at least 1")
        if max_concurrent_activity_tasks < 1:
            raise ValueError("max_concurrent_activity_tasks must be at least 1")
        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval must be positive")

        self._poll_timeout = poll_timeout
        self._poll_http_timeout = poll_timeout
        self.max_concurrent_workflow_tasks = max_concurrent_workflow_tasks
        self.max_concurrent_activity_tasks = max_concurrent_activity_tasks
        self._stop = asyncio.Event()
        self._wf_semaphore = asyncio.Semaphore(max_concurrent_workflow_tasks)
        self._act_semaphore = asyncio.Semaphore(max_concurrent_activity_tasks)
        self._shutdown_timeout = shutdown_timeout
        self._in_flight: set[asyncio.Task[Any]] = set()
        self._query_tasks_supported = False
        self._query_thread_stop = threading.Event()
        self._query_thread: threading.Thread | None = None
        # In-flight slot accounting feeds the periodic heartbeat so operators
        # see free-slot counts without the worker having to re-derive them at
        # shutdown. Counters are bumped/decremented around dispatch.
        self._workflow_inflight = 0
        self._activity_inflight = 0
        self._heartbeat_interval = float(heartbeat_interval)
        self._process_started_at = time.time()
        self._process_started_at_iso = (
            datetime.fromtimestamp(self._process_started_at, timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
        # CPU sampling baseline. The heartbeat reports an *instantaneous*
        # cpu_percent — CPU time burned in the interval since the previous
        # heartbeat, divided by that interval — rather than the lifetime
        # average. ``_last_cpu_sample_at`` is None until the first sample
        # has been taken; subsequent samples diff against it.
        self._last_cpu_sample_at: float | None = None
        self._last_cpu_total_seconds: float = 0.0
        configured_metrics = metrics if metrics is not None else getattr(client, "metrics", NOOP_METRICS)
        self.metrics: MetricsRecorder = configured_metrics or NOOP_METRICS
        self.interceptors = tuple(interceptors)
        self.external_storage = (
            external_storage
            if external_storage is not None
            else getattr(client, "external_storage", None)
        )
        if external_storage_threshold_bytes is not None and external_storage_threshold_bytes < 1:
            raise ValueError("external_storage_threshold_bytes must be at least 1 when provided")
        client_external_storage_threshold_bytes = getattr(client, "external_storage_threshold_bytes", None)
        self.external_storage_threshold_bytes = (
            external_storage_threshold_bytes
            if external_storage_threshold_bytes is not None
            else (
                client_external_storage_threshold_bytes
                if isinstance(client_external_storage_threshold_bytes, int)
                else None
            )
        )
        client_external_storage_cache = getattr(client, "external_storage_cache", None)
        self.external_storage_cache = (
            external_storage_cache
            if external_storage_cache is not None
            else (
                client_external_storage_cache
                if client_external_storage_cache is not None
                else ExternalPayloadCache()
            )
        )

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

    def _external_storage_completion_kwargs(self) -> dict[str, Any]:
        if self.external_storage is None or self.external_storage_threshold_bytes is None:
            return {}
        return {
            "external_storage": self.external_storage,
            "external_storage_threshold_bytes": self.external_storage_threshold_bytes,
        }

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
        server_long_poll_timeout = _server_long_poll_timeout(info)
        if server_long_poll_timeout is not None:
            self._poll_http_timeout = max(self._poll_http_timeout, server_long_poll_timeout + 5.0)
        log.debug(
            "server compatibility accepted: app_version=%s control_plane=%s worker_protocol=%s",
            info.get("version", "unknown"),
            _manifest_version(info.get("control_plane")),
            _manifest_version(info.get("worker_protocol")),
        )

        ack = await self.client.register_worker(
            worker_id=self.worker_id,
            task_queue=self.task_queue,
            supported_workflow_types=list(self.workflows),
            workflow_definition_fingerprints=self.workflow_definition_fingerprints,
            supported_activity_types=list(self.activities),
            max_concurrent_workflow_tasks=self.max_concurrent_workflow_tasks,
            max_concurrent_activity_tasks=self.max_concurrent_activity_tasks,
            build_id=self.build_id,
            capabilities=[QUERY_TASKS_CAPABILITY] if self._query_tasks_supported else None,
            task_slots=self._current_task_slots(),
            process_metrics=self._current_process_metrics(),
        )
        # Adapt to the server-advertised cadence when present so a cluster
        # can pin the worker fleet's heartbeat beat without each worker
        # passing the cadence explicitly. Falls back to the constructor
        # value when the server has not advertised a cadence.
        if isinstance(ack, dict):
            advertised = ack.get("heartbeat_interval_seconds")
            if isinstance(advertised, int) and advertised > 0:
                self._heartbeat_interval = float(advertised)
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
        raw_args = task.get("arguments")
        try:
            codec = _validate_payload_codec(codec)
            decoded = serializer.decode_envelope(
                raw_args,
                codec=codec,
                external_storage=self.external_storage,
                external_storage_cache=self.external_storage_cache,
            )
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
        command_codec = _command_payload_codec(codec)

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
                external_storage=self.external_storage,
                external_storage_cache=self.external_storage_cache,
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
                external_storage=self.external_storage,
                external_storage_threshold_bytes=self.external_storage_threshold_bytes,
            )
            log.info(
                "completing workflow update task %s for update %s with %s",
                task_id,
                update_id,
                command["type"],
            )
            try:
                await self._complete_workflow_task_with_retry(
                    task_id=task_id,
                    attempt=attempt,
                    commands=[command],
                )
            except Exception as e:
                log.warning("failed to complete workflow update task %s: %s", task_id, e)
                if _should_fail_workflow_task_after_completion_error(e):
                    await self._report_workflow_task_after_completion_error(task_id, attempt, e)
                    return None
            return [command]

        try:
            outcome = replay(
                cls,
                history,
                start_input,
                workflow_id=task.get("workflow_id"),
                run_id=run_id,
                payload_codec=codec,
                external_storage=self.external_storage,
                external_storage_cache=self.external_storage_cache,
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
            external_storage=self.external_storage,
            external_storage_threshold_bytes=self.external_storage_threshold_bytes,
        )
        log.info(
            "completing workflow task %s with %d command(s): %s",
            task_id,
            len(commands),
            [c["type"] for c in commands],
        )
        try:
            await self._complete_workflow_task_with_retry(
                task_id=task_id,
                attempt=attempt,
                commands=commands,
            )
        except Exception as e:
            log.warning("failed to complete workflow task %s: %s", task_id, e)
            if _should_fail_workflow_task_after_completion_error(e):
                await self._report_workflow_task_after_completion_error(task_id, attempt, e)
                return None
        return commands

    async def _complete_workflow_task_with_retry(
        self,
        *,
        task_id: str,
        attempt: int,
        commands: list[dict[str, Any]],
    ) -> None:
        for completion_attempt in range(1, _WORKFLOW_TASK_COMPLETION_MAX_ATTEMPTS + 1):
            try:
                await self.client.complete_workflow_task(
                    task_id=task_id,
                    lease_owner=self.worker_id,
                    workflow_task_attempt=attempt,
                    commands=commands,
                )
                return
            except Exception as error:
                if (
                    completion_attempt >= _WORKFLOW_TASK_COMPLETION_MAX_ATTEMPTS
                    or not _should_retry_workflow_task_completion_error(error)
                ):
                    raise

                delay_index = min(
                    completion_attempt - 1,
                    len(_WORKFLOW_TASK_COMPLETION_RETRY_DELAYS) - 1,
                )
                log.warning(
                    "retrying workflow task %s completion after attempt %d/%d failed: %s",
                    task_id,
                    completion_attempt,
                    _WORKFLOW_TASK_COMPLETION_MAX_ATTEMPTS,
                    error,
                )
                await asyncio.sleep(_WORKFLOW_TASK_COMPLETION_RETRY_DELAYS[delay_index])

    async def _report_workflow_task_after_completion_error(
        self,
        task_id: str,
        attempt: int,
        error: Exception,
    ) -> None:
        try:
            await self.client.fail_workflow_task(
                task_id=task_id,
                lease_owner=self.worker_id,
                workflow_task_attempt=attempt,
                message=f"workflow task completion failed after commands were produced: {error}",
                failure_type=type(error).__name__,
                stack_trace=traceback.format_exc(),
            )
        except Exception as fail_error:
            log.warning(
                "failed to report workflow task %s completion failure: %s",
                task_id,
                fail_error,
            )

    async def _run_activity_task(self, task: dict[str, Any]) -> str:
        task_id: str = task["task_id"]
        attempt_id: str = task.get("activity_attempt_id") or task.get("attempt_id", "")
        activity_type: str = task.get("activity_type", "")
        attempt_number: int = task.get("attempt_number", 1)
        raw_args = task.get("arguments")
        inbound_codec = task.get("payload_codec")
        try:
            inbound_codec = _validate_payload_codec(inbound_codec) or serializer.JSON_CODEC
            args = serializer.decode_envelope(
                raw_args,
                codec=inbound_codec,
                external_storage=self.external_storage,
                external_storage_cache=self.external_storage_cache,
            ) or []
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
        result_codec = inbound_codec

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
                **self._external_storage_completion_kwargs(),
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

    async def _run_query_task(self, task: dict[str, Any], *, client: Client | None = None) -> str:
        context = QueryTaskInterceptorContext(
            worker_id=self.worker_id,
            task_queue=self.task_queue,
            task=task,
        )

        async def call_core(ctx: QueryTaskInterceptorContext) -> str:
            return await self._run_query_task_core(ctx.task, client=client)

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

    async def _run_query_task_core(self, task: dict[str, Any], *, client: Client | None = None) -> str:
        client = client or self.client
        query_task_id: str = task["query_task_id"]
        attempt: int = task.get("query_task_attempt", 1)
        wf_type: str = task.get("workflow_type", "")
        query_name: str = task.get("query_name", "")
        codec = task.get("payload_codec")
        try:
            codec = _validate_payload_codec(codec)
        except ValueError as e:
            await self._fail_query_task(
                query_task_id,
                attempt,
                f"cannot decode query payload with codec {codec!r}: {e}.",
                reason="query_payload_decode_failed",
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
                client=client,
            )
            return "failed"

        result_codec = _command_payload_codec(codec)
        history = _query_history_events(
            task.get("history_events", []),
            task.get("history_export"),
            default_codec=codec,
        )

        cls = self.workflows.get(wf_type)
        if cls is None:
            await self._fail_query_task(
                query_task_id,
                attempt,
                f"no workflow registered for type {wf_type!r}",
                reason="query_workflow_type_not_registered",
                failure_type="WorkflowTypeNotRegistered",
                client=client,
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
                external_storage=self.external_storage,
                external_storage_cache=self.external_storage_cache,
            )
            if inspect.isawaitable(result):
                result = await result
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
                client=client,
            )
            return "failed"
        except QueryFailed as e:
            message = str(e)
            if "unknown query" in message:
                reason = "rejected_unknown_query"
            elif message.startswith("workflow replay failed before query:"):
                reason = "query_workflow_state_unavailable"
            else:
                reason = "query_rejected"
            await self._fail_query_task(
                query_task_id,
                attempt,
                message,
                reason=reason,
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
                client=client,
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
                client=client,
            )
            return "failed"

        try:
            await client.complete_query_task(
                query_task_id=query_task_id,
                lease_owner=self.worker_id,
                query_task_attempt=attempt,
                result=result,
                codec=result_codec,
                workflow_id=task.get("workflow_id"),
                run_id=task.get("run_id"),
                query_name=query_name,
                **self._external_storage_completion_kwargs(),
            )
        except ServerError as e:
            if _is_final_query_task_rejection(e):
                log.info(
                    "query task %s completion was rejected after the task ended server-side: %s",
                    query_task_id,
                    e.reason(),
                )
                return "expired"
            server_reason = e.reason()
            await self._fail_query_task(
                query_task_id,
                attempt,
                str(e) or "query result completion was rejected by the server",
                reason=server_reason if server_reason else "query_result_completion_failed",
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
                client=client,
            )
            return "failed"
        except AvroNotInstalledError as e:
            await self._fail_query_task(
                query_task_id,
                attempt,
                (
                    "cannot encode query result with codec 'avro': "
                    f"{e}. Reinstall durable-workflow with its runtime dependencies."
                ),
                reason="query_result_encode_failed",
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
                client=client,
            )
            return "failed"
        except (TypeError, ValueError) as e:
            await self._fail_query_task(
                query_task_id,
                attempt,
                f"cannot encode query result with codec {result_codec!r}: {e}",
                reason="query_result_encode_failed",
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
                client=client,
            )
            return "failed"
        except Exception as e:
            await self._fail_query_task(
                query_task_id,
                attempt,
                str(e) or "query result completion failed",
                reason="query_result_completion_failed",
                failure_type=type(e).__name__,
                stack_trace=traceback.format_exc(),
                client=client,
            )
            return "failed"

        return "completed"

    def _decode_list_payload(self, raw: Any, *, codec: Any, context: str) -> list[Any]:
        try:
            decoded = serializer.decode_envelope(
                raw,
                codec=codec,
                external_storage=self.external_storage,
                external_storage_cache=self.external_storage_cache,
            )
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
        client: Client | None = None,
    ) -> None:
        client = client or self.client
        try:
            await client.fail_query_task(
                query_task_id=query_task_id,
                lease_owner=self.worker_id,
                query_task_attempt=attempt,
                message=message,
                reason=reason,
                failure_type=failure_type,
                stack_trace=stack_trace,
            )
        except ServerError as e:
            if _is_final_query_task_rejection(e):
                log.info(
                    "query task %s failure report was rejected after the task ended server-side: %s",
                    query_task_id,
                    e.reason(),
                )
                return
            log.warning("failed to report query task failure %s: %s", query_task_id, e)
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
                    timeout=self._poll_http_timeout,
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
        self._workflow_inflight += 1
        try:
            commands = await self._run_workflow_task(task)
            outcome = "completed" if commands is not None else "failed"
        except Exception as exc:
            log.exception("unhandled error in workflow task execution")
            await self._report_unhandled_workflow_task_error(task, exc)
            outcome = "failed"
        finally:
            self._workflow_inflight = max(0, self._workflow_inflight - 1)
            self._record_task_metrics("workflow", outcome, time.perf_counter() - task_start)
            self._wf_semaphore.release()

    async def _report_unhandled_workflow_task_error(
        self,
        task: dict[str, Any],
        error: Exception,
    ) -> None:
        task_id = _string_or_none(task.get("task_id"))
        if task_id is None:
            return

        attempt = task.get("workflow_task_attempt", 1)
        if not isinstance(attempt, int) or attempt < 1:
            attempt = 1

        try:
            await self.client.fail_workflow_task(
                task_id=task_id,
                lease_owner=self.worker_id,
                workflow_task_attempt=attempt,
                message=(
                    "unhandled workflow task execution error: "
                    f"{str(error) or type(error).__name__}"
                ),
                failure_type=type(error).__name__,
                stack_trace="".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                ),
            )
        except Exception as report_error:
            log.warning(
                "failed to report unhandled workflow task error for %s: %s",
                task_id,
                report_error,
            )

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
                    timeout=self._poll_http_timeout,
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
        self._activity_inflight += 1
        try:
            outcome = await self._run_activity_task(task)
        except Exception:
            log.exception("unhandled error in activity task execution")
        finally:
            self._activity_inflight = max(0, self._activity_inflight - 1)
            self._record_task_metrics("activity", outcome, time.perf_counter() - task_start)
            self._act_semaphore.release()

    async def _poll_query_tasks(self, *, client: Client | None = None, track_tasks: bool = True) -> None:
        client = client or self.client
        while not self._stop.is_set() and not self._query_thread_stop.is_set():
            try:
                poll_start = time.perf_counter()
                task = await client.poll_query_task(
                    worker_id=self.worker_id,
                    task_queue=self.task_queue,
                    timeout=self._poll_http_timeout,
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
            if track_tasks:
                self._track(self._dispatch_query_task(task, client=client))
            else:
                await self._dispatch_query_task(task, client=client)

    async def _dispatch_query_task(self, task: dict[str, Any], *, client: Client | None = None) -> None:
        task_start = time.perf_counter()
        outcome = "error"
        try:
            outcome = await self._run_query_task(task, client=client)
        except Exception:
            log.exception("unhandled error in query task execution")
        finally:
            self._record_task_metrics("query", outcome, time.perf_counter() - task_start)

    def _clone_client_for_query_tasks(self) -> Client:
        warning_config = self._payload_size_warning_config()

        return Client(
            self.client.base_url,
            token=self.client.token,
            control_token=self.client.control_token,
            worker_token=self.client.worker_token,
            namespace=self.client.namespace,
            timeout=getattr(self.client, "timeout", 60.0),
            retry_policy=self.client.retry_policy,
            metrics=self.metrics,
            payload_size_limit_bytes=(
                warning_config.limit_bytes
                if warning_config is not None
                else serializer.DEFAULT_PAYLOAD_SIZE_BYTES
            ),
            payload_size_warning_threshold_percent=(
                warning_config.threshold_percent
                if warning_config is not None
                else serializer.DEFAULT_WARNING_THRESHOLD_PERCENT
            ),
            payload_size_warnings=warning_config is not None,
            external_storage=self.external_storage,
            external_storage_threshold_bytes=self.external_storage_threshold_bytes,
            external_storage_cache=self.external_storage_cache,
        )

    def _can_clone_client_for_query_tasks(self) -> bool:
        return type(self.client) is Client

    def _start_query_task_thread(self) -> None:
        if self._query_thread is not None and self._query_thread.is_alive():
            return

        self._query_thread_stop.clear()
        self._query_thread = threading.Thread(
            target=self._run_query_task_thread,
            name=f"durable-workflow-query-poller-{self.worker_id}",
            daemon=True,
        )
        self._query_thread.start()

    def _run_query_task_thread(self) -> None:
        try:
            asyncio.run(self._query_task_thread_main())
        except Exception:
            log.exception("query task poller thread stopped unexpectedly")

    async def _query_task_thread_main(self) -> None:
        async with self._clone_client_for_query_tasks() as client:
            await self._poll_query_tasks(client=client, track_tasks=False)

    async def _stop_query_task_thread(self) -> None:
        self._query_thread_stop.set()
        thread = self._query_thread

        if thread is None or not thread.is_alive():
            return

        await asyncio.to_thread(
            thread.join,
            min(self._shutdown_timeout, self._poll_http_timeout + 1),
        )

    async def run(self) -> None:
        """Register the worker and poll until `stop()` is called or the task is cancelled."""
        await self._register()
        wf_loop = asyncio.create_task(self._poll_workflow_tasks())
        act_loop = asyncio.create_task(self._poll_activity_tasks())
        hb_loop = asyncio.create_task(self._heartbeat_loop())
        loops = [wf_loop, act_loop, hb_loop]
        if self._query_tasks_supported:
            if self._can_clone_client_for_query_tasks():
                self._start_query_task_thread()
            else:
                loops.append(asyncio.create_task(self._poll_query_tasks()))
        try:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*loops)
        finally:
            await self._stop_query_task_thread()

    async def _heartbeat_loop(self) -> None:
        """Periodically refresh the server-side worker registration.

        Reports current task-slot availability and basic process-level
        metrics so the worker management API, CLI worker listing, and
        Waterline Worker Status view can show free-slot counts and
        process health alongside ``last_heartbeat_at``. Cadence is the
        server-advertised ``heartbeat_interval_seconds`` (default 60s,
        bounded to [1s, 1h] cluster-wide) so workers stop being
        considered for task dispatch when they miss enough heartbeats.
        """
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._heartbeat_interval)
            except asyncio.TimeoutError:
                pass
            if self._stop.is_set():
                return
            try:
                ack = await self.client.heartbeat_worker(
                    worker_id=self.worker_id,
                    task_slots=self._current_task_slots(),
                    process_metrics=self._current_process_metrics(),
                )
            except Exception as e:
                log.warning("worker heartbeat failed: %s", e)
                continue
            if isinstance(ack, dict):
                advertised = ack.get("heartbeat_interval_seconds")
                if isinstance(advertised, int) and advertised > 0:
                    self._heartbeat_interval = float(advertised)

    def _current_task_slots(self) -> dict[str, int]:
        return {
            "workflow_available": max(
                0, self.max_concurrent_workflow_tasks - self._workflow_inflight
            ),
            "activity_available": max(
                0, self.max_concurrent_activity_tasks - self._activity_inflight
            ),
        }

    def _current_process_metrics(self) -> dict[str, Any]:
        import os
        import socket

        now = time.time()
        metrics: dict[str, Any] = {
            "process_uptime_seconds": int(now - self._process_started_at),
            "process_id": os.getpid(),
            "process_started_at": self._process_started_at_iso,
        }

        # ``memory_bytes`` is the *current* resident set size, not the
        # lifetime peak. ``resource.getrusage().ru_maxrss`` is the high-
        # water mark since the process started, which masks freed memory
        # and never decreases — wrong shape for a heartbeat metric meant
        # to show what the worker is using right now. On Linux we read
        # the second field of ``/proc/self/statm`` (resident pages) and
        # multiply by the page size. Platforms without ``/proc`` get no
        # ``memory_bytes`` field rather than a misleading lifetime peak.
        if sys.platform.startswith("linux"):
            try:
                with open("/proc/self/statm") as statm:
                    fields = statm.read().split()
                if len(fields) >= 2:
                    metrics["memory_bytes"] = int(fields[1]) * os.sysconf("SC_PAGE_SIZE")
            except (OSError, ValueError):
                pass

        # ``cpu_percent`` is the share of wall time the process spent on
        # CPU during the interval since the previous heartbeat — not the
        # lifetime average, which converges to a fixed value and stops
        # tracking live load. The first sample bootstraps from process
        # start so the very first heartbeat still has a number.
        try:
            import resource

            usage = resource.getrusage(resource.RUSAGE_SELF)
            cpu_seconds = float(usage.ru_utime) + float(usage.ru_stime)
            if self._last_cpu_sample_at is None:
                interval = max(0.001, now - self._process_started_at)
                delta_cpu = max(0.0, cpu_seconds)
            else:
                interval = max(0.001, now - self._last_cpu_sample_at)
                delta_cpu = max(0.0, cpu_seconds - self._last_cpu_total_seconds)
            metrics["cpu_percent"] = max(
                0.0, min(100.0, round((delta_cpu / interval) * 100.0, 2))
            )
            self._last_cpu_sample_at = now
            self._last_cpu_total_seconds = cpu_seconds
        except (ImportError, OSError):
            # ``resource`` is POSIX-only — Windows skips the CPU sample
            # but still reports pid + uptime + host so the operator
            # surface stays populated.
            pass

        try:
            host = socket.gethostname()
        except Exception:
            host = ""
        if isinstance(host, str) and host != "":
            metrics["host"] = host[:255]

        return metrics

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
        background_tasks: list[asyncio.Task[Any]] = []

        try:
            await self._register()
            background_tasks.append(asyncio.create_task(self._heartbeat_loop()))
            if self._query_tasks_supported:
                if self._can_clone_client_for_query_tasks():
                    self._start_query_task_thread()
                else:
                    background_tasks.append(asyncio.create_task(self._poll_query_tasks()))

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
                        timeout=self._poll_http_timeout,
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
                    timeout=self._poll_http_timeout,
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
            self._stop.set()
            for task in background_tasks:
                task.cancel()
            for task in background_tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await self.stop()

    async def stop(self) -> None:
        """Stop polling and drain in-flight tasks up to the configured shutdown timeout."""
        self._stop.set()
        await self._stop_query_task_thread()
        if self._in_flight:
            log.info("draining %d in-flight task(s)…", len(self._in_flight))
            done, pending = await asyncio.wait(
                self._in_flight, timeout=self._shutdown_timeout
            )
            for t in pending:
                t.cancel()
            if pending:
                log.warning("cancelled %d task(s) after shutdown timeout", len(pending))
