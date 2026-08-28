"""Async client for the Durable Workflow server's control and worker planes.

The :class:`Client` wraps the server's HTTP/JSON protocol. Control-plane
methods (``start_workflow``, ``signal_workflow``, ``describe_workflow``,
schedule management, …) are what callers use to drive workflows from outside.
Worker-plane methods (``register_worker``, ``deregister_worker_registration``,
``poll_workflow_task``, ``poll_query_task``, ``complete_activity_task``, …) are what the
:class:`~durable_workflow.Worker`
uses to run tasks; they are public so advanced users can build custom
workers, but most applications should not call them directly.

The module also defines the returned-value dataclasses (``WorkflowExecution``,
``WorkflowList``, ``ScheduleSpec``, ``ScheduleDescription``, …) and the
ergonomic handle classes (:class:`WorkflowHandle`, :class:`ScheduleHandle`)
that bind a workflow or schedule id to a :class:`Client` so you can call
methods without repeating the id on every call.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import os
import time
import uuid
import warnings
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

import httpx

from . import serializer
from .errors import (
    ExternalPayloadIntegrityMismatch,
    ExternalPayloadOversized,
    ExternalPayloadUnavailable,
    ExternalPayloadUnsupported,
    RuntimeCapabilityUnsupported,
    RuntimeDiscoveryUnavailable,
    ServerError,
    WorkflowCancelled,
    WorkflowFailed,
    WorkflowTerminated,
    _raise_for_status,
)
from .external_storage import (
    RUNTIME_EXTERNAL_PAYLOAD_REFERENCE_SCHEMA,
    ExternalPayloadCache,
    ExternalPayloadStoragePolicy,
    ExternalStorageDriver,
    RuntimeExternalPayloadReference,
)
from .metrics import CLIENT_REQUEST_DURATION_SECONDS, CLIENT_REQUESTS, NOOP_METRICS, MetricsRecorder
from .nexus import NexusOperationResult, nexus_request_payload
from .retry_policy import TransportRetryPolicy

PROTOCOL_VERSION = "1.16"
CONTROL_PLANE_VERSION = "2"
PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST: dict[str, dict[str, str | bool]] = {
    "local_activities": {
        "supported": False,
        "minimum_protocol_version": "1.18",
        "reason": "python_worker_does_not_execute_record_local_activity",
    },
    "worker_sessions": {
        "supported": False,
        "minimum_protocol_version": "1.18",
        "reason": "python_worker_has_no_typed_session_lifecycle",
    },
    "sticky_execution": {
        "supported": False,
        "minimum_protocol_version": "1.18",
        "reason": "python_worker_uses_complete_durable_history_replay",
    },
}
_MESSAGE_STREAMS_CAPABILITY = "message_streams"
_MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL = (1, 15)
CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA = "durable-workflow.v2.control-plane-request.contract"
CONTROL_PLANE_REQUEST_CONTRACT_VERSION = 1
_QUERY_TASKS_DISCOVERY_PATH = "worker_protocol.server_capabilities.query_tasks"
_UPDATE_WAIT_STAGES_DISCOVERY_PATH = (
    "control_plane.request_contract.operations.update.fields.wait_for.canonical_values"
)
_RUNTIME_EXTERNAL_PAYLOAD_DISCOVERY_PATH = (
    "namespace.external_payload_storage.transport"
)
_RUNTIME_EXTERNAL_PAYLOAD_TRANSPORT_SCHEMA = (
    "durable-workflow.v2.runtime-external-payload-transport.v1"
)
_RUNTIME_EXTERNAL_PAYLOAD_UPLOAD_SCHEMA = (
    "durable-workflow.v2.runtime-external-payload-upload.v1"
)
_RUNTIME_EXTERNAL_PAYLOAD_UPLOAD_PATH = "/external-payloads/v1"
_RUNTIME_EXTERNAL_PAYLOAD_FETCH_PATH_TEMPLATE = (
    "/external-payloads/v1/{referenceId}"
)
_RUNTIME_EXTERNAL_PAYLOAD_ERROR_BODY_LIMIT = 64 * 1024


def _default_sdk_version() -> str:
    try:
        return f"durable-workflow-python/{_pkg_version('durable-workflow')}"
    except PackageNotFoundError:
        return "durable-workflow-python/0.0.0+unknown"


DEFAULT_SDK_VERSION = _default_sdk_version()
_WORKER_POLL_MAX_SERVER_TIMEOUT_SECONDS = 60
_WORKER_POLL_HTTP_TIMEOUT_GRACE_SECONDS = 5.0


def _worker_poll_timeout_seconds(timeout: float | None) -> int | None:
    if timeout is None:
        return None

    value = float(timeout)
    if not math.isfinite(value) or value < 0:
        raise ValueError("timeout must be a non-negative finite number")

    seconds = int(math.ceil(value)) if value >= 1 else 0
    return min(seconds, _WORKER_POLL_MAX_SERVER_TIMEOUT_SECONDS)


def _worker_poll_http_timeout(timeout: float | None) -> float | None:
    if timeout is None:
        return None

    timeout_seconds = _worker_poll_timeout_seconds(timeout)
    assert timeout_seconds is not None

    if timeout_seconds == 0:
        return max(float(timeout), 1.0)

    return max(float(timeout), float(timeout_seconds) + _WORKER_POLL_HTTP_TIMEOUT_GRACE_SECONDS)


def _protocol_version_from_env(name: str, default: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default

    return value.strip()


def _worker_protocol_supports_message_streams() -> bool:
    version = _protocol_version_from_env(
        "DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION",
        PROTOCOL_VERSION,
    )
    parts = version.split(".")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        return False

    return (int(parts[0]), int(parts[1])) >= _MESSAGE_STREAMS_MINIMUM_WORKER_PROTOCOL


def _normalize_base_url(base_url: str) -> str:
    parsed = urlsplit(base_url)
    if parsed.query or parsed.fragment:
        raise ValueError("base_url must not include a query or fragment; pass only the server or managed-runtime URL")

    normalized_path = parsed.path.rstrip("/")
    if normalized_path.endswith("/api"):
        raise ValueError(
            "base_url must be the server or managed-runtime root without the SDK-owned '/api' suffix; "
            "remove the trailing '/api' because Client appends it automatically"
        )

    return base_url.rstrip("/")


def _route_for_metrics(path: str) -> str:
    clean_path = path.split("?", 1)[0]
    parts = [part for part in clean_path.strip("/").split("/") if part]
    if not parts:
        return "/"

    if parts[0] == "workflows" and len(parts) >= 2:
        parts[1] = "{workflow_id}"
        if len(parts) >= 4 and parts[2] in {"signal", "query", "update"}:
            parts[3] = "{name}"
        if len(parts) >= 4 and parts[2] == "runs":
            parts[3] = "{run_id}"
    elif parts[0] == "schedules" and len(parts) >= 2:
        parts[1] = "{schedule_id}"
    elif parts[0] in {"namespaces", "search-attributes"} and len(parts) >= 2:
        parts[1] = "{name}"
    elif parts[0] == "workers" and len(parts) >= 2:
        parts[1] = "{worker_id}"
    elif parts[:2] == ["worker", "registrations"] and len(parts) >= 3:
        parts[2] = "{worker_id}"
    elif parts[:2] == ["bridge-adapters", "webhook"] and len(parts) >= 3:
        parts[2] = "{adapter}"
    elif (
        parts[:2] == ["worker", "workflow-tasks"]
        or parts[:2] == ["worker", "activity-tasks"]
        or parts[:2] == ["worker", "query-tasks"]
    ) and len(parts) >= 3:
        parts[2] = "{task_id}"

    return "/" + "/".join(parts)


def _resolve_namespace_name(
    name: str | None,
    namespace_alias: str | None,
    *,
    method: str,
) -> str:
    """Resolve the namespace name accepted via ``name=`` or the deprecated ``namespace=`` alias."""
    if namespace_alias is not None:
        if name is not None:
            raise TypeError(
                f"{method}() received both 'name' and the deprecated alias "
                "'namespace'; pass only 'name'."
            )
        warnings.warn(
            f"{method}() argument 'namespace' is deprecated since 0.4.1; "
            "use 'name' to match describe_namespace, create_namespace, and "
            "update_namespace.",
            DeprecationWarning,
            stacklevel=3,
        )
        name = namespace_alias
    if name is None:
        raise TypeError(f"{method}() missing required argument: 'name'")
    return name


def _contains_inline_payload_envelope(value: object) -> bool:
    if isinstance(value, dict):
        if set(value) == {"codec", "blob"} and isinstance(value.get("blob"), str):
            return True
        if (
            value.get("type") == "record_side_effect"
            and isinstance(value.get("result"), str)
        ):
            return True
        return any(_contains_inline_payload_envelope(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_inline_payload_envelope(item) for item in value)
    return False


def _contains_runtime_payload_envelope(value: object) -> bool:
    if isinstance(value, dict):
        if "external_payload" in value:
            return True
        return any(_contains_runtime_payload_envelope(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_runtime_payload_envelope(item) for item in value)
    return False


def _contains_direct_external_storage_envelope(value: object) -> bool:
    if isinstance(value, dict):
        if "external_storage" in value:
            return True
        return any(
            _contains_direct_external_storage_envelope(item)
            for item in value.values()
        )
    if isinstance(value, list):
        return any(_contains_direct_external_storage_envelope(item) for item in value)
    return False


@dataclass(frozen=True)
class _RuntimeExternalPayloadTransport:
    threshold_bytes: int
    max_payload_bytes: int
    request_timeout_seconds: float
    status: str


@dataclass
class WorkflowExecution:
    """Current server view of one workflow execution."""

    workflow_id: str
    run_id: str | None
    workflow_type: str
    status: str | None = None
    namespace: str | None = None
    task_queue: str | None = None
    input: Any = None
    output: Any = None
    payload_codec: str | None = None
    memo: dict[str, Any] | None = None
    search_attributes: dict[str, Any] | None = None


@dataclass
class WorkflowList:
    """One page of workflow visibility results."""

    executions: list[WorkflowExecution]
    next_page_token: str | None = None


@dataclass
class WorkflowStreamDescription:
    """Lifecycle and backlog state for one run-scoped output stream."""

    workflow_id: str
    run_id: str
    stream_name: str
    status: str
    last_offset: int | None = None
    total_items: int = 0
    pending_items: int = 0
    error_reason: str | None = None
    opened_at: str | None = None
    last_appended_at: str | None = None
    closed_at: str | None = None
    retention_seconds: int | None = None
    raw: dict[str, Any] | None = None

    @property
    def terminal(self) -> bool:
        """Whether no more items can be appended to this stream."""
        return self.status in {"closed", "errored"}

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        workflow_id: str = "",
        run_id: str = "",
    ) -> WorkflowStreamDescription:
        return cls(
            workflow_id=str(data.get("workflow_id", workflow_id)),
            run_id=str(data.get("workflow_run_id", data.get("run_id", run_id))),
            stream_name=str(data.get("stream_name", "")),
            status=str(data.get("status", "open")),
            last_offset=int(data["last_offset"]) if data.get("last_offset") is not None else None,
            total_items=int(data.get("total_items", 0)),
            pending_items=int(data.get("pending_items", 0)),
            error_reason=data.get("error_reason"),
            opened_at=data.get("opened_at"),
            last_appended_at=data.get("last_appended_at"),
            closed_at=data.get("closed_at"),
            retention_seconds=(
                int(data["retention_seconds"])
                if data.get("retention_seconds") is not None
                else None
            ),
            raw=dict(data),
        )


@dataclass
class WorkflowStreamAppendItem:
    """One value or external payload reference to append to a stream."""

    payload: Any = None
    payload_reference: str | None = None
    item_type: str | None = None
    content_type: str | None = None
    idempotency_key: str | None = None


@dataclass
class WorkflowStreamItem:
    """One ordered durable stream item returned by a subscription."""

    offset: int
    payload: Any = None
    payload_envelope: Mapping[str, Any] | None = None
    payload_reference: str | None = None
    payload_codec: str | None = None
    idempotency_key: str | None = None
    item_type: str | None = None
    content_type: str | None = None
    origin: str | None = None
    origin_reference: str | None = None
    emitted_at: str | None = None
    raw: dict[str, Any] | None = None


@dataclass
class WorkflowStreamPage:
    """A resumable page of at-least-once stream deliveries."""

    stream: WorkflowStreamDescription
    items: list[WorkflowStreamItem]
    next_offset: int
    terminal: bool
    raw: dict[str, Any] | None = None


@dataclass
class WorkflowStreamAppendResult:
    """Offsets created or deduplicated by one append request."""

    stream: WorkflowStreamDescription
    accepted_offsets: list[int]
    accepted_count: int
    deduplicated_count: int
    raw: dict[str, Any] | None = None


@dataclass
class NamespaceDescription:
    """Server configuration for one workflow namespace."""

    name: str
    description: str | None = None
    retention_days: int | None = None
    status: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    deleted: dict[str, int] | None = None
    external_payload_storage: ExternalPayloadStoragePolicy | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> NamespaceDescription:
        external_payload_storage = None
        if isinstance(data.get("external_payload_storage"), dict):
            external_payload_storage = ExternalPayloadStoragePolicy.from_dict(data)
        deleted = None
        if isinstance(data.get("deleted"), dict):
            deleted = {
                str(key): value
                for key, value in data["deleted"].items()
                if isinstance(value, int)
            }

        return cls(
            name=str(data.get("name", "")),
            description=data.get("description"),
            retention_days=data.get("retention_days"),
            status=data.get("status"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            deleted=deleted,
            external_payload_storage=external_payload_storage,
        )


@dataclass
class NamespaceList:
    """Namespaces visible to the current control-plane identity."""

    namespaces: list[NamespaceDescription]


@dataclass
class StoragePayloadTestResult:
    """Result for one payload size exercised by the server storage probe."""

    status: str
    bytes: int | None = None
    sha256: str | None = None
    reference_uri: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StoragePayloadTestResult:
        return cls(
            status=str(data.get("status", "")),
            bytes=data.get("bytes"),
            sha256=data.get("sha256"),
            reference_uri=data.get("reference_uri"),
        )


@dataclass
class StorageTestResult:
    """Server response for an external payload storage probe."""

    status: str
    namespace: str | None = None
    driver: str | None = None
    small_payload: StoragePayloadTestResult | None = None
    large_payload: StoragePayloadTestResult | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> StorageTestResult:
        small = data.get("small_payload")
        large = data.get("large_payload")

        return cls(
            status=str(data.get("status", "")),
            namespace=data.get("namespace"),
            driver=data.get("driver"),
            small_payload=StoragePayloadTestResult.from_dict(small) if isinstance(small, dict) else None,
            large_payload=StoragePayloadTestResult.from_dict(large) if isinstance(large, dict) else None,
            raw=data,
        )


@dataclass
class WorkflowRun:
    """Current server view of one durable run in a workflow execution chain."""

    workflow_id: str
    run_id: str
    workflow_type: str
    status: str | None = None
    namespace: str | None = None
    task_queue: str | None = None
    run_number: int | None = None
    run_count: int | None = None
    is_current_run: bool | None = None
    status_bucket: str | None = None
    business_key: str | None = None
    compatibility: str | None = None
    payload_codec: str | None = None
    input: Any = None
    output: Any = None
    memo: dict[str, Any] | None = None
    search_attributes: dict[str, Any] | None = None
    actions: dict[str, Any] | None = None
    started_at: str | None = None
    closed_at: str | None = None
    last_progress_at: str | None = None
    closed_reason: str | None = None
    wait_kind: str | None = None
    wait_reason: str | None = None
    execution_timeout_seconds: int | None = None
    run_timeout_seconds: int | None = None
    execution_deadline_at: str | None = None
    run_deadline_at: str | None = None

    @classmethod
    def from_dict(
        cls, data: dict[str, Any], *, workflow_id: str | None = None, run_id: str | None = None
    ) -> WorkflowRun:
        return cls(
            workflow_id=data.get("workflow_id", workflow_id or ""),
            run_id=data.get("run_id", run_id or ""),
            workflow_type=data.get("workflow_type", ""),
            status=data.get("status"),
            namespace=data.get("namespace"),
            task_queue=data.get("task_queue"),
            run_number=data.get("run_number"),
            run_count=data.get("run_count"),
            is_current_run=data.get("is_current_run"),
            status_bucket=data.get("status_bucket"),
            business_key=data.get("business_key"),
            compatibility=data.get("compatibility"),
            payload_codec=data.get("payload_codec"),
            input=data.get("input"),
            output=data.get("output"),
            memo=data.get("memo") if isinstance(data.get("memo"), dict) else None,
            search_attributes=(
                data.get("search_attributes") if isinstance(data.get("search_attributes"), dict) else None
            ),
            actions=data.get("actions") if isinstance(data.get("actions"), dict) else None,
            started_at=data.get("started_at"),
            closed_at=data.get("closed_at"),
            last_progress_at=data.get("last_progress_at"),
            closed_reason=data.get("closed_reason"),
            wait_kind=data.get("wait_kind"),
            wait_reason=data.get("wait_reason"),
            execution_timeout_seconds=data.get("execution_timeout_seconds"),
            run_timeout_seconds=data.get("run_timeout_seconds"),
            execution_deadline_at=data.get("execution_deadline_at"),
            run_deadline_at=data.get("run_deadline_at"),
        )


@dataclass
class WorkflowCommandResult:
    """Machine-readable outcome returned by workflow control commands."""

    workflow_id: str
    outcome: str
    command_status: str | None = None
    command_id: str | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, workflow_id: str | None = None) -> WorkflowCommandResult:
        return cls(
            workflow_id=data.get("workflow_id", workflow_id or ""),
            outcome=data.get("outcome", ""),
            command_status=data.get("command_status"),
            command_id=data.get("command_id"),
            raw=data,
        )


@dataclass
class WorkflowRunList:
    """All known durable runs for one workflow execution, oldest first."""

    workflow_id: str
    run_count: int
    runs: list[WorkflowRun]


@dataclass
class StandaloneActivityExecution:
    """Server view of one standalone activity execution.

    Standalone activities run as top-level durable jobs anchored by a
    server-managed host run; this dataclass collapses the host-run state
    and the underlying activity execution state into one description so
    callers do not need to navigate between two surfaces to inspect a
    single job.
    """

    activity_id: str
    workflow_run_id: str | None
    activity_execution_id: str | None
    workflow_type: str
    activity_type: str | None
    activity_class: str | None
    task_queue: str | None
    namespace: str | None
    status: str | None
    activity_status: str | None
    closed_reason: str | None
    business_key: str | None
    payload_codec: str | None
    started_at: str | None
    closed_at: str | None
    last_progress_at: str | None
    last_heartbeat_at: str | None
    schedule_to_start_deadline_at: str | None
    schedule_to_close_deadline_at: str | None
    attempt_count: int | None
    result: Any = None

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        *,
        activity_id: str | None = None,
        result: Any = None,
    ) -> StandaloneActivityExecution:
        return cls(
            activity_id=data.get("activity_id", activity_id or ""),
            workflow_run_id=data.get("workflow_run_id"),
            activity_execution_id=data.get("activity_execution_id"),
            workflow_type=data.get("workflow_type", ""),
            activity_type=data.get("activity_type"),
            activity_class=data.get("activity_class"),
            task_queue=data.get("task_queue"),
            namespace=data.get("namespace"),
            status=data.get("status"),
            activity_status=data.get("activity_status"),
            closed_reason=data.get("closed_reason"),
            business_key=data.get("business_key"),
            payload_codec=data.get("payload_codec"),
            started_at=data.get("started_at"),
            closed_at=data.get("closed_at"),
            last_progress_at=data.get("last_progress_at"),
            last_heartbeat_at=data.get("last_heartbeat_at"),
            schedule_to_start_deadline_at=data.get("schedule_to_start_deadline_at"),
            schedule_to_close_deadline_at=data.get("schedule_to_close_deadline_at"),
            attempt_count=data.get("attempt_count"),
            result=result,
        )


@dataclass
class StandaloneActivityList:
    """One page of standalone activity executions returned by the server."""

    activities: list[StandaloneActivityExecution]
    activity_count: int
    next_page_token: str | None = None


@dataclass
class SearchAttributeList:
    """Search attribute definitions available in the current namespace."""

    system_attributes: dict[str, str]
    custom_attributes: dict[str, str]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SearchAttributeList:
        system = data.get("system_attributes")
        custom = data.get("custom_attributes")

        return cls(
            system_attributes=dict(system) if isinstance(system, dict) else {},
            custom_attributes=dict(custom) if isinstance(custom, dict) else {},
        )


@dataclass
class TaskQueueTaskAdmission:
    """Workflow/activity admission state for one task queue."""

    status: str | None = None
    budget_source: str | None = None
    server_budget_source: str | None = None
    active_worker_count: int | None = None
    configured_slot_count: int | None = None
    leased_count: int | None = None
    ready_count: int | None = None
    available_slot_count: int | None = None
    server_max_active_leases_per_queue: int | None = None
    server_active_lease_count: int | None = None
    server_remaining_active_lease_capacity: int | None = None
    server_max_active_leases_per_namespace: int | None = None
    server_namespace_active_lease_count: int | None = None
    server_remaining_namespace_active_lease_capacity: int | None = None
    server_max_dispatches_per_minute: int | None = None
    server_dispatch_count_this_minute: int | None = None
    server_remaining_dispatch_capacity: int | None = None
    server_max_dispatches_per_minute_per_namespace: int | None = None
    server_namespace_dispatch_count_this_minute: int | None = None
    server_remaining_namespace_dispatch_capacity: int | None = None
    server_dispatch_budget_group: str | None = None
    server_max_dispatches_per_minute_per_budget_group: int | None = None
    server_budget_group_dispatch_count_this_minute: int | None = None
    server_remaining_budget_group_dispatch_capacity: int | None = None
    server_lock_required: bool | None = None
    server_lock_supported: bool | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> TaskQueueTaskAdmission | None:
        if data is None:
            return None
        return cls(
            status=data.get("status"),
            budget_source=data.get("budget_source"),
            server_budget_source=data.get("server_budget_source"),
            active_worker_count=data.get("active_worker_count"),
            configured_slot_count=data.get("configured_slot_count"),
            leased_count=data.get("leased_count"),
            ready_count=data.get("ready_count"),
            available_slot_count=data.get("available_slot_count"),
            server_max_active_leases_per_queue=data.get("server_max_active_leases_per_queue"),
            server_active_lease_count=data.get("server_active_lease_count"),
            server_remaining_active_lease_capacity=data.get("server_remaining_active_lease_capacity"),
            server_max_active_leases_per_namespace=data.get("server_max_active_leases_per_namespace"),
            server_namespace_active_lease_count=data.get("server_namespace_active_lease_count"),
            server_remaining_namespace_active_lease_capacity=data.get(
                "server_remaining_namespace_active_lease_capacity"
            ),
            server_max_dispatches_per_minute=data.get("server_max_dispatches_per_minute"),
            server_dispatch_count_this_minute=data.get("server_dispatch_count_this_minute"),
            server_remaining_dispatch_capacity=data.get("server_remaining_dispatch_capacity"),
            server_max_dispatches_per_minute_per_namespace=data.get(
                "server_max_dispatches_per_minute_per_namespace"
            ),
            server_namespace_dispatch_count_this_minute=data.get("server_namespace_dispatch_count_this_minute"),
            server_remaining_namespace_dispatch_capacity=data.get("server_remaining_namespace_dispatch_capacity"),
            server_dispatch_budget_group=data.get("server_dispatch_budget_group"),
            server_max_dispatches_per_minute_per_budget_group=data.get(
                "server_max_dispatches_per_minute_per_budget_group"
            ),
            server_budget_group_dispatch_count_this_minute=data.get(
                "server_budget_group_dispatch_count_this_minute"
            ),
            server_remaining_budget_group_dispatch_capacity=data.get(
                "server_remaining_budget_group_dispatch_capacity"
            ),
            server_lock_required=data.get("server_lock_required"),
            server_lock_supported=data.get("server_lock_supported"),
        )


@dataclass
class TaskQueueQueryAdmission:
    """Worker-routed query-task admission state for one task queue."""

    status: str | None = None
    budget_source: str | None = None
    max_pending_per_queue: int | None = None
    approximate_pending_count: int | None = None
    remaining_pending_capacity: int | None = None
    lock_required: bool | None = None
    lock_supported: bool | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> TaskQueueQueryAdmission | None:
        if data is None:
            return None
        return cls(
            status=data.get("status"),
            budget_source=data.get("budget_source"),
            max_pending_per_queue=data.get("max_pending_per_queue"),
            approximate_pending_count=data.get("approximate_pending_count"),
            remaining_pending_capacity=data.get("remaining_pending_capacity"),
            lock_required=data.get("lock_required"),
            lock_supported=data.get("lock_supported"),
        )


@dataclass
class TaskQueueAdmission:
    """Server-side admission budgets for workflow, activity, and query tasks."""

    workflow_tasks: TaskQueueTaskAdmission | None = None
    activity_tasks: TaskQueueTaskAdmission | None = None
    query_tasks: TaskQueueQueryAdmission | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> TaskQueueAdmission:
        payload = data or {}
        return cls(
            workflow_tasks=TaskQueueTaskAdmission.from_dict(payload.get("workflow_tasks")),
            activity_tasks=TaskQueueTaskAdmission.from_dict(payload.get("activity_tasks")),
            query_tasks=TaskQueueQueryAdmission.from_dict(payload.get("query_tasks")),
            raw=payload,
        )


@dataclass
class TaskQueueDescription:
    """Current server visibility and admission state for one task queue."""

    name: str
    namespace: str | None = None
    stats: dict[str, Any] | None = None
    admission: TaskQueueAdmission | None = None
    pollers: list[dict[str, Any]] | None = None
    current_leases: list[dict[str, Any]] | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskQueueDescription:
        pollers = data.get("pollers")
        current_leases = data.get("current_leases")
        return cls(
            name=data.get("name", ""),
            namespace=data.get("namespace"),
            stats=data.get("stats"),
            admission=TaskQueueAdmission.from_dict(data.get("admission")),
            pollers=pollers if isinstance(pollers, list) else None,
            current_leases=current_leases if isinstance(current_leases, list) else None,
            raw=data,
        )


@dataclass
class TaskQueueList:
    """One task-queue visibility page returned by the server."""

    namespace: str | None
    task_queues: list[TaskQueueDescription]


@dataclass
class TaskQueueBuildIdCohort:
    """Per-build-id rollout state for one task queue.

    ``build_id`` is ``None`` for the cohort of workers that registered
    without a build identifier (the legacy unversioned default).
    """

    build_id: str | None
    rollout_status: str
    active_worker_count: int
    draining_worker_count: int
    stale_worker_count: int
    total_worker_count: int
    runtimes: list[str]
    sdk_versions: list[str]
    last_heartbeat_at: str | None = None
    first_seen_at: str | None = None
    drain_intent: str | None = None
    drained_at: str | None = None
    promoted_at: str | None = None
    rolled_back_at: str | None = None
    new_start_selected: bool = False
    workflow_definition_fingerprint_count: int = 0
    workflow_definition_fingerprint_conflicts: list[dict[str, Any]] | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskQueueBuildIdCohort:
        runtimes = data.get("runtimes")
        sdk_versions = data.get("sdk_versions")
        fingerprint_conflicts_raw = data.get("workflow_definition_fingerprint_conflicts")
        fingerprint_conflicts: list[dict[str, Any]] | None = None
        if isinstance(fingerprint_conflicts_raw, list):
            fingerprint_conflicts = []
            for item in fingerprint_conflicts_raw:
                if not isinstance(item, dict):
                    continue
                fingerprint_conflicts.append(
                    {str(key): value for key, value in item.items() if isinstance(key, str)}
                )
        return cls(
            build_id=data.get("build_id"),
            rollout_status=str(data.get("rollout_status") or ""),
            active_worker_count=int(data.get("active_worker_count") or 0),
            draining_worker_count=int(data.get("draining_worker_count") or 0),
            stale_worker_count=int(data.get("stale_worker_count") or 0),
            total_worker_count=int(data.get("total_worker_count") or 0),
            runtimes=[r for r in runtimes if isinstance(r, str)] if isinstance(runtimes, list) else [],
            sdk_versions=[v for v in sdk_versions if isinstance(v, str)] if isinstance(sdk_versions, list) else [],
            last_heartbeat_at=data.get("last_heartbeat_at"),
            first_seen_at=data.get("first_seen_at"),
            drain_intent=data.get("drain_intent") if isinstance(data.get("drain_intent"), str) else None,
            drained_at=data.get("drained_at") if isinstance(data.get("drained_at"), str) else None,
            promoted_at=data.get("promoted_at") if isinstance(data.get("promoted_at"), str) else None,
            rolled_back_at=data.get("rolled_back_at") if isinstance(data.get("rolled_back_at"), str) else None,
            new_start_selected=bool(data.get("new_start_selected")),
            workflow_definition_fingerprint_count=int(
                data.get("workflow_definition_fingerprint_count") or 0
            ),
            workflow_definition_fingerprint_conflicts=fingerprint_conflicts,
            raw=data,
        )


@dataclass
class TaskQueueBuildIdRollout:
    """Build-id rollout snapshot returned by the server for one task queue."""

    namespace: str | None
    task_queue: str
    stale_after_seconds: int | None
    build_ids: list[TaskQueueBuildIdCohort]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskQueueBuildIdRollout:
        items = data.get("build_ids")
        return cls(
            namespace=data.get("namespace"),
            task_queue=str(data.get("task_queue") or ""),
            stale_after_seconds=(
                int(data["stale_after_seconds"])
                if isinstance(data.get("stale_after_seconds"), int)
                else None
            ),
            build_ids=[
                TaskQueueBuildIdCohort.from_dict(item)
                for item in (items if isinstance(items, list) else [])
                if isinstance(item, dict)
            ],
        )


@dataclass
class TaskQueueBuildIdRolloutState:
    """Operator-recorded drain intent for one ``(task_queue, build_id)`` cohort.

    Returned by ``drain_task_queue_build_id``,
    ``promote_task_queue_build_id``, and ``resume_task_queue_build_id``.
    ``build_id`` is ``None`` for the unversioned cohort (workers registered
    without a build identifier). ``drain_intent`` is ``"active"`` or
    ``"draining"``. ``drained_at`` is set only when ``drain_intent`` is
    ``"draining"``; repeated drains do not shift the timestamp.
    ``promoted_at`` and ``new_start_selected`` identify the cohort currently
    selected for fresh workflow starts.
    """

    namespace: str | None
    task_queue: str
    build_id: str | None
    drain_intent: str
    drained_at: str | None
    promoted_at: str | None = None
    rolled_back_at: str | None = None
    new_start_selected: bool = False
    deployment: dict[str, Any] | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskQueueBuildIdRolloutState:
        deployment_raw = data.get("deployment")
        deployment = dict(deployment_raw) if isinstance(deployment_raw, dict) else None
        return cls(
            namespace=data.get("namespace"),
            task_queue=str(data.get("task_queue") or ""),
            build_id=data.get("build_id") if isinstance(data.get("build_id"), str) else None,
            drain_intent=str(data.get("drain_intent") or ""),
            drained_at=data.get("drained_at") if isinstance(data.get("drained_at"), str) else None,
            promoted_at=data.get("promoted_at") if isinstance(data.get("promoted_at"), str) else None,
            rolled_back_at=data.get("rolled_back_at") if isinstance(data.get("rolled_back_at"), str) else None,
            new_start_selected=bool(data.get("new_start_selected")),
            deployment=deployment,
            raw=data,
        )


@dataclass
class WorkerDescription:
    """Current server view of one registered worker."""

    worker_id: str
    task_queue: str | None = None
    runtime: str | None = None
    namespace: str | None = None
    sdk_version: str | None = None
    build_id: str | None = None
    status: str | None = None
    max_concurrent_workflow_tasks: int | None = None
    max_concurrent_activity_tasks: int | None = None
    supported_workflow_types: list[str] | None = None
    supported_activity_types: list[str] | None = None
    last_heartbeat_at: str | None = None
    registered_at: str | None = None
    updated_at: str | None = None
    task_slots: dict[str, int | None] | None = None
    process_metrics: dict[str, Any] | None = None
    heartbeat_interval_seconds: int | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, worker_id: str | None = None) -> WorkerDescription:
        workflow_types = data.get("supported_workflow_types")
        activity_types = data.get("supported_activity_types")
        task_slots = data.get("task_slots")
        process_metrics = data.get("process_metrics")

        return cls(
            worker_id=data.get("worker_id", worker_id or ""),
            task_queue=data.get("task_queue"),
            runtime=data.get("runtime"),
            namespace=data.get("namespace"),
            sdk_version=data.get("sdk_version"),
            build_id=data.get("build_id"),
            status=data.get("status"),
            max_concurrent_workflow_tasks=data.get("max_concurrent_workflow_tasks"),
            max_concurrent_activity_tasks=data.get("max_concurrent_activity_tasks"),
            supported_workflow_types=workflow_types if isinstance(workflow_types, list) else None,
            supported_activity_types=activity_types if isinstance(activity_types, list) else None,
            last_heartbeat_at=data.get("last_heartbeat_at"),
            registered_at=data.get("registered_at"),
            updated_at=data.get("updated_at"),
            task_slots=task_slots if isinstance(task_slots, dict) else None,
            process_metrics=process_metrics if isinstance(process_metrics, dict) else None,
            heartbeat_interval_seconds=(
                int(data["heartbeat_interval_seconds"])
                if isinstance(data.get("heartbeat_interval_seconds"), int)
                else None
            ),
            raw=data,
        )


@dataclass
class WorkerList:
    """Registered worker roster for one namespace."""

    namespace: str | None
    workers: list[WorkerDescription]
    stale_after_seconds: int | None = None


@dataclass
class ScheduleSpec:
    """Calendar or interval rules for a scheduled workflow."""

    cron_expressions: list[str] | None = None
    intervals: list[dict[str, str]] | None = None
    timezone: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.cron_expressions is not None:
            d["cron_expressions"] = self.cron_expressions
        if self.intervals is not None:
            d["intervals"] = self.intervals
        if self.timezone is not None:
            d["timezone"] = self.timezone
        return d


@dataclass
class ScheduleAction:
    """Workflow start request issued whenever a schedule fires."""

    workflow_type: str
    task_queue: str | None = None
    input: list[Any] | None = None
    execution_timeout_seconds: int | None = None
    run_timeout_seconds: int | None = None

    def to_dict(
        self,
        *,
        input_encoder: Callable[[list[Any]], dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        d: dict[str, Any] = {"workflow_type": self.workflow_type}
        if self.task_queue is not None:
            d["task_queue"] = self.task_queue
        if self.input is not None:
            d["input"] = input_encoder(self.input) if input_encoder else serializer.envelope(self.input)
        if self.execution_timeout_seconds is not None:
            d["execution_timeout_seconds"] = self.execution_timeout_seconds
        if self.run_timeout_seconds is not None:
            d["run_timeout_seconds"] = self.run_timeout_seconds
        return d


@dataclass
class ScheduleDescription:
    """Current server view of a schedule and its recent execution state."""

    schedule_id: str
    status: str | None = None
    spec: dict[str, Any] | None = None
    action: dict[str, Any] | None = None
    overlap_policy: str | None = None
    note: str | None = None
    memo: dict[str, Any] | None = None
    search_attributes: dict[str, Any] | None = None
    jitter_seconds: int | None = None
    max_runs: int | None = None
    remaining_actions: int | None = None
    fires_count: int = 0
    failures_count: int = 0
    next_fire_at: str | None = None
    last_fired_at: str | None = None
    latest_workflow_instance_id: str | None = None
    paused_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    info: dict[str, Any] | None = None


@dataclass
class ScheduleList:
    """One page of schedule visibility results."""

    schedules: list[ScheduleDescription]
    next_page_token: str | None = None


@dataclass
class ScheduleTriggerResult:
    """Outcome returned after manually triggering a schedule."""

    schedule_id: str
    outcome: str
    workflow_id: str | None = None
    run_id: str | None = None
    reason: str | None = None
    buffer_depth: int | None = None


@dataclass
class ScheduleBackfillResult:
    """Outcome returned after asking a schedule to backfill missed fires."""

    schedule_id: str
    outcome: str
    fires_attempted: int = 0
    results: list[dict[str, Any]] | None = None


@dataclass
class ScheduleHistoryEvent:
    """One entry in a schedule's audit history stream.

    Each event corresponds to a lifecycle transition recorded by the
    server (ScheduleCreated, SchedulePaused, ScheduleResumed,
    ScheduleUpdated, ScheduleTriggered, ScheduleTriggerSkipped, or
    ScheduleDeleted). The ``payload`` mirrors what the workflow engine
    recorded, including command-context attribution when the transition
    came from a mutating API call.
    """

    sequence: int
    event_type: str | None = None
    recorded_at: str | None = None
    workflow_instance_id: str | None = None
    workflow_run_id: str | None = None
    payload: dict[str, Any] | None = None
    id: str | None = None


@dataclass
class ScheduleHistoryPage:
    """One page of a schedule's audit history stream.

    ``next_cursor`` is the ``after_sequence`` value to request the next
    page when ``has_more`` is ``True``; it is ``None`` on the final page.
    """

    schedule_id: str
    events: list[ScheduleHistoryEvent]
    has_more: bool = False
    next_cursor: int | None = None
    namespace: str | None = None


@dataclass
class BridgeAdapterOutcome:
    """Machine-readable result returned by a bridge adapter event."""

    schema: str
    version: int
    adapter: str
    action: str | None
    accepted: bool
    outcome: str
    idempotency_key: str | None = None
    reason: str | None = None
    target: dict[str, Any] | None = None
    correlation: dict[str, Any] | None = None
    workflow_id: str | None = None
    run_id: str | None = None
    workflow_type: str | None = None
    control_plane_outcome: str | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BridgeAdapterOutcome:
        return cls(
            schema=str(data.get("schema", "")),
            version=int(data.get("version", 0)),
            adapter=str(data.get("adapter", "")),
            action=data.get("action"),
            accepted=bool(data.get("accepted", False)),
            outcome=str(data.get("outcome", "")),
            idempotency_key=data.get("idempotency_key"),
            reason=data.get("reason"),
            target=data.get("target") if isinstance(data.get("target"), dict) else None,
            correlation=data.get("correlation") if isinstance(data.get("correlation"), dict) else None,
            workflow_id=data.get("workflow_id"),
            run_id=data.get("run_id"),
            workflow_type=data.get("workflow_type"),
            control_plane_outcome=data.get("control_plane_outcome"),
            raw=data,
        )


class WorkflowHandle:
    """Convenience wrapper for operating on one workflow ID."""

    def __init__(self, client: Client, workflow_id: str, run_id: str | None = None, workflow_type: str = "") -> None:
        self._client = client
        self.workflow_id = workflow_id
        self.run_id = run_id
        self.workflow_type = workflow_type

    async def result(self, *, poll_interval: float = 0.5, timeout: float = 30.0) -> Any:
        """Block until this workflow terminates and return its result. See :meth:`Client.get_result`."""
        return await self._client.get_result(self, poll_interval=poll_interval, timeout=timeout)

    async def describe(self) -> WorkflowExecution:
        """Return the server's current view of this workflow. See :meth:`Client.describe_workflow`."""
        return await self._client.describe_workflow(self.workflow_id)

    async def get_history(self) -> Any:
        """Fetch this run's durable history. See :meth:`Client.get_history`."""
        if self.run_id is None:
            raise ValueError("run_id is required to fetch workflow history from a handle")
        return await self._client.get_history(self.workflow_id, self.run_id)

    async def export_history(self) -> Any:
        """Export this run's history as a replay bundle. See :meth:`Client.export_history`."""
        if self.run_id is None:
            raise ValueError("run_id is required to export workflow history from a handle")
        return await self._client.export_history(self.workflow_id, self.run_id)

    async def list_runs(self) -> WorkflowRunList:
        """List all runs in this workflow execution chain. See :meth:`Client.list_workflow_runs`."""
        return await self._client.list_workflow_runs(self.workflow_id)

    async def describe_run(self, run_id: str | None = None) -> WorkflowRun:
        """Return one run's detailed status. See :meth:`Client.describe_workflow_run`."""
        selected_run_id = run_id or self.run_id
        if selected_run_id is None:
            raise ValueError("run_id is required to describe a workflow run from a handle")
        return await self._client.describe_workflow_run(self.workflow_id, selected_run_id)

    async def signal(self, signal_name: str, args: list[Any] | None = None) -> None:
        """Deliver an external signal to this workflow. See :meth:`Client.signal_workflow`."""
        await self._client.signal_workflow(self.workflow_id, signal_name, args=args)

    async def append_message(
        self,
        stream_name: str,
        message_id: str,
        args: list[Any] | None = None,
    ) -> dict[str, Any]:
        """Append one idempotently identified message to a durable input stream."""
        return await self._client.append_message_stream(
            self.workflow_id,
            stream_name,
            message_id,
            args=args,
        )

    async def query(self, query_name: str, args: list[Any] | None = None) -> Any:
        """Execute a read-only query against this workflow. See :meth:`Client.query_workflow`."""
        return await self._client.query_workflow(self.workflow_id, query_name, args=args)

    async def cancel(self, *, reason: str | None = None) -> None:
        """Request graceful cancellation of this workflow. See :meth:`Client.cancel_workflow`."""
        await self._client.cancel_workflow(self.workflow_id, reason=reason)

    async def terminate(self, *, reason: str | None = None) -> None:
        """Forcefully stop this workflow. See :meth:`Client.terminate_workflow`."""
        await self._client.terminate_workflow(self.workflow_id, reason=reason)

    async def repair(self) -> WorkflowCommandResult:
        """Ask the server to repair this workflow. See :meth:`Client.repair_workflow`."""
        return await self._client.repair_workflow(self.workflow_id)

    async def archive(self, *, reason: str | None = None) -> WorkflowCommandResult:
        """Move this terminal workflow into the archive tier. See :meth:`Client.archive_workflow`."""
        return await self._client.archive_workflow(self.workflow_id, reason=reason)

    async def update(
        self,
        update_name: str,
        args: list[Any] | None = None,
        *,
        wait_for: str | None = None,
        wait_timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> Any:
        """Send a synchronous update to this workflow and wait for the result. See :meth:`Client.update_workflow`."""
        return await self._client.update_workflow(
            self.workflow_id,
            update_name,
            args=args,
            wait_for=wait_for,
            wait_timeout_seconds=wait_timeout_seconds,
            request_id=request_id,
        )


class StandaloneActivityHandle:
    """Convenience wrapper for operating on one standalone activity job.

    Returned by :meth:`Client.start_activity`. The underlying execution is
    a top-level durable job — the server records the activity inside its
    own host run so retries, deadlines, cancellation, and history surface
    through the existing activity infrastructure. The handle exposes the
    job-style operations (``describe``, ``result``, ``cancel``) without
    having to know that there is a host workflow run behind the scenes.
    """

    def __init__(
        self,
        client: Client,
        activity_id: str,
        *,
        workflow_run_id: str | None = None,
        activity_execution_id: str | None = None,
        workflow_type: str = "",
        activity_type: str = "",
    ) -> None:
        self._client = client
        self.activity_id = activity_id
        self.workflow_run_id = workflow_run_id
        self.activity_execution_id = activity_execution_id
        self.workflow_type = workflow_type
        self.activity_type = activity_type

    async def describe(self) -> StandaloneActivityExecution:
        """Fetch the server's current view of this standalone activity.

        See :meth:`Client.describe_activity`.
        """
        return await self._client.describe_activity(self.activity_id)

    async def result(
        self, *, poll_interval: float = 0.5, timeout: float = 30.0
    ) -> Any:
        """Block until the activity reaches a terminal outcome and return its result.

        See :meth:`Client.get_activity_result`.
        """
        return await self._client.get_activity_result(
            self, poll_interval=poll_interval, timeout=timeout
        )

    async def cancel(self, *, reason: str | None = None) -> None:
        """Request graceful cancellation of this standalone activity.

        Cancellation flows through the host run's workflow cancellation
        path; the next heartbeat or attempt boundary observes the request.
        """
        await self._client.cancel_workflow(self.activity_id, reason=reason)


class ScheduleHandle:
    """Convenience wrapper for operating on one schedule ID."""

    def __init__(self, client: Client, schedule_id: str) -> None:
        self._client = client
        self.schedule_id = schedule_id

    async def describe(self) -> ScheduleDescription:
        """Return the server's current view of this schedule. See :meth:`Client.describe_schedule`."""
        return await self._client.describe_schedule(self.schedule_id)

    async def update(
        self,
        *,
        spec: ScheduleSpec | None = None,
        action: ScheduleAction | None = None,
        overlap_policy: str | None = None,
        jitter_seconds: int | None = None,
        max_runs: int | None = None,
        memo: dict[str, Any] | None = None,
        search_attributes: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> None:
        """Update one or more fields of this schedule. See :meth:`Client.update_schedule`."""
        await self._client.update_schedule(
            self.schedule_id,
            spec=spec,
            action=action,
            overlap_policy=overlap_policy,
            jitter_seconds=jitter_seconds,
            max_runs=max_runs,
            memo=memo,
            search_attributes=search_attributes,
            note=note,
        )

    async def pause(self, *, note: str | None = None) -> None:
        """Pause this schedule so it stops firing. See :meth:`Client.pause_schedule`."""
        await self._client.pause_schedule(self.schedule_id, note=note)

    async def resume(self, *, note: str | None = None) -> None:
        """Resume this paused schedule. See :meth:`Client.resume_schedule`."""
        await self._client.resume_schedule(self.schedule_id, note=note)

    async def trigger(self, *, overlap_policy: str | None = None) -> ScheduleTriggerResult:
        """Fire this schedule immediately. See :meth:`Client.trigger_schedule`."""
        return await self._client.trigger_schedule(self.schedule_id, overlap_policy=overlap_policy)

    async def delete(self) -> None:
        """Delete this schedule. See :meth:`Client.delete_schedule`."""
        await self._client.delete_schedule(self.schedule_id)

    async def backfill(
        self,
        *,
        start_time: str,
        end_time: str,
        overlap_policy: str | None = None,
    ) -> ScheduleBackfillResult:
        """Fire this schedule for every moment in a past time range. See :meth:`Client.backfill_schedule`."""
        return await self._client.backfill_schedule(
            self.schedule_id, start_time=start_time, end_time=end_time, overlap_policy=overlap_policy,
        )

    async def history(
        self,
        *,
        limit: int | None = None,
        after_sequence: int | None = None,
    ) -> ScheduleHistoryPage:
        """Return one page of this schedule's audit history. See :meth:`Client.get_schedule_history`."""
        return await self._client.get_schedule_history(
            self.schedule_id,
            limit=limit,
            after_sequence=after_sequence,
        )

    def iter_history(
        self,
        *,
        limit: int | None = None,
        after_sequence: int | None = None,
    ) -> AsyncIterator[ScheduleHistoryEvent]:
        """Iterate every audit event for this schedule. See :meth:`Client.iter_schedule_history`."""
        return self._client.iter_schedule_history(
            self.schedule_id,
            limit=limit,
            after_sequence=after_sequence,
        )


class Client:
    """Async HTTP client for Durable Workflow control-plane and worker APIs.

    ``base_url`` is the server or managed-runtime prefix. Do not append the
    SDK-owned ``/api`` route prefix or include a query string or fragment.

    The client owns one `httpx.AsyncClient` connection pool. Use it as an async
    context manager or call `aclose()` when finished.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        control_token: str | None = None,
        worker_token: str | None = None,
        namespace: str = "default",
        timeout: float = 60.0,
        retry_policy: TransportRetryPolicy | None = None,
        metrics: MetricsRecorder | None = None,
        payload_size_limit_bytes: int = serializer.DEFAULT_PAYLOAD_SIZE_BYTES,
        payload_size_warning_threshold_percent: int = serializer.DEFAULT_WARNING_THRESHOLD_PERCENT,
        payload_size_warnings: bool = True,
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
        external_storage_cache: ExternalPayloadCache | None = None,
    ) -> None:
        self.base_url = _normalize_base_url(base_url)
        self.token = token
        self.control_token = control_token
        self.worker_token = worker_token
        self.namespace = namespace
        self.timeout = timeout
        self.retry_policy = retry_policy or TransportRetryPolicy()
        self.metrics = metrics or NOOP_METRICS
        self.payload_size_warning_config = (
            serializer.PayloadSizeWarningConfig(
                limit_bytes=payload_size_limit_bytes,
                threshold_percent=payload_size_warning_threshold_percent,
            )
            if payload_size_warnings
            else None
        )
        if external_storage_threshold_bytes is not None and external_storage_threshold_bytes < 1:
            raise ValueError("external_storage_threshold_bytes must be at least 1 when provided")
        self.external_storage = external_storage
        self.external_storage_threshold_bytes = external_storage_threshold_bytes
        self.external_storage_cache = (
            external_storage_cache
            if external_storage_cache is not None
            else ExternalPayloadCache()
        )
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)
        self._cluster_info: dict[str, Any] | None = None
        self._cluster_info_lock = asyncio.Lock()
        self._runtime_external_payload_transport_cache: (
            _RuntimeExternalPayloadTransport | None
        ) = None
        self._runtime_external_payload_transport_resolved = False

    async def aclose(self) -> None:
        """Close the underlying ``httpx`` connection pool.

        Equivalent to exiting the async-context-manager form of the client.
        Safe to call multiple times.
        """
        await self._http.aclose()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    def _headers(self, *, worker: bool = False) -> dict[str, str]:
        return self._headers_with_token(self._auth_token(worker=worker), worker=worker)

    def _headers_with_token(self, token: str | None, *, worker: bool) -> dict[str, str]:
        h: dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        h["X-Namespace"] = self.namespace
        if worker:
            h["X-Durable-Workflow-Protocol-Version"] = _protocol_version_from_env(
                "DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION",
                PROTOCOL_VERSION,
            )
        else:
            h["X-Durable-Workflow-Control-Plane-Version"] = _protocol_version_from_env(
                "DURABLE_WORKFLOW_CONTROL_PLANE_VERSION",
                CONTROL_PLANE_VERSION,
            )
        return h

    def _discovery_headers(self) -> dict[str, str]:
        """Select one credential accepted by the cluster discovery route."""
        if self.worker_token:
            return self._headers_with_token(self.worker_token, worker=True)
        if self.control_token:
            return self._headers_with_token(self.control_token, worker=False)
        return self._headers_with_token(self.token, worker=False)

    def _auth_token(self, *, worker: bool = False) -> str | None:
        if worker:
            token = self.worker_token or self.token
            if token:
                return token
            if self.control_token:
                raise ValueError(
                    "worker-plane requests require worker_token or the shared token; "
                    "control_token cannot authorize worker operations"
                )
            return None

        token = self.control_token or self.token
        if token:
            return token
        if self.worker_token:
            raise ValueError(
                "control-plane requests require control_token or the shared token; "
                "worker_token cannot authorize control operations"
            )
        return None

    def _payload_context(
        self,
        *,
        kind: str,
        workflow_id: str | None = None,
        workflow_type: str | None = None,
        run_id: str | None = None,
        activity_name: str | None = None,
        signal_name: str | None = None,
        update_name: str | None = None,
        query_name: str | None = None,
        schedule_id: str | None = None,
        task_queue: str | None = None,
    ) -> serializer.PayloadSizeWarningContext:
        return serializer.PayloadSizeWarningContext(
            kind=kind,
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            run_id=run_id,
            activity_name=activity_name,
            signal_name=signal_name,
            update_name=update_name,
            query_name=query_name,
            schedule_id=schedule_id,
            task_queue=task_queue,
            namespace=self.namespace,
        )

    def _payload_envelope(
        self,
        value: Any,
        *,
        kind: str,
        codec: str = serializer.AVRO_CODEC,
        workflow_id: str | None = None,
        workflow_type: str | None = None,
        run_id: str | None = None,
        activity_name: str | None = None,
        signal_name: str | None = None,
        update_name: str | None = None,
        query_name: str | None = None,
        schedule_id: str | None = None,
        task_queue: str | None = None,
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
    ) -> dict[str, Any]:
        warning_context = self._payload_context(
            kind=kind,
            workflow_id=workflow_id,
            workflow_type=workflow_type,
            run_id=run_id,
            activity_name=activity_name,
            signal_name=signal_name,
            update_name=update_name,
            query_name=query_name,
            schedule_id=schedule_id,
            task_queue=task_queue,
        )
        if external_storage_threshold_bytes is not None and external_storage_threshold_bytes < 1:
            raise ValueError("external_storage_threshold_bytes must be at least 1 when provided")
        payload_storage = external_storage if external_storage is not None else self.external_storage
        payload_storage_threshold_bytes = (
            external_storage_threshold_bytes
            if external_storage_threshold_bytes is not None
            else self.external_storage_threshold_bytes
        )
        if payload_storage is not None and payload_storage_threshold_bytes is not None:
            return serializer.external_storage_envelope(
                value,
                external_storage=payload_storage,
                threshold_bytes=payload_storage_threshold_bytes,
                codec=codec,
                size_warning=self.payload_size_warning_config,
                warning_context=warning_context,
            )
        return serializer.envelope(
            value,
            codec=codec,
            size_warning=self.payload_size_warning_config,
            warning_context=warning_context,
        )

    def _warn_json_payload_size(
        self,
        value: Any,
        *,
        kind: str,
        workflow_id: str | None = None,
        schedule_id: str | None = None,
        task_queue: str | None = None,
    ) -> None:
        serializer.warn_if_json_payload_near_limit(
            value,
            size_warning=self.payload_size_warning_config,
            warning_context=self._payload_context(
                kind=kind,
                workflow_id=workflow_id,
                schedule_id=schedule_id,
                task_queue=task_queue,
            ),
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        worker: bool = False,
        discovery: bool = False,
        json: Any = None,
        timeout: float | None = None,
        context: str = "",
    ) -> Any:
        if worker and discovery:
            raise ValueError("a request cannot be both worker-plane and cluster discovery")
        if discovery and path != "/cluster/info":
            raise ValueError("cluster discovery credentials can only authorize /cluster/info")

        request_json = json
        if (
            not discovery
            and json is not None
            and _contains_direct_external_storage_envelope(json)
        ):
            await self._require_direct_external_storage_support()
        if (
            not discovery
            and json is not None
            and _contains_inline_payload_envelope(json)
        ):
            transport = await self._runtime_external_payload_transport()
            if transport is not None:
                request_json = await self._externalize_runtime_payloads(
                    json,
                    worker=worker,
                    transport=transport,
                    uploaded={},
                )

        start = time.perf_counter()
        route = _route_for_metrics(path)
        discovery_uses_worker_token = discovery and bool(self.worker_token)
        plane = "worker" if worker or discovery_uses_worker_token else "control"
        status_code = "none"
        outcome = "pending"

        async def _do_request() -> httpx.Response:
            resp = await self._http.request(
                method,
                f"/api{path}",
                headers=self._discovery_headers() if discovery else self._headers(worker=worker),
                json=request_json,
                timeout=timeout,
            )
            # Raise HTTPStatusError for 4xx/5xx so retry policy can catch it
            resp.raise_for_status()
            return resp

        try:
            try:
                resp = await self.retry_policy.execute(_do_request)
            except httpx.HTTPStatusError as exc:
                status_code = str(exc.response.status_code)
                outcome = "http_error"
                # Convert to our custom exception types
                try:
                    body = exc.response.json()
                except ValueError:
                    body = exc.response.text
                _raise_for_status(exc.response.status_code, body, context=context)
                raise  # unreachable, but keeps type checker happy

            status_code = str(resp.status_code)
            if resp.status_code == 204 or not resp.content:
                outcome = "ok"
                return None
            result = resp.json()
            if not discovery and _contains_runtime_payload_envelope(result):
                transport = await self._runtime_external_payload_transport()
                if transport is None:
                    raise ExternalPayloadUnsupported(
                        "runtime returned an external payload reference without advertising "
                        "the authenticated namespace transport"
                    )
                result = await self._resolve_runtime_payloads(
                    result,
                    worker=worker,
                    transport=transport,
                )
            result = self._hydrate_result_envelopes(result)
            outcome = "ok"
            return result
        except Exception as exc:
            if outcome == "pending":
                outcome = type(exc).__name__
            raise
        finally:
            tags = {
                "method": method.upper(),
                "route": route,
                "plane": plane,
                "status_code": status_code,
                "outcome": outcome,
            }
            self.metrics.increment(CLIENT_REQUESTS, tags=tags)
            self.metrics.record(CLIENT_REQUEST_DURATION_SECONDS, time.perf_counter() - start, tags=tags)

    async def _runtime_external_payload_transport(
        self,
    ) -> _RuntimeExternalPayloadTransport | None:
        if self._runtime_external_payload_transport_resolved:
            return self._runtime_external_payload_transport_cache

        info = await self._runtime_discovery(
            operation="Client external payload transport",
            required_path=_RUNTIME_EXTERNAL_PAYLOAD_DISCOVERY_PATH,
        )
        namespace = info.get("namespace")
        policy = (
            namespace.get("external_payload_storage")
            if isinstance(namespace, dict)
            else None
        )
        capabilities = info.get("worker_protocol")
        capabilities = (
            capabilities.get("server_capabilities")
            if isinstance(capabilities, dict)
            else None
        )
        advertised = (
            capabilities.get("runtime_external_payload_transport") is True
            if isinstance(capabilities, dict)
            else False
        )

        if not isinstance(policy, dict):
            self._runtime_external_payload_transport_resolved = True
            return None
        manifest = policy.get("transport")
        if not advertised and not (
            isinstance(manifest, dict)
            and manifest.get("schema") == _RUNTIME_EXTERNAL_PAYLOAD_TRANSPORT_SCHEMA
        ):
            self._runtime_external_payload_transport_resolved = True
            return None
        if not isinstance(manifest, dict):
            raise ExternalPayloadUnsupported(
                "runtime external payload capability is missing its transport manifest"
            )
        if manifest.get("schema") != _RUNTIME_EXTERNAL_PAYLOAD_TRANSPORT_SCHEMA:
            raise ExternalPayloadUnsupported(
                "runtime external payload transport schema is unsupported"
            )
        if manifest.get("version") != 1:
            raise ExternalPayloadUnsupported(
                "runtime external payload transport version is unsupported"
            )
        if manifest.get("reference_schema") != RUNTIME_EXTERNAL_PAYLOAD_REFERENCE_SCHEMA:
            raise ExternalPayloadUnsupported(
                "runtime external payload reference schema is unsupported"
            )
        if manifest.get("mode") != "authenticated_namespace_runtime":
            raise ExternalPayloadUnsupported(
                "runtime external payload transport mode is unsupported"
            )

        upload = manifest.get("upload")
        fetch = manifest.get("fetch")
        if not (
            isinstance(upload, dict)
            and upload.get("method") == "POST"
            and upload.get("path") == f"/api{_RUNTIME_EXTERNAL_PAYLOAD_UPLOAD_PATH}"
            and isinstance(fetch, dict)
            and fetch.get("method") == "GET"
            and fetch.get("path_template")
            == f"/api{_RUNTIME_EXTERNAL_PAYLOAD_FETCH_PATH_TEMPLATE}"
        ):
            raise ExternalPayloadUnsupported(
                "runtime external payload transport paths are unsupported"
            )

        threshold_bytes = policy.get("threshold_bytes")
        limits = manifest.get("limits")
        max_payload_bytes = (
            limits.get("max_payload_bytes") if isinstance(limits, dict) else None
        )
        request_timeout_seconds = (
            limits.get("request_timeout_seconds") if isinstance(limits, dict) else None
        )
        if type(threshold_bytes) is not int or threshold_bytes < 1:
            raise ExternalPayloadUnsupported(
                "runtime external payload threshold_bytes must be a positive integer"
            )
        if type(max_payload_bytes) is not int or max_payload_bytes < threshold_bytes:
            raise ExternalPayloadUnsupported(
                "runtime external payload max_payload_bytes must be at least threshold_bytes"
            )
        if (
            isinstance(request_timeout_seconds, bool)
            or not isinstance(request_timeout_seconds, int | float)
            or request_timeout_seconds <= 0
        ):
            raise ExternalPayloadUnsupported(
                "runtime external payload request timeout must be positive"
            )

        status = policy.get("status")
        transport = _RuntimeExternalPayloadTransport(
            threshold_bytes=threshold_bytes,
            max_payload_bytes=max_payload_bytes,
            request_timeout_seconds=float(request_timeout_seconds),
            status=status if isinstance(status, str) else "unknown",
        )
        self._runtime_external_payload_transport_cache = transport
        self._runtime_external_payload_transport_resolved = True
        return transport

    async def _require_direct_external_storage_support(self) -> None:
        info = await self._runtime_discovery(
            operation="Client direct external storage",
            required_path=(
                "namespace.external_payload_storage.direct_provider_adapters"
            ),
        )
        worker_protocol = info.get("worker_protocol")
        capabilities = (
            worker_protocol.get("server_capabilities")
            if isinstance(worker_protocol, dict)
            else None
        )
        if not (
            isinstance(capabilities, dict)
            and capabilities.get("runtime_external_payload_transport") is True
        ):
            # Pre-runtime-transport self-hosted servers negotiated direct
            # references through their legacy namespace storage policy.
            return

        namespace = info.get("namespace")
        policy = (
            namespace.get("external_payload_storage")
            if isinstance(namespace, dict)
            else None
        )
        direct = (
            policy.get("direct_provider_adapters")
            if isinstance(policy, dict)
            else None
        )
        if not isinstance(direct, dict) and isinstance(policy, dict):
            manifest = policy.get("transport")
            direct = (
                manifest.get("direct_provider_adapters")
                if isinstance(manifest, dict)
                else None
            )
        if not (
            isinstance(direct, dict)
            and direct.get("capability_negotiated") is True
            and direct.get("enabled") is True
        ):
            raise RuntimeCapabilityUnsupported(
                "Client direct external storage",
                "direct_provider_adapters",
                "the namespace runtime does not accept direct provider references; "
                "use its runtime-mediated external payload transport",
            )

    async def _externalize_runtime_payloads(
        self,
        value: Any,
        *,
        worker: bool,
        transport: _RuntimeExternalPayloadTransport,
        uploaded: dict[tuple[str, str, int], RuntimeExternalPayloadReference],
    ) -> Any:
        if isinstance(value, dict):
            if (
                value.get("type") == "record_side_effect"
                and isinstance(value.get("result"), str)
            ):
                normalized_command = dict(value)
                externalized_result = await self._externalize_runtime_payloads(
                    {"codec": serializer.AVRO_CODEC, "blob": value["result"]},
                    worker=worker,
                    transport=transport,
                    uploaded=uploaded,
                )
                if "external_payload" in externalized_result:
                    normalized_command["result"] = externalized_result
                value = normalized_command

            if set(value) == {"codec", "blob"} and isinstance(value.get("blob"), str):
                codec = value.get("codec")
                if codec != serializer.AVRO_CODEC:
                    return dict(value)
                data = value["blob"].encode("utf-8")
                if len(data) <= transport.threshold_bytes:
                    return dict(value)
                if transport.status != "available":
                    raise ExternalPayloadUnavailable(
                        "runtime external payload storage is not available for this namespace"
                    )
                if len(data) > transport.max_payload_bytes:
                    raise ExternalPayloadOversized(
                        "encoded payload exceeds the runtime external payload limit"
                    )
                sha256 = hashlib.sha256(data).hexdigest()
                identity = (codec, sha256, len(data))
                reference = uploaded.get(identity)
                if reference is None:
                    reference = await self._upload_runtime_payload(
                        data,
                        codec=codec,
                        sha256=sha256,
                        worker=worker,
                        transport=transport,
                    )
                    uploaded[identity] = reference
                return {"codec": codec, "external_payload": reference.to_dict()}

            externalized = {
                key: await self._externalize_runtime_payloads(
                    item,
                    worker=worker,
                    transport=transport,
                    uploaded=uploaded,
                )
                for key, item in value.items()
            }
            result_envelope = externalized.get("result_envelope")
            if (
                "result" in externalized
                and isinstance(result_envelope, dict)
                and "external_payload" in result_envelope
            ):
                # Query completion retains the legacy decoded result field for
                # inline compatibility. Once its canonical envelope is
                # externalized, sending that duplicate would defeat offload.
                externalized["result"] = None
            return externalized
        if isinstance(value, list):
            return [
                await self._externalize_runtime_payloads(
                    item,
                    worker=worker,
                    transport=transport,
                    uploaded=uploaded,
                )
                for item in value
            ]
        return value

    async def _upload_runtime_payload(
        self,
        data: bytes,
        *,
        codec: str,
        sha256: str,
        worker: bool,
        transport: _RuntimeExternalPayloadTransport,
    ) -> RuntimeExternalPayloadReference:
        headers = self._headers(worker=worker)
        headers.update({
            "Accept": "application/json",
            "Content-Type": "application/octet-stream",
            "X-Durable-Workflow-Payload-Codec": codec,
            "X-Durable-Workflow-Payload-Size": str(len(data)),
            "X-Durable-Workflow-Payload-SHA256": sha256,
        })

        async def _do_request() -> httpx.Response:
            response = await self._http.request(
                "POST",
                f"/api{_RUNTIME_EXTERNAL_PAYLOAD_UPLOAD_PATH}",
                headers=headers,
                content=data,
                timeout=transport.request_timeout_seconds,
            )
            response.raise_for_status()
            return response

        try:
            response = await self.retry_policy.execute(_do_request)
        except httpx.HTTPStatusError as exc:
            self._raise_runtime_payload_response(exc.response)
            raise
        except httpx.TransportError as exc:
            raise ExternalPayloadUnavailable(
                f"runtime external payload upload failed: {exc}"
            ) from exc

        try:
            body = response.json()
        except ValueError as exc:
            raise ExternalPayloadUnsupported(
                "runtime external payload upload returned malformed JSON"
            ) from exc
        if not isinstance(body, dict):
            raise ExternalPayloadUnsupported(
                "runtime external payload upload response must be an object"
            )
        if (
            body.get("schema") != _RUNTIME_EXTERNAL_PAYLOAD_UPLOAD_SCHEMA
            or body.get("transport_version") != 1
        ):
            raise ExternalPayloadUnsupported(
                "runtime external payload upload response schema is unsupported"
            )
        reference = RuntimeExternalPayloadReference.from_dict(body.get("reference"))
        if (
            reference.codec != codec
            or reference.size_bytes != len(data)
            or reference.sha256 != sha256
        ):
            raise ExternalPayloadIntegrityMismatch(
                "runtime external payload upload returned conflicting integrity metadata",
                reference_id=reference.reference_id,
            )
        self.external_storage_cache.put(reference, data)
        return reference

    async def _resolve_runtime_payloads(
        self,
        value: Any,
        *,
        worker: bool,
        transport: _RuntimeExternalPayloadTransport,
    ) -> Any:
        if isinstance(value, dict):
            if "external_payload" in value:
                if set(value) != {"codec", "external_payload"}:
                    raise ExternalPayloadUnsupported(
                        "runtime external payload envelope must contain exactly codec and external_payload"
                    )
                reference = RuntimeExternalPayloadReference.from_dict(
                    value.get("external_payload")
                )
                if value.get("codec") != reference.codec:
                    raise ExternalPayloadIntegrityMismatch(
                        "runtime external payload envelope codec does not match its reference",
                        reference_id=reference.reference_id,
                    )
                data = await self._fetch_runtime_payload(
                    reference,
                    worker=worker,
                    transport=transport,
                )
                try:
                    blob = data.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ExternalPayloadIntegrityMismatch(
                        "runtime external payload bytes are not a UTF-8 payload blob",
                        reference_id=reference.reference_id,
                    ) from exc
                return {"codec": reference.codec, "blob": blob}
            return {
                key: await self._resolve_runtime_payloads(
                    item,
                    worker=worker,
                    transport=transport,
                )
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [
                await self._resolve_runtime_payloads(
                    item,
                    worker=worker,
                    transport=transport,
                )
                for item in value
            ]
        return value

    async def _fetch_runtime_payload(
        self,
        reference: RuntimeExternalPayloadReference,
        *,
        worker: bool,
        transport: _RuntimeExternalPayloadTransport,
    ) -> bytes:
        if reference.size_bytes > transport.max_payload_bytes:
            raise ExternalPayloadOversized(
                "runtime external payload reference exceeds the advertised transport limit",
                reference_id=reference.reference_id,
            )
        cached = self.external_storage_cache.get(reference)
        if cached is not None:
            return cached

        headers = self._headers(worker=worker)
        headers.update({
            "Accept": "application/octet-stream",
            "X-Durable-Workflow-Payload-Codec": reference.codec,
            "X-Durable-Workflow-Payload-Size": str(reference.size_bytes),
            "X-Durable-Workflow-Payload-SHA256": reference.sha256,
        })
        path = _RUNTIME_EXTERNAL_PAYLOAD_FETCH_PATH_TEMPLATE.replace(
            "{referenceId}", quote(reference.reference_id, safe="")
        )

        async def _do_request() -> tuple[httpx.Headers, bytes]:
            async with self._http.stream(
                "GET",
                f"/api{path}",
                headers=headers,
                timeout=transport.request_timeout_seconds,
            ) as response:
                if response.status_code >= 400:
                    body = await self._read_bounded_response(
                        response,
                        _RUNTIME_EXTERNAL_PAYLOAD_ERROR_BODY_LIMIT,
                    )
                    error_response = httpx.Response(
                        response.status_code,
                        headers=response.headers,
                        content=body,
                        request=response.request,
                    )
                    error_response.raise_for_status()

                data = await self._read_bounded_response(
                    response,
                    min(reference.size_bytes, transport.max_payload_bytes),
                )
                return response.headers, data

        try:
            response_headers, data = await self.retry_policy.execute(_do_request)
        except httpx.HTTPStatusError as exc:
            self._raise_runtime_payload_response(exc.response)
            raise
        except httpx.TransportError as exc:
            raise ExternalPayloadUnavailable(
                f"runtime external payload fetch failed: {exc}",
                reference_id=reference.reference_id,
            ) from exc

        expected_headers = {
            "X-Durable-Workflow-Payload-Codec": reference.codec,
            "X-Durable-Workflow-Payload-Size": str(reference.size_bytes),
            "X-Durable-Workflow-Payload-SHA256": reference.sha256,
        }
        if any(response_headers.get(key) != expected for key, expected in expected_headers.items()):
            raise ExternalPayloadIntegrityMismatch(
                "runtime external payload response metadata does not match its reference",
                reference_id=reference.reference_id,
            )
        if len(data) != reference.size_bytes:
            raise ExternalPayloadIntegrityMismatch(
                "runtime external payload size does not match its reference",
                reference_id=reference.reference_id,
            )
        if hashlib.sha256(data).hexdigest() != reference.sha256:
            raise ExternalPayloadIntegrityMismatch(
                "runtime external payload hash does not match its reference",
                reference_id=reference.reference_id,
            )
        self.external_storage_cache.put(reference, data)
        return data

    @staticmethod
    async def _read_bounded_response(response: httpx.Response, limit: int) -> bytes:
        chunks: list[bytes] = []
        size = 0
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > limit:
                raise ExternalPayloadIntegrityMismatch(
                    "runtime external payload response exceeded its declared bound"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    def _raise_runtime_payload_response(response: httpx.Response) -> None:
        try:
            body: object = response.json()
        except ValueError:
            body = response.text
        _raise_for_status(response.status_code, body, context="external_payload")

    def _hydrate_result_envelopes(self, value: Any) -> Any:
        if isinstance(value, dict):
            hydrated = {
                key: self._hydrate_result_envelopes(item)
                for key, item in value.items()
            }
            result_envelope = hydrated.get("result_envelope")
            if (
                hydrated.get("result") is None
                and isinstance(result_envelope, dict)
                and (
                    "blob" in result_envelope
                    or "external_storage" in result_envelope
                )
            ):
                hydrated["result"] = serializer.decode_envelope(
                    result_envelope,
                    external_storage=self.external_storage,
                    external_storage_cache=self.external_storage_cache,
                )
            return hydrated
        if isinstance(value, list):
            return [self._hydrate_result_envelopes(item) for item in value]
        return value

    async def _request_bridge_outcome(self, path: str, *, json: Any = None, context: str = "") -> dict[str, Any]:
        start = time.perf_counter()
        route = _route_for_metrics(path)
        status_code = "none"
        outcome = "pending"

        async def _do_request() -> httpx.Response:
            resp = await self._http.request(
                "POST",
                f"/api{path}",
                headers=self._headers(worker=False),
                json=json,
            )
            if resp.status_code != 422:
                resp.raise_for_status()
            return resp

        try:
            try:
                resp = await self.retry_policy.execute(_do_request)
            except httpx.HTTPStatusError as exc:
                status_code = str(exc.response.status_code)
                outcome = "http_error"
                try:
                    body = exc.response.json()
                except ValueError:
                    body = exc.response.text
                _raise_for_status(exc.response.status_code, body, context=context)
                raise

            status_code = str(resp.status_code)
            if not resp.content:
                raise ServerError(
                    resp.status_code,
                    {"reason": "invalid_bridge_outcome", "message": "expected JSON object, got empty response"},
                )
            data = resp.json()
            if not isinstance(data, dict):
                raise ServerError(
                    resp.status_code,
                    {
                        "reason": "invalid_bridge_outcome",
                        "message": f"expected JSON object, got {type(data).__name__}",
                    },
                )
            outcome = "bridge_rejected" if resp.status_code == 422 else "ok"
            return data
        except Exception as exc:
            if outcome == "pending":
                outcome = type(exc).__name__
            raise
        finally:
            tags = {
                "method": "POST",
                "route": route,
                "plane": "control",
                "status_code": status_code,
                "outcome": outcome,
            }
            self.metrics.increment(CLIENT_REQUESTS, tags=tags)
            self.metrics.record(CLIENT_REQUEST_DURATION_SECONDS, time.perf_counter() - start, tags=tags)

    async def get_cluster_info(self) -> dict[str, Any]:
        """Fetch server build identity, capabilities, and protocol manifests.

        Cluster discovery accepts a worker-scoped, control-scoped, or shared
        credential. Other client methods continue to require the credential
        for their own protocol plane.
        """
        result = await self._request("GET", "/cluster/info", discovery=True, context="get_cluster_info")
        if not isinstance(result, dict):
            raise ServerError(
                200,
                {"reason": "invalid_cluster_info", "message": f"expected JSON object, got {type(result).__name__}"},
            )
        self._cluster_info = result
        return result

    async def _runtime_discovery(
        self,
        *,
        operation: str,
        required_path: str,
    ) -> dict[str, Any]:
        if self._cluster_info is not None:
            return self._cluster_info

        async with self._cluster_info_lock:
            if self._cluster_info is not None:
                return self._cluster_info
            try:
                return await self.get_cluster_info()
            except Exception as error:
                raise RuntimeDiscoveryUnavailable(
                    operation,
                    required_path,
                    (
                        f"{operation} requires runtime discovery from GET /api/cluster/info, "
                        f"but discovery failed: {error}. Check discovery authorization and "
                        "Server availability before retrying."
                    ),
                    cause=error,
                ) from error

    async def _require_query_support(self) -> None:
        info = await self._runtime_discovery(
            operation="Client.query_workflow",
            required_path=_QUERY_TASKS_DISCOVERY_PATH,
        )
        worker_protocol = info.get("worker_protocol")
        capabilities = (
            worker_protocol.get("server_capabilities")
            if isinstance(worker_protocol, dict)
            else None
        )
        query_tasks = (
            capabilities.get("query_tasks")
            if isinstance(capabilities, dict)
            else None
        )
        if query_tasks is False:
            raise RuntimeCapabilityUnsupported(
                "Client.query_workflow",
                _QUERY_TASKS_DISCOVERY_PATH,
                (
                    "Client.query_workflow is unsupported by this runtime because "
                    f"{_QUERY_TASKS_DISCOVERY_PATH} is false. Upgrade Server or use "
                    "a runtime that advertises worker-routed query tasks."
                ),
            )
        if query_tasks is not True:
            raise RuntimeDiscoveryUnavailable(
                "Client.query_workflow",
                _QUERY_TASKS_DISCOVERY_PATH,
                (
                    "Client.query_workflow is unavailable because GET /api/cluster/info "
                    f"did not advertise {_QUERY_TASKS_DISCOVERY_PATH}=true. Check Server "
                    "compatibility and discovery authorization before retrying."
                ),
            )

    async def _require_update_wait_stage(self, wait_for: str) -> None:
        info = await self._runtime_discovery(
            operation="Client.update_workflow",
            required_path=_UPDATE_WAIT_STAGES_DISCOVERY_PATH,
        )
        control_plane = info.get("control_plane")
        request_contract = (
            control_plane.get("request_contract")
            if isinstance(control_plane, dict)
            else None
        )
        operations = (
            request_contract.get("operations")
            if isinstance(request_contract, dict)
            else None
        )
        update = operations.get("update") if isinstance(operations, dict) else None
        fields = update.get("fields") if isinstance(update, dict) else None
        wait_for_contract = (
            fields.get("wait_for") if isinstance(fields, dict) else None
        )
        raw_stages = (
            wait_for_contract.get("canonical_values")
            if isinstance(wait_for_contract, dict)
            else None
        )
        if (
            not isinstance(raw_stages, list)
            or not raw_stages
            or not all(
                isinstance(stage, str) and stage
                for stage in raw_stages
            )
        ):
            raise RuntimeDiscoveryUnavailable(
                "Client.update_workflow",
                _UPDATE_WAIT_STAGES_DISCOVERY_PATH,
                (
                    "Client.update_workflow is unavailable because GET /api/cluster/info "
                    "did not publish its supported update wait stages. Check Server "
                    "compatibility and discovery authorization before retrying."
                ),
            )

        supported_stages = tuple(raw_stages)
        if wait_for not in supported_stages:
            supported = ", ".join(repr(stage) for stage in supported_stages)
            raise RuntimeCapabilityUnsupported(
                "Client.update_workflow",
                f"update.wait_for={wait_for}",
                (
                    f"Client.update_workflow wait stage {wait_for!r} is unsupported by "
                    f"this runtime. Discovered stages: {supported}. Select a discovered "
                    "stage or upgrade Server."
                ),
                supported_values=supported_stages,
            )

    def get_workflow_handle(
        self, workflow_id: str, *, run_id: str | None = None, workflow_type: str = ""
    ) -> WorkflowHandle:
        """Return a :class:`WorkflowHandle` bound to an existing workflow instance.

        Does not round-trip to the server. Pass ``run_id`` to pin the handle to
        a specific run, otherwise the handle resolves to whichever run is
        current at the time each method is called. ``workflow_type`` is
        optional and used only in error messages.
        """
        return WorkflowHandle(self, workflow_id=workflow_id, run_id=run_id, workflow_type=workflow_type)

    async def execute_nexus_operation(
        self,
        endpoint_name: str,
        service_name: str,
        operation_name: str,
        arguments: Sequence[Any] | None = None,
        *,
        mode: str | None = None,
        wait_for: str | None = None,
        wait_timeout_seconds: int | None = None,
        idempotency_key: str | None = None,
        payload_codec: str | None = None,
        caller_namespace: str | None = None,
        caller_workflow_instance_id: str | None = None,
        caller_workflow_run_id: str | None = None,
        service_sdk_language: str | None = None,
        artifact_tuple: Mapping[str, Any] | None = None,
        published_artifact_worker_execution: bool | None = None,
        target_workflow_instance_id: str | None = None,
        target_workflow_run_id: str | None = None,
        connection: str | None = None,
        queue: str | None = None,
        business_key: str | None = None,
        labels: Mapping[str, Any] | None = None,
        memo: Mapping[str, Any] | None = None,
        search_attributes: Mapping[str, Any] | None = None,
        duplicate_start_policy: str | None = None,
        raise_on_failure: bool = True,
    ) -> NexusOperationResult:
        """Execute a Nexus service operation through the service-catalog API.

        Workflow code should normally yield
        :meth:`durable_workflow.workflow.WorkflowContext.call_nexus_service`;
        the worker uses this client method to make the single durable
        execute request and records the returned service-call surface into
        workflow history.
        """
        request_body = nexus_request_payload(
            arguments=list(arguments) if arguments is not None else [],
            payload_codec=payload_codec,
            mode=mode,
            wait_for=wait_for,
            wait_timeout_seconds=wait_timeout_seconds,
            idempotency_key=idempotency_key,
            caller_namespace=caller_namespace or self.namespace,
            caller_workflow_instance_id=caller_workflow_instance_id,
            caller_workflow_run_id=caller_workflow_run_id,
            target_workflow_instance_id=target_workflow_instance_id,
            target_workflow_run_id=target_workflow_run_id,
            connection=connection,
            queue=queue,
            business_key=business_key,
            labels=labels,
            memo=memo,
            search_attributes=search_attributes,
            duplicate_start_policy=duplicate_start_policy,
        )
        path = (
            f"/service-endpoints/{quote(endpoint_name, safe='')}"
            f"/services/{quote(service_name, safe='')}"
            f"/operations/{quote(operation_name, safe='')}/execute"
        )
        context = f"{endpoint_name}/{service_name}/{operation_name}"

        try:
            data = await self._request("POST", path, json=request_body, context=context)
        except ServerError as exc:
            if exc.status != 409 or not isinstance(exc.body, dict):
                raise
            result = NexusOperationResult.from_response(
                exc.body,
                endpoint_name=endpoint_name,
                service_name=service_name,
                operation_name=operation_name,
                request_payload=request_body,
                service_sdk_language=service_sdk_language,
                artifact_tuple=artifact_tuple,
                published_artifact_worker_execution=published_artifact_worker_execution,
            )
            if raise_on_failure:
                raise result.to_failure() from exc
            return result

        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_nexus_operation_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )

        result = NexusOperationResult.from_response(
            data,
            endpoint_name=endpoint_name,
            service_name=service_name,
            operation_name=operation_name,
            request_payload=request_body,
            service_sdk_language=service_sdk_language,
            artifact_tuple=artifact_tuple,
            published_artifact_worker_execution=published_artifact_worker_execution,
        )
        if raise_on_failure and result.is_failure:
            raise result.to_failure()
        return result

    # ── Health ─────────────────────────────────────────────────────────
    async def health(self) -> dict[str, Any]:
        """Call the server's ``/api/health`` endpoint and return the JSON response."""
        result = await self._request("GET", "/health")
        if not isinstance(result, dict):
            raise ServerError(
                200,
                {"reason": "invalid_health_response", "message": f"expected JSON object, got {type(result).__name__}"},
            )
        return result

    # ── Namespaces ────────────────────────────────────────────────────
    async def list_namespaces(self) -> NamespaceList:
        """List namespaces visible to the current control-plane identity."""
        data = await self._request("GET", "/namespaces")
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_namespace_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        items = data.get("namespaces", [])
        return NamespaceList(
            namespaces=[
                NamespaceDescription.from_dict(item)
                for item in items
                if isinstance(item, dict)
            ],
        )

    async def describe_namespace(self, name: str) -> NamespaceDescription:
        """Return configuration and status for one namespace."""
        data = await self._request("GET", f"/namespaces/{quote(name, safe='')}", context=name)
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_namespace_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return NamespaceDescription.from_dict(data)

    async def create_namespace(
        self,
        name: str,
        *,
        description: str | None = None,
        retention_days: int = 30,
    ) -> NamespaceDescription:
        """Create a workflow namespace and return the server representation."""
        data = await self._request(
            "POST",
            "/namespaces",
            json={
                "name": name,
                "description": description,
                "retention_days": retention_days,
            },
            context=name,
        )
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_namespace_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return NamespaceDescription.from_dict(data)

    async def update_namespace(
        self,
        name: str,
        *,
        description: str | None = None,
        retention_days: int | None = None,
    ) -> NamespaceDescription:
        """Update namespace metadata. Only provided fields are sent."""
        body: dict[str, Any] = {}
        if description is not None:
            body["description"] = description
        if retention_days is not None:
            body["retention_days"] = retention_days

        data = await self._request("PUT", f"/namespaces/{quote(name, safe='')}", json=body, context=name)
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_namespace_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return NamespaceDescription.from_dict(data)

    async def delete_namespace(self, name: str) -> NamespaceDescription:
        """Delete a namespace through the server lifecycle surface."""
        data = await self._request("DELETE", f"/namespaces/{quote(name, safe='')}", context=name)
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_namespace_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return NamespaceDescription.from_dict(data)

    async def set_namespace_external_storage(
        self,
        name: str | None = None,
        *,
        driver: str,
        enabled: bool = True,
        threshold_bytes: int | None = None,
        config: dict[str, Any] | None = None,
        namespace: str | None = None,
    ) -> NamespaceDescription:
        """Configure external payload storage for a namespace.

        The first positional argument is the namespace ``name``, matching
        :meth:`describe_namespace`, :meth:`create_namespace`, and
        :meth:`update_namespace`. The ``namespace=`` keyword is accepted as a
        deprecated alias from the 0.4.0 release and emits a
        :class:`DeprecationWarning`; it will be removed in a future release.
        """
        name = _resolve_namespace_name(
            name, namespace, method="set_namespace_external_storage"
        )
        body: dict[str, Any] = {
            "driver": driver,
            "enabled": enabled,
        }
        if threshold_bytes is not None:
            body["threshold_bytes"] = threshold_bytes
        if config is not None:
            body["config"] = config

        data = await self._request(
            "PUT",
            f"/namespaces/{quote(name, safe='')}/external-storage",
            json=body,
            context=name,
        )
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_namespace_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return NamespaceDescription.from_dict(data)

    async def test_external_storage(
        self,
        *,
        driver: str | None = None,
        small_payload_bytes: int | None = None,
        large_payload_bytes: int | None = None,
    ) -> StorageTestResult:
        """Ask the server to verify its configured external payload storage."""
        body: dict[str, Any] = {}
        if small_payload_bytes is not None:
            body["small_payload_bytes"] = small_payload_bytes
        if large_payload_bytes is not None:
            body["large_payload_bytes"] = large_payload_bytes
        if driver is not None:
            body["driver"] = driver

        data = await self._request("POST", "/storage/test", json=body, context=driver or "storage")
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_storage_test_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return StorageTestResult.from_dict(data)

    # ── System maintenance ────────────────────────────────────────────
    async def repair_status(self) -> dict[str, Any]:
        """Return the current task repair policy and candidate snapshot.

        Mirrors ``dw system:repair-status``. Operator surface; the caller
        must be authenticated with admin scope.
        """
        data = await self._request("GET", "/system/repair", context="repair_status")
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_repair_status_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    async def repair_pass(
        self,
        *,
        run_ids: list[str] | None = None,
        instance_id: str | None = None,
    ) -> dict[str, Any]:
        """Run one task repair sweep on the server.

        Mirrors ``dw system:repair-pass``. Without filters the server runs
        a full-scope pass; pass ``run_ids`` or ``instance_id`` to narrow
        the sweep. Operator surface; requires admin scope.
        """
        body: dict[str, Any] = {}
        if run_ids:
            body["run_ids"] = list(run_ids)
        if instance_id is not None:
            body["instance_id"] = instance_id

        data = await self._request("POST", "/system/repair/pass", json=body, context="repair_pass")
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_repair_pass_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    async def retention_status(self) -> dict[str, Any]:
        """Return history-retention diagnostics for the current namespace.

        Mirrors ``dw system:retention-status``. The response reports the
        namespace retention window, the cutoff, and the run IDs currently
        eligible for pruning up to the server's scan limit. Operator
        surface; requires admin scope.
        """
        data = await self._request("GET", "/system/retention", context="retention_status")
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_retention_status_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    async def retention_pass(
        self,
        *,
        run_ids: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Run one history-retention enforcement sweep on the server.

        Mirrors ``dw system:retention-pass``. Without filters the server
        prunes expired terminal runs from the namespace up to its scan
        limit; pass ``run_ids`` to narrow the sweep or ``limit`` to bound
        how many runs a single pass processes. Operator surface; requires
        admin scope.
        """
        body: dict[str, Any] = {}
        if run_ids:
            body["run_ids"] = list(run_ids)
        if limit is not None:
            body["limit"] = limit

        data = await self._request("POST", "/system/retention/pass", json=body, context="retention_pass")
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_retention_pass_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    async def activity_timeout_status(self) -> dict[str, Any]:
        """Return activity-timeout diagnostics for the current namespace.

        Mirrors ``dw system:activity-timeout-status``. The response lists
        activity execution IDs that have passed their start-to-close or
        schedule-to-close deadline and are eligible for forced timeout.
        Operator surface; requires admin scope.
        """
        data = await self._request(
            "GET", "/system/activity-timeouts", context="activity_timeout_status"
        )
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_activity_timeout_status_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    async def activity_timeout_pass(
        self,
        *,
        execution_ids: list[str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Run one activity-timeout enforcement sweep on the server.

        Mirrors ``dw system:activity-timeout-pass``. Without filters the
        server enforces timeouts for any expired activity executions up
        to its scan limit; pass ``execution_ids`` to narrow the sweep or
        ``limit`` to bound how many executions a single pass processes.
        Operator surface; requires admin scope.
        """
        body: dict[str, Any] = {}
        if execution_ids:
            body["execution_ids"] = list(execution_ids)
        if limit is not None:
            body["limit"] = limit

        data = await self._request(
            "POST",
            "/system/activity-timeouts/pass",
            json=body,
            context="activity_timeout_pass",
        )
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_activity_timeout_pass_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    # ── Task queues ────────────────────────────────────────────────────
    async def list_task_queues(self) -> TaskQueueList:
        """List task queues with server-side admission status.

        Admission data describes server budgets and observed backlog. Worker
        constructor limits remain local semaphores that are advertised during
        registration.
        """
        data = await self._request("GET", "/task-queues")
        items = data.get("task_queues", []) if isinstance(data, dict) else []
        return TaskQueueList(
            namespace=data.get("namespace") if isinstance(data, dict) else None,
            task_queues=[
                TaskQueueDescription.from_dict(item)
                for item in items
                if isinstance(item, dict)
            ],
        )

    async def describe_task_queue(self, name: str) -> TaskQueueDescription:
        """Return backlog, poller, lease, and admission detail for ``name``."""
        data = await self._request("GET", f"/task-queues/{quote(name, safe='')}", context=name)
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_task_queue_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return TaskQueueDescription.from_dict(data)

    async def list_task_queue_build_ids(self, task_queue: str) -> TaskQueueBuildIdRollout:
        """Return the build-id rollout snapshot for ``task_queue``.

        Use this before draining or removing an older build to confirm which
        build cohorts can still claim work on the queue. Unversioned workers
        are grouped under a cohort whose ``build_id`` is ``None``.
        """
        data = await self._request(
            "GET",
            f"/task-queues/{quote(task_queue, safe='')}/build-ids",
            context=task_queue,
        )
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_task_queue_build_ids_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return TaskQueueBuildIdRollout.from_dict(data)

    async def drain_task_queue_build_id(
        self,
        task_queue: str,
        build_id: str | None,
    ) -> TaskQueueBuildIdRolloutState:
        """Mark a build-id cohort as draining so it stops claiming new tasks.

        Workers registered under ``build_id`` keep running their in-flight
        work but are blocked from claiming fresh tasks, and future workers
        that heartbeat under the same ``build_id`` land as draining too.
        Pass ``None`` to drain the unversioned cohort (workers registered
        without a build identifier). Idempotent: repeated drains do not
        shift the recorded ``drained_at`` timestamp.
        """
        return await self._mutate_task_queue_build_id_rollout(
            task_queue,
            build_id,
            action="drain",
        )

    async def promote_task_queue_build_id(
        self,
        task_queue: str,
        build_id: str | None,
    ) -> TaskQueueBuildIdRolloutState:
        """Select a build-id cohort for fresh workflow starts on a task queue.

        New workflow starts pin to ``build_id`` after promotion. Existing
        workflow runs keep their stamped compatibility marker and continue
        routing only to compatible workers. Pass ``None`` to promote the
        unversioned cohort.
        """
        return await self._mutate_task_queue_build_id_rollout(
            task_queue,
            build_id,
            action="promote",
        )

    async def resume_task_queue_build_id(
        self,
        task_queue: str,
        build_id: str | None,
    ) -> TaskQueueBuildIdRolloutState:
        """Revert a previous drain so a build-id cohort can claim work again.

        Resuming clears both ``drain_intent`` and ``drained_at`` for the
        cohort and flips any still-running workers back to ``active``.
        Pass ``None`` to resume the unversioned cohort. Idempotent:
        resuming an already-active cohort is a no-op.
        """
        return await self._mutate_task_queue_build_id_rollout(
            task_queue,
            build_id,
            action="resume",
        )

    async def _mutate_task_queue_build_id_rollout(
        self,
        task_queue: str,
        build_id: str | None,
        *,
        action: str,
    ) -> TaskQueueBuildIdRolloutState:
        data = await self._request(
            "POST",
            f"/task-queues/{quote(task_queue, safe='')}/build-ids/{action}",
            json={"build_id": build_id},
            context=task_queue,
        )
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": f"invalid_task_queue_build_id_{action}_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return TaskQueueBuildIdRolloutState.from_dict(data)

    # ── Search attributes ─────────────────────────────────────────────
    async def list_search_attributes(self) -> SearchAttributeList:
        """List system and custom search attribute definitions for this namespace."""
        data = await self._request("GET", "/search-attributes")
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_search_attribute_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return SearchAttributeList.from_dict(data)

    async def create_search_attribute(self, name: str, attribute_type: str) -> dict[str, Any]:
        """Register a custom search attribute and return the server response."""
        data = await self._request(
            "POST",
            "/search-attributes",
            json={"name": name, "type": attribute_type},
            context=name,
        )
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_search_attribute_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    async def delete_search_attribute(self, name: str) -> dict[str, Any]:
        """Remove a custom search attribute and return the server response."""
        data = await self._request("DELETE", f"/search-attributes/{quote(name, safe='')}", context=name)
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_search_attribute_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    # ── Workers ───────────────────────────────────────────────────────
    async def list_workers(
        self,
        *,
        task_queue: str | None = None,
        status: str | None = None,
    ) -> WorkerList:
        """List registered workers in the current namespace."""
        params: dict[str, str] = {}
        if task_queue is not None:
            params["task_queue"] = task_queue
        if status is not None:
            params["status"] = status

        path = "/workers"
        if params:
            path = f"{path}?{urlencode(params)}"

        data = await self._request("GET", path)
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_worker_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        items = data.get("workers", [])

        return WorkerList(
            namespace=data.get("namespace"),
            workers=[
                WorkerDescription.from_dict(item)
                for item in items
                if isinstance(item, dict)
            ],
            stale_after_seconds=(
                int(data["stale_after_seconds"])
                if isinstance(data.get("stale_after_seconds"), int)
                else None
            ),
        )

    async def describe_worker(self, worker_id: str) -> WorkerDescription:
        """Return runtime, capacity, heartbeat, and type support for one worker."""
        data = await self._request("GET", f"/workers/{quote(worker_id, safe='')}", context=worker_id)
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_worker_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return WorkerDescription.from_dict(data, worker_id=worker_id)

    async def deregister_worker(self, worker_id: str) -> dict[str, Any]:
        """Remove a stale or retired worker from the server roster."""
        data = await self._request("DELETE", f"/workers/{quote(worker_id, safe='')}", context=worker_id)
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_worker_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    # ── Workflows ──────────────────────────────────────────────────────
    async def start_workflow(
        self,
        *,
        workflow_type: str,
        task_queue: str,
        workflow_id: str,
        input: list[Any] | None = None,
        execution_timeout_seconds: int = 3600,
        run_timeout_seconds: int = 600,
        duplicate_policy: str | None = None,
        memo: dict[str, Any] | None = None,
        search_attributes: dict[str, Any] | None = None,
        priority: int | None = None,
        fairness_key: str | None = None,
        fairness_weight: int | None = None,
        build_id: str | None = None,
        compatibility: str | None = None,
    ) -> WorkflowHandle:
        """Start a new workflow instance and return a handle bound to it.

        ``workflow_type`` is the language-neutral type key registered via
        :func:`durable_workflow.workflow.defn`. ``task_queue`` selects which
        worker pool picks up the workflow. ``workflow_id`` is the
        caller-supplied instance id — if it collides with an existing
        workflow, behavior depends on ``duplicate_policy``
        (``reject`` | ``allow`` | ``terminate_existing``).

        ``input`` is a list of positional arguments passed to the workflow's
        ``run`` method; the SDK encodes the list with the default payload
        codec (Avro). ``execution_timeout_seconds`` covers the entire workflow
        execution across all runs (including continue-as-new), while
        ``run_timeout_seconds`` applies to this single run only.

        ``memo`` and ``search_attributes`` attach operator-facing metadata to
        the instance; see the main docs site for the key/value rules.

        ``priority`` is an integer in the range ``0..9`` (lower numbers run
        first when workers on a shared task queue are saturated; default
        ``5``). ``fairness_key`` tags the workload class — typically a
        tenant id, team name, or workflow type — so dispatch on a shared
        task queue can be rebalanced across declared classes under
        contention; tasks without a key share one class. ``fairness_weight``
        (``1..1000``, default ``1``) lets a class take a proportionally
        larger share of dispatch slots versus other classes on the same
        queue.

        ``build_id`` pins the new run to a specific worker build when
        multiple worker versions share a task queue. ``compatibility`` is an
        alias accepted by the server for SDK-neutral callers; when both are
        provided, the server gives ``build_id`` precedence.
        """
        body: dict[str, Any] = {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "task_queue": task_queue,
            "input": self._payload_envelope(
                input if input is not None else [],
                kind="workflow_input",
                workflow_id=workflow_id,
                task_queue=task_queue,
            ),
            "execution_timeout_seconds": execution_timeout_seconds,
            "run_timeout_seconds": run_timeout_seconds,
        }
        if duplicate_policy is not None:
            body["duplicate_policy"] = duplicate_policy
        if memo is not None:
            body["memo"] = memo
        if search_attributes is not None:
            self._warn_json_payload_size(
                search_attributes,
                kind="search_attributes",
                workflow_id=workflow_id,
                task_queue=task_queue,
            )
            body["search_attributes"] = search_attributes
        if priority is not None:
            body["priority"] = priority
        if fairness_key is not None:
            body["fairness_key"] = fairness_key
        if fairness_weight is not None:
            body["fairness_weight"] = fairness_weight
        if build_id is not None:
            body["build_id"] = build_id
        if compatibility is not None:
            body["compatibility"] = compatibility
        data = await self._request("POST", "/workflows", json=body, context=workflow_id)
        return WorkflowHandle(
            self,
            workflow_id=data["workflow_id"],
            run_id=data.get("run_id"),
            workflow_type=data["workflow_type"],
        )

    async def describe_workflow(self, workflow_id: str) -> WorkflowExecution:
        """Return the server's current view of a workflow instance.

        Resolves to the newest durable run in the instance's chain (including
        any continue-as-new runs). Decodes the recorded ``input`` and
        ``output`` envelopes when present.
        """
        data = await self._request("GET", f"/workflows/{workflow_id}", context=workflow_id)
        input_val = data.get("input")
        output_val = data.get("output")
        envelope_jobs: list[tuple[str, Any]] = []
        if data.get("input_envelope"):
            envelope_jobs.append(("input", data["input_envelope"]))
        if data.get("output_envelope"):
            envelope_jobs.append(("output", data["output_envelope"]))
        if envelope_jobs:
            decoded = serializer.decode_envelopes(
                [envelope for _, envelope in envelope_jobs],
                external_storage=self.external_storage,
                external_storage_cache=self.external_storage_cache,
            )
            for (field, _), value in zip(envelope_jobs, decoded, strict=True):
                if field == "input":
                    input_val = value
                else:
                    output_val = value
        return WorkflowExecution(
            workflow_id=data.get("workflow_id", workflow_id),
            run_id=data.get("run_id"),
            workflow_type=data.get("workflow_type", ""),
            status=data.get("status"),
            namespace=data.get("namespace"),
            task_queue=data.get("task_queue"),
            input=input_val,
            output=output_val,
            payload_codec=data.get("payload_codec"),
            memo=data.get("memo") if isinstance(data.get("memo"), dict) else None,
            search_attributes=(
                data.get("search_attributes") if isinstance(data.get("search_attributes"), dict) else None
            ),
        )

    async def list_workflows(
        self,
        *,
        workflow_type: str | None = None,
        status: str | None = None,
        query: str | None = None,
        page_size: int | None = None,
        next_page_token: str | None = None,
    ) -> WorkflowList:
        """Page through workflow instances, optionally filtered by type, status, or query string.

        Pass the returned :attr:`WorkflowList.next_page_token` as
        ``next_page_token`` on the next call to fetch the following page; the
        token is ``None`` when there are no more pages.
        """
        params: dict[str, str] = {}
        if workflow_type is not None:
            params["workflow_type"] = workflow_type
        if status is not None:
            params["status"] = status
        if query is not None:
            params["query"] = query
        if page_size is not None:
            params["page_size"] = str(page_size)
        if next_page_token is not None:
            params["next_page_token"] = next_page_token

        qs = urlencode(params)
        path = f"/workflows?{qs}" if qs else "/workflows"
        data = await self._request("GET", path)
        items = data.get("workflows", [])
        executions = [
            WorkflowExecution(
                workflow_id=item.get("workflow_id", ""),
                run_id=item.get("run_id"),
                workflow_type=item.get("workflow_type", ""),
                status=item.get("status"),
                namespace=item.get("namespace"),
                task_queue=item.get("task_queue"),
                payload_codec=item.get("payload_codec"),
                memo=item.get("memo") if isinstance(item.get("memo"), dict) else None,
                search_attributes=(
                    item.get("search_attributes") if isinstance(item.get("search_attributes"), dict) else None
                ),
            )
            for item in items
        ]
        return WorkflowList(
            executions=executions,
            next_page_token=data.get("next_page_token"),
        )

    async def get_history(self, workflow_id: str, run_id: str) -> Any:
        """Fetch the full durable history for one specific run of a workflow."""
        return await self._request(
            "GET", f"/workflows/{workflow_id}/runs/{run_id}/history", context=workflow_id
        )

    async def export_history(self, workflow_id: str, run_id: str) -> Any:
        """Export one workflow run history as a replay bundle."""
        return await self._request(
            "GET", f"/workflows/{workflow_id}/runs/{run_id}/history/export", context=workflow_id
        )

    async def list_workflow_runs(self, workflow_id: str) -> WorkflowRunList:
        """List all durable runs in one workflow execution chain, oldest first."""
        data = await self._request("GET", f"/workflows/{workflow_id}/runs", context=workflow_id)
        runs = [
            WorkflowRun.from_dict(item, workflow_id=data.get("workflow_id", workflow_id))
            for item in data.get("runs", [])
        ]
        return WorkflowRunList(
            workflow_id=data.get("workflow_id", workflow_id),
            run_count=data.get("run_count", len(runs)),
            runs=runs,
        )

    async def describe_workflow_run(self, workflow_id: str, run_id: str) -> WorkflowRun:
        """Return detailed status, payload, and actionability for one specific workflow run."""
        data = await self._request("GET", f"/workflows/{workflow_id}/runs/{run_id}", context=workflow_id)
        return WorkflowRun.from_dict(data, workflow_id=workflow_id, run_id=run_id)

    @staticmethod
    def _workflow_stream_path(workflow_id: str, run_id: str, stream_name: str | None = None) -> str:
        path = (
            f"/workflows/{quote(workflow_id, safe='')}/runs/"
            f"{quote(run_id, safe='')}/streams"
        )
        if stream_name is not None:
            path += f"/{quote(stream_name, safe='')}"
        return path

    async def list_workflow_streams(
        self,
        workflow_id: str,
        run_id: str,
    ) -> list[WorkflowStreamDescription]:
        """List every service-mode output stream for one workflow run."""
        data = await self._request(
            "GET",
            self._workflow_stream_path(workflow_id, run_id),
            context=workflow_id,
        )
        response_workflow_id = str(data.get("workflow_id", workflow_id))
        response_run_id = str(data.get("workflow_run_id", run_id))
        return [
            WorkflowStreamDescription.from_dict(
                stream,
                workflow_id=response_workflow_id,
                run_id=response_run_id,
            )
            for stream in data.get("streams", [])
            if isinstance(stream, Mapping)
        ]

    async def describe_workflow_stream(
        self,
        workflow_id: str,
        run_id: str,
        stream_name: str,
    ) -> WorkflowStreamDescription:
        """Describe lifecycle, offsets, and pending count for one output stream."""
        data = await self._request(
            "GET",
            self._workflow_stream_path(workflow_id, run_id, stream_name),
            context=workflow_id,
        )
        stream = data.get("stream", data)
        if not isinstance(stream, Mapping):
            raise ValueError("workflow stream response must contain a stream object")
        return WorkflowStreamDescription.from_dict(
            stream,
            workflow_id=str(data.get("workflow_id", workflow_id)),
            run_id=str(data.get("workflow_run_id", run_id)),
        )

    async def subscribe_workflow_stream(
        self,
        workflow_id: str,
        run_id: str,
        stream_name: str,
        *,
        from_offset: int = 0,
        max_items: int = 100,
        wait_seconds: int = 0,
        cancel_event: asyncio.Event | None = None,
    ) -> WorkflowStreamPage:
        """Read a page from ``from_offset`` with bounded long polling.

        Delivery is at least once. Persist ``next_offset`` only after the
        page's effects are durable, and process redelivered items idempotently.
        Cancelling this coroutine, or setting ``cancel_event``, aborts the
        outstanding HTTP request.
        """
        if from_offset < 0:
            raise ValueError("from_offset must be non-negative")
        if not 1 <= max_items <= 500:
            raise ValueError("max_items must be between 1 and 500")
        if not 0 <= wait_seconds <= 60:
            raise ValueError("wait_seconds must be between 0 and 60")

        query = urlencode(
            {
                "from": from_offset,
                "max_items": max_items,
                "wait_seconds": wait_seconds,
            }
        )
        path = f"{self._workflow_stream_path(workflow_id, run_id, stream_name)}/items?{query}"
        request_task = asyncio.create_task(
            self._request(
                "GET",
                path,
                timeout=max(float(wait_seconds) + _WORKER_POLL_HTTP_TIMEOUT_GRACE_SECONDS, 5.0),
                context=workflow_id,
            )
        )
        cancel_task: asyncio.Task[bool] | None = None
        try:
            if cancel_event is None:
                data = await request_task
            else:
                cancel_task = asyncio.create_task(cancel_event.wait())
                done, _ = await asyncio.wait(
                    {request_task, cancel_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if cancel_task in done:
                    request_task.cancel()
                    await asyncio.gather(request_task, return_exceptions=True)
                    raise asyncio.CancelledError
                data = await request_task
        finally:
            if not request_task.done():
                request_task.cancel()
                await asyncio.gather(request_task, return_exceptions=True)
            if cancel_task is not None:
                cancel_task.cancel()
                await asyncio.gather(cancel_task, return_exceptions=True)

        stream_data = data.get("stream")
        if not isinstance(stream_data, Mapping):
            raise ValueError("workflow stream response must contain a stream object")
        response_workflow_id = str(data.get("workflow_id", workflow_id))
        response_run_id = str(data.get("workflow_run_id", run_id))
        stream = WorkflowStreamDescription.from_dict(
            stream_data,
            workflow_id=response_workflow_id,
            run_id=response_run_id,
        )
        items: list[WorkflowStreamItem] = []
        for raw_item in data.get("items", []):
            if not isinstance(raw_item, Mapping):
                continue
            envelope = raw_item.get("payload")
            payload = envelope
            payload_reference = raw_item.get("payload_reference")
            if isinstance(envelope, Mapping) and (
                "blob" in envelope or "external_storage" in envelope
            ):
                if "external_storage" not in envelope or self.external_storage is not None:
                    payload = serializer.decode_envelope(
                        envelope,
                        external_storage=self.external_storage,
                        external_storage_cache=self.external_storage_cache,
                    )
                else:
                    payload = None
                external_reference = envelope.get("external_storage")
                if payload_reference is None and isinstance(external_reference, Mapping):
                    payload_reference = external_reference.get("uri")
            items.append(
                WorkflowStreamItem(
                    offset=int(raw_item.get("offset", 0)),
                    payload=payload,
                    payload_envelope=envelope if isinstance(envelope, Mapping) else None,
                    payload_reference=(
                        str(payload_reference) if payload_reference is not None else None
                    ),
                    payload_codec=raw_item.get("payload_codec"),
                    idempotency_key=raw_item.get("idempotency_key"),
                    item_type=raw_item.get("item_type"),
                    content_type=raw_item.get("content_type"),
                    origin=raw_item.get("origin"),
                    origin_reference=raw_item.get("origin_reference"),
                    emitted_at=raw_item.get("emitted_at"),
                    raw=dict(raw_item),
                )
            )
        return WorkflowStreamPage(
            stream=stream,
            items=items,
            next_offset=int(data.get("next_offset", from_offset + len(items))),
            terminal=bool(data.get("terminal", stream.terminal)),
            raw=dict(data),
        )

    async def iter_workflow_stream(
        self,
        workflow_id: str,
        run_id: str,
        stream_name: str,
        *,
        from_offset: int = 0,
        max_items: int = 100,
        wait_seconds: int = 30,
        cancel_event: asyncio.Event | None = None,
    ) -> AsyncIterator[WorkflowStreamItem]:
        """Yield items in offset order until the stream reaches a terminal state."""
        next_offset = from_offset
        while True:
            page = await self.subscribe_workflow_stream(
                workflow_id,
                run_id,
                stream_name,
                from_offset=next_offset,
                max_items=max_items,
                wait_seconds=wait_seconds,
                cancel_event=cancel_event,
            )
            for item in page.items:
                yield item
            next_offset = page.next_offset
            if page.terminal:
                return

    async def append_workflow_stream(
        self,
        workflow_id: str,
        run_id: str,
        stream_name: str,
        items: Sequence[WorkflowStreamAppendItem],
        *,
        max_pending_items: int | None = None,
    ) -> WorkflowStreamAppendResult:
        """Append typed items, using configured external storage when selected."""
        if not items:
            raise ValueError("items must not be empty")
        body_items: list[dict[str, Any]] = []
        for item in items:
            wire_item: dict[str, Any] = {}
            if item.payload is not None or item.payload_reference is None:
                envelope = self._payload_envelope(
                    item.payload,
                    kind="workflow_stream_item",
                    workflow_id=workflow_id,
                    run_id=run_id,
                )
                wire_item["payload"] = envelope
                wire_item["payload_codec"] = serializer.AVRO_CODEC
                external_reference = envelope.get("external_storage")
                if item.payload_reference is None and isinstance(external_reference, Mapping):
                    wire_item["payload_reference"] = external_reference.get("uri")
            if item.payload_reference is not None:
                wire_item["payload_reference"] = item.payload_reference
                wire_item.setdefault("payload_codec", serializer.AVRO_CODEC)
            if item.idempotency_key is not None:
                wire_item["idempotency_key"] = item.idempotency_key
            if item.item_type is not None:
                wire_item["item_type"] = item.item_type
            if item.content_type is not None:
                wire_item["content_type"] = item.content_type
            body_items.append(wire_item)
        body: dict[str, Any] = {"items": body_items}
        if max_pending_items is not None:
            if max_pending_items < 1:
                raise ValueError("max_pending_items must be at least 1")
            body["max_pending_items"] = max_pending_items
        data = await self._request(
            "POST",
            f"{self._workflow_stream_path(workflow_id, run_id, stream_name)}/items",
            json=body,
            context=workflow_id,
        )
        stream_data = data.get("stream")
        if not isinstance(stream_data, Mapping):
            raise ValueError("workflow stream response must contain a stream object")
        return WorkflowStreamAppendResult(
            stream=WorkflowStreamDescription.from_dict(
                stream_data,
                workflow_id=str(data.get("workflow_id", workflow_id)),
                run_id=str(data.get("workflow_run_id", run_id)),
            ),
            accepted_offsets=[int(offset) for offset in data.get("accepted_offsets", [])],
            accepted_count=int(data.get("accepted", 0)),
            deduplicated_count=int(data.get("deduped", 0)),
            raw=dict(data),
        )

    async def close_workflow_stream(
        self,
        workflow_id: str,
        run_id: str,
        stream_name: str,
        *,
        error_reason: str | None = None,
        retention_seconds: int | None = None,
    ) -> WorkflowStreamDescription:
        """Close a stream, or mark it errored when ``error_reason`` is set."""
        body: dict[str, Any] = {}
        if error_reason is not None:
            if not error_reason:
                raise ValueError("error_reason must not be empty")
            body["error_reason"] = error_reason
        if retention_seconds is not None:
            if retention_seconds < 1:
                raise ValueError("retention_seconds must be at least 1")
            body["retention_seconds"] = retention_seconds
        data = await self._request(
            "POST",
            f"{self._workflow_stream_path(workflow_id, run_id, stream_name)}/close",
            json=body,
            context=workflow_id,
        )
        stream = data.get("stream", data)
        if not isinstance(stream, Mapping):
            raise ValueError("workflow stream response must contain a stream object")
        return WorkflowStreamDescription.from_dict(
            stream,
            workflow_id=str(data.get("workflow_id", workflow_id)),
            run_id=str(data.get("workflow_run_id", run_id)),
        )

    # ── Standalone Activities ─────────────────────────────────────────
    #
    # Activities can run as top-level durable jobs without a wrapper
    # workflow. The same activity definition registered via
    # ``@activity.defn(name=...)`` is reusable inside a workflow's
    # ``activity()`` invocation and as a standalone job — the server
    # handles dispatch, retry, deadline, cancellation, and history
    # projection through the existing activity infrastructure.

    async def start_activity(
        self,
        *,
        activity_type: str,
        task_queue: str,
        activity_id: str | None = None,
        activity_class: str | None = None,
        input: list[Any] | None = None,
        business_key: str | None = None,
        retry_policy: dict[str, Any] | None = None,
        start_to_close_timeout_seconds: int | None = None,
        schedule_to_start_timeout_seconds: int | None = None,
        schedule_to_close_timeout_seconds: int | None = None,
        heartbeat_timeout_seconds: int | None = None,
    ) -> StandaloneActivityHandle:
        """Start a standalone activity and return a handle bound to it.

        ``activity_type`` is the language-neutral activity type key
        registered via :func:`durable_workflow.activity.defn`. The same
        activity definition can be invoked inside a workflow with the
        worker's ``activity()`` call and also dispatched here as a
        top-level durable job — there is no separate "job activity"
        decorator. ``task_queue`` selects the worker pool that will run
        the activity.

        ``activity_id`` is an optional caller-supplied identifier (must be
        URL-safe; same character rules as ``workflow_id``). When omitted,
        the server generates one. ``activity_class`` is an optional
        free-form label that surfaces on the listing/show endpoints — it
        does not affect dispatch.

        ``input`` is a list of positional arguments passed to the
        activity, encoded with the default payload codec.
        ``retry_policy`` follows the same shape used inside a workflow
        (``max_attempts``, ``backoff_seconds``,
        ``non_retryable_error_types``). The four timeout knobs map
        one-to-one onto the same fields applied to activities scheduled
        from inside a workflow.

        Returns a :class:`StandaloneActivityHandle` so the caller can
        inspect the activity, await its result, or cancel it without
        having to know that the server records the work inside a host
        workflow run.
        """
        body: dict[str, Any] = {
            "activity_type": activity_type,
            "task_queue": task_queue,
        }
        if activity_id is not None:
            body["activity_id"] = activity_id
        if activity_class is not None:
            body["activity_class"] = activity_class
        if business_key is not None:
            body["business_key"] = business_key
        if input is not None:
            body["input"] = self._payload_envelope(
                input,
                kind="activity_input",
                activity_name=activity_type,
                task_queue=task_queue,
            )
        if retry_policy is not None:
            body["retry_policy"] = retry_policy
        if start_to_close_timeout_seconds is not None:
            body["start_to_close_timeout_seconds"] = start_to_close_timeout_seconds
        if schedule_to_start_timeout_seconds is not None:
            body["schedule_to_start_timeout_seconds"] = schedule_to_start_timeout_seconds
        if schedule_to_close_timeout_seconds is not None:
            body["schedule_to_close_timeout_seconds"] = schedule_to_close_timeout_seconds
        if heartbeat_timeout_seconds is not None:
            body["heartbeat_timeout_seconds"] = heartbeat_timeout_seconds

        data = await self._request(
            "POST",
            "/activities",
            json=body,
            context=activity_id or activity_type,
        )

        return StandaloneActivityHandle(
            self,
            activity_id=data.get("activity_id", activity_id or ""),
            workflow_run_id=data.get("workflow_run_id"),
            activity_execution_id=data.get("activity_execution_id"),
            workflow_type=data.get("workflow_type", ""),
            activity_type=data.get("activity_type", activity_type),
        )

    def get_activity_handle(
        self,
        activity_id: str,
        *,
        workflow_run_id: str | None = None,
        activity_execution_id: str | None = None,
        activity_type: str = "",
    ) -> StandaloneActivityHandle:
        """Return a :class:`StandaloneActivityHandle` bound to an existing
        standalone activity. Does not round-trip to the server.
        """
        return StandaloneActivityHandle(
            self,
            activity_id=activity_id,
            workflow_run_id=workflow_run_id,
            activity_execution_id=activity_execution_id,
            activity_type=activity_type,
        )

    async def describe_activity(self, activity_id: str) -> StandaloneActivityExecution:
        """Return the server's current view of one standalone activity.

        The returned ``result`` field is decoded from the server's payload
        envelope when the activity has completed; for not-yet-terminal
        activities it is ``None``.
        """
        data = await self._request(
            "GET", f"/activities/{quote(activity_id, safe='._:-')}", context=activity_id
        )
        result_envelope = data.get("result")
        result_value: Any = None
        if (
            isinstance(result_envelope, dict)
            and (result_envelope.get("blob") is not None or result_envelope.get("external_storage") is not None)
        ):
            result_value = serializer.decode_envelope(
                result_envelope,
                codec=result_envelope.get("codec") or data.get("payload_codec"),
                external_storage=self.external_storage,
                external_storage_cache=self.external_storage_cache,
            )
        return StandaloneActivityExecution.from_dict(
            data,
            activity_id=activity_id,
            result=result_value,
        )

    async def list_activities(
        self,
        *,
        status: str | None = None,
        page_size: int | None = None,
        next_page_token: str | None = None,
    ) -> StandaloneActivityList:
        """Page through standalone activities visible to the calling namespace.

        ``status`` filters on the host-run status bucket and accepts
        ``running``, ``completed``, or ``failed``.
        """
        params: dict[str, str] = {}
        if status is not None:
            params["status"] = status
        if page_size is not None:
            params["page_size"] = str(page_size)
        if next_page_token is not None:
            params["next_page_token"] = next_page_token

        qs = urlencode(params)
        path = f"/activities?{qs}" if qs else "/activities"
        data = await self._request("GET", path)
        items = data.get("activities", [])
        executions = [
            StandaloneActivityExecution.from_dict(item)
            for item in items
        ]
        return StandaloneActivityList(
            activities=executions,
            activity_count=data.get("activity_count", len(executions)),
            next_page_token=data.get("next_page_token"),
        )

    async def get_activity_result(
        self,
        handle: StandaloneActivityHandle,
        *,
        poll_interval: float = 0.5,
        timeout: float = 30.0,
    ) -> Any:
        """Poll a standalone activity until it reaches a terminal outcome.

        Returns the decoded activity result on success, raises
        :class:`~durable_workflow.errors.WorkflowFailed` on failure (the
        host run carries the activity's failure as its terminal state),
        :class:`~durable_workflow.errors.WorkflowCancelled` /
        :class:`~durable_workflow.errors.WorkflowTerminated` for the
        respective lifecycle outcomes, or :class:`TimeoutError` if the
        activity is still running after ``timeout`` seconds.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            desc = await self.describe_activity(handle.activity_id)
            status = desc.status
            if status in ("completed", "failed", "terminated", "canceled", "cancelled"):
                if status == "completed":
                    return desc.result
                if status == "failed":
                    raise WorkflowFailed(
                        f"standalone activity {handle.activity_id} failed",
                    )
                if status == "terminated":
                    raise WorkflowTerminated(
                        desc.closed_reason or "standalone activity was terminated",
                    )
                raise WorkflowCancelled(
                    desc.closed_reason or "standalone activity was cancelled",
                )
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(
                    f"standalone activity {handle.activity_id} not terminal after {timeout}s "
                    f"(status={status})"
                )
            await asyncio.sleep(poll_interval)

    async def send_webhook_bridge_event(
        self,
        adapter: str,
        *,
        action: str,
        idempotency_key: str,
        target: dict[str, Any],
        input: dict[str, Any] | None = None,
        correlation: dict[str, Any] | None = None,
    ) -> BridgeAdapterOutcome:
        """Send one bounded webhook bridge event and return its contract outcome.

        The bridge endpoint intentionally returns machine-readable rejected
        outcomes as HTTP 422. This method returns those outcomes instead of
        raising :class:`InvalidArgument`, while auth and unexpected server
        failures still use the normal SDK exception mapping.
        """
        body: dict[str, Any] = {
            "action": action,
            "idempotency_key": idempotency_key,
            "target": target,
        }
        if input is not None:
            body["input"] = input
        if correlation is not None:
            body["correlation"] = correlation

        data = await self._request_bridge_outcome(
            f"/bridge-adapters/webhook/{quote(adapter, safe='._:-')}",
            json=body,
            context=f"bridge adapter {adapter}",
        )
        return BridgeAdapterOutcome.from_dict(data)

    async def signal_workflow(
        self, workflow_id: str, signal_name: str, *, args: list[Any] | None = None
    ) -> None:
        """Deliver an external signal to a running workflow.

        Signals are fire-and-forget: the server records the signal in durable
        history and returns immediately. They do not wait for the workflow to
        observe the signal. See the main docs for how to declare the allowed
        signal names on a workflow type.
        """
        body: dict[str, Any] = {}
        if args:
            body["input"] = self._payload_envelope(
                args,
                kind="signal",
                workflow_id=workflow_id,
                signal_name=signal_name,
            )
        await self._request("POST", f"/workflows/{workflow_id}/signal/{signal_name}", json=body, context=workflow_id)

    async def append_message_stream(
        self,
        workflow_id: str,
        stream_name: str,
        message_id: str,
        *,
        args: list[Any] | None = None,
    ) -> dict[str, Any]:
        """Append repeated input without requiring workflow-authored cursor bookkeeping."""
        body: dict[str, Any] = {"message_id": message_id}
        if args is not None:
            body["input"] = self._payload_envelope(
                args,
                kind="message_stream",
                workflow_id=workflow_id,
            )
        result = await self._request(
            "POST",
            f"/workflows/{quote(workflow_id, safe='._:-')}/message-streams/{quote(stream_name, safe='._:-')}/messages",
            json=body,
            context=workflow_id,
        )
        return dict(result)

    async def query_workflow(
        self, workflow_id: str, query_name: str, *, args: list[Any] | None = None
    ) -> Any:
        """Execute a named read-only query against a workflow and return the result.

        Queries are synchronous and non-mutating. The server runs the named
        query handler inside the workflow process and returns the decoded
        result. Raises :class:`~durable_workflow.errors.QueryFailed` if the
        query was rejected or the handler errored.
        """
        await self._require_query_support()
        body: dict[str, Any] = {}
        if args:
            body["input"] = self._payload_envelope(
                args,
                kind="query",
                workflow_id=workflow_id,
                query_name=query_name,
            )
        return await self._request(
            "POST", f"/workflows/{workflow_id}/query/{query_name}", json=body, context=workflow_id
        )

    async def cancel_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
        """Request graceful cancellation of a workflow's current run.

        Cancellation is cooperative: the server delivers a cancellation signal
        that the workflow can observe and handle (e.g. to roll back via a
        saga). Compare with :meth:`terminate_workflow`, which is forceful.
        """
        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        await self._request("POST", f"/workflows/{workflow_id}/cancel", json=body, context=workflow_id)

    async def terminate_workflow(self, workflow_id: str, *, reason: str | None = None) -> None:
        """Forcefully stop a workflow without giving it a chance to clean up.

        Prefer :meth:`cancel_workflow` when the workflow code can implement
        graceful shutdown. Termination is an operator escape hatch.
        """
        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        await self._request("POST", f"/workflows/{workflow_id}/terminate", json=body, context=workflow_id)

    async def repair_workflow(self, workflow_id: str) -> WorkflowCommandResult:
        """Ask the server to repair a stalled workflow, returning the command outcome."""
        data = await self._request("POST", f"/workflows/{workflow_id}/repair", json={}, context=workflow_id)
        return WorkflowCommandResult.from_dict(data, workflow_id=workflow_id)

    async def archive_workflow(self, workflow_id: str, *, reason: str | None = None) -> WorkflowCommandResult:
        """Move a terminal workflow into the archive tier, returning the command outcome."""
        body: dict[str, Any] = {}
        if reason is not None:
            body["reason"] = reason
        data = await self._request("POST", f"/workflows/{workflow_id}/archive", json=body, context=workflow_id)
        return WorkflowCommandResult.from_dict(data, workflow_id=workflow_id)

    async def update_workflow(
        self,
        workflow_id: str,
        update_name: str,
        *,
        args: list[Any] | None = None,
        wait_for: str | None = None,
        wait_timeout_seconds: int | None = None,
        request_id: str | None = None,
    ) -> Any:
        """Send a synchronous update to a running workflow and wait for the result.

        Updates are request/response calls to a named handler on the workflow;
        the handler may mutate durable workflow state and return a value.
        ``wait_for`` selects how long the server waits before returning —
        typically ``completed`` to block until the handler finishes, or
        ``accepted`` to return once the request is durably accepted and routed.
        When the workflow contract declares a validator, a capable Server does
        not return that accepted state until synchronous validation succeeds.
        Validator rejection and validation-routing failures remain typed and
        do not silently fall back to post-accept handler failure.

        ``request_id`` lets the caller deduplicate retries. Raises
        :class:`~durable_workflow.errors.UpdateRejected` when the workflow's
        validator rejects the update, or
        :class:`~durable_workflow.errors.UpdateValidationFailed` when the
        declared validation boundary cannot be enforced.
        """
        await self._require_update_wait_stage(wait_for or "accepted")
        body: dict[str, Any] = {}
        if args:
            body["input"] = self._payload_envelope(
                args,
                kind="update",
                workflow_id=workflow_id,
                update_name=update_name,
            )
        if wait_for is not None:
            body["wait_for"] = wait_for
        if wait_timeout_seconds is not None:
            body["wait_timeout_seconds"] = wait_timeout_seconds
        body["request_id"] = request_id or f"sdk-python-update-{uuid.uuid4().hex}"
        return await self._request(
            "POST", f"/workflows/{workflow_id}/update/{update_name}", json=body, context=workflow_id
        )

    async def get_result(
        self,
        handle: WorkflowHandle,
        *,
        poll_interval: float = 0.5,
        timeout: float = 30.0,
    ) -> Any:
        """Poll a workflow until it reaches a terminal state and return its result.

        Raises :class:`~durable_workflow.errors.WorkflowFailed`,
        :class:`~durable_workflow.errors.WorkflowCancelled`, or
        :class:`~durable_workflow.errors.WorkflowTerminated` if the workflow
        ended in a non-success state, or :class:`TimeoutError` if ``timeout``
        seconds elapse before the workflow terminates.
        """
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            desc = await self.describe_workflow(handle.workflow_id)
            status = desc.status
            if status in ("completed", "failed", "terminated", "canceled", "cancelled"):
                run_id = handle.run_id or desc.run_id
                if run_id is None:
                    raise WorkflowFailed("no run_id available to fetch history")
                history = await self.get_history(handle.workflow_id, run_id)
                events = history.get("events", [])
                for ev in reversed(events):
                    etype = ev.get("event_type")
                    payload = ev.get("payload") or {}
                    if etype == "WorkflowCompleted":
                        return serializer.decode_envelope(
                            payload.get("output"),
                            codec=payload.get("payload_codec") or desc.payload_codec,
                            external_storage=self.external_storage,
                            external_storage_cache=self.external_storage_cache,
                        )
                    if etype == "WorkflowFailed":
                        raise WorkflowFailed(
                            payload.get("message", "workflow failed"),
                            payload.get("exception_class"),
                        )
                    if etype == "WorkflowTerminated":
                        raise WorkflowTerminated(
                            payload.get("reason", "workflow was terminated")
                        )
                    if etype == "WorkflowCancelled":
                        raise WorkflowCancelled(
                            payload.get("reason", "workflow was cancelled")
                        )
                return None
            if asyncio.get_running_loop().time() > deadline:
                raise TimeoutError(
                    f"workflow {handle.workflow_id} not terminal after {timeout}s (status={status})"
                )
            await asyncio.sleep(poll_interval)

    # ── Schedules ─────────────────────────────────────────────────────
    def get_schedule_handle(self, schedule_id: str) -> ScheduleHandle:
        """Return a :class:`ScheduleHandle` bound to an existing schedule.

        Does not round-trip to the server. Use :meth:`describe_schedule` via
        the handle when you need to verify the schedule actually exists.
        """
        return ScheduleHandle(self, schedule_id=schedule_id)

    async def create_schedule(
        self,
        *,
        schedule_id: str | None = None,
        spec: ScheduleSpec,
        action: ScheduleAction,
        overlap_policy: str | None = None,
        jitter_seconds: int | None = None,
        max_runs: int | None = None,
        memo: dict[str, Any] | None = None,
        search_attributes: dict[str, Any] | None = None,
        paused: bool = False,
        note: str | None = None,
    ) -> ScheduleHandle:
        """Create a new schedule and return a handle bound to it.

        ``spec`` describes when the schedule fires (cron expressions,
        intervals, calendars); ``action`` describes what it starts (typically
        a workflow). ``overlap_policy`` controls what happens when a fire
        would overlap an already-running action — ``skip``, ``buffer_one``,
        ``buffer_all``, ``cancel_other``, or ``terminate_other``. Pass
        ``paused=True`` to create the schedule in a paused state and resume
        it later with :meth:`resume_schedule`.
        """
        body: dict[str, Any] = {
            "spec": spec.to_dict(),
            "action": action.to_dict(
                input_encoder=lambda value: self._payload_envelope(
                    value,
                    kind="schedule_input",
                    workflow_type=action.workflow_type,
                    schedule_id=schedule_id,
                    task_queue=action.task_queue,
                )
            ),
        }
        if schedule_id is not None:
            body["schedule_id"] = schedule_id
        if overlap_policy is not None:
            body["overlap_policy"] = overlap_policy
        if jitter_seconds is not None:
            body["jitter_seconds"] = jitter_seconds
        if max_runs is not None:
            body["max_runs"] = max_runs
        if memo is not None:
            body["memo"] = memo
        if search_attributes is not None:
            self._warn_json_payload_size(
                search_attributes,
                kind="schedule_search_attributes",
                schedule_id=schedule_id,
            )
            body["search_attributes"] = search_attributes
        if paused:
            body["paused"] = True
        if note is not None:
            body["note"] = note
        data = await self._request("POST", "/schedules", json=body)
        sid = data.get("schedule_id", schedule_id or "")
        return ScheduleHandle(self, schedule_id=sid)

    async def list_schedules(
        self,
        *,
        status: str | None = None,
        workflow_type: str | None = None,
        query: str | None = None,
        page_size: int | None = None,
        next_page_token: str | None = None,
    ) -> ScheduleList:
        """Return one server-filtered schedule visibility page.

        Status, workflow type, and visibility-query filters combine with AND
        semantics. Continuation tokens are opaque: pass a non-null token back
        unchanged with the same namespace and filters. ``None`` terminates the
        traversal. The server validates page sizes, predicates, and cursors.
        """
        params: dict[str, str] = {}
        if status is not None:
            params["status"] = status
        if workflow_type is not None:
            params["workflow_type"] = workflow_type
        if query is not None:
            params["query"] = query
        if page_size is not None:
            params["page_size"] = str(page_size)
        if next_page_token is not None:
            params["next_page_token"] = next_page_token

        qs = urlencode(params)
        path = f"/schedules?{qs}" if qs else "/schedules"
        data = await self._request("GET", path, context="schedule.list")
        items = data.get("schedules", [])
        schedules = [
            ScheduleDescription(
                schedule_id=item.get("schedule_id", ""),
                status=item.get("status"),
                spec=item.get("spec"),
                action=item.get("action"),
                overlap_policy=item.get("overlap_policy"),
                note=item.get("note"),
                memo=item.get("memo") if isinstance(item.get("memo"), dict) else None,
                search_attributes=(
                    item.get("search_attributes")
                    if isinstance(item.get("search_attributes"), dict)
                    else None
                ),
                jitter_seconds=item.get("jitter_seconds"),
                max_runs=item.get("max_runs"),
                remaining_actions=item.get("remaining_actions"),
                fires_count=item.get("fires_count", 0),
                failures_count=item.get("failures_count", 0),
                next_fire_at=item.get("next_fire_at"),
                last_fired_at=item.get("last_fired_at"),
                latest_workflow_instance_id=item.get("latest_workflow_instance_id"),
                paused_at=item.get("paused_at"),
                created_at=item.get("created_at"),
                updated_at=item.get("updated_at"),
                info=item.get("info") if isinstance(item.get("info"), dict) else None,
            )
            for item in items
        ]
        return ScheduleList(
            schedules=schedules,
            next_page_token=data.get("next_page_token"),
        )

    async def describe_schedule(self, schedule_id: str) -> ScheduleDescription:
        """Return the server's current view of a schedule, including status and fire counters."""
        data = await self._request("GET", f"/schedules/{schedule_id}", context=schedule_id)
        return ScheduleDescription(
            schedule_id=data.get("schedule_id", schedule_id),
            status=data.get("status"),
            spec=data.get("spec"),
            action=data.get("action"),
            overlap_policy=data.get("overlap_policy"),
            note=data.get("note"),
            memo=data.get("memo"),
            search_attributes=data.get("search_attributes"),
            jitter_seconds=data.get("jitter_seconds"),
            max_runs=data.get("max_runs"),
            remaining_actions=data.get("remaining_actions"),
            fires_count=data.get("fires_count", 0),
            failures_count=data.get("failures_count", 0),
            next_fire_at=data.get("next_fire_at"),
            last_fired_at=data.get("last_fired_at"),
            latest_workflow_instance_id=data.get("latest_workflow_instance_id"),
            paused_at=data.get("paused_at"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            info=data.get("info"),
        )

    async def update_schedule(
        self,
        schedule_id: str,
        *,
        spec: ScheduleSpec | None = None,
        action: ScheduleAction | None = None,
        overlap_policy: str | None = None,
        jitter_seconds: int | None = None,
        max_runs: int | None = None,
        memo: dict[str, Any] | None = None,
        search_attributes: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> None:
        """Update one or more fields of an existing schedule.

        Pass ``None`` for any field you don't want to change. Unknown fields
        are ignored by the server.
        """
        body: dict[str, Any] = {}
        if spec is not None:
            body["spec"] = spec.to_dict()
        if action is not None:
            body["action"] = action.to_dict(
                input_encoder=lambda value: self._payload_envelope(
                    value,
                    kind="schedule_input",
                    workflow_type=action.workflow_type,
                    schedule_id=schedule_id,
                    task_queue=action.task_queue,
                )
            )
        if overlap_policy is not None:
            body["overlap_policy"] = overlap_policy
        if jitter_seconds is not None:
            body["jitter_seconds"] = jitter_seconds
        if max_runs is not None:
            body["max_runs"] = max_runs
        if memo is not None:
            body["memo"] = memo
        if search_attributes is not None:
            self._warn_json_payload_size(
                search_attributes,
                kind="schedule_search_attributes",
                schedule_id=schedule_id,
            )
            body["search_attributes"] = search_attributes
        if note is not None:
            body["note"] = note
        await self._request("PUT", f"/schedules/{schedule_id}", json=body, context=schedule_id)

    async def pause_schedule(self, schedule_id: str, *, note: str | None = None) -> None:
        """Pause a schedule so it stops firing until resumed.

        Optional ``note`` is recorded as operator metadata on the pause
        event. Pausing does not cancel workflows that are already running.
        """
        body: dict[str, Any] = {}
        if note is not None:
            body["note"] = note
        await self._request("POST", f"/schedules/{schedule_id}/pause", json=body, context=schedule_id)

    async def resume_schedule(self, schedule_id: str, *, note: str | None = None) -> None:
        """Resume a paused schedule so it begins firing again."""
        body: dict[str, Any] = {}
        if note is not None:
            body["note"] = note
        await self._request("POST", f"/schedules/{schedule_id}/resume", json=body, context=schedule_id)

    async def trigger_schedule(
        self, schedule_id: str, *, overlap_policy: str | None = None
    ) -> ScheduleTriggerResult:
        """Fire a schedule immediately, outside its normal schedule.

        The ``overlap_policy`` override applies only to this one manual fire.
        The returned :class:`ScheduleTriggerResult` reports whether the fire
        was accepted or skipped (e.g. due to overlap).
        """
        body: dict[str, Any] = {}
        if overlap_policy is not None:
            body["overlap_policy"] = overlap_policy
        data = await self._request(
            "POST", f"/schedules/{schedule_id}/trigger", json=body, context=schedule_id,
        )
        return ScheduleTriggerResult(
            schedule_id=data.get("schedule_id", schedule_id),
            outcome=data.get("outcome", ""),
            workflow_id=data.get("workflow_id"),
            run_id=data.get("run_id"),
            reason=data.get("reason"),
            buffer_depth=data.get("buffer_depth"),
        )

    async def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule. Running workflows the schedule already started are unaffected."""
        await self._request("DELETE", f"/schedules/{schedule_id}", context=schedule_id)

    async def backfill_schedule(
        self,
        schedule_id: str,
        *,
        start_time: str,
        end_time: str,
        overlap_policy: str | None = None,
    ) -> ScheduleBackfillResult:
        """Fire a schedule for every would-have-been moment in ``[start_time, end_time]``.

        Times are ISO-8601 strings. Useful to replay a period the schedule
        was paused or to seed historical runs. The returned
        :class:`ScheduleBackfillResult` reports how many fires were attempted
        and the outcome of each.
        """
        body: dict[str, Any] = {
            "start_time": start_time,
            "end_time": end_time,
        }
        if overlap_policy is not None:
            body["overlap_policy"] = overlap_policy
        data = await self._request(
            "POST", f"/schedules/{schedule_id}/backfill", json=body, context=schedule_id,
        )
        return ScheduleBackfillResult(
            schedule_id=data.get("schedule_id", schedule_id),
            outcome=data.get("outcome", ""),
            fires_attempted=data.get("fires_attempted", 0),
            results=data.get("results"),
        )

    async def get_schedule_history(
        self,
        schedule_id: str,
        *,
        limit: int | None = None,
        after_sequence: int | None = None,
    ) -> ScheduleHistoryPage:
        """Return one page of the audit history stream for a schedule.

        The page is ordered by ``sequence`` ascending. Use
        ``after_sequence=page.next_cursor`` to request the next page while
        ``page.has_more`` is ``True``, or call :meth:`iter_schedule_history`
        to walk every remaining event with paging hidden.

        History is available for deleted schedules: the audit stream
        records ``ScheduleDeleted`` and survives the schedule's removal
        exactly so operators can review what happened.

        ``limit`` is clamped by the server between 1 and 500 (default
        100). ``after_sequence`` must be a non-negative integer; invalid
        values raise :class:`~durable_workflow.errors.InvalidArgument`
        through the shared 4xx mapping.
        """
        if limit is not None and limit < 1:
            raise ValueError("limit must be >= 1")
        if after_sequence is not None and after_sequence < 0:
            raise ValueError("after_sequence must be >= 0")

        params: dict[str, str] = {}
        if limit is not None:
            params["limit"] = str(limit)
        if after_sequence is not None:
            params["after_sequence"] = str(after_sequence)

        path = f"/schedules/{schedule_id}/history"
        if params:
            path = f"{path}?{urlencode(params)}"

        data = await self._request("GET", path, context=schedule_id)
        raw_events = data.get("events") or []
        events = [
            ScheduleHistoryEvent(
                sequence=int(item.get("sequence", 0)),
                event_type=item.get("event_type"),
                recorded_at=item.get("recorded_at"),
                workflow_instance_id=item.get("workflow_instance_id"),
                workflow_run_id=item.get("workflow_run_id"),
                payload=item.get("payload") if isinstance(item.get("payload"), dict) else None,
                id=item.get("id"),
            )
            for item in raw_events
        ]

        raw_cursor = data.get("next_cursor")
        next_cursor: int | None
        if raw_cursor is None:
            next_cursor = None
        else:
            try:
                next_cursor = int(raw_cursor)
            except (TypeError, ValueError):
                next_cursor = None

        return ScheduleHistoryPage(
            schedule_id=data.get("schedule_id", schedule_id),
            events=events,
            has_more=bool(data.get("has_more", False)),
            next_cursor=next_cursor,
            namespace=data.get("namespace"),
        )

    async def iter_schedule_history(
        self,
        schedule_id: str,
        *,
        limit: int | None = None,
        after_sequence: int | None = None,
    ) -> AsyncIterator[ScheduleHistoryEvent]:
        """Yield every audit event for a schedule, paging under the hood.

        Each element is a :class:`ScheduleHistoryEvent`. Paging stops once
        the server reports ``has_more=False``.
        """
        cursor = after_sequence
        while True:
            page = await self.get_schedule_history(
                schedule_id,
                limit=limit,
                after_sequence=cursor,
            )
            for event in page.events:
                yield event
            if not page.has_more or page.next_cursor is None:
                return
            cursor = page.next_cursor

    # ── Worker protocol ────────────────────────────────────────────────
    async def register_worker(
        self,
        *,
        worker_id: str,
        task_queue: str,
        supported_workflow_types: list[str] | None = None,
        workflow_definition_fingerprints: dict[str, str] | None = None,
        workflow_command_contracts: dict[str, dict[str, Any]] | None = None,
        supported_activity_types: list[str] | None = None,
        max_concurrent_workflow_tasks: int | None = None,
        max_concurrent_activity_tasks: int | None = None,
        runtime: str = "python",
        sdk_version: str | None = None,
        build_id: str | None = None,
        capabilities: list[str] | None = None,
        capability_manifest: dict[str, dict[str, Any]] | None = None,
        task_slots: dict[str, int] | None = None,
        process_metrics: dict[str, Any] | None = None,
        heartbeat_interval_seconds: int | None = None,
    ) -> Any:
        """Register this process with the server as a worker for ``task_queue``.

        Called by :class:`~durable_workflow.Worker` at startup. Most
        applications should not call this directly — create a
        :class:`~durable_workflow.Worker` instead.
        """
        if sdk_version is None:
            sdk_version = DEFAULT_SDK_VERSION
        if max_concurrent_workflow_tasks is not None and max_concurrent_workflow_tasks < 1:
            raise ValueError("max_concurrent_workflow_tasks must be at least 1")
        if max_concurrent_activity_tasks is not None and max_concurrent_activity_tasks < 1:
            raise ValueError("max_concurrent_activity_tasks must be at least 1")
        if (
            capabilities
            and _MESSAGE_STREAMS_CAPABILITY in capabilities
            and not _worker_protocol_supports_message_streams()
        ):
            raise ValueError("message streams require worker protocol 1.15 or newer")

        body: dict[str, Any] = {
            "worker_id": worker_id,
            "task_queue": task_queue,
            "runtime": runtime,
            "sdk_version": sdk_version,
            "supported_workflow_types": supported_workflow_types or [],
            "supported_activity_types": supported_activity_types or [],
        }
        if workflow_definition_fingerprints is not None:
            body["workflow_definition_fingerprints"] = workflow_definition_fingerprints
        if workflow_command_contracts is not None:
            body["workflow_command_contracts"] = workflow_command_contracts
        if capabilities is not None:
            body["capabilities"] = [capability for capability in capabilities if capability]
        if capability_manifest is not None:
            body["capability_manifest"] = capability_manifest
        if build_id is not None:
            body["build_id"] = build_id
        if max_concurrent_workflow_tasks is not None:
            body["max_concurrent_workflow_tasks"] = max_concurrent_workflow_tasks
        if max_concurrent_activity_tasks is not None:
            body["max_concurrent_activity_tasks"] = max_concurrent_activity_tasks
        if task_slots is not None:
            body["task_slots"] = task_slots
        if process_metrics is not None:
            body["process_metrics"] = process_metrics
        if heartbeat_interval_seconds is not None:
            body["heartbeat_interval_seconds"] = heartbeat_interval_seconds
        return await self._request("POST", "/worker/register", worker=True, json=body)

    async def deregister_worker_registration(self, worker_id: str) -> dict[str, Any]:
        """Remove this runtime's successful worker-plane registration.

        Called by :class:`~durable_workflow.Worker` after its pollers and
        in-flight tasks have drained. This is distinct from the operator-only
        :meth:`deregister_worker` control-plane management operation. Returns
        the worker deregistration envelope, including recovered workflow-task
        count.
        """
        try:
            data = await self._request(
                "DELETE",
                f"/worker/registrations/{quote(worker_id, safe='')}",
                worker=True,
                context=worker_id,
            )
        except ServerError as error:
            if error.status == 404 and error.reason() == "worker_not_found":
                return {
                    "worker_id": worker_id,
                    "outcome": "already_deregistered",
                    "recovered_workflow_task_count": 0,
                }
            raise
        if not isinstance(data, dict):
            raise ServerError(
                200,
                {
                    "reason": "invalid_worker_deregistration_response",
                    "message": f"expected JSON object, got {type(data).__name__}",
                },
            )
        return data

    async def heartbeat_worker(
        self,
        *,
        worker_id: str,
        task_slots: dict[str, int] | None = None,
        process_metrics: dict[str, Any] | None = None,
        heartbeat_interval_seconds: int | None = None,
    ) -> Any:
        """Send a worker-fleet heartbeat to refresh liveness and report state.

        Workers should call this on a steady cadence (default 60s, advertised
        by the server in the register/heartbeat acknowledgement) so operators
        can answer "what workers are polling task queue X right now, what's
        their slot capacity, when did each last check in" via the worker
        management API, the CLI worker listing, and the operator Worker
        Status view.

        ``task_slots`` is an optional dict with any subset of
        ``workflow_available``, ``activity_available``, ``session_available``
        — the count of currently free slots for each family. The server
        clamps each value into ``[0, max_concurrent_*]``.

        ``process_metrics`` is an optional dict with any subset of
        ``cpu_percent``, ``memory_bytes``, ``process_uptime_seconds``,
        ``process_id``, ``process_started_at``, and ``host`` — the SDK
        reports only what it has cheap access to, and the server records
        exactly what was reported.

        Returns the server acknowledgement, which includes the advertised
        ``heartbeat_interval_seconds`` and ``stale_after_seconds`` so the
        worker can adapt its cadence on the fly.

        Most applications create a :class:`~durable_workflow.Worker`, which
        runs this on a background asyncio task — call this directly only when
        driving the worker protocol by hand (smoke tests, custom runtimes).
        """
        body: dict[str, Any] = {"worker_id": worker_id}
        if task_slots:
            body["task_slots"] = task_slots
        if process_metrics:
            body["process_metrics"] = process_metrics
        if heartbeat_interval_seconds is not None:
            body["heartbeat_interval_seconds"] = heartbeat_interval_seconds
        return await self._request("POST", "/worker/heartbeat", worker=True, json=body)

    async def poll_workflow_task_response(
        self,
        *,
        worker_id: str,
        task_queue: str,
        timeout: float = 35.0,
        build_id: str | None = None,
        history_page_size: int | None = None,
        poll_request_id: str | None = None,
        task_kinds: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Long-poll for the next workflow task on ``task_queue``.

        Returns the full worker-protocol response envelope, including
        ``poll_status`` when the server has no task to lease. Worker-plane
        endpoint — most applications use :class:`~durable_workflow.Worker`
        rather than calling this directly. ``timeout`` controls the server
        long-poll window; the HTTP request uses a small grace margin.

        ``task_kinds`` opts into the Server's multiplexed workflow-work poll
        contract. Validator-capable workers pass ``("workflow",
        "update_validation")`` so one admission reservation and one long poll
        can discover either kind without leasing more work than the worker can
        execute. Servers advertise this additive request shape through
        ``worker_protocol.server_capabilities.synchronous_update_validation``.
        """
        body: dict[str, Any] = {
            "worker_id": worker_id,
            "task_queue": task_queue,
            "poll_request_id": poll_request_id or f"wf-poll-{uuid.uuid4().hex}",
        }
        timeout_seconds = _worker_poll_timeout_seconds(timeout)
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        if build_id:
            body["build_id"] = build_id
        if history_page_size is not None:
            body["history_page_size"] = history_page_size
        if task_kinds is not None:
            if isinstance(task_kinds, str):
                raise ValueError("task_kinds must be a sequence of task-kind strings")
            normalized_task_kinds = list(task_kinds)
            if not normalized_task_kinds or any(
                not isinstance(task_kind, str) or task_kind == ""
                for task_kind in normalized_task_kinds
            ):
                raise ValueError("task_kinds must contain at least one non-empty string")
            if len(set(normalized_task_kinds)) != len(normalized_task_kinds):
                raise ValueError("task_kinds must not contain duplicates")
            body["task_kinds"] = normalized_task_kinds
        http_timeout = _worker_poll_http_timeout(timeout)

        for _ in range(2):
            try:
                data = await self._request(
                    "POST",
                    "/worker/workflow-tasks/poll",
                    worker=True,
                    json=body,
                    timeout=http_timeout,
                )
            except httpx.TimeoutException:
                continue

            return data if isinstance(data, dict) else {}

        return {"task": None, "poll_status": "timeout"}

    async def poll_workflow_task(
        self,
        *,
        worker_id: str,
        task_queue: str,
        timeout: float = 35.0,
        build_id: str | None = None,
        history_page_size: int | None = None,
        poll_request_id: str | None = None,
        task_kinds: Sequence[str] | None = None,
    ) -> Any:
        """Long-poll for the next workflow task on ``task_queue``.

        Returns the task payload, or ``None`` on poll timeout. Worker-plane
        endpoint — most applications use :class:`~durable_workflow.Worker`
        rather than calling this directly. ``timeout`` controls the server
        long-poll window; the HTTP request uses a small grace margin.
        """
        data = await self.poll_workflow_task_response(
            worker_id=worker_id,
            task_queue=task_queue,
            timeout=timeout,
            build_id=build_id,
            history_page_size=history_page_size,
            poll_request_id=poll_request_id,
            task_kinds=task_kinds,
        )

        return data.get("task")

    async def complete_workflow_task(
        self,
        *,
        task_id: str,
        lease_owner: str,
        workflow_task_attempt: int,
        commands: list[dict[str, Any]],
        message_stream_cursors: list[dict[str, Any]] | None = None,
        message_stream_waits: list[dict[str, Any]] | None = None,
    ) -> Any:
        """Report successful execution of a workflow task with its emitted commands.

        Worker-plane endpoint, called by :class:`~durable_workflow.Worker`.
        ``commands`` is the list of serialized commands the workflow yielded
        for this task.
        """
        if (message_stream_cursors or message_stream_waits) and not _worker_protocol_supports_message_streams():
            raise ValueError("message stream completion metadata requires worker protocol 1.15 or newer")

        body: dict[str, Any] = {
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "commands": commands,
        }
        if message_stream_cursors:
            body["message_stream_cursors"] = message_stream_cursors
        if message_stream_waits:
            body["message_stream_waits"] = message_stream_waits
        return await self._request(
            "POST", f"/worker/workflow-tasks/{task_id}/complete", worker=True, json=body
        )

    async def fail_workflow_task(
        self,
        *,
        task_id: str,
        lease_owner: str,
        workflow_task_attempt: int,
        message: str,
        failure_type: str | None = None,
        stack_trace: str | None = None,
    ) -> Any:
        """Report a workflow task failure so the server can schedule a retry.

        Worker-plane endpoint. Task failures (e.g. non-determinism) are
        distinct from workflow failures (``FailWorkflow`` commands).
        """
        failure: dict[str, Any] = {"message": message}
        if failure_type is not None:
            failure["type"] = failure_type
        if stack_trace is not None:
            failure["stack_trace"] = stack_trace
        body: dict[str, Any] = {
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
            "failure": failure,
        }
        return await self._request(
            "POST", f"/worker/workflow-tasks/{task_id}/fail", worker=True, json=body
        )

    async def workflow_task_history(
        self,
        *,
        task_id: str,
        next_history_page_token: str | None = None,
        page_token: str | None = None,
        lease_owner: str,
        workflow_task_attempt: int,
    ) -> Any:
        """Page through extra history events while the worker is executing a long task.

        Worker-plane endpoint. The first page of history is delivered inline
        with the workflow task; this endpoint fetches subsequent pages.
        """
        if next_history_page_token is None:
            next_history_page_token = page_token
        elif page_token is not None and page_token != next_history_page_token:
            raise ValueError("page_token must match next_history_page_token when both are provided")
        if next_history_page_token is None:
            raise ValueError("next_history_page_token is required")

        body: dict[str, Any] = {
            "next_history_page_token": next_history_page_token,
            "lease_owner": lease_owner,
            "workflow_task_attempt": workflow_task_attempt,
        }
        return await self._request(
            "POST", f"/worker/workflow-tasks/{task_id}/history", worker=True, json=body
        )

    async def poll_query_task(
        self,
        *,
        worker_id: str,
        task_queue: str,
        timeout: float = 35.0,
        build_id: str | None = None,
        poll_request_id: str | None = None,
    ) -> Any:
        """Long-poll for the next workflow query task on ``task_queue``.

        Query tasks are ephemeral worker-plane requests created when the server
        must route a control-plane query to a non-PHP workflow runtime.
        ``timeout`` controls the server long-poll window; the HTTP request
        uses a small grace margin.
        """
        body: dict[str, Any] = {
            "worker_id": worker_id,
            "task_queue": task_queue,
            "poll_request_id": poll_request_id or f"query-poll-{uuid.uuid4().hex}",
        }
        timeout_seconds = _worker_poll_timeout_seconds(timeout)
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        if build_id:
            body["build_id"] = build_id
        http_timeout = _worker_poll_http_timeout(timeout)

        for _ in range(2):
            try:
                data = await self._request(
                    "POST",
                    "/worker/query-tasks/poll",
                    worker=True,
                    json=body,
                    timeout=http_timeout,
                )
            except httpx.TimeoutException:
                continue

            return (data or {}).get("task")

        return None

    async def complete_query_task(
        self,
        *,
        query_task_id: str,
        lease_owner: str,
        query_task_attempt: int,
        result: Any,
        codec: str = serializer.AVRO_CODEC,
        workflow_id: str | None = None,
        run_id: str | None = None,
        query_name: str | None = None,
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
    ) -> Any:
        """Submit a query result through the Avro payload envelope."""
        if codec != serializer.AVRO_CODEC:
            raise ValueError(
                "unsupported_payload_codec: workflow payload codec "
                f"{codec!r} is not supported by Durable Workflow 2.0; use codec='avro' "
                "with the fixed Avro Value schema and single-object framing. JSON remains "
                "the HTTP document transport, not a workflow payload codec."
            )
        body: dict[str, Any] = {
            "lease_owner": lease_owner,
            "query_task_attempt": query_task_attempt,
            "result": result,
            "result_envelope": self._payload_envelope(
                result,
                codec=codec,
                kind="query_result",
                workflow_id=workflow_id,
                run_id=run_id,
                query_name=query_name,
                external_storage=external_storage,
                external_storage_threshold_bytes=external_storage_threshold_bytes,
            ),
        }
        return await self._request(
            "POST", f"/worker/query-tasks/{query_task_id}/complete", worker=True, json=body
        )

    async def fail_query_task(
        self,
        *,
        query_task_id: str,
        lease_owner: str,
        query_task_attempt: int,
        message: str,
        reason: str = "query_rejected",
        failure_type: str | None = None,
        stack_trace: str | None = None,
    ) -> Any:
        """Report a failed worker-routed query task."""
        failure: dict[str, Any] = {"message": message, "reason": reason}
        if failure_type is not None:
            failure["type"] = failure_type
        if stack_trace is not None:
            failure["stack_trace"] = stack_trace
        body: dict[str, Any] = {
            "lease_owner": lease_owner,
            "query_task_attempt": query_task_attempt,
            "failure": failure,
        }
        return await self._request("POST", f"/worker/query-tasks/{query_task_id}/fail", worker=True, json=body)

    async def poll_update_validation_task(
        self,
        *,
        worker_id: str,
        task_queue: str,
        timeout: float = 35.0,
    ) -> Any:
        """Long-poll for a synchronous pre-accept update validation task."""
        body: dict[str, Any] = {
            "worker_id": worker_id,
            "task_queue": task_queue,
        }
        timeout_seconds = _worker_poll_timeout_seconds(timeout)
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        http_timeout = _worker_poll_http_timeout(timeout)

        for _ in range(2):
            try:
                data = await self._request(
                    "POST",
                    "/worker/update-validation-tasks/poll",
                    worker=True,
                    json=body,
                    timeout=http_timeout,
                )
            except httpx.TimeoutException:
                continue

            return (data or {}).get("task")

        return None

    async def approve_update_validation_task(
        self,
        *,
        update_validation_task_id: str,
        lease_owner: str,
        update_validation_attempt: int,
    ) -> Any:
        """Approve one leased update validation task."""
        return await self._request(
            "POST",
            f"/worker/update-validation-tasks/{quote(update_validation_task_id, safe='')}/approve",
            worker=True,
            json={
                "lease_owner": lease_owner,
                "update_validation_attempt": update_validation_attempt,
            },
        )

    async def reject_update_validation_task(
        self,
        *,
        update_validation_task_id: str,
        lease_owner: str,
        update_validation_attempt: int,
        message: str,
        reason: str,
        failure_type: str | None = None,
        stack_trace: str | None = None,
        validation_errors: dict[str, Any] | None = None,
    ) -> Any:
        """Return a typed validator rejection or validation execution failure."""
        failure: dict[str, Any] = {"message": message, "reason": reason}
        if failure_type is not None:
            failure["type"] = failure_type
        if stack_trace is not None:
            failure["stack_trace"] = stack_trace
        if validation_errors is not None:
            failure["validation_errors"] = validation_errors
        return await self._request(
            "POST",
            f"/worker/update-validation-tasks/{quote(update_validation_task_id, safe='')}/reject",
            worker=True,
            json={
                "lease_owner": lease_owner,
                "update_validation_attempt": update_validation_attempt,
                "failure": failure,
            },
        )

    async def poll_activity_task(
        self,
        *,
        worker_id: str,
        task_queue: str,
        timeout: float = 35.0,
        build_id: str | None = None,
        poll_request_id: str | None = None,
    ) -> Any:
        """Long-poll for the next activity task on ``task_queue``.

        Returns the task payload, or ``None`` on poll timeout. Worker-plane
        endpoint — typically used by :class:`~durable_workflow.Worker`.
        ``timeout`` controls the server long-poll window; the HTTP request
        uses a small grace margin.
        """
        body: dict[str, Any] = {
            "worker_id": worker_id,
            "task_queue": task_queue,
            "poll_request_id": poll_request_id or f"activity-poll-{uuid.uuid4().hex}",
        }
        timeout_seconds = _worker_poll_timeout_seconds(timeout)
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        if build_id:
            body["build_id"] = build_id
        http_timeout = _worker_poll_http_timeout(timeout)

        for _ in range(2):
            try:
                data = await self._request(
                    "POST",
                    "/worker/activity-tasks/poll",
                    worker=True,
                    json=body,
                    timeout=http_timeout,
                )
            except httpx.TimeoutException:
                continue

            return (data or {}).get("task")

        return None

    async def complete_activity_task(
        self,
        *,
        task_id: str,
        activity_attempt_id: str,
        lease_owner: str,
        result: Any,
        codec: str = serializer.AVRO_CODEC,
        activity_name: str | None = None,
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
    ) -> Any:
        """Report successful activity execution and submit the encoded result."""
        body: dict[str, Any] = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "result": self._payload_envelope(
                result,
                codec=codec,
                kind="activity_result",
                activity_name=activity_name,
                external_storage=external_storage,
                external_storage_threshold_bytes=external_storage_threshold_bytes,
            ),
        }
        return await self._request(
            "POST", f"/worker/activity-tasks/{task_id}/complete", worker=True, json=body
        )

    async def fail_activity_task(
        self,
        *,
        task_id: str,
        activity_attempt_id: str,
        lease_owner: str,
        message: str,
        failure_type: str | None = None,
        failure_class: str | None = None,
        failure_code: int | None = None,
        stack_trace: str | None = None,
        non_retryable: bool = False,
        details: Any | None = None,
        codec: str = serializer.AVRO_CODEC,
        activity_name: str | None = None,
    ) -> Any:
        """Report a failed activity attempt.

        Pass ``non_retryable=True`` to signal that this class of error will
        not be fixed by retrying — the server then surfaces the failure to
        the workflow immediately instead of scheduling another attempt.
        """
        failure: dict[str, Any] = {"message": message}
        if failure_type is not None:
            failure["type"] = failure_type
        if failure_class is not None:
            failure["class"] = failure_class
        if failure_code is not None:
            failure["code"] = failure_code
        if stack_trace is not None:
            failure["stack_trace"] = stack_trace
        if non_retryable:
            failure["non_retryable"] = True
        if details is not None:
            failure["details"] = self._payload_envelope(
                details,
                codec=codec,
                kind="activity_failure_details",
                activity_name=activity_name,
            )
        body: dict[str, Any] = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
            "failure": failure,
        }
        return await self._request(
            "POST", f"/worker/activity-tasks/{task_id}/fail", worker=True, json=body
        )

    async def heartbeat_activity_task(
        self,
        *,
        task_id: str,
        activity_attempt_id: str,
        lease_owner: str,
        details: dict[str, Any] | None = None,
    ) -> Any:
        """Send a liveness heartbeat for a running activity attempt.

        Worker-plane endpoint. Most code calls
        :meth:`~durable_workflow.ActivityContext.heartbeat` instead, which
        additionally raises :class:`~durable_workflow.errors.ActivityCancelled`
        when the server reports the activity should stop.
        """
        body: dict[str, Any] = {
            "activity_attempt_id": activity_attempt_id,
            "lease_owner": lease_owner,
        }
        if details is not None:
            body["details"] = details
        return await self._request(
            "POST", f"/worker/activity-tasks/{task_id}/heartbeat", worker=True, json=body
        )
