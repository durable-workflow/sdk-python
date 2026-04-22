from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

EXTERNAL_TASK_INPUT_SCHEMA = "durable-workflow.v2.external-task-input"
EXTERNAL_TASK_INPUT_CONTRACT_SCHEMA = "durable-workflow.v2.external-task-input.contract"
EXTERNAL_TASK_INPUT_MEDIA_TYPE = "application/vnd.durable-workflow.external-task-input+json"
EXTERNAL_TASK_INPUT_VERSION = 1

ExternalTaskInputKind = Literal["activity_task", "workflow_task"]


@dataclass(frozen=True)
class ExternalTaskInputError(ValueError):
    """Raised when a leased external task input envelope violates the v1 contract."""

    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class ExternalTaskInputIdentity:
    id: str
    kind: ExternalTaskInputKind
    attempt: int
    task_queue: str
    handler: str | None
    connection: str | None
    idempotency_key: str
    activity_attempt_id: str | None = None
    compatibility: str | None = None


@dataclass(frozen=True)
class ExternalTaskWorkflowContext:
    id: str
    run_id: str
    status: str | None = None
    resume: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ExternalTaskLease:
    owner: str
    expires_at: str
    heartbeat_endpoint: str


@dataclass(frozen=True)
class ExternalTaskInput:
    kind: ExternalTaskInputKind
    task: ExternalTaskInputIdentity
    workflow: ExternalTaskWorkflowContext
    lease: ExternalTaskLease
    payloads: Mapping[str, Any]
    headers: Mapping[str, Any]
    deadlines: Mapping[str, Any] | None = None
    history: Mapping[str, Any] | None = None

    @property
    def is_activity_task(self) -> bool:
        return self.kind == "activity_task"

    @property
    def is_workflow_task(self) -> bool:
        return self.kind == "workflow_task"


def parse_external_task_input(envelope: Mapping[str, Any]) -> ExternalTaskInput:
    """Parse and validate a v1 carrier-neutral external task input envelope.

    Unknown optional fields are ignored, matching the server contract's additive
    field policy. Required top-level and known nested fields are enforced so an
    SDK carrier can fail closed before invoking user handlers.
    """

    _require_value(envelope, "schema", EXTERNAL_TASK_INPUT_SCHEMA)
    _require_value(envelope, "version", EXTERNAL_TASK_INPUT_VERSION)

    task_fields = _require_mapping(envelope, "task")
    kind = _require_task_kind(task_fields, "kind")
    task = _parse_task(task_fields, kind)
    workflow = _parse_workflow(_require_mapping(envelope, "workflow"), kind)
    lease = _parse_lease(_require_mapping(envelope, "lease"))
    payloads = _parse_payloads(_require_mapping(envelope, "payloads"))
    headers = _require_mapping(envelope, "headers")

    if kind == "activity_task":
        return ExternalTaskInput(
            kind=kind,
            task=task,
            workflow=workflow,
            lease=lease,
            payloads=payloads,
            deadlines=_parse_deadlines(_require_mapping(envelope, "deadlines")),
            headers=headers,
        )

    return ExternalTaskInput(
        kind=kind,
        task=task,
        workflow=workflow,
        lease=lease,
        payloads=payloads,
        history=_parse_history(_require_mapping(envelope, "history")),
        headers=headers,
    )


def parse_external_task_input_artifact(artifact: Mapping[str, Any]) -> ExternalTaskInput:
    """Validate a cluster-info fixture artifact and parse its embedded example."""

    artifact_name = _require_str(artifact, "artifact")
    if not artifact_name.startswith("durable-workflow.v2.external-task-input."):
        raise ExternalTaskInputError(f"Unsupported external task input artifact [{artifact_name}].")

    _require_value(artifact, "media_type", EXTERNAL_TASK_INPUT_MEDIA_TYPE)
    _require_value(artifact, "schema", EXTERNAL_TASK_INPUT_SCHEMA)
    _require_value(artifact, "version", EXTERNAL_TASK_INPUT_VERSION)

    example = _require_mapping(artifact, "example")
    expected_sha = _require_str(artifact, "sha256")
    actual_sha = _sha256_json(example)
    if actual_sha != expected_sha:
        raise ExternalTaskInputError(
            f"External task input artifact [{artifact_name}] sha256 mismatch: "
            f"expected {expected_sha}, got {actual_sha}."
        )

    return parse_external_task_input(example)


def _parse_task(task: Mapping[str, Any], kind: ExternalTaskInputKind) -> ExternalTaskInputIdentity:
    attempt = _require_int(task, "attempt")
    if attempt < 1:
        raise ExternalTaskInputError("External task input task.attempt must be >= 1.")

    handler: str | None
    compatibility: str | None
    activity_attempt_id: str | None

    if kind == "activity_task":
        activity_attempt_id = _require_str(task, "activity_attempt_id")
        handler = _require_str(task, "handler")
        compatibility = None
    else:
        activity_attempt_id = None
        handler = _require_optional_str(task, "handler")
        compatibility = _require_optional_str(task, "compatibility")

    return ExternalTaskInputIdentity(
        id=_require_str(task, "id"),
        kind=kind,
        attempt=attempt,
        activity_attempt_id=activity_attempt_id,
        task_queue=_require_str(task, "task_queue"),
        handler=handler,
        connection=_require_optional_str(task, "connection"),
        compatibility=compatibility,
        idempotency_key=_require_str(task, "idempotency_key"),
    )


def _parse_workflow(workflow: Mapping[str, Any], kind: ExternalTaskInputKind) -> ExternalTaskWorkflowContext:
    if kind == "workflow_task":
        status = _require_optional_str(workflow, "status")
        resume = _require_mapping(workflow, "resume")
    else:
        status = None
        resume = None

    return ExternalTaskWorkflowContext(
        id=_require_str(workflow, "id"),
        run_id=_require_str(workflow, "run_id"),
        status=status,
        resume=resume,
    )


def _parse_lease(lease: Mapping[str, Any]) -> ExternalTaskLease:
    return ExternalTaskLease(
        owner=_require_str(lease, "owner"),
        expires_at=_require_str(lease, "expires_at"),
        heartbeat_endpoint=_require_str(lease, "heartbeat_endpoint"),
    )


def _parse_payloads(payloads: Mapping[str, Any]) -> Mapping[str, Any]:
    _require_nullable_mapping(payloads, "arguments")
    return payloads


def _parse_deadlines(deadlines: Mapping[str, Any]) -> Mapping[str, Any]:
    for key in ("schedule_to_start", "start_to_close", "schedule_to_close", "heartbeat"):
        _require_optional_str(deadlines, key)
    return deadlines


def _parse_history(history: Mapping[str, Any]) -> Mapping[str, Any]:
    _require_sequence(history, "events")
    _require_int(history, "last_sequence")
    _require_optional_str(history, "next_page_token")
    _require_optional_str(history, "encoding")
    return history


def _sha256_json(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    if key not in value:
        raise ExternalTaskInputError(f"External task input is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, Mapping):
        raise ExternalTaskInputError(f"External task input field [{key}] must be an object.")
    return item


def _require_nullable_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any] | None:
    if key not in value:
        raise ExternalTaskInputError(f"External task input is missing required field [{key}].")
    item = value[key]
    if item is None:
        return None
    if not isinstance(item, Mapping):
        raise ExternalTaskInputError(f"External task input field [{key}] must be an object or null.")
    return item


def _require_sequence(value: Mapping[str, Any], key: str) -> Sequence[Any]:
    if key not in value:
        raise ExternalTaskInputError(f"External task input is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, Sequence) or isinstance(item, (str, bytes, bytearray)):
        raise ExternalTaskInputError(f"External task input field [{key}] must be an array.")
    return item


def _require_task_kind(value: Mapping[str, Any], key: str) -> ExternalTaskInputKind:
    kind = _require_str(value, key)
    if kind == "activity_task" or kind == "workflow_task":
        return cast(ExternalTaskInputKind, kind)
    raise ExternalTaskInputError(f"Unsupported external task input kind [{kind}].")


def _require_str(value: Mapping[str, Any], key: str) -> str:
    if key not in value:
        raise ExternalTaskInputError(f"External task input is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, str):
        raise ExternalTaskInputError(f"External task input field [{key}] must be a string.")
    return item


def _require_optional_str(value: Mapping[str, Any], key: str) -> str | None:
    if key not in value:
        raise ExternalTaskInputError(f"External task input is missing required field [{key}].")
    item = value[key]
    if item is None:
        return None
    if not isinstance(item, str):
        raise ExternalTaskInputError(f"External task input field [{key}] must be a string or null.")
    return item


def _require_int(value: Mapping[str, Any], key: str) -> int:
    if key not in value:
        raise ExternalTaskInputError(f"External task input is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, int) or isinstance(item, bool):
        raise ExternalTaskInputError(f"External task input field [{key}] must be an integer.")
    return item


def _require_value(value: Mapping[str, Any], key: str, expected: object) -> None:
    if key not in value:
        raise ExternalTaskInputError(f"External task input is missing required field [{key}].")
    if value[key] != expected:
        raise ExternalTaskInputError(
            f"External task input field [{key}] must be {expected!r}; got {value[key]!r}."
        )
