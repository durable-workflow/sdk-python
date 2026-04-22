from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

EXTERNAL_TASK_RESULT_SCHEMA = "durable-workflow.v2.external-task-result"
EXTERNAL_TASK_RESULT_CONTRACT_SCHEMA = "durable-workflow.v2.external-task-result.contract"
EXTERNAL_TASK_RESULT_MEDIA_TYPE = "application/vnd.durable-workflow.external-task-result+json"
EXTERNAL_TASK_RESULT_VERSION = 1

ExternalTaskResultKind = Literal["success", "failure", "malformed_output"]

_FAILURE_KINDS = {
    "application",
    "timeout",
    "cancellation",
    "malformed_output",
    "handler_crash",
    "decode_failure",
    "unsupported_payload",
}

_FAILURE_CLASSIFICATIONS = {
    "application_error",
    "timeout",
    "cancelled",
    "deadline_exceeded",
    "handler_crash",
    "decode_failure",
    "malformed_output",
    "unsupported_payload_codec",
    "unsupported_payload_reference",
}


@dataclass(frozen=True)
class ExternalTaskResultError(ValueError):
    """Raised when a handler result envelope violates the v1 result contract."""

    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True)
class ExternalTaskIdentity:
    id: str
    kind: str
    attempt: int
    idempotency_key: str


@dataclass(frozen=True)
class ExternalTaskFailure:
    kind: str
    classification: str
    message: str
    type: str | None
    stack_trace: str | None
    timeout_type: str | None
    cancelled: bool
    details: Mapping[str, Any] | None

    @property
    def deadline_exceeded(self) -> bool:
        return self.classification == "deadline_exceeded" or self.timeout_type == "deadline_exceeded"

    @property
    def handler_crash(self) -> bool:
        return self.classification == "handler_crash" or self.kind == "handler_crash"

    @property
    def decode_failure(self) -> bool:
        return self.classification == "decode_failure" or self.kind == "decode_failure"

    @property
    def unsupported_payload(self) -> bool:
        return self.kind == "unsupported_payload" or self.classification in {
            "unsupported_payload_codec",
            "unsupported_payload_reference",
        }


@dataclass(frozen=True)
class ExternalTaskResult:
    kind: ExternalTaskResultKind
    task: ExternalTaskIdentity
    recorded: bool
    metadata: Mapping[str, Any]
    result: Mapping[str, Any] | None = None
    failure: ExternalTaskFailure | None = None
    raw_output: Mapping[str, Any] | None = None
    retryable: bool = False

    @property
    def succeeded(self) -> bool:
        return self.kind == "success"

    @property
    def failed(self) -> bool:
        return self.kind in {"failure", "malformed_output"}

    @property
    def malformed_output(self) -> bool:
        return self.kind == "malformed_output"

    @property
    def cancelled(self) -> bool:
        return self.failure.cancelled if self.failure is not None else False

    @property
    def deadline_exceeded(self) -> bool:
        return self.failure.deadline_exceeded if self.failure is not None else False

    @property
    def handler_crash(self) -> bool:
        return self.failure.handler_crash if self.failure is not None else False

    @property
    def decode_failure(self) -> bool:
        return self.failure.decode_failure if self.failure is not None else False

    @property
    def unsupported_payload(self) -> bool:
        return self.failure.unsupported_payload if self.failure is not None else False

    @property
    def failure_kind(self) -> str | None:
        return self.failure.kind if self.failure is not None else None

    @property
    def failure_classification(self) -> str | None:
        return self.failure.classification if self.failure is not None else None


def parse_external_task_result(envelope: Mapping[str, Any]) -> ExternalTaskResult:
    """Parse and validate a v1 external task result envelope.

    The parser follows the server contract's additive-field policy: unknown
    optional fields are ignored, while required fields and known enum values are
    enforced so carriers can make stable retryability and failure decisions.
    """

    _require_value(envelope, "schema", EXTERNAL_TASK_RESULT_SCHEMA)
    _require_value(envelope, "version", EXTERNAL_TASK_RESULT_VERSION)

    outcome = _require_mapping(envelope, "outcome")
    status = _require_str(outcome, "status")
    recorded = _require_bool(outcome, "recorded")
    task = _parse_task(_require_mapping(envelope, "task"))
    metadata = _require_mapping(envelope, "metadata")

    if status == "succeeded":
        return ExternalTaskResult(
            kind="success",
            task=task,
            recorded=recorded,
            metadata=metadata,
            result=_require_nullable_mapping(envelope, "result"),
        )

    if status != "failed":
        raise ExternalTaskResultError(f"Unsupported external task result status [{status}].")

    failure = _parse_failure(_require_mapping(envelope, "failure"))
    retryable = _require_bool(outcome, "retryable")
    if failure.kind == "malformed_output" or failure.classification == "malformed_output" or "raw_output" in envelope:
        return ExternalTaskResult(
            kind="malformed_output",
            task=task,
            recorded=recorded,
            metadata=metadata,
            failure=failure,
            raw_output=_require_mapping(envelope, "raw_output"),
            retryable=retryable,
        )

    return ExternalTaskResult(
        kind="failure",
        task=task,
        recorded=recorded,
        metadata=metadata,
        failure=failure,
        retryable=retryable,
    )


def parse_external_task_result_artifact(artifact: Mapping[str, Any]) -> ExternalTaskResult:
    """Validate a cluster-info fixture artifact and parse its embedded example."""

    artifact_name = _require_str(artifact, "artifact")
    if not artifact_name.startswith("durable-workflow.v2.external-task-result."):
        raise ExternalTaskResultError(f"Unsupported external task result artifact [{artifact_name}].")

    _require_value(artifact, "media_type", EXTERNAL_TASK_RESULT_MEDIA_TYPE)
    _require_value(artifact, "schema", EXTERNAL_TASK_RESULT_SCHEMA)
    _require_value(artifact, "version", EXTERNAL_TASK_RESULT_VERSION)

    example = _require_mapping(artifact, "example")
    expected_sha = _require_str(artifact, "sha256")
    actual_sha = _sha256_json(example)
    if actual_sha != expected_sha:
        raise ExternalTaskResultError(
            f"External task result artifact [{artifact_name}] sha256 mismatch: "
            f"expected {expected_sha}, got {actual_sha}."
        )

    return parse_external_task_result(example)


def _parse_task(task: Mapping[str, Any]) -> ExternalTaskIdentity:
    attempt = _require_int(task, "attempt")
    if attempt < 1:
        raise ExternalTaskResultError("External task result task.attempt must be >= 1.")

    return ExternalTaskIdentity(
        id=_require_str(task, "id"),
        kind=_require_str(task, "kind"),
        attempt=attempt,
        idempotency_key=_require_str(task, "idempotency_key"),
    )


def _parse_failure(failure: Mapping[str, Any]) -> ExternalTaskFailure:
    kind = _require_str(failure, "kind")
    if kind not in _FAILURE_KINDS:
        raise ExternalTaskResultError(f"Unsupported external task failure kind [{kind}].")

    classification = _require_str(failure, "classification")
    if classification not in _FAILURE_CLASSIFICATIONS:
        raise ExternalTaskResultError(
            f"Unsupported external task failure classification [{classification}]."
        )

    cancelled = _require_bool(failure, "cancelled")
    if classification == "cancelled" and not cancelled:
        raise ExternalTaskResultError("Cancelled external task failures must set failure.cancelled=true.")

    return ExternalTaskFailure(
        kind=kind,
        classification=classification,
        message=_require_str(failure, "message"),
        type=_require_optional_str(failure, "type"),
        stack_trace=_require_optional_str(failure, "stack_trace"),
        timeout_type=_require_optional_str(failure, "timeout_type"),
        cancelled=cancelled,
        details=_require_nullable_mapping(failure, "details"),
    )


def _sha256_json(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    if key not in value:
        raise ExternalTaskResultError(f"External task result is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, Mapping):
        raise ExternalTaskResultError(f"External task result field [{key}] must be an object.")
    return item


def _require_nullable_mapping(value: Mapping[str, Any], key: str) -> Mapping[str, Any] | None:
    if key not in value:
        raise ExternalTaskResultError(f"External task result is missing required field [{key}].")
    item = value[key]
    if item is None:
        return None
    if not isinstance(item, Mapping):
        raise ExternalTaskResultError(f"External task result field [{key}] must be an object or null.")
    return item


def _require_str(value: Mapping[str, Any], key: str) -> str:
    if key not in value:
        raise ExternalTaskResultError(f"External task result is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, str):
        raise ExternalTaskResultError(f"External task result field [{key}] must be a string.")
    return item


def _require_optional_str(value: Mapping[str, Any], key: str) -> str | None:
    if key not in value:
        raise ExternalTaskResultError(f"External task result is missing required field [{key}].")
    item = value[key]
    if item is None:
        return None
    if not isinstance(item, str):
        raise ExternalTaskResultError(f"External task result field [{key}] must be a string or null.")
    return item


def _require_bool(value: Mapping[str, Any], key: str) -> bool:
    if key not in value:
        raise ExternalTaskResultError(f"External task result is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, bool):
        raise ExternalTaskResultError(f"External task result field [{key}] must be a boolean.")
    return item


def _require_int(value: Mapping[str, Any], key: str) -> int:
    if key not in value:
        raise ExternalTaskResultError(f"External task result is missing required field [{key}].")
    item = value[key]
    if not isinstance(item, int) or isinstance(item, bool):
        raise ExternalTaskResultError(f"External task result field [{key}] must be an integer.")
    return item


def _require_value(value: Mapping[str, Any], key: str, expected: object) -> None:
    if key not in value:
        raise ExternalTaskResultError(f"External task result is missing required field [{key}].")
    if value[key] != expected:
        raise ExternalTaskResultError(
            f"External task result field [{key}] must be [{expected}], got [{value[key]}]."
        )
