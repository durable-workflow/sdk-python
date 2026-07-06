"""Nexus service-operation request and replay result helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .errors import NexusOperationFailed

NEXUS_OPERATION_RESULT_SCHEMA = "durable-workflow.v2.sdk-python.nexus-operation-result"
NEXUS_OPERATION_RESULT_VERSION = 1
NEXUS_CALLER_SDK_LANGUAGE = "sdk-python"

_FAILURE_STATUSES = {"failed", "cancelled", "canceled", "timed_out", "timeout"}
_FAILURE_OUTCOMES = {
    "failed",
    "handler_failed",
    "cancelled",
    "canceled",
    "timed_out",
    "timeout",
    "dispatch_failed",
    "handler_unavailable",
    "worker_unavailable",
}


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _json_default(value: Any) -> str:
    return str(value)


def _clean_dict(values: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in values.items() if value is not None}


def stable_nexus_idempotency_key(
    *,
    workflow_id: str,
    run_id: str,
    ordinal: int,
    endpoint_name: str,
    service_name: str,
    operation_name: str,
    arguments: Sequence[Any],
) -> str:
    """Return a deterministic idempotency key for one workflow-side Nexus call."""
    source = {
        "schema": "durable-workflow.v2.sdk-python.nexus-idempotency-key",
        "workflow_id": workflow_id,
        "run_id": run_id,
        "ordinal": ordinal,
        "endpoint_name": endpoint_name,
        "service_name": service_name,
        "operation_name": operation_name,
        "arguments": list(arguments),
    }
    encoded = json.dumps(source, sort_keys=True, separators=(",", ":"), default=_json_default)
    return f"dw-py-nexus-{hashlib.sha256(encoded.encode()).hexdigest()}"


def nexus_request_payload(
    *,
    arguments: Sequence[Any],
    payload_codec: str | None = None,
    mode: str | None = None,
    wait_for: str | None = None,
    wait_timeout_seconds: int | None = None,
    idempotency_key: str | None = None,
    caller_namespace: str | None = None,
    caller_workflow_instance_id: str | None = None,
    caller_workflow_run_id: str | None = None,
    target_workflow_instance_id: str | None = None,
    target_workflow_run_id: str | None = None,
    connection: str | None = None,
    queue: str | None = None,
    business_key: str | None = None,
    labels: Mapping[str, Any] | None = None,
    memo: Mapping[str, Any] | None = None,
    search_attributes: Mapping[str, Any] | None = None,
    duplicate_start_policy: str | None = None,
) -> dict[str, Any]:
    """Build the server execute-operation request body."""
    return _clean_dict({
        "arguments": list(arguments),
        "payload_codec": payload_codec,
        "mode_override": mode,
        "wait_for": wait_for,
        "wait_timeout_seconds": wait_timeout_seconds,
        "idempotency_key": idempotency_key,
        "caller_namespace": caller_namespace,
        "caller_workflow_instance_id": caller_workflow_instance_id,
        "caller_workflow_run_id": caller_workflow_run_id,
        "target_workflow_instance_id": target_workflow_instance_id,
        "target_workflow_run_id": target_workflow_run_id,
        "connection": connection,
        "queue": queue,
        "business_key": business_key,
        "labels": dict(labels) if labels is not None else None,
        "memo": dict(memo) if memo is not None else None,
        "search_attributes": dict(search_attributes) if search_attributes is not None else None,
        "duplicate_start_policy": duplicate_start_policy,
    })


@dataclass(frozen=True)
class NexusOperationResult:
    """Durable result recorded for a Nexus service operation."""

    accepted: bool
    service_call_id: str
    endpoint_name: str
    service_name: str
    operation_name: str
    caller_workflow_instance_id: str | None = None
    caller_workflow_run_id: str | None = None
    caller_sdk_language: str = NEXUS_CALLER_SDK_LANGUAGE
    service_sdk_language: str | None = None
    request_payload: dict[str, Any] = field(default_factory=dict)
    response_or_failure_surface: dict[str, Any] = field(default_factory=dict)
    artifact_tuple: dict[str, Any] | None = None
    published_artifact_worker_execution: bool | None = None
    status: str | None = None
    outcome: str | None = None
    reason: str | None = None
    message: str | None = None
    service_error_type: str | None = None
    caller_observed_error_type: str | None = None
    typed_error_message: str | None = None
    result: Any = None

    @classmethod
    def from_response(
        cls,
        data: Mapping[str, Any],
        *,
        endpoint_name: str,
        service_name: str,
        operation_name: str,
        request_payload: Mapping[str, Any],
        service_sdk_language: str | None = None,
        artifact_tuple: Mapping[str, Any] | None = None,
        published_artifact_worker_execution: bool | None = None,
    ) -> NexusOperationResult:
        call = data.get("service_call")
        source = call if isinstance(call, Mapping) else data
        service_call_id = (
            _optional_str(source.get("service_call_id"))
            or _optional_str(source.get("id"))
            or _optional_str(data.get("service_call_id"))
            or _optional_str(data.get("id"))
            or ""
        )
        accepted = data.get("accepted", source.get("accepted", True)) is not False
        raw = dict(data)
        response_value = source.get("result", data.get("result"))
        return cls(
            accepted=accepted,
            service_call_id=service_call_id,
            endpoint_name=endpoint_name,
            service_name=service_name,
            operation_name=_optional_str(source.get("operation_name")) or operation_name,
            caller_workflow_instance_id=_optional_str(source.get("caller_workflow_instance_id"))
            or _optional_str(data.get("caller_workflow_instance_id"))
            or _optional_str(request_payload.get("caller_workflow_instance_id")),
            caller_workflow_run_id=_optional_str(source.get("caller_workflow_run_id"))
            or _optional_str(data.get("caller_workflow_run_id"))
            or _optional_str(request_payload.get("caller_workflow_run_id")),
            service_sdk_language=service_sdk_language,
            request_payload=dict(request_payload),
            response_or_failure_surface=raw,
            artifact_tuple=dict(artifact_tuple) if artifact_tuple is not None else None,
            published_artifact_worker_execution=published_artifact_worker_execution,
            status=_optional_str(source.get("status")) or _optional_str(data.get("status")),
            outcome=_optional_str(data.get("outcome")) or _optional_str(source.get("outcome")),
            reason=_optional_str(data.get("reason")) or _optional_str(source.get("reason")),
            message=_optional_str(data.get("message")) or _optional_str(source.get("message")),
            service_error_type=_optional_str(source.get("service_error_type"))
            or _optional_str(data.get("service_error_type")),
            caller_observed_error_type=_optional_str(source.get("caller_observed_error_type"))
            or _optional_str(data.get("caller_observed_error_type")),
            typed_error_message=_optional_str(source.get("typed_error_message"))
            or _optional_str(data.get("typed_error_message")),
            result=response_value,
        )

    @classmethod
    def from_recorded_payload(cls, value: Any, *, expected: Mapping[str, Any]) -> NexusOperationResult:
        if not isinstance(value, Mapping):
            raise ValueError("recorded Nexus operation result must be a mapping")
        if value.get("schema") != NEXUS_OPERATION_RESULT_SCHEMA:
            raise ValueError("recorded Nexus operation result schema is not recognized")
        if value.get("version") != NEXUS_OPERATION_RESULT_VERSION:
            raise ValueError("recorded Nexus operation result version is not supported")

        for key in ("endpoint_name", "service_name", "operation_name", "idempotency_key"):
            expected_value = expected.get(key)
            recorded_value = value.get(key)
            if expected_value is not None and recorded_value is not None and expected_value != recorded_value:
                raise ValueError(
                    f"recorded Nexus operation {key} {recorded_value!r} does not match current "
                    f"workflow command {expected_value!r}"
                )

        response_surface = value.get("response_or_failure_surface")
        request_payload = value.get("request_payload")
        artifact_tuple = value.get("artifact_tuple")
        result = cls(
            accepted=value.get("accepted") is not False,
            service_call_id=str(value.get("service_call_id") or ""),
            endpoint_name=str(value.get("endpoint_name") or expected.get("endpoint_name") or ""),
            service_name=str(value.get("service_name") or expected.get("service_name") or ""),
            operation_name=str(value.get("operation_name") or expected.get("operation_name") or ""),
            caller_workflow_instance_id=_optional_str(value.get("caller_workflow_instance_id")),
            caller_workflow_run_id=_optional_str(value.get("caller_workflow_run_id")),
            caller_sdk_language=str(value.get("caller_sdk_language") or NEXUS_CALLER_SDK_LANGUAGE),
            service_sdk_language=_optional_str(value.get("service_sdk_language")),
            request_payload=dict(request_payload) if isinstance(request_payload, Mapping) else {},
            response_or_failure_surface=dict(response_surface) if isinstance(response_surface, Mapping) else {},
            artifact_tuple=dict(artifact_tuple) if isinstance(artifact_tuple, Mapping) else None,
            published_artifact_worker_execution=value.get("published_artifact_worker_execution")
            if isinstance(value.get("published_artifact_worker_execution"), bool)
            else None,
            status=_optional_str(value.get("status")),
            outcome=_optional_str(value.get("outcome")),
            reason=_optional_str(value.get("reason")),
            message=_optional_str(value.get("message")),
            service_error_type=_optional_str(value.get("service_error_type")),
            caller_observed_error_type=_optional_str(value.get("caller_observed_error_type")),
            typed_error_message=_optional_str(value.get("typed_error_message")),
            result=value.get("result"),
        )
        if result.is_failure:
            raise result.to_failure()
        return result

    @property
    def is_failure(self) -> bool:
        status = (self.status or "").lower()
        outcome = (self.outcome or "").lower()
        return (
            not self.accepted
            or status in _FAILURE_STATUSES
            or outcome in _FAILURE_OUTCOMES
            or outcome.startswith("rejected")
            or self.service_error_type is not None
            or self.caller_observed_error_type is not None
            or self.typed_error_message is not None
        )

    def to_failure(self) -> NexusOperationFailed:
        message = (
            self.typed_error_message
            or self.message
            or self.reason
            or self.outcome
            or self.status
            or "Nexus service operation failed"
        )
        return NexusOperationFailed(
            message,
            service_call_id=self.service_call_id or None,
            endpoint_name=self.endpoint_name,
            service_name=self.service_name,
            operation_name=self.operation_name,
            status=self.status,
            outcome=self.outcome,
            reason=self.reason,
            service_error_type=self.service_error_type,
            caller_observed_error_type=self.caller_observed_error_type,
            typed_error_message=self.typed_error_message,
            body=self.response_or_failure_surface,
        )

    def to_recorded_payload(self) -> dict[str, Any]:
        return _clean_dict({
            "schema": NEXUS_OPERATION_RESULT_SCHEMA,
            "version": NEXUS_OPERATION_RESULT_VERSION,
            "accepted": self.accepted,
            "service_call_id": self.service_call_id,
            "endpoint_name": self.endpoint_name,
            "service_name": self.service_name,
            "operation_name": self.operation_name,
            "idempotency_key": self.request_payload.get("idempotency_key"),
            "caller_workflow_instance_id": self.caller_workflow_instance_id,
            "caller_workflow_run_id": self.caller_workflow_run_id,
            "caller_sdk_language": self.caller_sdk_language,
            "service_sdk_language": self.service_sdk_language,
            "request_payload": self.request_payload,
            "response_or_failure_surface": self.response_or_failure_surface,
            "artifact_tuple": self.artifact_tuple,
            "published_artifact_worker_execution": self.published_artifact_worker_execution,
            "status": self.status,
            "outcome": self.outcome,
            "reason": self.reason,
            "message": self.message,
            "service_error_type": self.service_error_type,
            "caller_observed_error_type": self.caller_observed_error_type,
            "typed_error_message": self.typed_error_message,
            "result": self.result,
        })
