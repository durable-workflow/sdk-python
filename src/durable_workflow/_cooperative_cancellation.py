"""Canonical cancellation state used by service workflow replay.

An observation supplies request identity. Only the durable delivery event
authorizes an exception at an authored workflow call.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .cancellation import CancellationContext
from .errors import NonDeterministicReplayError

REQUEST_EVENT = "CooperativeCancellationRequested"
DELIVERY_EVENT = "CooperativeCancellationDelivered"
_CALL_KINDS = {"activity", "local_activity", "timer", "condition", "signal", "child", "parallel", "selection_handle"}
_RESOLUTION_EVENTS = {
    "ActivityCompleted",
    "ActivityFailed",
    "ActivityCancelled",
    "ActivityTimedOut",
    "TimerFired",
    "TimerCancelled",
    "ConditionWaitSatisfied",
    "ConditionWaitTimedOut",
    "SignalApplied",
    "ChildRunCompleted",
    "ChildRunFailed",
    "ChildRunCancelled",
    "ChildRunTerminated",
}


def _invalid(detail: str, sequence: int = 0) -> NonDeterministicReplayError:
    return NonDeterministicReplayError(
        sequence,
        "canonical cooperative cancellation",
        [REQUEST_EVENT, DELIVERY_EVENT],
        detail=detail,
    )


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _invalid(f"{field} must be a non-empty string")
    return value


def _positive(value: Any, field: str, *, maximum: int = 2**63 - 1) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise _invalid(f"{field} must be a positive integer within {maximum}")
    return value


def _timestamp(value: Any, field: str) -> str:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _invalid(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise _invalid(f"{field} must include its timezone")
    return text


@dataclass(frozen=True)
class CancellationRequest:
    request_id: str
    requested_at: str
    cleanup_deadline_at: str
    history_refresh_page_token: str | None = None
    context: CancellationContext | None = None

    @classmethod
    def from_observation(cls, value: Mapping[str, Any]) -> CancellationRequest:
        request = cls(
            _text(value.get("request_id"), "request_id"),
            _timestamp(value.get("requested_at"), "requested_at"),
            _timestamp(value.get("cleanup_deadline_at"), "cleanup_deadline_at"),
            _text(value.get("history_refresh_page_token"), "history_refresh_page_token"),
        )
        request._validate_deadline()
        return request

    def _validate_deadline(self) -> None:
        requested = datetime.fromisoformat(self.requested_at.replace("Z", "+00:00"))
        deadline = datetime.fromisoformat(self.cleanup_deadline_at.replace("Z", "+00:00"))
        if deadline <= requested:
            raise _invalid("cleanup deadline must follow the original request")


@dataclass(frozen=True)
class CancellationDelivery:
    request_id: str
    sequence: int
    call_kind: str
    sequence_span: int = 1
    operation_sequence: int | None = None
    operation_sequence_span: int = 1

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> CancellationDelivery:
        request_id = _text(payload.get("workflow_command_id"), "workflow_command_id")
        sequence = _positive(payload.get("sequence"), "sequence")
        kind = _text(payload.get("call_kind"), "call_kind")
        span = _positive(payload.get("sequence_span", 1), "sequence_span", maximum=1000)
        operation = payload.get("operation_sequence")
        operation_span = _positive(payload.get("operation_sequence_span", 1), "operation_sequence_span", maximum=1000)
        if kind not in _CALL_KINDS or (kind != "parallel" and span != 1):
            raise _invalid("delivery kind and span do not describe a durable call", sequence)
        if sequence > 2**63 - 1 - span:
            raise _invalid("delivery sequence range overflows", sequence)
        if kind == "selection_handle":
            operation = _positive(operation, "operation_sequence")
            if operation >= sequence or operation_span > sequence - operation:
                raise _invalid("selection handle must name an earlier authored operation", sequence)
        elif operation is not None or operation_span != 1:
            raise _invalid("only selection handles may carry an operation range", sequence)
        return cls(request_id, sequence, kind, span, operation, operation_span)

    def interrupts(self, sequence: int) -> bool:
        base = self.operation_sequence if self.operation_sequence is not None else self.sequence
        span = self.operation_sequence_span if self.operation_sequence is not None else self.sequence_span
        return base <= sequence < base + span


@dataclass(frozen=True)
class CancellationHistory:
    request: CancellationRequest | None
    delivery: CancellationDelivery | None
    request_index: int
    delivery_index: int | None
    resolved_before_request: frozenset[int]
    failed_before_request: frozenset[int]
    selected_before_request: frozenset[tuple[int, int]]

    def eligible(self, sequence: int, span: int = 1) -> bool:
        if self.request is None or self.delivery is not None:
            return False
        sequences = set(range(sequence, sequence + span))
        return (
            not sequences <= self.resolved_before_request
            and not sequences & self.failed_before_request
            and (sequence, span) not in self.selected_before_request
        )


def read_cancellation_history(
    events: Sequence[dict[str, Any]],
    *,
    run_id: str = "",
    observation: Mapping[str, Any] | None = None,
) -> CancellationHistory:
    request = CancellationRequest.from_observation(observation) if observation is not None else None
    request_index = len(events)
    delivery: CancellationDelivery | None = None
    delivery_index: int | None = None
    saw_request = False
    for index, event in enumerate(events):
        kind = event.get("event_type") or event.get("type")
        if kind not in {REQUEST_EVENT, DELIVERY_EVENT}:
            continue
        payload = event.get("payload")
        if not isinstance(payload, Mapping):
            raise _invalid("canonical event is missing its payload")
        request_id = _text(payload.get("workflow_command_id"), "workflow_command_id")
        if event.get("workflow_command_id", request_id) != request_id:
            raise _invalid("event and payload disagree on the original request identity")
        event_run = _text(payload.get("workflow_run_id"), "workflow_run_id")
        if run_id and event_run != run_id:
            raise _invalid("cancellation belongs to a different workflow run")
        if kind == REQUEST_EVENT:
            if saw_request or delivery is not None:
                raise _invalid("history must contain one request before delivery")
            context = None
            if "cancellation" in payload:
                snapshot = payload["cancellation"]
                if not isinstance(snapshot, Mapping):
                    raise _invalid("canonical cancellation context must be an object")
                try:
                    context = CancellationContext.from_dict(snapshot)
                except ValueError as error:
                    raise _invalid(str(error)) from error
                local = context.lineage[-1]
                recorded_at = _timestamp(event.get("recorded_at", event.get("timestamp")), "recorded_at")
                if (
                    context.request_id != request_id or local.workflow_run_id != event_run
                    or ("workflow_instance_id" in payload
                        and local.workflow_instance_id != payload["workflow_instance_id"])
                    or context.deadline != datetime.fromisoformat(
                        _timestamp(payload.get("cleanup_deadline_at"), "cleanup_deadline_at").replace("Z", "+00:00"),
                    )
                    or context.requested_at > datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
                    or "reason" in payload and context.reason != payload["reason"]
                ):
                    raise _invalid("canonical cancellation context does not match its request event")
            canonical_request = CancellationRequest(
                request_id,
                context.requested_at.isoformat(timespec="microseconds").replace("+00:00", "Z")
                if context is not None
                else request.requested_at
                if request is not None
                else _timestamp(
                    event.get("recorded_at", event.get("timestamp")),
                    "requested_at",
                ),
                _timestamp(payload.get("cleanup_deadline_at"), "cleanup_deadline_at"),
                request.history_refresh_page_token if request is not None else None,
                context,
            )
            if request is not None and (
                request.request_id != canonical_request.request_id
                or datetime.fromisoformat(request.cleanup_deadline_at.replace("Z", "+00:00"))
                != datetime.fromisoformat(canonical_request.cleanup_deadline_at.replace("Z", "+00:00"))
            ):
                raise _invalid("observation changes the original request or cleanup deadline")
            request = canonical_request
            request_index = index
            saw_request = True
        else:
            if not saw_request or request is None or delivery is not None:
                raise _invalid("delivery requires one earlier canonical request and one marker")
            delivery = CancellationDelivery.from_payload(payload)
            if delivery.request_id != request.request_id:
                raise _invalid("delivery names a different request", delivery.sequence)
            if "cancellation" in payload:
                snapshot = payload["cancellation"]
                if not isinstance(snapshot, Mapping) or request.context is None:
                    raise _invalid("delivery changes the canonical cancellation context", delivery.sequence)
                try:
                    delivered_context = CancellationContext.from_dict(snapshot)
                except ValueError as error:
                    raise _invalid(str(error), delivery.sequence) from error
                if delivered_context != request.context:
                    raise _invalid("delivery changes the canonical cancellation context", delivery.sequence)
            delivery_index = index
    resolved: set[int] = set()
    failed: set[int] = set()
    selected: set[tuple[int, int]] = set()
    for event in events[:request_index]:
        kind = event.get("event_type") or event.get("type")
        payload = event.get("payload") or {}
        if not isinstance(payload, Mapping):
            continue
        sequence = payload.get("sequence")
        if kind == "SelectionResolved":
            base = payload.get("selection_group_base_sequence")
            span = payload.get("selection_group_size")
            if type(base) is int and base > 0 and type(span) is int and 1 <= span <= 1000:
                selected.add((base, span))
        elif kind == "SelectionOperationCancelled":
            base = payload.get("member_base_sequence")
            span = payload.get("member_size")
            if type(base) is int and base > 0 and type(span) is int and 1 <= span <= 1000:
                resolved.update(range(base, base + span))
        if type(sequence) is int and sequence > 0 and kind in _RESOLUTION_EVENTS:
            resolved.add(sequence)
            if kind in {
                "ActivityFailed",
                "ActivityTimedOut",
                "ActivityCancelled",
                "ChildRunFailed",
                "ChildRunCancelled",
                "ChildRunTerminated",
            }:
                failed.add(sequence)
    state = CancellationHistory(
        request, delivery, request_index, delivery_index, frozenset(resolved), frozenset(failed), frozenset(selected),
    )
    if delivery is not None:
        base = delivery.operation_sequence or delivery.sequence
        span = delivery.operation_sequence_span if delivery.operation_sequence is not None else delivery.sequence_span
        interrupted = set(range(base, base + span))
        if interrupted <= resolved or interrupted & failed or (base, span) in selected:
            raise _invalid("delivery cannot replace an earlier committed result", delivery.sequence)
    return state
