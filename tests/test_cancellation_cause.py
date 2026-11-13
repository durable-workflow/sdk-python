"""The optional top-level ``cause`` of a cancellation context (workflow#863).

Vectors ported from Core's ``tests/Unit/V2/CancellationContextCauseTest.php``.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import pytest

from durable_workflow import (
    CancellationContext,
    ScopedCancellationContext,
    WorkflowCancelled,
    WorkflowTimeoutCleanupRequested,
)
from durable_workflow.errors import _cancellation_requested
from durable_workflow.workflow import CompleteWorkflow, FailWorkflow, WorkflowContext, replay


def snapshot() -> dict[str, Any]:
    return {
        "schema": "durable-workflow.cancellation-context/v1",
        "request_id": "req-1",
        "root_request_id": "req-1",
        "root_workflow_instance_id": "instance-1",
        "root_workflow_run_id": "run-1",
        "parent_request_id": None,
        "reason": "work deadline passed",
        "requester": {"type": "system", "id": "timeout_cleanup"},
        "source": "system",
        "requested_at": "2026-11-13T00:01:00.000000Z",
        "cleanup_deadline_at": "2026-11-13T00:01:10.000000Z",
        "lineage": [{"request_id": "req-1", "workflow_instance_id": "instance-1", "workflow_run_id": "run-1"}],
    }


def timeout_snapshot(**fields: Any) -> dict[str, Any]:
    return {**snapshot(), "cause": "timeout", **fields}


def test_an_ordinary_context_is_unchanged_and_stays_a_cancellation() -> None:
    context = CancellationContext.from_dict(snapshot())

    assert context.cause is None
    assert not context.is_timeout
    assert context.to_dict() == snapshot()
    assert json.dumps(context.to_dict()) == json.dumps(snapshot())
    assert type(_cancellation_requested("cancelled", request_id="req-1", context=context)) is WorkflowCancelled


def test_an_explicit_cancellation_cause_is_ordinary_and_round_trips() -> None:
    context = CancellationContext.from_dict({**snapshot(), "cause": "cancellation"})

    assert context.cause == "cancellation"
    assert not context.is_timeout
    assert context.to_dict() == {**snapshot(), "cause": "cancellation"}
    assert type(_cancellation_requested("cancelled", request_id="req-1", context=context)) is WorkflowCancelled


def test_a_null_cause_is_an_ordinary_cancellation() -> None:
    context = CancellationContext.from_dict({**snapshot(), "cause": None})

    assert context.cause is None
    assert context == CancellationContext.from_dict(snapshot())


def test_a_timeout_cause_round_trips_and_delivers_the_timeout_subtype() -> None:
    context = CancellationContext.from_dict(timeout_snapshot())

    assert context.cause == "timeout"
    assert context.is_timeout
    assert context.timeout_kind is None and context.work_deadline_at is None
    assert context.to_dict() == timeout_snapshot()
    assert list(context.to_dict()).index("cause") == list(context.to_dict()).index("lineage") + 1
    exception = _cancellation_requested("cancelled", request_id="req-1", context=context)
    assert isinstance(exception, WorkflowTimeoutCleanupRequested)
    assert isinstance(exception, WorkflowCancelled)
    assert exception.context is context
    assert exception.request_id == "req-1"


def test_timeout_fields_round_trip() -> None:
    value = timeout_snapshot(timeout_kind="execution", work_deadline_at="2026-11-13T00:01:00.000000Z")
    context = CancellationContext.from_dict(value)

    assert context.timeout_kind == "execution"
    assert context.work_deadline_at == datetime(2026, 11, 13, 0, 1, tzinfo=timezone.utc)
    assert context.cleanup_deadline_at == datetime(2026, 11, 13, 0, 1, 10, tzinfo=timezone.utc)
    assert context.to_dict() == value
    assert CancellationContext.from_dict(context.to_dict()) == context


def test_the_timeout_subtype_is_base_exception_rooted_and_not_task_cancellation() -> None:
    assert issubclass(WorkflowTimeoutCleanupRequested, WorkflowCancelled)
    assert issubclass(WorkflowTimeoutCleanupRequested, BaseException)
    assert not issubclass(WorkflowTimeoutCleanupRequested, Exception)
    assert not issubclass(WorkflowTimeoutCleanupRequested, asyncio.CancelledError)


def test_a_descendant_is_cancelled_not_timed_out() -> None:
    # Built the way Core's forDescendant builds it: the cause is not inherited.
    descendant = timeout_snapshot()
    del descendant["cause"]
    descendant.update({
        "request_id": "req-child", "parent_request_id": "req-1",
        "lineage": [*descendant["lineage"], {
            "request_id": "req-child", "workflow_instance_id": "child-instance", "workflow_run_id": "child-run",
        }],
    })
    context = CancellationContext.from_dict(descendant)

    assert not context.is_timeout
    assert context.to_dict() == descendant
    assert type(_cancellation_requested("cancelled", request_id="req-child", context=context)) is WorkflowCancelled


@pytest.mark.parametrize("cause", ["deadline", "Timeout", "", 1, True, ["timeout"], {"cause": "timeout"}])
def test_an_unknown_cause_is_refused(cause: Any) -> None:
    with pytest.raises(ValueError, match="cause must be cancellation or timeout"):
        CancellationContext.from_dict({**snapshot(), "cause": cause})


@pytest.mark.parametrize("cause", [None, "cancellation"])
@pytest.mark.parametrize("field,value", [
    ("timeout_kind", "execution"), ("work_deadline_at", "2026-11-13T00:01:00.000000Z"),
])
def test_timeout_fields_require_the_timeout_cause(cause: str | None, field: str, value: str) -> None:
    ordinary = snapshot() if cause is None else {**snapshot(), "cause": cause}
    with pytest.raises(ValueError, match="timeout fields require the timeout cause"):
        CancellationContext.from_dict({**ordinary, field: value})


@pytest.mark.parametrize("field,value,message", [
    ("timeout_kind", "workflow", "timeout_kind must be execution or run"),
    ("timeout_kind", 1, "timeout_kind must be execution or run"),
    ("work_deadline_at", "2026-11-13 00:01:00", "ISO date, time and timezone"),
    ("work_deadline_at", 1, "ISO date, time and timezone"),
    ("work_deadline_at", "2026-11-13T00:01:10.000000Z", "work deadline must precede its cleanup deadline"),
    ("work_deadline_at", "2026-11-13T00:02:00.000000Z", "work deadline must precede its cleanup deadline"),
])
def test_invalid_timeout_fields_are_refused(field: str, value: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        CancellationContext.from_dict(timeout_snapshot(**{field: value}))


def test_a_scoped_root_keeps_the_run_request_cause() -> None:
    # Core's ScopedCancellationContext::fromRunContext keeps the run context's keys.
    scoped = ScopedCancellationContext.from_dict({
        "schema": "durable-workflow.scoped-cancellation-context/v1",
        "root_context": timeout_snapshot(),
        "lineage": [{
            "request_id": "req-1", "workflow_instance_id": "instance-1", "workflow_run_id": "run-1",
            "scope_id": "root", "cleanup_deadline_at": "2026-11-13T00:01:10.000000Z",
        }],
    })

    assert scoped.root_context.is_timeout
    assert scoped.to_dict()["root_context"] == timeout_snapshot()


def request(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationRequested", "recorded_at": "2026-11-13T00:01:00Z",
        "payload": {
            "workflow_command_id": "req-1", "workflow_instance_id": "instance-1", "workflow_run_id": "run-1",
            "reason": "work deadline passed", "cleanup_deadline_at": "2026-11-13T00:01:10.000000Z",
            "cancellation": context,
        },
    }


def delivery(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationDelivered",
        "payload": {
            "workflow_command_id": "req-1", "workflow_run_id": "run-1", "sequence": 1,
            "call_kind": "timer", "cancellation": context,
        },
    }


class ExistingCancellationHandler:
    """Cancellation handling written before timeout cleanup existed."""

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_timer(10)
        except WorkflowCancelled as cancelled:
            assert cancelled.context is ctx.cancellation_context
            try:
                ctx.throw_if_cancellation_requested()
            except WorkflowCancelled as again:
                assert type(again) is type(cancelled)
            return {"type": type(cancelled).__name__, "cause": cancelled.context.cause}
        return None


@pytest.mark.parametrize("context,expected", [
    (snapshot(), {"type": "WorkflowCancelled", "cause": None}),
    (timeout_snapshot(), {"type": "WorkflowTimeoutCleanupRequested", "cause": "timeout"}),
])
def test_existing_cancellation_handlers_catch_the_timeout_at_the_authored_call(
    context: dict[str, Any], expected: dict[str, Any],
) -> None:
    outcome = replay(ExistingCancellationHandler, [request(context), delivery(context)], [], run_id="run-1")

    assert outcome.commands == [CompleteWorkflow(expected)]


class Uncaught:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.start_timer(10)


def test_an_uncaught_timeout_request_reports_its_type() -> None:
    outcome = replay(Uncaught, [request(timeout_snapshot()), delivery(timeout_snapshot())], [], run_id="run-1")

    assert len(outcome.commands) == 1 and isinstance(outcome.commands[0], FailWorkflow)
    assert outcome.commands[0].exception_type == "WorkflowTimeoutCleanupRequested"
