from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import CancellationContext
from durable_workflow._cooperative_cancellation import read_cancellation_history
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.workflow import CompleteWorkflow, FailWorkflow, ScheduleActivity, WorkflowContext, replay
from tests.test_cooperative_cancellation import completed_activity


def snapshot() -> dict[str, Any]:
    return json.loads((Path(__file__).parent / "fixtures/cooperative-cancellation-context.json").read_text())


def request() -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationRequested", "recorded_at": "2026-10-01T00:00:05Z",
        "payload": {
            "workflow_command_id": "request-1", "workflow_instance_id": "child-instance", "workflow_run_id": "run-1",
            "reason": "maintenance", "cleanup_deadline_at": "2026-10-01T00:00:30.123456Z", "cancellation": snapshot(),
        },
    }


def delivery() -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationDelivered",
        "payload": {
            "workflow_command_id": "request-1", "workflow_run_id": "run-1", "sequence": 1,
            "call_kind": "timer", "cancellation": snapshot(),
        },
    }


def observation() -> dict[str, Any]:
    return {
        "request_id": "request-1", "requested_at": "2026-10-01T00:00:05Z",
        "cleanup_deadline_at": "2026-10-01T00:00:30.123456Z", "history_refresh_page_token": "opaque-first-page",
        "cancellation": {**snapshot(), "reason": "untrusted observation"},
    }


def test_context_and_nested_metadata_are_immutable() -> None:
    original = snapshot()
    context = CancellationContext.from_dict(original)
    original["reason"] = "changed"
    original["requester"]["id"] = "changed"
    original["lineage"][0]["request_id"] = "changed"
    detached = context.to_dict()
    detached["requester"]["id"] = "changed"
    detached["lineage"].reverse()
    assert context.to_dict() == snapshot()
    assert context.request_id == "request-1"
    assert context.root_request_id == context.parent_request_id == "root-1"
    assert context.deadline == datetime(2026, 10, 1, 0, 0, 30, 123456, timezone.utc)
    assert context.requested_at == datetime(2026, 10, 1, 0, 0, 0, 123456, timezone.utc)
    with pytest.raises(FrozenInstanceError):
        context.reason = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        context.requester["id"] = "changed"  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        context.lineage[0].request_id = "changed"  # type: ignore[misc]


def test_timezone_and_object_key_order_do_not_change_the_context() -> None:
    value = snapshot()
    value["requested_at"] = "2026-09-30T20:00:00.123456-04:00"
    value["cleanup_deadline_at"] = "2026-09-30T20:00:30.123456-04:00"
    value["requester"] = dict(reversed(list(value["requester"].items())))
    value["lineage"] = [dict(reversed(list(entry.items()))) for entry in value["lineage"]]
    assert CancellationContext.from_dict(value) == CancellationContext.from_dict(snapshot())


@pytest.mark.parametrize("field,value", [
    ("schema", "unknown"), ("request_id", ""), ("source", " "), ("parent_request_id", "wrong"),
    ("root_request_id", "wrong"), ("root_workflow_run_id", "wrong"), ("reason", []), ("requester", []),
    ("lineage", []), ("requested_at", "2026-02-30T00:00:00Z"),
    ("cleanup_deadline_at", "2026-10-01T00:00:00.123456Z"),
])
def test_invalid_context_is_rejected(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        CancellationContext.from_dict({**snapshot(), field: value})


@pytest.mark.parametrize("field", ["request_id", "workflow_run_id"])
def test_lineage_cannot_cycle(field: str) -> None:
    value = snapshot()
    value["lineage"][1][field] = value["lineage"][0][field]
    with pytest.raises(ValueError, match="cycle"):
        CancellationContext.from_dict(value)


def test_lineage_order_and_requester_metadata_are_validated() -> None:
    value = snapshot()
    value["lineage"].reverse()
    with pytest.raises(ValueError, match="identities"):
        CancellationContext.from_dict(value)
    value = snapshot()
    value["requester"]["authorization"] = "unsupported"
    with pytest.raises(ValueError, match="unsupported metadata"):
        CancellationContext.from_dict(value)


def test_canonical_child_keeps_root_time_after_expired_local_admission() -> None:
    event = request()
    event["recorded_at"] = "2026-10-01T00:00:31Z"
    state = read_cancellation_history([event], run_id="run-1")
    assert state.request is not None
    assert state.request.requested_at == "2026-10-01T00:00:00.123456Z"
    assert state.request.cleanup_deadline_at == "2026-10-01T00:00:30.123456Z"
    assert state.request.context == CancellationContext.from_dict(snapshot())
    assert state.delivery is None


def test_observation_keeps_refresh_route_and_history_supplies_context() -> None:
    observed = read_cancellation_history([], run_id="run-1", observation=observation())
    assert observed.request is not None and observed.request.context is None
    state = read_cancellation_history([request(), delivery()], run_id="run-1", observation=observation())
    assert state.request is not None
    assert state.request.context == CancellationContext.from_dict(snapshot())
    assert state.request.history_refresh_page_token == "opaque-first-page"
    assert state.request.requested_at == "2026-10-01T00:00:00.123456Z"


@pytest.mark.parametrize("field,value", [
    ("workflow_command_id", "other"), ("workflow_instance_id", "other"),
    ("cleanup_deadline_at", "2026-10-01T00:00:35Z"), ("reason", "changed"), ("cancellation", None),
])
def test_context_must_match_canonical_local_request(field: str, value: Any) -> None:
    event = request()
    event["payload"][field] = value
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([event], run_id="run-1")


def test_context_must_name_local_run_and_not_postdate_admission() -> None:
    event = request()
    event["payload"]["cancellation"]["lineage"][1]["workflow_run_id"] = "other"
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([event], run_id="run-1")
    event = request()
    event["recorded_at"] = "2026-09-30T23:59:59Z"
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([event], run_id="run-1")


def test_delivery_cannot_change_accepted_context() -> None:
    event = delivery()
    event["payload"]["cancellation"]["reason"] = "changed"
    with pytest.raises(NonDeterministicReplayError, match="changes the canonical"):
        read_cancellation_history([request(), event], run_id="run-1")


class ContextCleanup:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        assert ctx.cancellation_context is None
        try:
            yield ctx.start_timer(10)
        except WorkflowCancelled as cancelled:
            assert cancelled.context is not None
            assert cancelled.context is ctx.cancellation_context
            with ctx.cancellation_shield():
                assert cancelled.context is ctx.cancellation_context
                ctx.throw_if_cancellation_requested()
                yield ctx.schedule_activity("cleanup", [])
            return cancelled.context.to_dict()
        return None


def test_cold_replay_exposes_same_context_only_at_delivery_and_cleanup() -> None:
    history = [request(), delivery()]
    first = replay(ContextCleanup, history, [], run_id="run-1")
    assert len(first.commands) == 1 and isinstance(first.commands[0], ScheduleActivity)
    assert first.commands[0].activity_type == "cleanup"
    history.append(completed_activity(2, "cleanup", "cleaned"))
    for _restart in range(2):
        assert replay(ContextCleanup, history, [], run_id="run-1").commands == [CompleteWorkflow(snapshot())]


class ExplicitContextCheck:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_timer(10)
        except WorkflowCancelled:
            try:
                ctx.throw_if_cancellation_requested()
            except WorkflowCancelled as cancelled:
                assert cancelled.request_id == "request-1"
                assert cancelled.context == CancellationContext.from_dict(snapshot())
                raise


def test_explicit_check_keeps_delivered_context() -> None:
    outcome = replay(ExplicitContextCheck, [request(), delivery()], [], run_id="run-1")
    assert len(outcome.commands) == 1 and isinstance(outcome.commands[0], FailWorkflow)
    assert outcome.commands[0].exception_type == "WorkflowCancelled"
