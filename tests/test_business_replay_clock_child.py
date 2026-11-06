from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from durable_workflow import ReplayClockUnavailable, serializer
from durable_workflow.errors import ChildWorkflowFailed, NonDeterministicReplayError, QueryFailed
from durable_workflow.workflow import CompleteWorkflow, FailWorkflow, WorkflowContext, query_state, replay
from tests.business_clock_workflows import ChildBusinessClockWorkflow, clock_text
from tests.test_business_replay_clock import result

START = "2026-11-01T00:00:00.000000+00:00"
COMPLETED = "2026-11-01T00:00:05.654321+00:00"
FAILED = "2026-11-01T00:00:03.123456+00:00"


def child_rows(sequence: int, kind: str, time: Any, *, started: bool = True) -> list[dict[str, Any]]:
    payload = {"sequence": sequence, "child_call_id": f"call-{sequence}",
               "child_workflow_instance_id": f"child-{sequence}", "child_workflow_run_id": f"run-{sequence}",
               "child_workflow_type": "clock.child"}
    rows = [{"event_type": "ChildWorkflowScheduled", "recorded_at": START, "payload": dict(payload)}]
    if started:
        rows.append({"event_type": "ChildRunStarted", "recorded_at": START, "payload": dict(payload)})
    if kind == "ChildRunCompleted":
        payload["result"] = serializer.envelope("recorded")
    else:
        payload["message"] = "recorded child failure"
    payload.update(closed_at="2098-01-01T00:00:00Z", reused_recorded_at="2099-01-01T00:00:00Z")
    rows.append({"event_type": kind, "recorded_at": time, "payload": payload})
    return rows


def history() -> list[dict[str, Any]]:
    return [{"event_type": "WorkflowStarted", "recorded_at": START, "payload": {}},
            *child_rows(1, "ChildRunCompleted", COMPLETED), *child_rows(2, "ChildRunFailed", FAILED)]


def execute(events: list[dict[str, Any]]) -> dict[str, Any]:
    return dict(result(events, ChildBusinessClockWorkflow))


def test_actual_completion_caught_failure_and_cold_replay_use_parent_resolution_time() -> None:
    events = history()
    events.append({"event_type": "WorkflowTaskCompleted", "recorded_at": "2099-01-01T00:00:00Z", "payload": {}})
    original = deepcopy(events)
    first = execute(events)
    assert first == execute(events)
    assert first == {"clock": [None, COMPLETED, FAILED], "legacy": [START] * 3, "repeated": True,
                     "result": "recorded", "failure": {"kind": "child_workflow", "run": "run-2",
                                                         "message": "recorded child failure"}}
    assert events == original


@pytest.mark.parametrize("time,expected", [(COMPLETED, COMPLETED), (FAILED, FAILED),
    ("2026-11-01T02:00:03.123456+02:00", FAILED)])
def test_equal_backward_and_offset_times_preserve_the_exact_consumed_instant(time: str, expected: str) -> None:
    events = history()
    events[-1]["recorded_at"] = time
    assert execute(events)["clock"] == [None, COMPLETED, expected]


@pytest.mark.parametrize("kind,failure_kind", [("ChildRunCompleted", None), ("ChildRunFailed", "child_workflow"),
    ("ChildRunCancelled", "cancelled"), ("ChildRunTerminated", "terminated")])
def test_each_child_terminal_preserves_its_original_outcome_and_own_time(kind: str, failure_kind: str | None) -> None:
    class Single:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                value = yield ctx.start_child_workflow("clock.child", [])
                outcome = value
            except ChildWorkflowFailed as error:
                outcome = (error.failure_kind, error.child_workflow_run_id)
            return outcome, clock_text(ctx), ctx.replay_now() == ctx.replay_now()

    rows = child_rows(1, kind, COMPLETED)
    expected = "recorded" if failure_kind is None else (failure_kind, "run-1")
    assert result(rows, Single) == (expected, COMPLETED, True)


@pytest.mark.parametrize("field", ["child_call_id", "child_workflow_instance_id", "child_workflow_run_id"])
@pytest.mark.parametrize("row", [1, 2, 3])
@pytest.mark.parametrize("replacement", [None, "", "foreign", 17])
def test_missing_empty_conflicting_or_wrong_type_identity_keeps_legacy_result_without_time(
    field: str, row: int, replacement: Any,
) -> None:
    events = history()
    if replacement is None:
        events[row]["payload"].pop(field)
    else:
        events[row]["payload"][field] = replacement
    value = execute(events)
    assert value["result"] == "recorded"
    assert value["clock"] == [None, None, FAILED]


def test_type_is_required_and_a_conflicting_alias_cannot_supply_authority() -> None:
    missing = history()
    for row in missing[1:4]:
        row["payload"].pop("child_workflow_type")
    assert execute(missing)["clock"] == [None, None, FAILED]
    conflict = history()
    for row in conflict[1:4]:
        row["payload"].update(workflow_type="clock.child", child_workflow_type="foreign.child")
    assert execute(conflict)["clock"] == [None, None, FAILED]


def test_existing_child_type_mismatch_still_raises_the_original_validation_error() -> None:
    events = history()
    for row in events[1:4]:
        row["payload"]["child_workflow_type"] = "foreign.child"
    with pytest.raises(NonDeterministicReplayError):
        execute(events)


@pytest.mark.parametrize("bad", [None, False, 17, "", "not-a-time", "2026-11-01T00:00:03",
                               "2026-02-30T00:00:00Z", "2026-11-01T24:00:00Z"])
def test_invalid_resolution_time_does_not_borrow_other_time_then_next_child_can_recover(bad: Any) -> None:
    events = history()
    events[3]["recorded_at"] = bad
    assert execute(events)["clock"] == [None, None, FAILED]


def test_public_timestamp_is_supported_but_invalid_present_value_cannot_fall_back() -> None:
    events = history()
    # Keep the established legacy start-clock carrier; this changes only the
    # public child rows consumed by the additive clock.
    for row in events[1:]:
        row["timestamp"] = row.pop("recorded_at")
    assert execute(events)["clock"] == [None, COMPLETED, FAILED]
    events[3]["recorded_at"] = COMPLETED
    events[3]["timestamp"] = "invalid"
    assert execute(events)["clock"] == [None, None, FAILED]


def test_optional_start_is_supported_but_duplicate_and_changing_run_lineage_are_unavailable() -> None:
    events = history()
    del events[2]
    assert execute(events)["clock"] == [None, COMPLETED, FAILED]
    duplicate = history()
    other = deepcopy(duplicate[3])
    other["recorded_at"] = "2099-01-01T00:00:00Z"
    duplicate.insert(4, other)
    assert execute(duplicate)["clock"] == [None, None, FAILED]
    changed_run = history()
    for row in changed_run[2:4]:
        row["payload"]["child_workflow_run_id"] = "continued-child-run"
    assert execute(changed_run)["clock"] == [None, None, FAILED]


def test_cross_call_or_unindexed_start_cannot_complete_another_childs_proof() -> None:
    events = history()
    events[2]["payload"]["sequence"] = "1"
    assert execute(events)["clock"] == [None, None, FAILED]
    events = history()
    for row in events[4:]:
        row["payload"]["child_call_id"] = "call-1"
    assert execute(events)["clock"] == [None, None, None]


@pytest.mark.parametrize("row", [1, 2])
def test_duplicate_opening_or_start_keeps_legacy_outcome_but_not_time(row: int) -> None:
    events = history()
    events.insert(row, deepcopy(events[row]))
    value = execute(events)
    assert value["result"] == "recorded"
    assert value["clock"] == [None, None, FAILED]


def test_contradictory_terminals_cannot_supply_time_or_change_legacy_failure() -> None:
    observed: list[str | None] = []

    class Parent:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.start_child_workflow("clock.child", [])
            observed.append(clock_text(ctx))
            yield ctx.start_child_workflow("clock.child", [])

    events = history()
    conflicting = deepcopy(events[3])
    conflicting["event_type"] = "ChildRunFailed"
    conflicting["payload"]["message"] = "contradictory failure"
    events.insert(4, conflicting)
    outcome = replay(Parent, events, [], payload_codec="avro")
    try:
        assert len(outcome.commands) == 1
        failure = outcome.commands[0]
        assert isinstance(failure, FailWorkflow)
        assert failure.exception_type == "ChildWorkflowFailed"
        assert failure.message == "contradictory failure"
    finally:
        outcome.close()
    assert observed == [None]


def test_query_prefix_nested_replay_error_and_scope_exit_preserve_parent_time() -> None:
    class Parent:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.start_child_workflow("clock.child", [])
            before = ctx.replay_now()
            assert execute(history())["clock"] == [None, COMPLETED, FAILED]
            assert ctx.replay_now() == before
            with pytest.raises(QueryFailed, match="child-clock-query-error"):
                query_state(ChildBusinessClockWorkflow, history(), [], "explode", payload_codec="avro")
            assert ctx.replay_now() == before
            return clock_text(ctx)

    assert result(history()[:4], Parent) == COMPLETED
    assert query_state(ChildBusinessClockWorkflow, history(), [], "clock", payload_codec="avro") == FAILED
    prefix = history()[:4]
    assert query_state(ChildBusinessClockWorkflow, prefix, [], "clock", payload_codec="avro") == COMPLETED
    prefix.append({"event_type": "WorkflowTaskCompleted", "recorded_at": "2099-01-01T00:00:00Z", "payload": {}})
    assert query_state(ChildBusinessClockWorkflow, prefix, [], "clock", payload_codec="avro") is None


def test_child_time_does_not_survive_an_inline_boundary_on_first_or_cold_replay() -> None:
    retained: list[WorkflowContext] = []

    class Inline:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            retained.append(ctx)
            yield ctx.start_child_workflow("clock.child", [])
            before = clock_text(ctx)
            yield ctx.side_effect(lambda: "recorded")
            return before, clock_text(ctx)

    events = history()[:4]
    outcome = replay(Inline, events, [], payload_codec="avro")
    try:
        assert isinstance(outcome.commands[-1], CompleteWorkflow)
        assert outcome.commands[-1].result == (COMPLETED, None)
    finally:
        outcome.close()
    events.append({"event_type": "SideEffectRecorded", "payload": {
        "sequence": 2, "result": serializer.envelope("recorded")}, "recorded_at": FAILED})
    assert result(events, Inline) == (COMPLETED, None)
    with pytest.raises(ReplayClockUnavailable):
        retained[0].replay_now()
