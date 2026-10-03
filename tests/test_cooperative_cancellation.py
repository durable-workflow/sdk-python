from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from durable_workflow import serializer
from durable_workflow._cooperative_cancellation import CancellationDelivery, read_cancellation_history
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, StartTimer, WorkflowContext, replay
from tests.test_durable_selection import _activity_completed, _activity_scheduled, _winner_marker


def observation() -> dict[str, Any]:
    return {
        "request_id": "request-1",
        "requested_at": "2026-09-30T12:00:00Z",
        "cleanup_deadline_at": "2026-09-30T12:10:00Z",
        "history_refresh_page_token": "opaque-first-page",
    }


def request() -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationRequested",
        "workflow_command_id": "request-1",
        "recorded_at": "2026-09-30T12:00:00.000120Z",
        "payload": {
            "workflow_command_id": "request-1",
            "workflow_run_id": "run-1",
            "cleanup_deadline_at": "2026-09-30T12:10:00Z",
        },
    }


def marker(sequence: int = 2, call_kind: str = "activity", **fields: Any) -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationDelivered",
        "workflow_command_id": "request-1",
        "payload": {
            "workflow_command_id": "request-1",
            "workflow_run_id": "run-1",
            "sequence": sequence,
            "call_kind": call_kind,
            **fields,
        },
    }


def test_observation_retains_original_identity_without_becoming_delivery() -> None:
    state = read_cancellation_history([request()], run_id="run-1", observation=observation())
    assert state.request is not None
    assert state.request.requested_at == observation()["requested_at"]
    assert state.request.history_refresh_page_token == "opaque-first-page"
    assert state.delivery is None
    assert state.eligible(1)


def test_prior_result_remains_resolved_but_later_result_is_eligible() -> None:
    result = {"event_type": "ActivityCompleted", "payload": {"sequence": 1}}
    later = {"event_type": "TimerFired", "payload": {"sequence": 2}}
    state = read_cancellation_history([result, request(), later])
    assert not state.eligible(1)
    assert state.eligible(2)
    assert state.eligible(1, 2)


def test_prior_parallel_failure_wins_over_a_later_request() -> None:
    state = read_cancellation_history(
        [
            {"event_type": "ActivityFailed", "payload": {"sequence": 1}},
            request(),
        ]
    )
    assert not state.eligible(1, 2)


def test_cold_history_preserves_one_canonical_delivery_and_its_range() -> None:
    state = read_cancellation_history([request(), marker(2, "parallel", sequence_span=3)], run_id="run-1")
    assert state.delivery == CancellationDelivery("request-1", 2, "parallel", 3)
    assert [state.delivery.interrupts(sequence) for sequence in range(1, 6)] == [False, True, True, True, False]
    assert not state.eligible(5)


def test_selection_handle_targets_its_earlier_operation_range() -> None:
    state = read_cancellation_history(
        [
            request(),
            marker(5, "selection_handle", operation_sequence=2, operation_sequence_span=2),
        ]
    )
    assert state.delivery is not None
    assert state.delivery.interrupts(2)
    assert state.delivery.interrupts(3)
    assert not state.delivery.interrupts(5)


@pytest.mark.parametrize(
    "fields",
    [
        {"sequence": True},
        {"sequence": 0},
        {"sequence": -1},
        {"sequence": 2**63 - 1},
        {"call_kind": "side_effect"},
        {"sequence_span": 2},
        {"sequence_span": 0},
        {"call_kind": "parallel", "sequence_span": 1001},
        {"operation_sequence": 1},
        {"operation_sequence_span": 2},
        {"call_kind": "selection_handle"},
        {"call_kind": "selection_handle", "operation_sequence": 2},
        {"call_kind": "selection_handle", "operation_sequence": 1, "operation_sequence_span": 2},
    ],
)
def test_invalid_marker_ranges_fail_closed(fields: dict[str, Any]) -> None:
    event = marker()
    event["payload"].update(fields)
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([request(), event])


@pytest.mark.parametrize(
    "history",
    [
        [marker()],
        [marker(), request()],
        [request(), request()],
        [request(), marker(), marker()],
    ],
)
def test_missing_reordered_or_duplicate_canonical_markers_fail_closed(history: list[dict[str, Any]]) -> None:
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history(history)


@pytest.mark.parametrize("field", ["request_id", "cleanup_deadline_at"])
def test_observation_cannot_replace_original_request(field: str) -> None:
    observed = observation()
    observed[field] = "different" if field == "request_id" else "2026-09-30T12:20:00Z"
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([request()], observation=observed)


@pytest.mark.parametrize(
    "field,value",
    [
        ("request_id", ""),
        ("requested_at", "2026-09-30T12:00:00"),
        ("cleanup_deadline_at", "2026-09-30T11:00:00Z"),
        ("history_refresh_page_token", ""),
    ],
)
def test_invalid_observation_fails_closed(field: str, value: Any) -> None:
    observed = observation()
    observed[field] = value
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([], observation=observed)


def test_delivery_run_and_request_identity_are_checked() -> None:
    for field, value in [("workflow_run_id", "other-run"), ("workflow_command_id", "other-request")]:
        event = deepcopy(marker())
        event["payload"][field] = value
        with pytest.raises(NonDeterministicReplayError):
            read_cancellation_history([request(), event], run_id="run-1")


def completed_activity(sequence: int, name: str, result: Any) -> dict[str, Any]:
    return {
        "event_type": "ActivityCompleted",
        "payload": {
            "sequence": sequence,
            "activity_type": name,
            "result": serializer.envelope(result),
        },
    }


class PriorResultCleanup:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        assert not ctx.is_cancellation_requested
        ctx.throw_if_cancellation_requested()
        previous = yield ctx.schedule_activity("previous", [])
        try:
            yield ctx.start_timer(30)
        except WorkflowCancelled as exc:
            assert ctx.is_cancellation_requested
            with ctx.cancellation_shield():
                ctx.throw_if_cancellation_requested()
                cleanup = yield ctx.schedule_activity("cleanup", [previous])
            return {"previous": previous, "cleanup": cleanup, "request_id": exc.request_id}
        return "not cancelled"


def test_observation_and_request_do_not_throw_before_the_authored_call() -> None:
    prior = completed_activity(1, "previous", "already committed")
    for history in [[prior], [prior, request()]]:
        outcome = replay(PriorResultCleanup, history, [], run_id="run-1", cancellation_request=observation())
        assert outcome.commands == []
        assert outcome.cancellation_delivery == CancellationDelivery("request-1", 2, "timer")


def test_cold_replay_reaches_same_delivery_and_durable_cleanup_boundary() -> None:
    history = [
        completed_activity(1, "previous", "already committed"),
        {"event_type": "TimerScheduled", "payload": {"sequence": 2, "timer_kind": "durable_timer"}},
        request(),
        {"event_type": "TimerCancelled", "payload": {"sequence": 2, "timer_kind": "durable_timer"}},
        marker(2, "timer"),
    ]
    for _restart in range(2):
        outcome = replay(PriorResultCleanup, history, [], run_id="run-1")
        assert outcome.cancellation_delivery is None
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "cleanup"
        assert outcome.commands[0].arguments == ["already committed"]

    history.extend(
        [
            {"event_type": "ActivityScheduled", "payload": {"sequence": 3, "activity_type": "cleanup"}},
            completed_activity(3, "cleanup", "cleanup committed"),
        ]
    )
    for _restart in range(2):
        outcome = replay(PriorResultCleanup, history, [], run_id="run-1")
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == {
            "previous": "already committed",
            "cleanup": "cleanup committed",
            "request_id": "request-1",
        }


class ScalarCleanup:
    def run(self, ctx: WorkflowContext, kind: str):  # type: ignore[no-untyped-def]
        call = {
            "activity": lambda: ctx.schedule_activity("forward", []),
            "local_activity": lambda: ctx.local_activity("forward", []),
            "timer": lambda: ctx.start_timer(30),
            "child": lambda: ctx.start_child_workflow("forward-child", []),
            "condition": lambda: ctx.wait_condition(lambda: False, key="forward-wait"),
        }[kind]()
        try:
            yield call
        except WorkflowCancelled as exc:
            with ctx.cancellation_shield():
                yield ctx.start_timer(1)
            return exc.request_id
        return "not cancelled"


@pytest.mark.parametrize("kind", ["activity", "local_activity", "timer", "child", "condition"])
def test_scalar_marker_interrupts_at_call_and_cleanup_does_not_redeliver(kind: str) -> None:
    calls: list[Any] = []
    history = [request(), marker(1, kind)]
    outcome = replay(ScalarCleanup, history, [kind], run_id="run-1", local_activity_executor=calls.append)
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], StartTimer)
    assert outcome.commands[0].delay_seconds == 1
    assert calls == []
    outcome = replay(
        ScalarCleanup,
        history
        + [
            {"event_type": "TimerFired", "payload": {"sequence": 2, "timer_kind": "durable_timer"}},
        ],
        [kind],
        run_id="run-1",
    )
    assert outcome.commands == [CompleteWorkflow("request-1")]


def test_pending_local_call_is_delivered_before_executing_the_callable() -> None:
    calls: list[Any] = []
    outcome = replay(
        ScalarCleanup, [request()], ["local_activity"], run_id="run-1", local_activity_executor=calls.append
    )
    assert calls == []
    assert outcome.commands == []
    assert outcome.cancellation_delivery == CancellationDelivery("request-1", 1, "local_activity")


class ParallelCleanup:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [ctx.schedule_activity("first", []), [ctx.start_timer(30), ctx.start_child_workflow("child", [])]]
        except WorkflowCancelled:
            with ctx.cancellation_shield():
                yield ctx.schedule_activity("cleanup", [])
            return "cleaned"


def test_parallel_delivery_preserves_flat_span_and_cleanup_cursor_on_restart() -> None:
    observed = replay(ParallelCleanup, [request()], [], run_id="run-1")
    assert observed.cancellation_delivery == CancellationDelivery("request-1", 1, "parallel", 3)
    history = [request(), marker(1, "parallel", sequence_span=3), completed_activity(4, "cleanup", "done")]
    assert replay(ParallelCleanup, history, [], run_id="run-1").commands == [CompleteWorkflow("cleaned")]


@pytest.mark.parametrize(
    "history,kind",
    [
        ([request(), marker(1, "timer")], "activity"),
        ([{"event_type": "TimerFired", "payload": {"sequence": 1}}, request(), marker(2, "activity")], "timer"),
        ([request(), marker(1, "parallel", sequence_span=2)], "timer"),
    ],
)
def test_changed_or_unreached_delivery_boundary_is_non_deterministic(history: list[dict[str, Any]], kind: str) -> None:
    with pytest.raises(NonDeterministicReplayError):
        replay(ScalarCleanup, history, [kind], run_id="run-1")


class ShieldBeforeRequest:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        with ctx.cancellation_shield():
            yield ctx.start_timer(1)
        try:
            yield ctx.start_timer(2)
        except WorkflowCancelled as exc:
            return exc.request_id


def test_shield_defers_first_delivery_without_resetting_request_identity() -> None:
    first = replay(ShieldBeforeRequest, [request()], [], run_id="run-1")
    assert first.commands == [StartTimer(1)]
    assert first.cancellation_delivery is None
    history = [request(), {"event_type": "TimerFired", "payload": {"sequence": 1}}]
    second = replay(ShieldBeforeRequest, history, [], run_id="run-1")
    assert second.cancellation_delivery == CancellationDelivery("request-1", 2, "timer")
    cold = replay(ShieldBeforeRequest, history + [marker(2, "timer")], [], run_id="run-1")
    assert cold.commands == [CompleteWorkflow("request-1")]


class SelectionCleanup:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            selected = yield ctx.select({
                "slow": ctx.schedule_activity("slow-activity", []),
                "fast": ctx.schedule_activity("fast-activity", []),
            })
            yield selected.handles["slow"].await_result()
        except WorkflowCancelled as exc:
            with ctx.cancellation_shield():
                yield ctx.start_timer(1)
            return exc.request_id


def selection_history() -> list[dict[str, Any]]:
    return [
        _activity_scheduled(0, "slow"), _activity_scheduled(1, "fast"),
        _activity_completed(1, "fast", "winner"), _winner_marker(),
    ]


def test_resolved_selection_replays_before_pending_loser_delivery() -> None:
    outcome = replay(SelectionCleanup, selection_history() + [request()], [], run_id="run-1")
    assert outcome.commands == []
    assert outcome.cancellation_delivery == CancellationDelivery("request-1", 3, "selection_handle", 1, 1)


def test_selection_handle_cold_delivery_preserves_opening_identity_and_cleanup_cursor() -> None:
    history = selection_history() + [request(), marker(3, "selection_handle", operation_sequence=1)]
    for _restart in range(2):
        outcome = replay(SelectionCleanup, history, [], run_id="run-1")
        assert outcome.commands == [StartTimer(1)]
    history.append({"event_type": "TimerFired", "payload": {"sequence": 4}})
    assert replay(SelectionCleanup, history, [], run_id="run-1").commands == [CompleteWorkflow("request-1")]


def test_first_selection_call_delivers_as_one_parallel_boundary() -> None:
    outcome = replay(SelectionCleanup, [request()], [], run_id="run-1")
    assert outcome.cancellation_delivery == CancellationDelivery("request-1", 1, "parallel", 2)
    cold = replay(SelectionCleanup, [request(), marker(1, "parallel", sequence_span=2)], [], run_id="run-1")
    assert cold.commands == [StartTimer(1)]


def test_marker_cannot_replace_a_result_committed_before_request() -> None:
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([completed_activity(1, "previous", "result"), request(), marker(1)])


class ReopenedConditionCleanup:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.wait_condition(lambda: False, key="forward-wait")
        except WorkflowCancelled as exc:
            with ctx.cancellation_shield():
                yield ctx.schedule_activity("cleanup", [])
            return exc.request_id


def reopened_condition_history() -> list[dict[str, Any]]:
    return [
        {"event_type": "ConditionWaitOpened", "payload": {
            "sequence": 1, "condition_key": "forward-wait", "condition_wait_id": "wait-1",
        }},
        {"event_type": "ConditionWaitSatisfied", "payload": {
            "sequence": 1, "condition_wait_id": "wait-1",
        }},
        {"event_type": "ConditionWaitOpened", "payload": {
            "sequence": 2, "condition_key": "forward-wait", "condition_wait_id": "wait-2",
        }},
        request(),
    ]


def test_request_during_a_false_condition_reopen_uses_the_actual_wait_occurrence() -> None:
    outcome = replay(ReopenedConditionCleanup, reopened_condition_history(), [], run_id="run-1")
    assert outcome.commands == []
    assert outcome.cancellation_delivery == CancellationDelivery("request-1", 2, "condition")


def test_cold_delivery_during_a_false_reopen_consumes_the_interrupted_wait_once() -> None:
    history = reopened_condition_history() + [marker(2, "condition")]
    for _restart in range(2):
        outcome = replay(ReopenedConditionCleanup, history, [], run_id="run-1")
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "cleanup"
    history.append(completed_activity(3, "cleanup", "done"))
    assert replay(ReopenedConditionCleanup, history, [], run_id="run-1").commands == [CompleteWorkflow("request-1")]
