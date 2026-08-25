from __future__ import annotations

from typing import Any

from durable_workflow import serializer, workflow
from durable_workflow.errors import ChildWorkflowFailed
from durable_workflow.workflow import (
    CompleteWorkflow,
    FailWorkflow,
    ScheduleActivity,
    StartChildWorkflow,
    StartTimer,
    WorkflowContext,
    commands_to_server_commands,
    replay,
)


def _entry(kind: str, base: int, size: int, index: int) -> dict[str, Any]:
    prefix = {
        "activity": "parallel-activities",
        "child": "parallel-children",
        "timer": "parallel-timers",
        "mixed": "parallel-calls",
    }[kind]
    return {
        "parallel_group_id": f"{prefix}:{base}:{size}",
        "parallel_group_kind": kind,
        "parallel_group_base_sequence": base,
        "parallel_group_size": size,
        "parallel_group_index": index,
    }


def _paths() -> list[list[dict[str, Any]]]:
    outer = [_entry("mixed", 1, 3, index) for index in range(3)]
    return [
        [outer[0]],
        [outer[1], _entry("mixed", 2, 2, 0)],
        [outer[2], _entry("mixed", 2, 2, 1)],
    ]


def _parallel_event(
    event_type: str,
    sequence: int,
    path: list[dict[str, Any]],
    *,
    result: Any = None,
    **details: Any,
) -> dict[str, Any]:
    payload = {
        "sequence": sequence,
        **details,
        **path[-1],
        "parallel_group_path": path,
    }
    if event_type in {"ActivityCompleted", "ChildRunCompleted"}:
        payload["result"] = serializer.envelope(result, codec=serializer.AVRO_CODEC)
    return {"event_type": event_type, "payload": payload}


@workflow.defn(name="nested-parallel-authoring")
class NestedParallelWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (
            yield [
                ctx.schedule_activity("first", []),
                [
                    ctx.start_child_workflow("second", []),
                    ctx.start_timer(5),
                ],
            ]
        )


@workflow.defn(name="nested-parallel-failure")
class NestedParallelFailureWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [
                ctx.schedule_activity("first", []),
                [
                    ctx.start_child_workflow("second", []),
                    ctx.start_timer(5),
                ],
            ]
        except ChildWorkflowFailed as exc:
            return {"failure": str(exc), "child": exc.child_workflow_type}
        return "unexpected"


def test_nested_parallel_emits_flat_commands_with_stable_full_paths() -> None:
    outcome = replay(NestedParallelWorkflow, [], [])
    assert [type(command) for command in outcome.commands] == [
        ScheduleActivity,
        StartChildWorkflow,
        StartTimer,
    ]

    commands = commands_to_server_commands(outcome.commands, "parallel-workers")
    for command, path in zip(commands, _paths(), strict=True):
        assert command["parallel_group_path"] == path
        assert command["parallel_group_id"] == path[-1]["parallel_group_id"]


def test_nested_parallel_completed_replay_restores_input_shape_and_ignores_duplicate_delivery() -> None:
    paths = _paths()
    timer = _parallel_event("TimerFired", 3, paths[2], timer_kind="durable_timer")
    history = [
        timer,
        _parallel_event(
            "ChildRunCompleted",
            2,
            paths[1],
            result="two",
            child_workflow_type="second",
        ),
        _parallel_event("ActivityCompleted", 1, paths[0], result="one", activity_type="first"),
        timer,
    ]

    for _restart_or_replay in range(2):
        outcome = replay(NestedParallelWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == ["one", ["two", None]]


def test_nested_parallel_failure_is_positional_and_keeps_late_completion() -> None:
    paths = _paths()
    history = [
        _parallel_event("TimerFired", 3, paths[2], timer_kind="durable_timer"),
        _parallel_event(
            "ChildRunFailed",
            2,
            paths[1],
            child_workflow_type="second",
            message="nested child failed",
        ),
        _parallel_event("ActivityCompleted", 1, paths[0], result="one", activity_type="first"),
    ]

    outcome = replay(NestedParallelFailureWorkflow, history, [])
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == {
        "failure": "nested child failed",
        "child": "second",
    }


def test_nested_parallel_pending_history_restarts_without_rescheduling() -> None:
    paths = _paths()
    history = [
        _parallel_event("ActivityScheduled", 1, paths[0], activity_type="first"),
        _parallel_event("ChildWorkflowScheduled", 2, paths[1], workflow_type="second"),
        _parallel_event("TimerScheduled", 3, paths[2], timer_kind="durable_timer"),
    ]

    for _restart in range(2):
        outcome = replay(NestedParallelWorkflow, history, [])
        assert [type(command) for command in outcome.commands] == [
            ScheduleActivity,
            StartChildWorkflow,
            StartTimer,
        ]


@workflow.defn(name="trip-saga-authoring")
class TripSagaWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        def forward(saga):  # type: ignore[no-untyped-def]
            flight = yield ctx.schedule_activity("trip.reserve-flight", [])
            saga.add_compensation("trip.cancel-flight", [flight])
            ctx.throw_if_cancellation_requested()
            hotel = yield ctx.schedule_activity("trip.reserve-hotel", [])
            saga.add_compensation("trip.cancel-hotel", [hotel])
            yield ctx.schedule_activity("trip.charge", [])
            return {"status": "booked"}

        return (yield from ctx.saga().run(forward))


def _activity_event(
    event_type: str,
    sequence: int,
    activity_type: str,
    *,
    result: Any = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "sequence": sequence,
        "activity_type": activity_type,
        "message": f"{activity_type} failed",
        "exception_type": "PlannedFailure",
    }
    if event_type == "ActivityCompleted":
        payload["result"] = serializer.envelope(result, codec=serializer.AVRO_CODEC)
    return {"event_type": event_type, "payload": payload}


def _failed_trip_history() -> list[dict[str, Any]]:
    return [
        _activity_event("ActivityCompleted", 1, "trip.reserve-flight", result="flight-1"),
        _activity_event("ActivityCompleted", 2, "trip.reserve-hotel", result="hotel-1"),
        _activity_event("ActivityFailed", 3, "trip.charge"),
    ]


def test_saga_compensates_in_reverse_order_across_restart_and_duplicate_delivery() -> None:
    hotel_compensated = _activity_event("ActivityCompleted", 4, "trip.cancel-hotel")
    history = [*_failed_trip_history(), hotel_compensated, hotel_compensated]

    for _restart in range(2):
        outcome = replay(TripSagaWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "trip.cancel-flight"
        assert outcome.commands[0].arguments == ["flight-1"]


def test_saga_compensation_failure_preserves_both_failures() -> None:
    history = [
        *_failed_trip_history(),
        _activity_event("ActivityFailed", 4, "trip.cancel-hotel"),
    ]

    outcome = replay(TripSagaWorkflow, history, [])
    assert len(outcome.commands) == 1
    command = outcome.commands[0]
    assert isinstance(command, FailWorkflow)
    assert command.exception_type == "SagaCompensationFailed"
    assert command.exception is not None
    assert command.exception["initiating_failure"]["type"] == "ActivityFailed"
    assert command.exception["compensation_failure"] == {
        "type": "ActivityFailed",
        "class": "durable_workflow.errors.ActivityFailed",
        "message": "trip.cancel-hotel failed",
        "activity_type": "trip.cancel-hotel",
        "registration_order": 2,
    }


def test_saga_cooperative_cancellation_runs_registered_compensation() -> None:
    history = [
        _activity_event("ActivityCompleted", 1, "trip.reserve-flight", result="flight-1"),
    ]

    outcome = replay(TripSagaWorkflow, history, [], cancel_requested=True)
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], ScheduleActivity)
    assert outcome.commands[0].activity_type == "trip.cancel-flight"


def test_saga_normal_completion_does_not_schedule_compensations() -> None:
    history = [
        _activity_event("ActivityCompleted", 1, "trip.reserve-flight", result="flight-1"),
        _activity_event("ActivityCompleted", 2, "trip.reserve-hotel", result="hotel-1"),
        _activity_event("ActivityCompleted", 3, "trip.charge", result="charged"),
    ]

    outcome = replay(TripSagaWorkflow, history, [])
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == {"status": "booked"}
