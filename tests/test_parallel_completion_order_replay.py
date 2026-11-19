from __future__ import annotations

from itertools import permutations
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import ActivityFailed, ChildWorkflowFailed
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext, replay

THREE_SIBLING_ORDERS = list(permutations(range(3)))


def _parallel_metadata(kind: str, size: int, index: int) -> dict[str, Any]:
    prefix = {
        "activity": "parallel-activities",
        "child": "parallel-children",
        "timer": "parallel-timers",
        "mixed": "parallel-calls",
    }[kind]
    entry = {
        "parallel_group_id": f"{prefix}:1:{size}",
        "parallel_group_kind": kind,
        "parallel_group_base_sequence": 1,
        "parallel_group_size": size,
        "parallel_group_index": index,
    }
    return {**entry, "parallel_group_path": [entry]}


def _terminal_event(
    event_type: str,
    kind: str,
    size: int,
    index: int,
    *,
    value: Any = None,
    **details: Any,
) -> dict[str, Any]:
    payload = {
        "sequence": index + 1,
        **details,
        **_parallel_metadata(kind, size, index),
    }
    if event_type == "ActivityCompleted":
        payload["result"] = serializer.encode(value, codec=serializer.AVRO_CODEC)
    elif event_type == "ChildRunCompleted":
        payload["output"] = serializer.encode(value, codec=serializer.AVRO_CODEC)
        payload["payload_codec"] = serializer.AVRO_CODEC
    return {"event_type": event_type, "payload": payload}


def _history_in_completion_order(events: list[dict[str, Any]], order: tuple[int, ...]) -> list[dict[str, Any]]:
    history: list[dict[str, Any]] = []
    for event_sequence, sibling_index in enumerate(order, start=100):
        history.append({"sequence": event_sequence, **events[sibling_index]})
    return history


def _completed_result(workflow_type: type, history: list[dict[str, Any]]) -> Any:
    outcome = replay(workflow_type, history, [], payload_codec=serializer.AVRO_CODEC)
    assert len(outcome.commands) == 1
    command = outcome.commands[0]
    assert isinstance(command, CompleteWorkflow)
    return command.result


@workflow.defn(name="parallel-completion-order-activities")
class ParallelCompletionOrderActivities:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (
            yield [
                ctx.schedule_activity("activity-a", []),
                ctx.schedule_activity("activity-b", []),
                ctx.schedule_activity("activity-c", []),
            ]
        )


@workflow.defn(name="parallel-completion-order-repeated-activities")
class ParallelCompletionOrderRepeatedActivities:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        deferred = ctx.schedule_activity("shared-activity", [])
        return (yield [deferred, deferred, deferred])


@workflow.defn(name="parallel-completion-order-children")
class ParallelCompletionOrderChildren:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (
            yield [
                ctx.start_child_workflow("child-a", []),
                ctx.start_child_workflow("child-b", []),
                ctx.start_child_workflow("child-c", []),
            ]
        )


@workflow.defn(name="parallel-completion-order-timers")
class ParallelCompletionOrderTimers:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield [ctx.start_timer(30), ctx.start_timer(20), ctx.start_timer(10)]
        return "all-timers-fired"


@workflow.defn(name="parallel-completion-order-mixed")
class ParallelCompletionOrderMixed:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (
            yield [
                ctx.schedule_activity("mixed-activity", []),
                ctx.start_child_workflow("mixed-child", []),
                ctx.start_timer(10),
            ]
        )


@workflow.defn(name="parallel-completion-order-signal-interleaving")
class ParallelCompletionOrderSignalInterleaving:
    def __init__(self) -> None:
        self.markers: list[str] = []

    @workflow.signal("mark")
    def mark(self, value: str) -> None:
        self.markers.append(value)

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        results = yield [
            ctx.schedule_activity("signal-a", []),
            ctx.schedule_activity("signal-b", []),
        ]
        return {"results": results, "markers": self.markers}


@workflow.defn(name="parallel-completion-order-activity-failures")
class ParallelCompletionOrderActivityFailures:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [
                ctx.schedule_activity("activity-ok", []),
                ctx.schedule_activity("activity-fails-first", []),
                ctx.schedule_activity("activity-fails-second", []),
            ]
        except ActivityFailed as exc:
            return {"activity_type": exc.activity_type, "message": str(exc)}
        return "unexpected-success"


@workflow.defn(name="parallel-completion-order-child-failures")
class ParallelCompletionOrderChildFailures:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [
                ctx.start_child_workflow("child-ok", []),
                ctx.start_child_workflow("child-fails-first", []),
                ctx.start_child_workflow("child-fails-second", []),
            ]
        except ChildWorkflowFailed as exc:
            return {"child_workflow_type": exc.child_workflow_type, "message": str(exc)}
        return "unexpected-success"


@workflow.defn(name="parallel-completion-order-mixed-failures")
class ParallelCompletionOrderMixedFailures:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [
                ctx.start_child_workflow("mixed-child-fails-first", []),
                ctx.schedule_activity("mixed-activity-fails-second", []),
                ctx.start_timer(10),
            ]
        except (ActivityFailed, ChildWorkflowFailed) as exc:
            return {"failure_type": type(exc).__name__, "message": str(exc)}
        return "unexpected-success"


@workflow.defn(name="parallel-completion-order-repeated-failures")
class ParallelCompletionOrderRepeatedFailures:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        deferred = ctx.schedule_activity("shared-failure", [])
        try:
            yield [deferred, deferred]
        except ActivityFailed as exc:
            return str(exc)
        return "unexpected-success"


@pytest.mark.parametrize("completion_order", THREE_SIBLING_ORDERS)
def test_distinct_activity_results_follow_workflow_position(completion_order: tuple[int, ...]) -> None:
    events = [
        _terminal_event(
            "ActivityCompleted",
            "activity",
            3,
            index,
            value=f"result-{name}",
            activity_type=f"activity-{name}",
        )
        for index, name in enumerate(("a", "b", "c"))
    ]

    assert _completed_result(
        ParallelCompletionOrderActivities,
        _history_in_completion_order(events, completion_order),
    ) == ["result-a", "result-b", "result-c"]


@pytest.mark.parametrize("completion_order", THREE_SIBLING_ORDERS)
def test_repeated_identical_activity_results_stay_bound_to_yielded_positions(
    completion_order: tuple[int, ...],
) -> None:
    events = [
        _terminal_event(
            "ActivityCompleted",
            "activity",
            3,
            index,
            value=f"position-{index}",
            activity_type="shared-activity",
        )
        for index in range(3)
    ]

    assert _completed_result(
        ParallelCompletionOrderRepeatedActivities,
        _history_in_completion_order(events, completion_order),
    ) == ["position-0", "position-1", "position-2"]


@pytest.mark.parametrize("completion_order", THREE_SIBLING_ORDERS)
def test_child_results_follow_workflow_position(completion_order: tuple[int, ...]) -> None:
    events = [
        _terminal_event(
            "ChildRunCompleted",
            "child",
            3,
            index,
            value=f"result-{name}",
            child_workflow_type=f"child-{name}",
        )
        for index, name in enumerate(("a", "b", "c"))
    ]

    assert _completed_result(
        ParallelCompletionOrderChildren,
        _history_in_completion_order(events, completion_order),
    ) == ["result-a", "result-b", "result-c"]


@pytest.mark.parametrize("completion_order", THREE_SIBLING_ORDERS)
def test_timer_group_replays_for_every_completion_order(completion_order: tuple[int, ...]) -> None:
    events = [_terminal_event("TimerFired", "timer", 3, index, timer_kind="sleep") for index in range(3)]

    assert (
        _completed_result(
            ParallelCompletionOrderTimers,
            _history_in_completion_order(events, completion_order),
        )
        == "all-timers-fired"
    )


@pytest.mark.parametrize("completion_order", THREE_SIBLING_ORDERS)
def test_mixed_results_follow_workflow_position(completion_order: tuple[int, ...]) -> None:
    events = [
        _terminal_event(
            "ActivityCompleted",
            "mixed",
            3,
            0,
            value="activity-result",
            activity_type="mixed-activity",
        ),
        _terminal_event(
            "ChildRunCompleted",
            "mixed",
            3,
            1,
            value="child-result",
            child_workflow_type="mixed-child",
        ),
        _terminal_event("TimerFired", "mixed", 3, 2, timer_kind="sleep"),
    ]

    assert _completed_result(
        ParallelCompletionOrderMixed,
        _history_in_completion_order(events, completion_order),
    ) == ["activity-result", "child-result", None]


def test_signal_keeps_chronological_position_between_reverse_order_terminals() -> None:
    later_position = _terminal_event(
        "ActivityCompleted",
        "activity",
        2,
        1,
        value="result-b",
        activity_type="signal-b",
    )
    earlier_position = _terminal_event(
        "ActivityCompleted",
        "activity",
        2,
        0,
        value="result-a",
        activity_type="signal-a",
    )
    history = [
        {"sequence": 100, **later_position},
        {
            "sequence": 101,
            "event_type": "SignalReceived",
            "payload": {
                "signal_name": "mark",
                "arguments": serializer.encode(["between"], codec=serializer.AVRO_CODEC),
                "payload_codec": serializer.AVRO_CODEC,
            },
        },
        {"sequence": 102, **earlier_position},
    ]

    assert _completed_result(ParallelCompletionOrderSignalInterleaving, history) == {
        "results": ["result-a", "result-b"],
        "markers": ["between"],
    }


@pytest.mark.parametrize("completion_order", THREE_SIBLING_ORDERS)
def test_activity_failure_selection_follows_workflow_position(completion_order: tuple[int, ...]) -> None:
    events = [
        _terminal_event(
            "ActivityCompleted",
            "activity",
            3,
            0,
            value="ok",
            activity_type="activity-ok",
        ),
        _terminal_event(
            "ActivityFailed",
            "activity",
            3,
            1,
            activity_type="activity-fails-first",
            message="first positional activity failure",
            exception_type="FirstFailure",
        ),
        _terminal_event(
            "ActivityFailed",
            "activity",
            3,
            2,
            activity_type="activity-fails-second",
            message="second positional activity failure",
            exception_type="SecondFailure",
        ),
    ]

    assert _completed_result(
        ParallelCompletionOrderActivityFailures,
        _history_in_completion_order(events, completion_order),
    ) == {
        "activity_type": "activity-fails-first",
        "message": "first positional activity failure",
    }


@pytest.mark.parametrize("completion_order", THREE_SIBLING_ORDERS)
def test_child_failure_selection_follows_workflow_position(completion_order: tuple[int, ...]) -> None:
    events = [
        _terminal_event(
            "ChildRunCompleted",
            "child",
            3,
            0,
            value="ok",
            child_workflow_type="child-ok",
        ),
        _terminal_event(
            "ChildRunFailed",
            "child",
            3,
            1,
            child_workflow_type="child-fails-first",
            message="first positional child failure",
        ),
        _terminal_event(
            "ChildRunFailed",
            "child",
            3,
            2,
            child_workflow_type="child-fails-second",
            message="second positional child failure",
        ),
    ]

    assert _completed_result(
        ParallelCompletionOrderChildFailures,
        _history_in_completion_order(events, completion_order),
    ) == {
        "child_workflow_type": "child-fails-first",
        "message": "first positional child failure",
    }


@pytest.mark.parametrize("completion_order", THREE_SIBLING_ORDERS)
def test_mixed_failure_selection_follows_workflow_position(completion_order: tuple[int, ...]) -> None:
    events = [
        _terminal_event(
            "ChildRunFailed",
            "mixed",
            3,
            0,
            child_workflow_type="mixed-child-fails-first",
            message="first positional mixed failure",
        ),
        _terminal_event(
            "ActivityFailed",
            "mixed",
            3,
            1,
            activity_type="mixed-activity-fails-second",
            message="second positional mixed failure",
        ),
        _terminal_event("TimerFired", "mixed", 3, 2, timer_kind="sleep"),
    ]

    assert _completed_result(
        ParallelCompletionOrderMixedFailures,
        _history_in_completion_order(events, completion_order),
    ) == {
        "failure_type": "ChildWorkflowFailed",
        "message": "first positional mixed failure",
    }


@pytest.mark.parametrize("completion_order", [(0, 1), (1, 0)])
def test_repeated_identical_failures_stay_bound_to_yielded_positions(
    completion_order: tuple[int, ...],
) -> None:
    events = [
        _terminal_event(
            "ActivityFailed",
            "activity",
            2,
            index,
            activity_type="shared-failure",
            message=f"position-{index}-failed",
        )
        for index in range(2)
    ]

    assert (
        _completed_result(
            ParallelCompletionOrderRepeatedFailures,
            _history_in_completion_order(events, completion_order),
        )
        == "position-0-failed"
    )
