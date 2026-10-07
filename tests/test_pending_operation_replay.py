"""Scheduled scalar work stays the same operation when signals wake replay."""
from __future__ import annotations

from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext, query_state, replay


@workflow.defn(name="tests.replay.pending-operation")
class PendingOperation:
    def __init__(self) -> None:
        self.total = 0
        self.released = False

    @workflow.signal("increment")
    def increment(self, amount: int) -> None:
        self.total += amount

    @workflow.signal("release")
    def release(self) -> None:
        self.released = True

    @workflow.query("total")
    def current(self) -> int:
        return self.total

    def run(self, ctx: WorkflowContext, kind: str):  # type: ignore[no-untyped-def]
        first = yield ctx.schedule_activity("echo", ["sample"])
        if kind == "timer":
            yield ctx.sleep(120)
        elif kind == "activity":
            yield ctx.schedule_activity("work", [first])
        else:
            yield ctx.start_child_workflow("child", [first])
        yield ctx.wait_condition(lambda: self.released, key="release")
        return self.total


def initial_history() -> list[dict[str, Any]]:
    return [
        {"event_type": "ActivityCompleted", "payload": {
            "sequence": 1, "activity_type": "echo", "result": serializer.envelope("sample"),
        }},
    ]


OPENINGS = {
    "timer": {"event_type": "TimerScheduled", "payload": {
        "sequence": 2, "timer_id": "original-timer", "delay_seconds": 120,
        "fire_at": "2026-10-07T06:02:00Z",
    }},
    "activity": {"event_type": "ActivityScheduled", "payload": {
        "sequence": 2, "activity_type": "work", "activity_execution_id": "original-activity",
    }},
    "child": {"event_type": "ChildWorkflowScheduled", "payload": {
        "sequence": 2, "workflow_type": "child", "child_workflow_instance_id": "original-child",
    }},
}


@pytest.mark.parametrize("kind", OPENINGS)
def test_signal_replay_does_not_reschedule_pending_operation(kind: str) -> None:
    history = initial_history() + [OPENINGS[kind]]
    for amount in (7, 11):
        history.append({"event_type": "SignalReceived", "payload": {
            "signal_name": "increment", "arguments": serializer.envelope([amount]),
        }})
        # Each call constructs a fresh workflow instance, as a replacement worker does.
        outcome = replay(PendingOperation, history, [kind])
        assert outcome.commands == []
        assert query_state(PendingOperation, history, [kind], "total") == sum(
            serializer.decode_envelope(event["payload"]["arguments"])[0]
            for event in history if event["event_type"] == "SignalReceived"
        )


@pytest.mark.parametrize("kind", OPENINGS)
def test_new_operation_is_still_scheduled(kind: str) -> None:
    outcome = replay(PendingOperation, initial_history(), [kind])
    assert len(outcome.commands) == 1
    assert outcome.commands[0].to_server_command("queue")["type"] == {
        "timer": "start_timer", "activity": "schedule_activity", "child": "start_child_workflow",
    }[kind]


def test_original_timer_completion_consumes_signals_once() -> None:
    history = initial_history() + [OPENINGS["timer"],
        {"event_type": "SignalReceived", "payload": {
            "signal_name": "increment", "arguments": serializer.envelope([7]),
        }},
        {"event_type": "TimerFired", "payload": {"sequence": 2, "timer_id": "original-timer"}},
        {"event_type": "SignalReceived", "payload": {
            "signal_name": "release", "arguments": serializer.envelope([]),
        }},
    ]
    outcome = replay(PendingOperation, history, ["timer"])
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == 7


def test_pending_timer_still_rejects_changed_delay() -> None:
    changed = {"event_type": "TimerScheduled", "payload": {**OPENINGS["timer"]["payload"], "delay_seconds": 121}}
    with pytest.raises(NonDeterministicReplayError) as captured:
        replay(PendingOperation, initial_history() + [changed], ["timer"])
    assert captured.value.workflow_sequence == 2
    assert captured.value.recorded_event_types == ["TimerScheduled"]


@pytest.mark.parametrize("kind", ["activity", "child"])
def test_original_external_operation_completion_consumes_signals_once(kind: str) -> None:
    terminal = {
        "event_type": "ActivityCompleted" if kind == "activity" else "ChildRunCompleted",
        "payload": {**OPENINGS[kind]["payload"], "result": serializer.envelope("original-result")},
    }
    history = initial_history() + [OPENINGS[kind],
        {"event_type": "SignalReceived", "payload": {
            "signal_name": "increment", "arguments": serializer.envelope([7]),
        }}, terminal,
        {"event_type": "SignalReceived", "payload": {
            "signal_name": "release", "arguments": serializer.envelope([]),
        }},
    ]
    assert replay(PendingOperation, history, [kind]).commands == [CompleteWorkflow(7)]


@pytest.mark.parametrize("kind", OPENINGS)
def test_recorded_operation_cannot_be_replaced_by_a_different_kind(kind: str) -> None:
    different = "activity" if kind == "timer" else "timer"
    with pytest.raises(NonDeterministicReplayError) as captured:
        replay(PendingOperation, initial_history() + [OPENINGS[kind]], [different])
    assert captured.value.workflow_sequence == 2


def test_legacy_timer_without_recorded_delay_still_waits_for_original_operation() -> None:
    legacy = {"event_type": "TimerScheduled", "payload": {"sequence": 2, "timer_id": "original-timer"}}
    assert replay(PendingOperation, initial_history() + [legacy], ["timer"]).commands == []


def test_already_duplicated_timers_are_not_silently_skipped() -> None:
    duplicate = {"event_type": "TimerScheduled", "payload": {
        **OPENINGS["timer"]["payload"], "sequence": 3, "timer_id": "duplicate-timer",
    }}
    history = initial_history() + [OPENINGS["timer"], duplicate,
        {"event_type": "TimerFired", "payload": {"sequence": 2, "timer_id": "original-timer"}},
        {"event_type": "TimerFired", "payload": {"sequence": 3, "timer_id": "duplicate-timer"}},
    ]
    with pytest.raises(NonDeterministicReplayError) as captured:
        replay(PendingOperation, history, ["timer"])
    assert captured.value.workflow_sequence == 3
