from __future__ import annotations

from collections.abc import Generator
from typing import Any

from durable_workflow import serializer, workflow
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext, commands_to_server_commands, replay


@workflow.defn(name="tests.ordered-condition-input")
class Approval:
    def __init__(self) -> None:
        self.approved = False

    @workflow.signal("approval")
    def approval(self, accepted: bool) -> None:
        self.approved = accepted

    def run(self, ctx: WorkflowContext) -> Generator[Any, Any, bool]:
        return (yield ctx.wait_condition(lambda: self.approved, key="approval", timeout=2))


def test_a_replacement_worker_replays_the_first_applied_true_prefix() -> None:
    opening = replay(Approval, [], [], allow_ordered_condition_inputs=True)
    opened, = commands_to_server_commands(opening.commands, "test")
    assert opened.pop("type") == "open_condition_wait"
    assert opened["condition_wait_input_order"] == "durable_receipt_order"
    opened.update(sequence=1, condition_wait_id="approval-wait")
    timer = {**opened, "timer_id": "original-timer", "timer_kind": "condition_timeout", "delay_seconds": 2}

    def event(sequence: int, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"sequence": sequence, "event_type": name, "payload": payload}

    receipts = [
        event(5, "SignalReceived", {"workflow_command_id": "approve", "signal_name": "approval",
                                    "arguments": serializer.envelope([True])}),
        event(6, "SignalReceived", {"workflow_command_id": "withdraw", "signal_name": "approval",
                                    "arguments": serializer.envelope([False])}),
    ]
    history = [event(3, "ConditionWaitOpened", opened), event(4, "TimerScheduled", timer),
               *receipts, event(7, "TimerFired", timer)]
    still_pending = replay(Approval, history, [], allow_ordered_condition_inputs=True, pending_condition_input=(5,))
    assert not still_pending.commands

    history.extend([
        event(8, "SignalApplied", {"workflow_command_id": "approve", "signal_name": "approval"}),
        event(9, "SignalApplied", {"workflow_command_id": "withdraw", "signal_name": "approval"}),
    ])
    outcome = replay(Approval, history, [], allow_ordered_condition_inputs=True, pending_condition_input=(None,))
    commands = commands_to_server_commands(outcome.commands, "test")
    assert commands[0] == {"type": "resolve_condition_wait", "condition_wait_id": "approval-wait",
                           "satisfied": True, "input_receipt_sequence": 5}
    assert isinstance(outcome.commands[-1], CompleteWorkflow) and outcome.commands[-1].result is True

    history.append(event(10, "ConditionWaitSatisfied", {
        **opened, "input_receipt_sequence": 5, "timer_fired_sequence": 7, "timer_id": "original-timer",
    }))
    recorded = replay(Approval, history, [])
    assert isinstance(recorded.commands[-1], CompleteWorkflow) and recorded.commands[-1].result is True
    assert [command["type"] for command in commands_to_server_commands(recorded.commands, "test")] == ["complete_workflow"]
