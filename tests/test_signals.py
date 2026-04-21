from __future__ import annotations

import logging
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import WorkflowPayloadDecodeError
from durable_workflow.workflow import (
    CompleteWorkflow,
    ScheduleActivity,
    WorkflowContext,
    replay,
)


@workflow.defn(name="approval-workflow")
class ApprovalWorkflow:
    def __init__(self) -> None:
        self.approved: bool = False
        self.approvals: list[str] = []

    @workflow.signal("approve")
    def on_approve(self, by: str) -> None:
        self.approved = True
        self.approvals.append(by)

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("wait_for_approval", [])
        return {"approved": self.approved, "approvals": list(self.approvals)}


@workflow.defn(name="typed-signal-workflow")
class TypedSignalWorkflow:
    def __init__(self) -> None:
        self.count: int = 0

    @workflow.signal("increment")
    def on_increment(self, amount: int) -> None:
        self.count += amount

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("tick", [])
        yield ctx.schedule_activity("tick", [])
        return self.count


class TestSignalDecoratorRegistry:
    def test_defn_collects_signal_methods_into_registry(self) -> None:
        signals = ApprovalWorkflow.__workflow_signals__  # type: ignore[attr-defined]

        assert signals == {"approve": "on_approve"}

    def test_signal_decorator_sets_signal_name_marker(self) -> None:
        assert ApprovalWorkflow.on_approve.__signal_name__ == "approve"  # type: ignore[attr-defined]

    def test_workflow_without_signal_methods_has_empty_registry(self) -> None:
        @workflow.defn(name="no-signals")
        class PlainWorkflow:
            def run(self, ctx: WorkflowContext) -> str:
                return "done"

        assert PlainWorkflow.__workflow_signals__ == {}  # type: ignore[attr-defined]


def _activity_completed_event(result: Any) -> dict[str, Any]:
    return {
        "event_type": "ActivityCompleted",
        "payload": {"result": serializer.envelope(result), "payload_codec": serializer.AVRO_CODEC},
    }


def _signal_received_event(name: str, args: list[Any]) -> dict[str, Any]:
    return {
        "event_type": "SignalReceived",
        "payload": {
            "signal_name": name,
            "value": serializer.envelope(args),
            "payload_codec": serializer.AVRO_CODEC,
        },
    }


class TestSignalDispatchDuringReplay:
    def test_signal_mutates_workflow_state_before_completion(self) -> None:
        events = [
            _signal_received_event("approve", ["alice"]),
            _activity_completed_event("done"),
        ]

        outcome = replay(ApprovalWorkflow, events, [])

        # Workflow returns — CompleteWorkflow emitted with mutated state.
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == {"approved": True, "approvals": ["alice"]}

    def test_multiple_signals_apply_in_history_order(self) -> None:
        events = [
            _signal_received_event("increment", [3]),
            _activity_completed_event(None),
            _signal_received_event("increment", [5]),
            _activity_completed_event(None),
        ]

        outcome = replay(TypedSignalWorkflow, events, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == 8

    def test_signal_before_any_activity_still_applies(self) -> None:
        events = [
            _signal_received_event("approve", ["bob"]),
        ]

        outcome = replay(ApprovalWorkflow, events, [])

        # Activity has not completed yet — workflow yields ScheduleActivity and
        # the signal has already been applied to instance state.
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)

    def test_unknown_signal_is_silently_dropped(self) -> None:
        events = [
            _signal_received_event("not_a_real_signal", ["payload"]),
            _activity_completed_event("done"),
        ]

        outcome = replay(ApprovalWorkflow, events, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        # Handler for "approve" was never invoked because no matching signal
        # arrived — state remains the default.
        assert outcome.commands[0].result == {"approved": False, "approvals": []}

    def test_signal_with_no_args_decodes_to_empty_handler_call(self) -> None:
        @workflow.defn(name="ping-workflow")
        class PingWorkflow:
            def __init__(self) -> None:
                self.pings: int = 0

            @workflow.signal("ping")
            def on_ping(self) -> None:
                self.pings += 1

            def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
                yield ctx.schedule_activity("step", [])
                return self.pings

        events = [
            _signal_received_event("ping", []),
            _signal_received_event("ping", []),
            _activity_completed_event(None),
        ]

        outcome = replay(PingWorkflow, events, [])

        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == 2

    def test_signal_payload_decode_failure_logs_workflow_context(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        events = [
            {
                "event_id": "evt-7",
                "event_type": "SignalReceived",
                "payload": {
                    "signal_name": "approve",
                    "value": "{not valid json",
                    "payload_codec": "json",
                },
            },
        ]

        with (
            caplog.at_level(logging.ERROR, logger="durable_workflow.workflow.replay"),
            pytest.raises(WorkflowPayloadDecodeError) as exc_info,
        ):
            replay(ApprovalWorkflow, events, [], workflow_id="wf-1", run_id="run-1", payload_codec="json")

        err = exc_info.value
        assert err.workflow_id == "wf-1"
        assert err.run_id == "run-1"
        assert err.event_id == "evt-7"
        assert err.receiver_kind == "signal"
        assert err.receiver_name == "approve"
        assert err.codec == "json"
        assert err.payload_head == "{not valid json"
        assert err.exception_type == "ValueError"

        assert len(caplog.records) == 1
        payload = caplog.records[0].durable_workflow_payload_decode
        assert payload["workflow_id"] == "wf-1"
        assert payload["run_id"] == "run-1"
        assert payload["event_id"] == "evt-7"
        assert payload["signal_name"] == "approve"
        assert payload["codec"] == "json"
        assert payload["payload_head"] == "{not valid json"
        assert payload["exception_type"] == "ValueError"
