from __future__ import annotations

from collections.abc import Generator
from copy import deepcopy
from typing import Any
from unittest.mock import patch

from durable_workflow import serializer, workflow
from durable_workflow.errors import ActivityFailed
from durable_workflow.workflow import (
    CompleteUpdate,
    CompleteWorkflow,
    ScheduleActivity,
    StartTimer,
    WaitCondition,
    WorkflowContext,
    apply_update,
    query_state,
    replay,
)


@workflow.defn(name="tests.replay.update-signal-condition-timer")
class UpdateSignalConditionTimerWorkflow:
    def __init__(self) -> None:
        self.message: str | None = None
        self.signal_marker: str | None = None
        self.finish_result: dict[str, Any] | None = None
        self.failure_type: str | None = None

    @workflow.query("current")
    def current(self) -> dict[str, Any]:
        return {
            "message": self.message,
            "signal_marker": self.signal_marker,
            "finish_result": self.finish_result,
            "failure_type": self.failure_type,
        }

    @workflow.update("set_message")
    def set_message(self, message: str) -> dict[str, str]:
        self.message = message
        return {"message": message}

    @workflow.signal("condition_ready")
    def condition_ready(self, marker: str) -> None:
        self.signal_marker = marker

    @workflow.signal("finish")
    def finish(self, result: dict[str, Any]) -> None:
        self.finish_result = dict(result)

    def run(self, ctx: WorkflowContext) -> Generator[Any, Any, dict[str, Any]]:
        yield ctx.sleep(1)
        successful = yield ctx.schedule_activity("successful", [{"attempt": 1}])

        try:
            yield ctx.schedule_activity("non-retryable-failure", [])
        except ActivityFailed as exc:
            self.failure_type = exc.exception_type

        condition_completed = yield ctx.wait_condition(
            lambda: self.message is not None and self.signal_marker is not None,
            key="update-and-signal",
            timeout=1,
        )
        # Accepted receivers make this condition immediately true. It has no
        # durable history row and must not consume the following activity.
        yield ctx.wait_condition(
            lambda: self.message is not None and self.signal_marker is not None,
            key="accepted-receivers",
        )
        after_condition = yield ctx.schedule_activity(
            "after-condition",
            [self.message, self.signal_marker],
        )
        yield ctx.wait_condition(
            lambda: self.finish_result is not None,
            key="finish",
            timeout=30,
        )

        return {
            "status": "completed",
            "successful": successful,
            "failure_type": self.failure_type,
            "message": self.message,
            "signal_marker": self.signal_marker,
            "condition_completed": condition_completed,
            "after_condition": after_condition,
            "result": self.finish_result,
        }


def _event(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"event_type": event_type, "payload": payload}


def _payload(value: Any) -> dict[str, Any]:
    return serializer.envelope(value)


def _history() -> list[dict[str, Any]]:
    return [
        _event("WorkflowStarted", {"workflow_type": "tests.replay.update-signal-condition-timer"}),
        _event("TimerScheduled", {"sequence": 1, "timer_kind": "sleep"}),
        _event("TimerFired", {"sequence": 1, "timer_kind": "sleep"}),
        _event(
            "ActivityScheduled",
            {"sequence": 2, "activity_type": "successful"},
        ),
        _event(
            "ActivityStarted",
            {"sequence": 2, "activity_type": "successful"},
        ),
        _event(
            "ActivityCompleted",
            {
                "sequence": 2,
                "activity_type": "successful",
                "result": _payload({"activity": "successful"}),
            },
        ),
        _event(
            "ActivityScheduled",
            {"sequence": 3, "activity_type": "non-retryable-failure"},
        ),
        _event(
            "ActivityStarted",
            {"sequence": 3, "activity_type": "non-retryable-failure"},
        ),
        _event(
            "ActivityFailed",
            {
                "sequence": 3,
                "activity_type": "non-retryable-failure",
                "message": "planned typed failure",
                "exception_type": "PlannedFailure",
                "exception_class": "tests.PlannedFailure",
                "non_retryable": True,
            },
        ),
        _event(
            "ConditionWaitOpened",
            {
                "sequence": 8,
                "condition_wait_id": "condition:8",
                "condition_key": "update-and-signal",
                "timeout_seconds": 1,
            },
        ),
        # The internal timer owns its sequence but remains self-identifying.
        _event(
            "TimerScheduled",
            {
                "sequence": 9,
                "timer_id": "condition-timer:9",
                "delay_seconds": 1,
                "timer_kind": "condition_timeout",
                "condition_wait_id": "condition:8",
                "condition_key": "update-and-signal",
            },
        ),
        _event(
            "UpdateAccepted",
            {
                "update_id": "update-1",
                "update_name": "set_message",
                "arguments": _payload(["updated"]),
            },
        ),
        _event(
            "UpdateApplied",
            {
                "sequence": 8,
                "update_id": "update-1",
                "update_name": "set_message",
                "arguments": _payload(["updated"]),
            },
        ),
        _event(
            "UpdateCompleted",
            {
                "sequence": 8,
                "update_id": "update-1",
                "update_name": "set_message",
                "result": _payload({"message": "updated"}),
            },
        ),
        _event(
            "SignalReceived",
            {
                "workflow_sequence": 8,
                "signal_name": "condition_ready",
                "value": _payload(["delivered"]),
            },
        ),
        _event(
            "TimerFired",
            {
                "sequence": 9,
                "timer_id": "condition-timer:9",
                "delay_seconds": 1,
                "timer_kind": "condition_timeout",
                "condition_wait_id": "condition:8",
                "condition_key": "update-and-signal",
            },
        ),
        _event(
            "ActivityScheduled",
            {"sequence": 10, "activity_type": "after-condition"},
        ),
        _event(
            "ActivityStarted",
            {"sequence": 10, "activity_type": "after-condition"},
        ),
        _event(
            "ActivityCompleted",
            {
                "sequence": 10,
                "activity_type": "after-condition",
                "result": _payload({"activity": "after-condition"}),
            },
        ),
        _event(
            "ConditionWaitOpened",
            {
                "sequence": 11,
                "condition_wait_id": "condition:11",
                "condition_key": "finish",
                "timeout_seconds": 30,
            },
        ),
        _event(
            "TimerScheduled",
            {
                "sequence": 11,
                "timer_kind": "condition_timeout",
                "condition_wait_id": "condition:11",
                "condition_key": "finish",
                "delay_seconds": 30,
            },
        ),
        _event(
            "SignalReceived",
            {
                "workflow_sequence": 11,
                "signal_name": "finish",
                "value": _payload([{"typed": "non-null"}]),
            },
        ),
        _event(
            "ConditionWaitSatisfied",
            {
                "sequence": 11,
                "condition_wait_id": "condition:11",
                "condition_key": "finish",
            },
        ),
        _event("WorkflowCompleted", {"result": _payload({"typed": "non-null"})}),
    ]


def _legacy_history() -> list[dict[str, Any]]:
    history = deepcopy(_history())
    for event in history:
        payload = event.get("payload", {})
        if payload.get("timer_id") != "condition-timer:9":
            continue
        payload.pop("timer_kind", None)
        payload.pop("condition_wait_id", None)
        payload.pop("condition_key", None)
    return history


class TestUpdateSignalConditionReplay:
    def test_current_history_uses_explicit_timeout_identity_without_legacy_classification(self) -> None:
        with patch(
            "durable_workflow.workflow._legacy_condition_timeout_timer_sequence_aliases",
            side_effect=AssertionError("current history entered the legacy timeout classifier"),
        ):
            outcome = replay(UpdateSignalConditionTimerWorkflow, _history(), [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)

    def test_legacy_metadata_poor_timeout_history_remains_replayable(self) -> None:
        outcome = replay(UpdateSignalConditionTimerWorkflow, _legacy_history(), [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result["status"] == "completed"

    def test_update_task_replay_does_not_advance_the_ordinary_command_cursor(self) -> None:
        history = _history()
        accepted_index = next(index for index, event in enumerate(history) if event["event_type"] == "UpdateAccepted")

        state_before_update = query_state(
            UpdateSignalConditionTimerWorkflow,
            history[:accepted_index],
            [],
            "current",
        )
        assert state_before_update == {
            "message": None,
            "signal_marker": None,
            "finish_result": None,
            "failure_type": "PlannedFailure",
        }

        command = apply_update(
            UpdateSignalConditionTimerWorkflow,
            history[: accepted_index + 1],
            [],
            "update-1",
        )
        assert isinstance(command, CompleteUpdate)
        assert command.result == {"message": "updated"}

        applied_index = next(index for index, event in enumerate(history) if event["event_type"] == "UpdateApplied")
        ordinary_replay = replay(
            UpdateSignalConditionTimerWorkflow,
            history[: applied_index + 1],
            [],
        )
        assert len(ordinary_replay.commands) == 1
        assert isinstance(ordinary_replay.commands[0], WaitCondition)

    def test_every_history_prefix_cold_replays_each_workflow_sequence_once(self) -> None:
        history = _history()
        expected_commands = [
            StartTimer,
            StartTimer,
            StartTimer,
            ScheduleActivity,
            ScheduleActivity,
            ScheduleActivity,
            ScheduleActivity,
            ScheduleActivity,
            ScheduleActivity,
            WaitCondition,
            WaitCondition,
            WaitCondition,
            WaitCondition,
            WaitCondition,
            WaitCondition,
            ScheduleActivity,
            ScheduleActivity,
            ScheduleActivity,
            ScheduleActivity,
            WaitCondition,
            WaitCondition,
            WaitCondition,
            CompleteWorkflow,
            CompleteWorkflow,
            CompleteWorkflow,
        ]

        for prefix_length, expected_command in enumerate(expected_commands):
            outcome = replay(
                UpdateSignalConditionTimerWorkflow,
                history[:prefix_length],
                [],
            )

            assert len(outcome.commands) == 1, prefix_length
            assert isinstance(outcome.commands[0], expected_command), prefix_length
            if prefix_length in (15, 16, 17, 18):
                assert isinstance(outcome.commands[0], ScheduleActivity)
                assert outcome.commands[0].activity_type == "after-condition"
                assert outcome.commands[0].arguments == ["updated", "delivered"]

        terminal = replay(UpdateSignalConditionTimerWorkflow, history, [])
        assert len(terminal.commands) == 1
        assert isinstance(terminal.commands[0], CompleteWorkflow)
        assert terminal.commands[0].result == {
            "status": "completed",
            "successful": {"activity": "successful"},
            "failure_type": "PlannedFailure",
            "message": "updated",
            "signal_marker": "delivered",
            "condition_completed": False,
            "after_condition": {"activity": "after-condition"},
            "result": {"typed": "non-null"},
        }
