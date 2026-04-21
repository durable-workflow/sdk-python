from __future__ import annotations

import logging

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import WorkflowPayloadDecodeError
from durable_workflow.workflow import (
    CompleteUpdate,
    FailUpdate,
    WorkflowContext,
    apply_update,
    query_state,
    replay,
)


@workflow.defn(name="update-receiver")
class UpdateReceiver:
    def __init__(self) -> None:
        self.approved = False
        self.priority = 0

    @workflow.update("approve")
    def approve(self, approved: bool) -> dict[str, bool]:
        self.approved = approved
        return {"approved": self.approved}

    @approve.validator  # type: ignore[attr-defined]
    def validate_approve(self, approved: bool) -> None:
        if not isinstance(approved, bool):
            raise ValueError("approved must be boolean")

    @workflow.update("set_priority")
    def set_priority(self, priority: int) -> int:
        self.priority = priority
        return self.priority

    @workflow.update_validator("set_priority")
    def validate_priority(self, priority: int) -> None:
        if priority < 0:
            raise ValueError("priority must be non-negative")

    def run(self, ctx: WorkflowContext) -> str:
        return "waiting"


class TestUpdateDecoratorRegistry:
    def test_defn_collects_update_methods_into_registry(self) -> None:
        updates = UpdateReceiver.__workflow_updates__  # type: ignore[attr-defined]

        assert updates == {
            "approve": "approve",
            "set_priority": "set_priority",
        }

    def test_defn_collects_update_validators_into_registry(self) -> None:
        validators = UpdateReceiver.__workflow_update_validators__  # type: ignore[attr-defined]

        assert validators == {
            "approve": "validate_approve",
            "set_priority": "validate_priority",
        }

    def test_update_decorator_sets_update_name_marker(self) -> None:
        assert UpdateReceiver.approve.__update_name__ == "approve"  # type: ignore[attr-defined]

    def test_update_validator_marker_is_set_for_validator_styles(self) -> None:
        assert UpdateReceiver.validate_approve.__update_validator_name__ == "approve"  # type: ignore[attr-defined]
        assert UpdateReceiver.validate_priority.__update_validator_name__ == "set_priority"  # type: ignore[attr-defined]

    def test_workflow_without_update_methods_has_empty_registries(self) -> None:
        @workflow.defn(name="no-updates")
        class PlainWorkflow:
            def run(self, ctx: WorkflowContext) -> str:
                return "done"

        assert PlainWorkflow.__workflow_updates__ == {}  # type: ignore[attr-defined]
        assert PlainWorkflow.__workflow_update_validators__ == {}  # type: ignore[attr-defined]


@workflow.defn(name="stateful-update-receiver")
class StatefulUpdateReceiver:
    def __init__(self) -> None:
        self.count = 0

    @workflow.signal("increment")
    def increment_signal(self, amount: int) -> None:
        self.count += amount

    @workflow.update("increment")
    def increment_update(self, amount: int) -> dict[str, int]:
        self.count += amount
        return {"count": self.count}

    @increment_update.validator  # type: ignore[attr-defined]
    def validate_increment(self, amount: int) -> None:
        if amount < 0:
            raise ValueError("amount must be non-negative")

    @workflow.update("explode")
    def explode(self) -> None:
        raise RuntimeError("update exploded")

    @workflow.query("count")
    def current_count(self) -> int:
        return self.count

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("wait", [])
        return self.count


def _signal_received_event(name: str, args: list[object]) -> dict[str, object]:
    return {
        "event_type": "SignalReceived",
        "payload": {
            "signal_name": name,
            "value": serializer.encode(args, codec="json"),
            "payload_codec": "json",
        },
    }


def _workflow_started_event() -> dict[str, object]:
    return {
        "event_type": "WorkflowStarted",
        "payload": {"timestamp": "2026-04-21T00:00:00Z"},
    }


def _update_accepted_event(
    update_id: str,
    name: str,
    args: list[object],
) -> dict[str, object]:
    return {
        "event_type": "UpdateAccepted",
        "payload": {
            "update_id": update_id,
            "update_name": name,
            "arguments": serializer.encode(args, codec="json"),
            "payload_codec": "json",
        },
    }


def _update_applied_event(
    update_id: str,
    name: str,
    args: list[object],
) -> dict[str, object]:
    return {
        "event_type": "UpdateApplied",
        "payload": {
            "update_id": update_id,
            "update_name": name,
            "arguments": serializer.encode(args, codec="json"),
            "payload_codec": "json",
        },
    }


class TestUpdateApplicationReplay:
    def test_start_boundary_update_applies_after_workflow_initialization(self) -> None:
        @workflow.defn(name="start-boundary-update-receiver")
        class StartBoundaryUpdateReceiver:
            def __init__(self) -> None:
                self.messages: list[str] = []

            @workflow.update("append")
            def append(self, value: str) -> list[str]:
                self.messages.append(value)
                return list(self.messages)

            @workflow.query("messages")
            def read_messages(self) -> list[str]:
                return list(self.messages)

            def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
                self.messages = ["started"]
                yield ctx.schedule_activity("wait", [])
                return list(self.messages)

        history = [
            _workflow_started_event(),
            _update_applied_event("upd-start", "append", ["after-start"]),
        ]

        assert query_state(
            StartBoundaryUpdateReceiver,
            history,
            [],
            "messages",
            payload_codec="json",
        ) == ["started", "after-start"]

    def test_replay_applies_committed_update_events_to_workflow_state(self) -> None:
        history = [
            _update_applied_event("upd-1", "increment", [3]),
            _signal_received_event("increment", [4]),
        ]

        assert query_state(
            StatefulUpdateReceiver,
            history,
            [],
            "count",
            payload_codec="json",
        ) == 7

    def test_apply_update_completes_accepted_update_with_handler_result(self) -> None:
        history = [
            _signal_received_event("increment", [2]),
            _update_accepted_event("upd-2", "increment", [5]),
        ]

        command = apply_update(
            StatefulUpdateReceiver,
            history,
            [],
            "upd-2",
            payload_codec="json",
        )

        assert isinstance(command, CompleteUpdate)
        assert command.update_id == "upd-2"
        assert command.result == {"count": 7}

    def test_apply_update_fails_unknown_updates(self) -> None:
        command = apply_update(
            StatefulUpdateReceiver,
            [_update_accepted_event("upd-3", "missing", [])],
            [],
            "upd-3",
            payload_codec="json",
        )

        assert isinstance(command, FailUpdate)
        assert command.update_id == "upd-3"
        assert command.exception_type == "UnknownUpdate"
        assert "unknown update" in command.message

    def test_apply_update_fails_validator_rejections(self) -> None:
        command = apply_update(
            StatefulUpdateReceiver,
            [_update_accepted_event("upd-4", "increment", [-1])],
            [],
            "upd-4",
            payload_codec="json",
        )

        assert isinstance(command, FailUpdate)
        assert command.update_id == "upd-4"
        assert command.exception_type == "ValueError"
        assert "amount must be non-negative" in command.message

    def test_apply_update_fails_handler_exceptions(self) -> None:
        command = apply_update(
            StatefulUpdateReceiver,
            [_update_accepted_event("upd-5", "explode", [])],
            [],
            "upd-5",
            payload_codec="json",
        )

        assert isinstance(command, FailUpdate)
        assert command.update_id == "upd-5"
        assert command.exception_type == "RuntimeError"
        assert "update exploded" in command.message

    def test_replay_update_payload_decode_failure_logs_workflow_context(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        history = [
            {
                "event_id": "evt-8",
                "event_type": "UpdateApplied",
                "payload": {
                    "update_id": "upd-bad",
                    "update_name": "increment",
                    "arguments": "{not valid json",
                    "payload_codec": "json",
                },
            },
        ]

        with (
            caplog.at_level(logging.ERROR, logger="durable_workflow.workflow.replay"),
            pytest.raises(WorkflowPayloadDecodeError) as exc_info,
        ):
            replay(StatefulUpdateReceiver, history, [], workflow_id="wf-2", run_id="run-2", payload_codec="json")

        err = exc_info.value
        assert err.workflow_id == "wf-2"
        assert err.run_id == "run-2"
        assert err.event_id == "evt-8"
        assert err.receiver_kind == "update"
        assert err.receiver_name == "increment"
        assert err.codec == "json"
        assert err.payload_head == "{not valid json"
        assert err.exception_type == "ValueError"

        assert len(caplog.records) == 1
        payload = caplog.records[0].durable_workflow_payload_decode
        assert payload["workflow_id"] == "wf-2"
        assert payload["run_id"] == "run-2"
        assert payload["event_id"] == "evt-8"
        assert payload["update_name"] == "increment"
        assert payload["codec"] == "json"
        assert payload["payload_head"] == "{not valid json"
        assert payload["exception_type"] == "ValueError"

    def test_apply_update_decode_failure_uses_typed_error_context(self) -> None:
        command = apply_update(
            StatefulUpdateReceiver,
            [
                {
                    "event_id": "evt-9",
                    "event_type": "UpdateAccepted",
                    "payload": {
                        "update_id": "upd-bad",
                        "update_name": "increment",
                        "arguments": "{not valid json",
                        "payload_codec": "json",
                    },
                },
            ],
            [],
            "upd-bad",
            workflow_id="wf-3",
            run_id="run-3",
            payload_codec="json",
        )

        assert isinstance(command, FailUpdate)
        assert command.update_id == "upd-bad"
        assert command.exception_type == "WorkflowPayloadDecodeError"
        assert "update 'increment' payload decode failed" in command.message
