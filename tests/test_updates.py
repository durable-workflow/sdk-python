from __future__ import annotations

from durable_workflow import serializer, workflow
from durable_workflow.workflow import (
    CompleteUpdate,
    FailUpdate,
    WorkflowContext,
    apply_update,
    query_state,
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
