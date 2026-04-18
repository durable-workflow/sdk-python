from __future__ import annotations

from durable_workflow import workflow
from durable_workflow.workflow import WorkflowContext


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
