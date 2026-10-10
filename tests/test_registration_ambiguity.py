from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import Client, DuplicateRegistrationError, DurableWorkflowError, Worker, activity, workflow
from durable_workflow.testing import WorkflowEnvironment


def run(self: Any, ctx: Any) -> int:
    return 1


def first(self: Any, *args: Any) -> None:
    pass


def second(self: Any, *args: Any) -> None:
    pass


def definition(name: str, cls_name: str, members: dict[str, Any] | None = None, bases: tuple[type, ...] = ()) -> type:
    return workflow.defn(name=name)(type(cls_name, bases, {"run": run, **(members or {})}))


@pytest.mark.parametrize("consumer", ["worker", "replayer", "environment"])
def test_conflicting_workflow_names_report_both_definitions(consumer: str) -> None:
    first_cls = definition("shared-workflow", "FirstWorkflow")
    second_cls = definition("shared-workflow", "SecondWorkflow")
    client = AsyncMock(spec=Client)
    with pytest.raises(ValueError) as captured:
        if consumer == "worker":
            Worker(client, task_queue="registration", workflows=iter([first_cls, second_cls]))
        elif consumer == "replayer":
            workflow.Replayer(workflows=iter([first_cls, second_cls]))
        else:
            env = WorkflowEnvironment()
            env.register_workflow(first_cls)
            env.register_workflow(second_cls)
    assert captured.value.registration_kind == "workflow"  # type: ignore[attr-defined]
    assert captured.value.wire_name == "shared-workflow"  # type: ignore[attr-defined]
    assert "FirstWorkflow" in str(captured.value)
    assert "SecondWorkflow" in str(captured.value)
    assert client.mock_calls == []


@pytest.mark.parametrize("same_definition", [False, True])
def test_worker_rejects_duplicate_activities_before_network(same_definition: bool) -> None:
    first_activity = activity.defn(name="shared-activity")(first)
    second_activity = first_activity if same_definition else activity.defn(name="shared-activity")(second)
    client = AsyncMock(spec=Client)
    with pytest.raises(ValueError) as captured:
        Worker(client, task_queue="registration", activities=iter([first_activity, second_activity]))
    assert captured.value.registration_kind == "activity"  # type: ignore[attr-defined]
    assert captured.value.wire_name == "shared-activity"  # type: ignore[attr-defined]
    assert "first" in str(captured.value)
    assert ("first" if same_definition else "second") in str(captured.value)
    assert client.mock_calls == []


@pytest.mark.parametrize("consumer", ["worker", "replayer"])
def test_duplicate_list_entry_is_rejected(consumer: str) -> None:
    cls = definition("same-entry", "RepeatedWorkflow")
    with pytest.raises(ValueError, match="same-entry"):
        if consumer == "worker":
            Worker(AsyncMock(spec=Client), task_queue="registration", workflows=[cls, cls])
        else:
            workflow.Replayer(workflows=[cls, cls])


@pytest.mark.parametrize("kind", ["signal", "query", "update", "update_validator"])
@pytest.mark.parametrize("inherited", [False, True])
def test_conflicting_handler_names_are_rejected_at_decoration(kind: str, inherited: bool) -> None:
    decorate: Callable[..., Any] = getattr(workflow, kind)
    # Use distinct functions so metadata from one handler kind cannot bleed
    # into the next parameterized definition.
    def first_handler(self: Any, *args: Any) -> None:
        pass

    def second_handler(self: Any, *args: Any) -> None:
        pass

    bases = ()
    members = {"first_handler": decorate("approve")(first_handler)}
    if inherited:
        base = definition(f"base-{kind}", "BaseWorkflow", members)
        bases = (base,)
        members = {}
    members["second_handler"] = decorate("approve")(second_handler)
    with pytest.raises(ValueError) as captured:
        definition(f"ambiguous-{kind}", "AmbiguousWorkflow", members, bases)
    assert captured.value.registration_kind == kind  # type: ignore[attr-defined]
    assert captured.value.wire_name == "approve"  # type: ignore[attr-defined]
    assert "first_handler" in str(captured.value)
    assert "second_handler" in str(captured.value)


@pytest.mark.parametrize("kind", ["signal", "query", "update", "update_validator"])
def test_overriding_the_same_python_attribute_is_unambiguous(kind: str) -> None:
    decorate: Callable[..., Any] = getattr(workflow, kind)
    def original(self: Any) -> str:
        return "original"

    def replacement(self: Any) -> str:
        return "replacement"

    base = definition(f"override-base-{kind}", "OverrideBase", {"handler": decorate("approve")(original)})
    child = definition(
        f"override-child-{kind}", "OverrideChild", {"handler": decorate("approve")(replacement)}, (base,),
    )
    assert child().handler() == "replacement"


def test_handler_names_are_scoped_by_kind() -> None:
    cls = definition("scoped-handlers", "ScopedHandlers", {
        "signal": workflow.signal("approve")(lambda self: None),
        "query": workflow.query("approve")(lambda self: None),
        "update": workflow.update("approve")(lambda self: None),
        "validator": workflow.update_validator("approve")(lambda self: None),
    })
    assert cls.__workflow_signals__ == {"approve": "signal"}
    assert cls.__workflow_queries__ == {"approve": "query"}
    assert cls.__workflow_updates__ == {"approve": "update"}
    assert cls.__workflow_update_validators__ == {"approve": "validator"}


def test_distinct_names_and_separate_workers_remain_supported() -> None:
    first_cls = definition("one", "FirstDistinct")
    second_cls = definition("two", "SecondDistinct")
    alternative = definition("one", "AlternativeDefinition")
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="registration", workflows=iter([first_cls, second_cls]))
    assert worker.workflows == {"one": first_cls, "two": second_cls}
    assert Worker(client, task_queue="other", workflows=[alternative]).workflows == {"one": alternative}
    assert workflow.Replayer(workflows=[first_cls, second_cls]).replay([], workflow_type="one").commands[0].result == 1
    assert client.mock_calls == []


def test_environment_registration_is_idempotent_for_automatic_execution() -> None:
    cls = definition("environment-idempotent", "EnvironmentWorkflow")
    env = WorkflowEnvironment()
    env.register_workflow(cls)
    env.register_workflow(cls)
    assert env.execute_workflow(cls) == 1
    assert env.execute_workflow(cls) == 1


def test_mock_replacement_is_an_explicit_test_setting() -> None:
    env = WorkflowEnvironment()
    env.register_activity("mock", lambda: "first")
    env.register_activity("mock", lambda: "second")
    assert env._activity_fns["mock"]() == "second"


def test_duplicate_error_is_a_public_structured_sdk_error() -> None:
    error = DuplicateRegistrationError("workflow", "duplicate", run, first)
    assert isinstance(error, (DurableWorkflowError, ValueError))
    assert error.registration_kind == "workflow"
    assert error.wire_name == "duplicate"
    assert error.first_definition.endswith(".run")
    assert error.second_definition.endswith(".first")
