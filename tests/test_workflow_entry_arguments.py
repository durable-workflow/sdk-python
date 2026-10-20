from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.errors import DurableWorkflowError
from durable_workflow.workflow import CompleteWorkflow, FailWorkflow, WorkflowContext, replay


@workflow.defn(name="entry.required")
class RequiredArgumentWorkflow:
    calls = 0

    def run(self, ctx: WorkflowContext, amount: int, required: int) -> int:
        type(self).calls += 1
        return amount + required


@workflow.defn(name="entry.keyword-only")
class KeywordOnlyWorkflow:
    def run(self, ctx: WorkflowContext, amount: int, *, required: int) -> int:
        return amount + required


@workflow.defn(name="entry.excess")
class ExcessArgumentWorkflow:
    def run(self, ctx: WorkflowContext) -> int:
        return 7


@workflow.defn(name="entry.optional")
class OptionalArgumentWorkflow:
    def run(self, ctx: WorkflowContext, amount: int, extra: int = 0) -> int:
        return amount + extra


@workflow.defn(name="entry.variadic")
class VariadicWorkflow:
    def run(self, ctx: WorkflowContext, *amounts: int) -> int:
        return sum(amounts)


@workflow.defn(name="entry.body-error")
class BodyTypeErrorWorkflow:
    def run(self, ctx: WorkflowContext, amount: int) -> int:
        raise TypeError("application body failed")


@workflow.defn(name="entry.generator-body-error")
class GeneratorBodyTypeErrorWorkflow:
    def run(self, ctx: WorkflowContext, amount: int):  # type: ignore[no-untyped-def]
        if False:
            yield ctx.start_timer(1)
        raise TypeError("generator application body failed")


@pytest.mark.parametrize("definition", [RequiredArgumentWorkflow, KeywordOnlyWorkflow, ExcessArgumentWorkflow])
@pytest.mark.parametrize("committed_wait", [False, True])
def test_entry_binding_refuses_without_running_body_or_rewriting_input(
    definition: type, committed_wait: bool,
) -> None:
    history = [
        {"event_type": "ConditionWaitOpened", "payload": {
            "sequence": 1, "condition_wait_id": "original-wait", "condition_key": "ready",
        }},
    ] if committed_wait else []
    original = deepcopy(history)
    arguments: list[Any] = [{"private": "input-must-not-appear-in-diagnostics"}]
    RequiredArgumentWorkflow.calls = 0
    with pytest.raises(DurableWorkflowError, match="workflow_entry_arguments_mismatch") as captured:
        replay(definition, history, arguments)
    assert type(captured.value).__name__ == "WorkflowEntryArgumentsMismatchError"
    assert getattr(captured.value, "reason") == "workflow_entry_arguments_mismatch"
    assert "input-must-not-appear-in-diagnostics" not in str(captured.value)
    assert RequiredArgumentWorkflow.calls == 0
    assert arguments == [{"private": "input-must-not-appear-in-diagnostics"}]
    assert history == original


@pytest.mark.parametrize("definition", [BodyTypeErrorWorkflow, GeneratorBodyTypeErrorWorkflow])
def test_genuine_body_type_errors_remain_workflow_failures(definition: type) -> None:
    outcome = replay(definition, [], [7])
    assert len(outcome.commands) == 1
    failure = outcome.commands[0]
    assert isinstance(failure, FailWorkflow)
    assert failure.exception_type == "TypeError"
    assert "application body failed" in failure.message


@pytest.mark.parametrize(
    ("definition", "arguments", "expected"),
    [(OptionalArgumentWorkflow, [7], 7), (VariadicWorkflow, [7, 2], 9)],
)
def test_compatible_entry_arguments_complete(definition: type, arguments: list[int], expected: int) -> None:
    command = replay(definition, [], arguments).commands[0]
    assert isinstance(command, CompleteWorkflow)
    assert command.result == expected


def test_python_annotations_do_not_become_runtime_argument_checks() -> None:
    command = replay(OptionalArgumentWorkflow, [], ["not-an-int", "-suffix"]).commands[0]
    assert isinstance(command, CompleteWorkflow)
    assert command.result == "not-an-int-suffix"
