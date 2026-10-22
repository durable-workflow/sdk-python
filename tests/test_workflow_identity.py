from __future__ import annotations

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.workflow import (
    CompleteWorkflow,
    ContinueAsNew,
    ScheduleActivity,
    WorkflowContext,
    replay,
)


@workflow.defn(name="identity-across-replay")
class IdentityAcrossReplay:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        before = [ctx.workflow_id, ctx.run_id]
        value = yield ctx.schedule_activity("identity", before)
        return {"before": before, "after": [ctx.workflow_id, ctx.run_id], "value": value}


@pytest.mark.parametrize("workflow_id", ["workflow-identity", "0", "17", "017"])
def test_identity_survives_activity_suspension_and_cold_replay(workflow_id: str) -> None:
    run_id = "original-run"
    first = replay(IdentityAcrossReplay, [], [], workflow_id=workflow_id, run_id=run_id)
    assert len(first.commands) == 1
    assert isinstance(first.commands[0], ScheduleActivity)
    assert first.commands[0].arguments == [workflow_id, run_id]

    history = [{"event_type": "ActivityCompleted", "payload": {
        "result": serializer.envelope("done", codec=serializer.AVRO_CODEC),
    }}]
    resumed = replay(IdentityAcrossReplay, history, [], workflow_id=workflow_id, run_id=run_id)
    assert len(resumed.commands) == 1
    assert isinstance(resumed.commands[0], CompleteWorkflow)
    assert resumed.commands[0].result == {
        "before": [workflow_id, run_id], "after": [workflow_id, run_id], "value": "done",
    }


@workflow.defn(name="identity-continue-as-new")
class IdentityContinueAsNew:
    def run(  # type: ignore[no-untyped-def]
        self, ctx: WorkflowContext, again: bool,
        prior_workflow_id: str | None = None, prior_run_id: str | None = None,
    ):
        if again:
            return ctx.continue_as_new(False, ctx.workflow_id, ctx.run_id)
        return {"current": [ctx.workflow_id, ctx.run_id], "prior": [prior_workflow_id, prior_run_id]}


def test_successor_context_has_same_workflow_and_new_run_identity() -> None:
    original = replay(IdentityContinueAsNew, [], [True], workflow_id="chain", run_id="run-one")
    assert len(original.commands) == 1
    command = original.commands[0]
    assert isinstance(command, ContinueAsNew)
    assert command.arguments == [False, "chain", "run-one"]

    successor = replay(
        IdentityContinueAsNew, [], command.arguments, workflow_id="chain", run_id="run-two",
    )
    assert len(successor.commands) == 1
    assert isinstance(successor.commands[0], CompleteWorkflow)
    assert successor.commands[0].result == {"current": ["chain", "run-two"], "prior": ["chain", "run-one"]}


@pytest.mark.parametrize("attribute", ["workflow_id", "run_id"])
def test_workflow_identity_is_read_only(attribute: str) -> None:
    ctx = WorkflowContext(workflow_id="workflow", run_id="run")
    with pytest.raises(AttributeError):
        setattr(ctx, attribute, "replacement")
