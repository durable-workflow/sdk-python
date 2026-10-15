from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from durable_workflow import Worker, serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import NonDeterministicReplayError, QueryFailed
from durable_workflow.workflow import WorkflowContext, apply_update, query_state, replay


@workflow.defn(name="tests.replay.condition-identity")
class ConditionIdentityWorkflow:
    @workflow.query("state")
    def state(self) -> str:
        return "waiting"

    @workflow.update("approve")
    def approve(self) -> str:
        return "approved"

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: False, key="approved")
        return "done"


def condition_history(field: str) -> list[dict]:
    return [{
        "event_type": "ConditionWaitOpened",
        "payload": {
            "sequence": 7,
            "condition_wait_id": "wait-original",
            "condition_key": "approved",
            field: "old-key" if field == "condition_key" else "sha256:different",
        },
    }]


@pytest.mark.parametrize("field", ["condition_key", "condition_definition_fingerprint"])
@pytest.mark.parametrize("consumer", ["replay", "query", "update"])
def test_condition_drift_is_not_a_business_failure(field: str, consumer: str) -> None:
    history = condition_history(field)
    error = QueryFailed if consumer == "query" else NonDeterministicReplayError
    with pytest.raises(error) as captured:
        if consumer == "query":
            query_state(ConditionIdentityWorkflow, history, [], "state")
        elif consumer == "update":
            apply_update(ConditionIdentityWorkflow, history, [], "update-original")
        else:
            replay(ConditionIdentityWorkflow, history, [])

    mismatch = captured.value.__cause__ if consumer == "query" else captured.value
    assert isinstance(mismatch, NonDeterministicReplayError)
    assert mismatch.workflow_sequence == 7
    assert "wait-original" in str(mismatch)
    assert "changed" in str(mismatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["condition_key", "condition_definition_fingerprint"])
@pytest.mark.parametrize("consumer", ["workflow", "update"])
async def test_worker_reports_drift_once_without_a_completion(field: str, consumer: str) -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="condition-drift", workflows=[ConditionIdentityWorkflow])
    task = {
        "task_id": "task-original",
        "workflow_id": "workflow-original",
        "run_id": "run-original",
        "workflow_type": "tests.replay.condition-identity",
        "workflow_task_attempt": 2,
        "history_events": condition_history(field),
        "arguments": serializer.envelope([], codec="avro"),
        "payload_codec": "avro",
    }
    if consumer == "update":
        task["workflow_update_id"] = "update-original"

    assert await worker._run_workflow_task_core(task) is None

    client.complete_workflow_task.assert_not_awaited()
    client.fail_workflow_task.assert_awaited_once()
    report = client.fail_workflow_task.await_args.kwargs
    assert report["task_id"] == "task-original"
    assert report["workflow_task_attempt"] == 2
    assert report["lease_owner"] == worker.worker_id
    assert report["failure_type"] == "NonDeterministicReplayError"
    assert "wait-original" in report["message"]


def test_application_predicate_failure_remains_terminal() -> None:
    @workflow.defn(name="tests.condition-predicate-failure")
    class PredicateFailure:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            def predicate() -> bool:
                raise ValueError("application failed")

            yield ctx.wait_condition(predicate, key="approved")

    outcome = replay(PredicateFailure, [], [])
    assert len(outcome.commands) == 1
    command = outcome.commands[0].to_server_command("condition-drift")
    assert command["type"] == "fail_workflow"
    assert command["exception_type"] == "ValueError"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["condition_key", "condition_definition_fingerprint"])
async def test_query_worker_reports_unavailable_state_without_changing_the_run(field: str) -> None:
    from tests.query_task_helpers import leased_query_task

    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="condition-drift", workflows=[ConditionIdentityWorkflow])
    outcome = await worker._run_query_task(leased_query_task(worker, {
        "query_task_id": "query-original",
        "query_task_attempt": 2,
        "workflow_id": "workflow-original",
        "run_id": "run-original",
        "workflow_type": "tests.replay.condition-identity",
        "query_name": "state",
        "history_events": condition_history(field),
        "workflow_arguments": serializer.envelope([], codec="avro"),
        "query_arguments": serializer.envelope([], codec="avro"),
        "payload_codec": "avro",
    }))

    assert outcome == "failed"
    client.complete_query_task.assert_not_awaited()
    client.complete_workflow_task.assert_not_awaited()
    client.fail_workflow_task.assert_not_awaited()
    client.fail_query_task.assert_awaited_once()
    report = client.fail_query_task.await_args.kwargs
    assert report["reason"] == "query_workflow_state_unavailable"
    assert "wait-original" in report["message"]
