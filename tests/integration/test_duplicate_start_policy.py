"""Qualify start policies against real admission, audit history and completion."""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import httpx
import pytest

from durable_workflow import Client, Worker, workflow
from durable_workflow.errors import InvalidArgument, WorkflowAlreadyStarted, WorkflowNotFound


@workflow.defn(name="tests.duplicate-policy.echo")
class PolicyWorkflow:
    def run(self, ctx: Any, value: Any) -> Any:
        yield from ()
        return value


@pytest.mark.asyncio
@pytest.mark.parametrize("initial_policy", [None, "fail", "use-existing"])
async def test_duplicate_policies_preserve_active_and_completed_original(
    server_url: str, server_token: str, initial_policy: str | None,
) -> None:
    suffix = uuid4().hex
    queue = f"policy-{suffix}"
    workflow_id = f"policy-{suffix}"
    value = {"integer": 9223372036854775807, "null": None, "text": "original 雪"}
    async with Client(server_url, token=server_token, namespace="default") as client:
        starts: list[tuple[dict[str, Any], int]] = []

        async def observe(response: httpx.Response) -> None:
            request = response.request
            if request.method == "POST" and request.url.path.endswith("/workflows"):
                starts.append((json.loads(request.content), response.status_code))

        client._http.event_hooks["response"].append(observe)

        async def start(policy: str | None, *, original: bool = False) -> Any:
            return await client.start_workflow(
                workflow_type="tests.duplicate-policy.echo", task_queue=queue,
                workflow_id=workflow_id, input=[value if original else "replacement"],
                execution_timeout_seconds=120 if original else 600,
                run_timeout_seconds=60 if original else 300, duplicate_policy=policy,
            )

        handle = await start(initial_policy, original=True)
        assert starts[-1][1] == 201
        assert starts[-1][0].get("duplicate_policy") == initial_policy
        assert ("duplicate_policy" in starts[-1][0]) is (initial_policy is not None)

        for completed in (False, True):
            if completed:
                worker = Worker(
                    client, task_queue=queue, workflows=[PolicyWorkflow], activities=[],
                    poll_timeout=0.5, shutdown_timeout=10,
                )
                await worker.run_until(workflow_id=workflow_id, timeout=30, poll_interval=0.1)
                assert await handle.result(timeout=30) == value

            before = await handle.describe_run()
            assert (before.status.lower() == "completed") is completed
            assert before.input is not None
            assert before.execution_deadline_at and before.run_deadline_at
            history = (await handle.get_history())["events"]

            for policy in (None, "fail", "use-existing"):
                returned = policy == "use-existing" and not completed
                if returned:
                    existing = await start(policy)
                    assert (existing.workflow_id, existing.run_id) == (handle.workflow_id, handle.run_id)
                else:
                    with pytest.raises(WorkflowAlreadyStarted) as duplicate:
                        await start(policy)
                    assert duplicate.value.workflow_id == workflow_id
                request, status = starts[-1]
                assert request.get("duplicate_policy") == policy
                assert ("duplicate_policy" in request) is (policy is not None)
                assert status == (200 if returned else 409)
                after_history = (await handle.get_history())["events"]
                assert after_history[:len(history)] == history
                # Canonical duplicate starts append an admission audit event,
                # while retaining the original business execution.
                assert [event["event_type"] for event in after_history[len(history):]] == [
                    "StartAccepted" if returned else "StartRejected",
                ]
                history = after_history

            for obsolete in ("reject", "allow", "terminate_existing"):
                with pytest.raises(InvalidArgument):
                    await start(obsolete)
                assert starts[-1][0]["duplicate_policy"] == obsolete
                assert starts[-1][1] == 422
                assert (await handle.get_history())["events"] == history

            after = await handle.describe_run()
            for field in ("run_id", "input", "status", "execution_deadline_at", "run_deadline_at", "output"):
                assert getattr(after, field) == getattr(before, field), field
            runs = await handle.list_runs()
            assert len(runs.runs) == 1
            assert runs.runs[0].run_id == handle.run_id
            assert sum(event["event_type"] == "WorkflowCompleted" for event in history) == int(completed)
        print(json.dumps({"initial_policy": initial_policy, "workflow_id": workflow_id,
                          "run_id": handle.run_id, "start_statuses": [status for _, status in starts]}))


@pytest.mark.asyncio
@pytest.mark.parametrize("obsolete", ["reject", "allow", "terminate_existing"])
async def test_invalid_policy_does_not_admit_a_fresh_workflow(
    server_url: str, server_token: str, obsolete: str,
) -> None:
    workflow_id = f"invalid-policy-{uuid4().hex}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        with pytest.raises(InvalidArgument):
            await client.start_workflow(
                workflow_type="tests.duplicate-policy.echo", task_queue="invalid-policy",
                workflow_id=workflow_id, input=[], duplicate_policy=obsolete,
            )
        with pytest.raises(WorkflowNotFound):
            await client.describe_workflow(workflow_id)
