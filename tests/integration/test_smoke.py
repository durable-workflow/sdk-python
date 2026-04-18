"""Smoke test: start a Python-authored workflow, drive it to completion, verify the result.

Requires a running Durable Workflow server. Set DURABLE_WORKFLOW_SERVER_URL
to the server base URL (e.g. http://server-server-1:8080).

    DURABLE_WORKFLOW_SERVER_URL=http://server-server-1:8080 \
    pytest tests/integration/ -v

This test drives the workflow task and activity task lifecycle manually
(sequential HTTP calls) rather than using the Worker class, because PHP's
built-in dev server is single-threaded and cannot handle the Worker's
concurrent poll requests. The CI docker-compose.test.yml should eventually
provide a multi-process server for a full Worker-based integration test.
"""
from __future__ import annotations

import uuid

import pytest

from durable_workflow import Client, activity, workflow
from durable_workflow.serializer import decode_envelope
from durable_workflow.workflow import replay


@activity.defn(name="smoke_greet")
def smoke_greet(name: str) -> str:
    return f"hello, {name}"


@workflow.defn(name="smoke_greeter")
class SmokeGreeterWorkflow:
    def run(self, ctx, *_ignored):  # type: ignore[no-untyped-def]
        greeting = yield ctx.schedule_activity("smoke_greet", ["smoke-test"])
        return {"greeting": greeting, "length": len(greeting)}


@pytest.mark.asyncio
async def test_health(server_url: str, server_token: str) -> None:
    async with Client(server_url, token=server_token, namespace="default") as client:
        health = await client.health()
        assert health["status"] == "serving"


@pytest.mark.asyncio
async def test_greeter_workflow_end_to_end(server_url: str, server_token: str) -> None:
    """Drive a workflow through its full lifecycle using sequential SDK calls."""
    task_queue = f"smoke-{uuid.uuid4().hex[:8]}"
    wf_id = f"smoke-{uuid.uuid4().hex[:8]}"
    worker_id = f"py-smoke-{uuid.uuid4().hex[:8]}"

    async with Client(server_url, token=server_token, namespace="default") as client:
        # 1. Register worker
        await client.register_worker(
            worker_id=worker_id,
            task_queue=task_queue,
            supported_workflow_types=["smoke_greeter"],
            supported_activity_types=["smoke_greet"],
        )

        # 2. Start workflow
        handle = await client.start_workflow(
            workflow_type="smoke_greeter",
            task_queue=task_queue,
            workflow_id=wf_id,
            input=[],
        )
        assert handle.workflow_id == wf_id
        assert handle.run_id is not None

        # 3. Poll for workflow task (short HTTP timeout — task should be ready)
        wf_task = await client.poll_workflow_task(
            worker_id=worker_id, task_queue=task_queue, timeout=10.0,
        )
        assert wf_task is not None, "expected a workflow task after starting workflow"
        task_id = wf_task["task_id"]
        history = wf_task.get("history_events", [])
        attempt = wf_task.get("workflow_task_attempt", 1)

        # Decode start input
        raw_args = wf_task.get("arguments")
        codec = wf_task.get("payload_codec")
        decoded = decode_envelope(raw_args, codec=codec)
        start_input = decoded if isinstance(decoded, list) else ([] if decoded is None else [decoded])

        # 4. Replay — should produce ScheduleActivity command
        outcome = replay(SmokeGreeterWorkflow, history, start_input, run_id=wf_task.get("run_id", ""))
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        server_cmd = cmd.to_server_command(task_queue, payload_codec=codec)
        assert server_cmd["type"] == "schedule_activity"

        # 5. Complete workflow task with ScheduleActivity command
        await client.complete_workflow_task(
            task_id=task_id,
            lease_owner=worker_id,
            workflow_task_attempt=attempt,
            commands=[server_cmd],
        )

        # 6. Poll for activity task
        act_task = await client.poll_activity_task(
            worker_id=worker_id, task_queue=task_queue, timeout=10.0,
        )
        assert act_task is not None, "expected an activity task after scheduling activity"
        act_task_id = act_task["task_id"]
        act_attempt_id = act_task.get("activity_attempt_id") or act_task.get("attempt_id", "")
        act_args = decode_envelope(act_task.get("arguments"), codec=act_task.get("payload_codec")) or []
        if not isinstance(act_args, list):
            act_args = [act_args]

        # 7. Run the activity function
        result = smoke_greet(*act_args)
        assert result == "hello, smoke-test"

        # 8. Complete the activity task
        await client.complete_activity_task(
            task_id=act_task_id,
            activity_attempt_id=act_attempt_id,
            lease_owner=worker_id,
            result=result,
        )

        # 9. Poll for the next workflow task (should have ActivityCompleted in history)
        wf_task2 = await client.poll_workflow_task(
            worker_id=worker_id, task_queue=task_queue, timeout=10.0,
        )
        assert wf_task2 is not None, "expected a workflow task after activity completion"
        task_id2 = wf_task2["task_id"]
        history2 = wf_task2.get("history_events", [])
        attempt2 = wf_task2.get("workflow_task_attempt", 1)

        decoded2 = decode_envelope(wf_task2.get("arguments"), codec=wf_task2.get("payload_codec"))
        start_input2 = decoded2 if isinstance(decoded2, list) else ([] if decoded2 is None else [decoded2])

        # 10. Replay with full history — should produce CompleteWorkflow
        outcome2 = replay(SmokeGreeterWorkflow, history2, start_input2, run_id=wf_task2.get("run_id", ""))
        assert len(outcome2.commands) == 1
        cmd2 = outcome2.commands[0]
        codec2 = wf_task2.get("payload_codec")
        server_cmd2 = cmd2.to_server_command(task_queue, payload_codec=codec2)
        assert server_cmd2["type"] == "complete_workflow"

        # 11. Complete the final workflow task
        await client.complete_workflow_task(
            task_id=task_id2,
            lease_owner=worker_id,
            workflow_task_attempt=attempt2,
            commands=[server_cmd2],
        )

        # 12. Verify final workflow state
        desc = await handle.describe()
        assert desc.status in ("completed", "Completed")
        assert desc.output == {"greeting": "hello, smoke-test", "length": 17}
