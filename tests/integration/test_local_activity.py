"""Qualify Python local activity recording and cold replay against Server."""
from __future__ import annotations

import uuid
from typing import Any

import pytest

from durable_workflow import Client, Worker, activity, workflow
from durable_workflow.client import PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST
from durable_workflow.workflow import replay


@workflow.defn(name="tests.python-local-activity")
class LocalActivityWorkflow:
    def run(self, ctx: Any, name: str) -> Any:
        result = yield ctx.local_activity("tests.python-local-greet", [name])
        return {"greeting": result}


_executions = 0


@activity.defn(name="tests.python-local-greet")
async def local_greet(name: str) -> str:
    global _executions
    _executions += 1
    await activity.context().heartbeat({"phase": "greeting"})
    return f"hello, {name}"


@pytest.mark.asyncio
async def test_local_activity_completion_survives_cold_replay(
    server_url: str,
    server_token: str,
) -> None:
    global _executions
    _executions = 0
    suffix = uuid.uuid4().hex[:8]
    queue = f"py-local-{suffix}"
    workflow_id = f"py-local-{suffix}"
    manifest = {
        **PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
        "local_activities": {
            "supported": True,
            "minimum_protocol_version": "1.18",
            "implementation": "record_local_activity",
        },
    }

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[LocalActivityWorkflow],
            activities=[local_greet],
            worker_id=f"py-local-worker-{suffix}",
        )
        await client.register_worker(
            worker_id=worker.worker_id,
            task_queue=queue,
            supported_workflow_types=list(worker.workflows),
            supported_activity_types=list(worker.activities),
            workflow_definition_fingerprints=worker.workflow_definition_fingerprints,
            workflow_command_contracts=worker.workflow_command_contracts,
            capabilities=["local_activities"],
            capability_manifest=manifest,
        )
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-local-activity",
                task_queue=queue,
                workflow_id=workflow_id,
                input=["Ada"],
            )
            task = await client.poll_workflow_task(
                worker_id=worker.worker_id,
                task_queue=queue,
                timeout=10.0,
            )
            assert task is not None
            commands = await worker._run_workflow_task(task)
            assert commands is not None
            assert [command["type"] for command in commands] == [
                "record_local_activity", "complete_workflow",
            ]
            assert _executions == 1
            assert await handle.result(timeout=10.0) == {"greeting": "hello, Ada"}

            history = await handle.get_history()
            events = history.get("events", history.get("history_events", []))
            event_types = [event["event_type"] for event in events]
            assert "ActivityScheduled" in event_types
            assert "ActivityStarted" in event_types
            assert "ActivityHeartbeatRecorded" in event_types
            assert "ActivityCompleted" in event_types
            assert "WorkflowCompleted" in event_types

            outcome = replay(
                LocalActivityWorkflow,
                events,
                ["Ada"],
                workflow_id=workflow_id,
                run_id=handle.run_id or "",
            )
            assert [command.__class__.__name__ for command in outcome.commands] == ["CompleteWorkflow"]
            assert _executions == 1
        finally:
            await client.deregister_worker_registration(worker.worker_id)
