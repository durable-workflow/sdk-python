"""A rejected condition deployment must leave the durable run recoverable."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from typing import Any

import pytest

from durable_workflow import Client, Worker, workflow


@workflow.defn(name="tests.condition-deployment-recovery")
class ConditionRecoveryWorkflow:
    def __init__(self) -> None:
        self.approved = False

    @workflow.signal("wake")
    def wake(self) -> None:
        pass

    @workflow.signal("approve")
    def approve(self) -> None:
        self.approved = True

    def run(self, ctx: Any) -> Any:
        # Deliberately unsafe configuration drift keeps the class fingerprint
        # stable, exercising the recorded condition's own identity protection.
        mode = os.environ.get("CONDITION_RECOVERY_MODE", "correct")
        key = "changed" if mode == "key" else "approved"
        predicate = (lambda: bool(self.approved)) if mode == "fingerprint" else (lambda: self.approved)
        yield ctx.wait_condition(predicate, key=key)
        return "approved"


async def execute_one() -> None:
    async with Client(
        os.environ["DURABLE_WORKFLOW_SERVER_URL"],
        token=os.environ["DURABLE_WORKFLOW_AUTH_TOKEN"], namespace="default",
    ) as client:
        worker = Worker(
            client, task_queue=os.environ["CONDITION_RECOVERY_QUEUE"],
            workflows=[ConditionRecoveryWorkflow],
            worker_id=os.environ["CONDITION_RECOVERY_WORKER"],
            max_concurrent_workflow_tasks=1,
        )
        await worker._register()
        task = await client.poll_workflow_task(
            worker_id=worker.worker_id, task_queue=worker.task_queue, timeout=20,
        )
        assert task is not None
        commands = await worker._run_workflow_task(task)
        print(json.dumps({
            "task_id": task["task_id"], "run_id": task["run_id"],
            "attempt": task["workflow_task_attempt"], "commands": commands,
            "fingerprint": worker.workflow_definition_fingerprints,
        }), flush=True)


async def cold_worker(server_url: str, token: str, queue: str, mode: str) -> dict[str, Any]:
    environment = {
        **os.environ, "DURABLE_WORKFLOW_SERVER_URL": server_url,
        "DURABLE_WORKFLOW_AUTH_TOKEN": token, "CONDITION_RECOVERY_QUEUE": queue,
        "CONDITION_RECOVERY_MODE": mode,
        "CONDITION_RECOVERY_WORKER": f"condition-worker-{uuid.uuid4().hex}",
    }
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-c",
        "import asyncio; from tests.integration.test_condition_recovery import execute_one; asyncio.run(execute_one())",
        env=environment, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=45)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert process.returncode == 0, stderr.decode()
    return json.loads(stdout.decode())


def history_events(document: dict[str, Any]) -> list[dict[str, Any]]:
    events = document.get("events", document.get("history_events"))
    assert isinstance(events, list)
    return events


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["key", "fingerprint"])
async def test_bad_deployment_then_cold_replacement_resumes_original_run(
    server_url: str, server_token: str, drift: str,
) -> None:
    suffix = uuid.uuid4().hex
    queue = f"condition-recovery-{suffix}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        handle = await client.start_workflow(
            workflow_type="tests.condition-deployment-recovery", task_queue=queue,
            workflow_id=f"condition-recovery-{suffix}", input=[],
        )
        initial = await cold_worker(server_url, server_token, queue, "correct")
        assert [command["type"] for command in initial["commands"]] == ["open_condition_wait"]
        original_history = history_events(await handle.get_history())
        await handle.signal("wake")

        rejected = await cold_worker(server_url, server_token, queue, drift)
        assert rejected["commands"] is None
        assert rejected["run_id"] == initial["run_id"] == handle.run_id
        assert rejected["fingerprint"] == initial["fingerprint"]
        assert (await handle.describe()).status not in {"failed", "completed", "terminated"}
        after_rejection = history_events(await handle.get_history())
        assert after_rejection[:len(original_history)] == original_history
        assert all(event["event_type"] != "WorkflowFailed" for event in after_rejection)

        resumed = await cold_worker(server_url, server_token, queue, "correct")
        assert resumed["run_id"] == handle.run_id
        assert resumed["fingerprint"] == initial["fingerprint"]
        assert [command["type"] for command in resumed["commands"]] == ["open_condition_wait"]
        assert resumed["attempt"] > rejected["attempt"]
        await handle.signal("approve")
        finished = await cold_worker(server_url, server_token, queue, "correct")
        assert [command["type"] for command in finished["commands"]] == ["complete_workflow"]
        assert await handle.result(timeout=15) == "approved"
        final_history = history_events(await handle.get_history())
        assert final_history[:len(original_history)] == original_history
        kinds = [event["event_type"] for event in final_history]
        assert "WorkflowFailed" not in kinds
        assert kinds.count("WorkflowCompleted") == 1
