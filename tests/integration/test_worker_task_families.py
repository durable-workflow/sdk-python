"""Exercise ordinary workers and count their real HTTP poll requests."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import uuid
from collections import Counter
from typing import Any

import httpx
import pytest

from durable_workflow import Client, Worker, activity, workflow


@activity.defn(name="tests.task-family.echo")
async def echo(value: str) -> str:
    return f"echo:{value}"


@workflow.defn(name="tests.task-family.activity")
class ActivityWorkflow:
    def run(self, ctx: Any, value: str) -> Any:
        return (yield ctx.schedule_activity("tests.task-family.echo", [value]))


@workflow.defn(name="tests.task-family.timer")
class TimerWorkflow:
    def run(self, ctx: Any, value: str) -> Any:
        yield ctx.sleep(2)
        return f"timer:{value}"


def record_polls(client: Client) -> Counter[str]:
    counts: Counter[str] = Counter()

    async def record(request: httpx.Request) -> None:
        for family in ("workflow", "activity", "query"):
            if request.url.path.endswith(f"/worker/{family}-tasks/poll"):
                counts[family] += 1

    client._http.event_hooks["request"].append(record)
    return counts


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["split", "mixed", "timer"])
@pytest.mark.parametrize("entry_point", ["run", "run_until"])
async def test_workers_execute_without_polling_empty_task_families(
    server_url: str, server_token: str, mode: str, entry_point: str,
) -> None:
    suffix = uuid.uuid4().hex[:12]
    queue = f"task-family-{suffix}"
    async with (
        Client(server_url, token=server_token, namespace="default") as control,
        Client(server_url, token=server_token, namespace="default") as workflow_client,
        Client(server_url, token=server_token, namespace="default") as activity_client,
    ):
        workflow_polls = record_polls(workflow_client)
        activity_polls = record_polls(activity_client)
        workflow_definition = TimerWorkflow if mode == "timer" else ActivityWorkflow
        worker = Worker(
            workflow_client, task_queue=queue,
            workflows=[workflow_definition],
            activities=[echo] if mode == "mixed" else [],
            poll_timeout=0.5, shutdown_timeout=10,
        )
        workers = [worker]
        if mode == "split":
            workers.append(Worker(
                activity_client, task_queue=queue, workflows=[], activities=[echo],
                poll_timeout=0.5, shutdown_timeout=10,
            ))

        handle = await control.start_workflow(
            workflow_type="tests.task-family.timer" if mode == "timer" else "tests.task-family.activity",
            task_queue=queue, workflow_id=f"task-family-{suffix}", input=[suffix],
        )
        runners = [asyncio.create_task(
            selected.run() if entry_point == "run" else selected.run_until(
                workflow_id=handle.workflow_id, timeout=30, poll_interval=0.05,
            )
        ) for selected in workers]
        try:
            expected = f"timer:{suffix}" if mode == "timer" else f"echo:{suffix}"
            assert await handle.result(timeout=30, poll_interval=0.1) == expected
            if entry_point == "run_until":
                terminal = await asyncio.wait_for(asyncio.gather(*runners), timeout=10)
                assert all((result.status or "").lower() == "completed" for result in terminal)
        finally:
            await asyncio.gather(*(selected.stop() for selected in workers))
            await asyncio.wait_for(asyncio.gather(*runners), timeout=10)

        assert workflow_polls["workflow"] > 0
        assert (workflow_polls["activity"] > 0) is (mode == "mixed")
        if mode == "split":
            assert activity_polls["activity"] > 0
            assert activity_polls["workflow"] == 0
            assert activity_polls["query"] == 0
        print(json.dumps({
            "sdk_version": importlib.metadata.version("durable-workflow"),
            "mode": mode, "entry_point": entry_point, "workflow_id": handle.workflow_id,
            "workflow_worker_polls": dict(workflow_polls),
            "activity_worker_polls": dict(activity_polls), "outcome": "pass",
        }), flush=True)
