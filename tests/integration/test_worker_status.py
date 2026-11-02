"""Worker readiness and liveness against a running Server (#168)."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from typing import Any

import pytest

from durable_workflow import Client, Worker, WorkerState, activity, workflow

release_activity = threading.Event()
activity_started = threading.Event()


@activity.defn(name="tests.worker-status.slow")
def slow(value: str) -> str:
    activity_started.set()
    release_activity.wait(30)
    return f"slow:{value}"


@workflow.defn(name="tests.worker-status.workflow")
class StatusWorkflow:
    def __init__(self) -> None:
        self.stage = "started"

    @workflow.query("stage")
    def current_stage(self) -> str:
        return self.stage

    def run(self, ctx: Any, value: str) -> Any:
        self.stage = "running_activity"
        result = yield ctx.schedule_activity("tests.worker-status.slow", [value])
        self.stage = "done"
        return result


async def eventually(predicate: Any, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition was not reached")
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_worker_status_follows_a_real_worker_lifecycle(server_url: str, server_token: str) -> None:
    release_activity.clear()
    activity_started.clear()
    suffix = uuid.uuid4().hex[:8]
    queue = f"worker-status-{suffix}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[StatusWorkflow],
            activities=[slow],
            poll_timeout=2.0,
            shutdown_timeout=20.0,
        )
        assert worker.status().ready_reason == "not_started"
        run_task = asyncio.create_task(worker.run())
        try:
            await eventually(lambda: worker.status().ready)
            status = worker.status()
            assert status.state is WorkerState.REGISTERED
            assert status.namespace == "default"
            assert status.task_queue == queue
            assert {"heartbeat", "workflow_poll", "activity_poll"} <= {role.role for role in status.roles}

            # Idle long polls keep the worker ready across several poll windows.
            await asyncio.sleep(5)
            assert worker.status().ready, worker.status().as_dict()

            handle = await client.start_workflow(
                workflow_type="tests.worker-status.workflow",
                workflow_id=f"worker-status-{suffix}",
                task_queue=queue,
                input=["x"],
            )
            await eventually(activity_started.is_set)
            # A blocked synchronous activity does not affect readiness or liveness.
            await asyncio.sleep(1)
            status = worker.status()
            assert status.ready, status.as_dict()
            assert status.live, status.as_dict()
            if "query_poll" in {role.role for role in status.roles}:
                response = await client.query_workflow(handle.workflow_id, "stage")
                assert response["result"] == "running_activity"
                assert worker.status().ready

            stop_task = asyncio.create_task(worker.stop())
            await eventually(lambda: worker.status().state is WorkerState.DRAINING)
            draining = worker.status()
            assert (draining.ready, draining.ready_reason, draining.live) == (False, "draining", True)
            release_activity.set()
            await stop_task
            await run_task
            stopped = worker.status()
            assert stopped.state is WorkerState.STOPPED
            assert (stopped.ready, stopped.live) == (False, False)
        finally:
            release_activity.set()
            if not run_task.done():
                await worker.stop()
                await run_task
