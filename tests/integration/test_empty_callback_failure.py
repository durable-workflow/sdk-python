from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import httpx
import pytest

from durable_workflow import Client, Worker, activity
from durable_workflow.errors import WorkflowFailed
from tests.worker_observation import stop_and_drain


@activity.defn(name="tests.empty-failure")
async def empty_failure(message: str) -> None:
    raise AssertionError(message)


@pytest.mark.parametrize("supervised", [False, True])
@pytest.mark.parametrize("message", ["", " \t\n"])
async def test_blank_callback_failure_reaches_a_terminal_caller(
    server_url: str, server_token: str, supervised: bool, message: str,
) -> None:
    identity = "empty-failure-" + uuid.uuid4().hex[:12]
    replies: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
    async with Client(server_url, worker_token=server_token, control_token=server_token) as client:
        async def record(response: httpx.Response) -> None:
            if response.request.url.path.endswith("/fail"):
                await response.aread()
                replies.append((response.status_code, json.loads(response.request.content), response.json()))

        client._http.event_hooks["response"].append(record)
        worker = Worker(client, task_queue=identity, worker_id=identity, activities=[empty_failure],
                        capabilities=["cooperative_cancellation"] if supervised else [])
        runner = asyncio.create_task(worker.run())
        try:
            handle = await client.start_activity(activity_type="tests.empty-failure", task_queue=identity,
                activity_id=identity, input=[message], retry_policy={"max_attempts": 1})
            with pytest.raises(WorkflowFailed):
                await handle.result(timeout=15, poll_interval=.05)
            assert (await handle.describe()).status == "failed"
            assert len(replies) == 1
            status, submitted, accepted = replies[0]
            assert status == 200
            assert accepted["recorded"] is True
            assert submitted["lease_owner"] == identity
            assert submitted["activity_attempt_id"]
            assert submitted["failure"]["message"] == "AssertionError"
            assert submitted["failure"]["type"] == "AssertionError"
            assert "empty_failure" in submitted["failure"]["stack_trace"]
        finally:
            await stop_and_drain([worker], [runner])
