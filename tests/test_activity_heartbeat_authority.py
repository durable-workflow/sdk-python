from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, serializer
from durable_workflow.activity import ActivityContext, ActivityInfo
from durable_workflow.client import Client
from durable_workflow.errors import ActivityLeaseAuthorityLost
from durable_workflow.worker import Worker

REASONS = ("activity_deadline_elapsed", "attempt_closed", "run_closed")


def refusing_client(reason: str) -> AsyncMock:
    client = AsyncMock(spec=Client)
    client.heartbeat_activity_task.return_value = {
        "can_continue": False, "cancel_requested": False, "reason": reason,
    }
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", REASONS)
@pytest.mark.parametrize("synchronous", (False, True))
async def test_heartbeat_refusal_carries_original_reason(reason: str, synchronous: bool) -> None:
    ctx = ActivityContext(info=ActivityInfo(
        task_id="original-task", activity_type="heartbeat-refusal", activity_attempt_id="original-attempt",
        attempt_number=1, task_queue="original-queue", worker_id="original-worker",
    ), client=refusing_client(reason))

    with pytest.raises(ActivityLeaseAuthorityLost) as refusal:
        if synchronous:
            await asyncio.to_thread(ctx.heartbeat_sync, {"progress": 1})
        else:
            await ctx.heartbeat({"progress": 1})

    assert refusal.value.task_id == "original-task"
    assert refusal.value.reason == reason
    assert not ctx.is_cancelled


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", REASONS)
@pytest.mark.parametrize("synchronous", (False, True))
async def test_default_worker_does_not_publish_after_heartbeat_refusal(
    reason: str, synchronous: bool,
) -> None:
    client = refusing_client(reason)
    continued: list[str] = []

    def sync_handler() -> str:
        activity.context().heartbeat_sync()
        continued.append("after refusal")
        return "late result"

    async def async_handler() -> str:
        await activity.context().heartbeat()
        continued.append("after refusal")
        return "late result"

    handler = activity.defn(name="heartbeat-refusal")(sync_handler if synchronous else async_handler)
    worker = Worker(client, task_queue="original-queue", workflows=[], activities=[handler])
    task = {
        "task_id": "original-task", "activity_attempt_id": "original-attempt",
        "activity_type": "heartbeat-refusal", "attempt_number": 1,
        "arguments": serializer.encode([], codec="avro"), "payload_codec": "avro",
    }

    assert await worker._run_activity_task(task) == "claim_aborted"
    assert continued == []
    client.complete_activity_task.assert_not_called()
    client.fail_activity_task.assert_not_called()
    with pytest.raises(RuntimeError, match="outside of an activity"):
        activity.context()


@pytest.mark.asyncio
async def test_swallowed_heartbeat_refusal_still_fences_worker_publication() -> None:
    client = refusing_client("attempt_closed")

    @activity.defn(name="heartbeat-refusal-caught")
    async def handler() -> str:
        try:
            await activity.context().heartbeat()
        except Exception:
            pass
        return "late result"

    worker = Worker(client, task_queue="original-queue", workflows=[], activities=[handler])
    task = {
        "task_id": "original-task", "activity_attempt_id": "original-attempt",
        "activity_type": "heartbeat-refusal-caught", "attempt_number": 1,
        "arguments": serializer.encode([], codec="avro"), "payload_codec": "avro",
    }

    assert await worker._run_activity_task(task) == "claim_aborted"
    client.complete_activity_task.assert_not_called()
    client.fail_activity_task.assert_not_called()
