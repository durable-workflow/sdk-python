from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import httpx
import pytest

from durable_workflow import activity, serializer
from durable_workflow._activity_lease import ActivityLeaseAuthority, current_activity_authority
from durable_workflow.activity import ActivityContext, ActivityInfo
from durable_workflow.client import Client
from durable_workflow.errors import ActivityLeaseAuthorityLost, NonRetryableError
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

    invocation = (asyncio.to_thread(ctx.heartbeat_sync, {"progress": 1}) if synchronous
                  else ctx.heartbeat({"progress": 1}))
    with pytest.raises(ActivityLeaseAuthorityLost) as refusal:
        await invocation

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
@pytest.mark.parametrize("same_original", (True, False))
async def test_refusal_fences_only_the_matching_original_client_authority(same_original: bool) -> None:
    paths: list[str] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        assert request.url.path.endswith("/heartbeat")
        return httpx.Response(200, json={
            "heartbeat_recorded": False, "can_continue": False,
            "cancel_requested": False, "reason": "attempt_closed",
        })

    async with Client("http://heartbeat.invalid", worker_token="synthetic-worker-token") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://heartbeat.invalid", transport=httpx.MockTransport(respond))
        reference = datetime.now(timezone.utc)
        def timestamp(value: datetime) -> str:
            return value.isoformat(timespec="microseconds").replace("+00:00", "Z")

        authority = ActivityLeaseAuthority.delivery(client, client.namespace, {
            "task_id": "original-task" if same_original else "different-task",
            "workflow_id": "workflow-1", "run_id": "run-1", "activity_execution_id": "execution-1",
            "activity_attempt_id": "original-attempt", "lease_owner": "original-worker",
            "task_queue": "original-queue", "attempt_number": 1,
            "lease_expires_at": timestamp(reference + timedelta(seconds=30)), "deadlines": {},
        }, owner="original-worker", task_queue="original-queue", server_time=timestamp(reference),
            request_started=time.monotonic())
        token = current_activity_authority.set(authority)
        try:
            ctx = ActivityContext(info=ActivityInfo(
                task_id="original-task", activity_type="heartbeat-refusal", activity_attempt_id="original-attempt",
                attempt_number=1, task_queue="original-queue", worker_id="original-worker",
            ), client=client)
            with pytest.raises(ActivityLeaseAuthorityLost, match="attempt_closed"):
                await ctx.heartbeat()
            if same_original:
                with pytest.raises(ActivityLeaseAuthorityLost, match="attempt_closed"):
                    await client.complete_activity_task(
                        task_id="original-task", activity_attempt_id="original-attempt",
                        lease_owner="original-worker", result="late result",
                    )
                assert paths == ["/api/worker/activity-tasks/original-task/heartbeat"]
            else:
                assert authority.remaining() > 0
        finally:
            current_activity_authority.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("late_error", (None, ValueError, NonRetryableError))
async def test_swallowed_heartbeat_refusal_still_fences_worker_publication(late_error: type[Exception] | None) -> None:
    client = refusing_client("attempt_closed")

    @activity.defn(name="heartbeat-refusal-caught")
    async def handler() -> str:
        with suppress(Exception):
            await activity.context().heartbeat()
        if late_error is not None:
            raise late_error("application error after closed attempt")
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
