from __future__ import annotations

import asyncio
import threading
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio

from durable_workflow import activity, serializer
from durable_workflow.client import Client
from durable_workflow.errors import ActivityCancelled, NonRetryableError
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.worker import Worker, _RemoteActivityExecutionAborted
from tests.test_worker import compatible_cluster_info


def task() -> dict[str, Any]:
    return {"task_id": "remote-task", "activity_attempt_id": "attempt", "activity_type": "remote",
            "payload_codec": "avro", "arguments": serializer.envelope([], codec="avro")}


def status() -> dict[str, Any]:
    return {"task_id": "remote-task", "activity_attempt_id": "attempt", "lease_owner": "owner",
            "can_continue": True, "cancel_requested": False, "reason": None, "heartbeat_recorded": False,
            "lease_expires_at": "2100-01-01T00:00:00Z", "deadlines": None, "worker_session": None}


@pytest_asyncio.fixture
async def owner(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    client = AsyncMock(spec=Client)
    client.get_cluster_info.return_value = compatible_cluster_info(worker_protocol={
        "version": "1.20", "server_capabilities": {"cooperative_cancellation": True},
    })
    client.register_worker.return_value = {"registered": True}
    client.activity_task_status.return_value = status()
    client.heartbeat_activity_task.return_value = status()
    worker = Worker(client, task_queue="queue", worker_id="owner", capabilities=["cooperative_cancellation"],
                    max_concurrent_activity_tasks=1)
    await worker._register()
    try:
        yield worker, client
    finally:
        await worker.stop()


@pytest.mark.parametrize("reason", ["cancel", "replacement", "backend", "shutdown"])
async def test_blocked_async_callback_is_abandoned_without_progress_or_publication(owner, reason: str) -> None:
    worker, client = owner
    entered, late_fenced = asyncio.Event(), asyncio.Event()

    async def callback() -> object:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            with pytest.raises(_RemoteActivityExecutionAborted):
                await activity.context().heartbeat({"late": True})
            late_fenced.set()
            return object()

    worker.activities["remote"] = callback
    execution = asyncio.create_task(worker._run_activity_task(task()))
    await asyncio.wait_for(entered.wait(), timeout=2)
    if reason == "cancel":
        client.activity_task_status.return_value = {**status(), "can_continue": False,
                                                    "cancel_requested": True, "reason": "activity_cancelled"}
    elif reason == "replacement":
        client.activity_task_status.return_value = {**status(), "activity_attempt_id": "replacement"}
    elif reason == "backend":
        client.activity_task_status.side_effect = OSError("backend unavailable")
    else:
        worker._local_activity_shutdown.set()
    assert await asyncio.wait_for(execution, timeout=3) == "claim_aborted"
    await asyncio.wait_for(late_fenced.wait(), timeout=2)
    client.heartbeat_activity_task.assert_not_awaited()
    client.complete_activity_task.assert_not_awaited()
    client.fail_activity_task.assert_not_awaited()


async def test_synchronous_callback_keeps_owner_available_and_fences_late_thread_heartbeat(owner) -> None:
    worker, client = owner
    entered, late_fenced = asyncio.Event(), asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()

    def callback() -> object:
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(timeout=10)
        with pytest.raises(_RemoteActivityExecutionAborted):
            asyncio.run(activity.context().heartbeat({"late": True}))
        loop.call_soon_threadsafe(late_fenced.set)
        return object()

    worker.activities["remote"] = callback
    execution = asyncio.create_task(worker._run_activity_task(task()))
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        client.activity_task_status.return_value = {**status(), "can_continue": False, "reason": "lease_expired"}
        assert await asyncio.wait_for(execution, timeout=3) == "claim_aborted"
        assert not late_fenced.is_set()  # The Python thread is still running, with its attempt fenced.
        assert worker._current_task_slots()["activity_available"] == 0
        poller = asyncio.create_task(worker._poll_activity_tasks())
        try:
            await asyncio.sleep(0.05)
            client.poll_activity_task.assert_not_awaited()
        finally:
            poller.cancel()
            with pytest.raises(asyncio.CancelledError):
                await poller
        release.set()
        await asyncio.wait_for(late_fenced.wait(), timeout=2)
        client.heartbeat_activity_task.assert_not_awaited()
        client.complete_activity_task.assert_not_awaited()
        client.fail_activity_task.assert_not_awaited()
    finally:
        release.set()


async def test_shutdown_expiry_of_tracked_remote_attempt_fences_a_cancellation_resistant_result(owner) -> None:
    worker, client = owner
    worker._shutdown_timeout = 0.01
    entered, late_fenced = asyncio.Event(), asyncio.Event()

    async def callback() -> object:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            with pytest.raises(_RemoteActivityExecutionAborted):
                await activity.context().heartbeat()
            late_fenced.set()
            return object()

    worker.activities["remote"] = callback
    execution = worker._track(worker._run_activity_task(task()))
    await asyncio.wait_for(entered.wait(), timeout=2)
    await asyncio.wait_for(worker.stop(), timeout=2)
    await asyncio.wait_for(late_fenced.wait(), timeout=2)
    assert execution.cancelled() or execution.result() == "claim_aborted"
    client.heartbeat_activity_task.assert_not_awaited()
    client.complete_activity_task.assert_not_awaited()
    client.fail_activity_task.assert_not_awaited()


@pytest.mark.parametrize("synchronous", [False, True])
async def test_authored_heartbeat_stays_on_owner_loop_and_preserves_typed_result(owner, synchronous: bool) -> None:
    worker, client = owner
    loop = asyncio.get_running_loop()
    observed_loops = []

    async def heartbeat(**kwargs: Any) -> dict[str, Any]:
        observed_loops.append(asyncio.get_running_loop())
        assert kwargs["details"] == {"authored": True}
        return status()

    async def asynchronous() -> dict[str, Any]:
        await activity.context().heartbeat({"authored": True})
        return {"bytes": b"\x00\xff", "value": 42, "null": None}

    def blocking() -> dict[str, Any]:
        asyncio.run(activity.context().heartbeat({"authored": True}))
        return {"bytes": b"\x00\xff", "value": 42, "null": None}

    client.heartbeat_activity_task.side_effect = heartbeat
    worker.activities["remote"] = blocking if synchronous else asynchronous
    assert await worker._run_activity_task(task()) == "completed"
    assert observed_loops == [loop]
    assert client.complete_activity_task.await_args.kwargs["result"] == {
        "bytes": b"\x00\xff", "value": 42, "null": None,
    }
    client.fail_activity_task.assert_not_awaited()


@pytest.mark.parametrize("field", ["lease", "heartbeat", "start_to_close", "schedule_to_close", "session", "malformed"])
async def test_invalid_or_elapsed_observed_bounds_prevent_callback_start(owner, field: str) -> None:
    worker, client = owner
    reply = deepcopy(status())
    expired = "2000-01-01T00:00:00Z"
    if field == "lease":
        reply["lease_expires_at"] = expired
    elif field == "malformed":
        reply["deadlines"] = "invalid"
    elif field == "session":
        reply["worker_session"] = {"status": "active", "lease_owner": "owner",
                                   "lease_expires_at": expired, "ttl_expires_at": "2100-01-01T00:00:00Z"}
    else:
        reply["deadlines"] = {field: expired}
    client.activity_task_status.return_value = reply
    callback = AsyncMock()
    worker.activities["remote"] = callback
    assert await worker._run_activity_task(task()) == "claim_aborted"
    callback.assert_not_awaited()
    client.complete_activity_task.assert_not_awaited()
    client.fail_activity_task.assert_not_awaited()


@pytest.mark.parametrize("error", [ValueError("original"), NonRetryableError("original"), ActivityCancelled()])
async def test_genuine_application_failure_preserves_classification(owner, error: Exception) -> None:
    worker, client = owner

    async def callback() -> None:
        raise error

    worker.activities["remote"] = callback
    outcome = await worker._run_activity_task(task())
    assert outcome in {"failed", "failed_non_retryable", "cancelled"}
    assert client.fail_activity_task.await_args.kwargs["failure_type"] == type(error).__name__
    assert client.fail_activity_task.await_args.kwargs.get("non_retryable", False) is isinstance(
        error, (NonRetryableError, ActivityCancelled),
    )
    client.complete_activity_task.assert_not_awaited()


async def test_client_observation_keeps_worker_credentials_namespace_and_fence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    async with Client("http://runtime.test", control_token="control", worker_token="worker", namespace="ns") as client:
        reply = httpx.Response(200, json=status(), request=httpx.Request("POST", "http://runtime.test"))
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=reply) as send:
            assert await client.activity_task_status(
                task_id="remote-task", activity_attempt_id="attempt", lease_owner="owner",
            ) == status()
        headers = send.await_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer worker"
        assert headers["X-Namespace"] == "ns"
        assert headers["X-Durable-Workflow-Protocol-Version"] == "1.20"
        assert send.await_args.kwargs["json"] == {"activity_attempt_id": "attempt", "lease_owner": "owner"}


async def test_client_observation_requires_explicit_protocol_before_io(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", raising=False)
    async with Client("http://runtime.test") as client:
        with (patch.object(client._http, "request", new_callable=AsyncMock) as send,
              pytest.raises(ValueError, match="1.20")):
            await client.activity_task_status(task_id="task", activity_attempt_id="attempt", lease_owner="owner")
        send.assert_not_awaited()


async def test_client_observation_bounds_the_total_retry_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    retry = TransportRetryPolicy(initial_backoff_seconds=100, max_backoff_seconds=100, jitter=False)
    async with Client("http://runtime.test", retry_policy=retry) as client:
        refused = httpx.Response(503, json={"reason": "backend_unavailable"},
                                 request=httpx.Request("POST", "http://runtime.test"))
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=refused) as send:
            started = asyncio.get_running_loop().time()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(client.activity_task_status(
                    task_id="task", activity_attempt_id="attempt", lease_owner="owner",
                ), timeout=7)
            assert asyncio.get_running_loop().time() - started < 6
        send.assert_awaited_once()
