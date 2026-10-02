from __future__ import annotations

import asyncio
import os
import shutil
import time
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio

from durable_workflow import activity, serializer
from durable_workflow.client import Client
from durable_workflow.errors import ActivityCancelled, NonRetryableError, ServerError
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.worker import Worker
from tests.test_activity_process import wait_for_exit, wait_for_file
from tests.test_worker import compatible_cluster_info


def task(*args: Any) -> dict[str, Any]:
    return {"task_id": "remote-task", "activity_attempt_id": "attempt", "activity_type": "remote",
            "payload_codec": "avro", "arguments": serializer.envelope(list(args), codec="avro")}


def status() -> dict[str, Any]:
    return {"task_id": "remote-task", "activity_attempt_id": "attempt", "lease_owner": "owner",
            "can_continue": True, "cancel_requested": False, "reason": None, "heartbeat_recorded": False,
            "lease_expires_at": "2100-01-01T00:00:00Z", "deadlines": None, "worker_session": None}


def cancellation_status() -> dict[str, Any]:
    return {**status(), "can_continue": False, "cancel_requested": True, "reason": "activity_cancelled",
            "cancellation_acknowledgement": {"request_id": "original-request", "root_request_id": "root-request",
                                             "cleanup_deadline_at": "2100-01-01T00:00:00Z",
                                             "cancellation_history_event_id": "cancellation-event",
                                             "callback_state": "unknown"}}


def stop_receipt() -> dict[str, Any]:
    return {"task_id": "remote-task", "activity_attempt_id": "attempt", "lease_owner": "owner",
            "request_id": "original-request", "acknowledged": True, "duplicate": False, "reason": None,
            "heartbeat_recorded": False, "history_event_id": "receipt-event"}


async def blocked_async(marker: str, heartbeat: bool = False) -> None:
    if heartbeat:
        await activity.context().heartbeat({"authored": True})
    Path(marker).write_text(str(os.getpid()))
    await asyncio.Event().wait()


def blocked_sync(marker: str, heartbeat: bool = False) -> None:
    if heartbeat:
        asyncio.run(activity.context().heartbeat({"authored": True}))
    Path(marker).write_text(str(os.getpid()))
    time.sleep(60)


def released_sync(marker: str) -> bytes:
    Path(marker).write_text(str(os.getpid()))
    while not Path(marker + ".release").exists():
        time.sleep(0.02)
    return b"late result"


async def typed_async() -> dict[str, Any]:
    await activity.context().heartbeat({"authored": True})
    return {"bytes": b"\x00\xff", "value": 42, "null": None, "task_id": activity.context().info.task_id}


def typed_sync() -> dict[str, Any]:
    return asyncio.run(typed_async())


async def application_failure(kind: str) -> None:
    if kind == "ValueError":
        raise ValueError("original")
    if kind == "NonRetryableError":
        raise NonRetryableError("original")
    raise ActivityCancelled()


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
    client.acknowledge_activity_cancellation.return_value = stop_receipt()
    worker = Worker(client, task_queue="queue", worker_id="owner", capabilities=["cooperative_cancellation"],
                    max_concurrent_activity_tasks=1)
    await worker._register()
    try:
        yield worker, client
    finally:
        await worker.stop()


@pytest.mark.parametrize("reason", ["cancel", "replacement", "backend", "shutdown"])
@pytest.mark.parametrize("synchronous", [False, True])
@pytest.mark.parametrize("heartbeat", [False, True])
async def test_authority_loss_stops_and_joins_the_actual_callback_before_reporting(
    owner: Any, tmp_path: Path, reason: str, synchronous: bool, heartbeat: bool,
) -> None:
    worker, client = owner
    marker = tmp_path / "callback"
    worker.activities["remote"] = blocked_sync if synchronous else blocked_async
    execution = asyncio.create_task(worker._run_activity_task(task(str(marker), heartbeat)))
    await wait_for_file(marker)
    pid = int(marker.read_text())

    async def acknowledge(**options: Any) -> dict[str, Any]:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        assert not worker._remote_activity_processes
        assert options == {"task_id": "remote-task", "activity_attempt_id": "attempt", "lease_owner": "owner",
                           "request_id": "original-request"}
        return stop_receipt()

    client.acknowledge_activity_cancellation.side_effect = acknowledge
    if reason == "cancel":
        client.activity_task_status.return_value = cancellation_status()
    elif reason == "replacement":
        client.activity_task_status.return_value = {**status(), "activity_attempt_id": "replacement"}
    elif reason == "backend":
        client.activity_task_status.side_effect = OSError("backend unavailable")
    else:
        worker._local_activity_shutdown.set()
    assert await asyncio.wait_for(execution, timeout=4.0) == "claim_aborted"
    await wait_for_exit(pid)
    assert not worker._remote_activity_processes
    assert worker._current_task_slots()["activity_available"] == 1
    assert client.heartbeat_activity_task.await_count == int(heartbeat)
    assert client.acknowledge_activity_cancellation.await_count == int(reason == "cancel")
    client.complete_activity_task.assert_not_awaited()
    client.fail_activity_task.assert_not_awaited()


async def test_shutdown_timeout_joins_the_remote_callback_before_returning(owner: Any, tmp_path: Path) -> None:
    worker, client = owner
    worker._shutdown_timeout = 0.01
    marker = tmp_path / "callback"
    worker.activities["remote"] = blocked_async
    execution = worker._track(worker._run_activity_task(task(str(marker))))
    await wait_for_file(marker)
    pid = int(marker.read_text())
    await asyncio.wait_for(worker.stop(), timeout=4.0)
    await wait_for_exit(pid)
    assert execution.cancelled() or execution.result() == "claim_aborted"
    assert not worker._remote_activity_processes
    client.complete_activity_task.assert_not_awaited()
    client.fail_activity_task.assert_not_awaited()


@pytest.mark.parametrize("synchronous", [False, True])
async def test_authored_heartbeat_stays_on_owner_loop_and_preserves_typed_result(owner: Any, synchronous: bool) -> None:
    worker, client = owner
    loop = asyncio.get_running_loop()
    observed_loops = []

    async def heartbeat(**kwargs: Any) -> dict[str, Any]:
        observed_loops.append(asyncio.get_running_loop())
        assert kwargs["details"] == {"authored": True}
        return status()

    client.heartbeat_activity_task.side_effect = heartbeat
    worker.activities["remote"] = typed_sync if synchronous else typed_async
    assert await worker._run_activity_task(task()) == "completed"
    assert observed_loops == [loop]
    assert client.complete_activity_task.await_args.kwargs["result"] == {
        "bytes": b"\x00\xff", "value": 42, "null": None, "task_id": "remote-task",
    }
    assert not worker._remote_activity_processes
    client.acknowledge_activity_cancellation.assert_not_awaited()
    client.fail_activity_task.assert_not_awaited()


@pytest.mark.parametrize("field", ["lease", "heartbeat", "start_to_close", "schedule_to_close", "session", "malformed"])
async def test_invalid_or_elapsed_observed_bounds_prevent_callback_start(owner: Any, field: str) -> None:
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
    client.acknowledge_activity_cancellation.assert_not_awaited()
    client.complete_activity_task.assert_not_awaited()
    client.fail_activity_task.assert_not_awaited()


@pytest.mark.parametrize("kind", ["ValueError", "NonRetryableError", "ActivityCancelled"])
async def test_genuine_application_failure_preserves_classification(owner: Any, kind: str) -> None:
    worker, client = owner
    worker.activities["remote"] = application_failure
    outcome = await worker._run_activity_task(task(kind))
    assert outcome in {"failed", "failed_non_retryable", "cancelled"}
    report = client.fail_activity_task.await_args.kwargs
    assert report["failure_type"] == kind
    assert report["non_retryable"] is (kind != "ValueError")
    assert report["failure_class"].endswith("." + kind)
    assert "application_failure" in report["stack_trace"]
    client.complete_activity_task.assert_not_awaited()


@pytest.mark.parametrize("refusal", ["refused", "unproved"])
async def test_stop_receipt_failure_never_becomes_result_or_failure(owner: Any, tmp_path: Path, refusal: str) -> None:
    worker, client = owner
    marker = tmp_path / "callback"
    worker.activities["remote"] = blocked_async
    execution = asyncio.create_task(worker._run_activity_task(task(str(marker))))
    await wait_for_file(marker)
    if refusal == "refused":
        client.acknowledge_activity_cancellation.side_effect = ServerError(409, {"reason": "request_mismatch"})
    else:
        client.acknowledge_activity_cancellation.return_value = {**stop_receipt(), "acknowledged": False}
    client.activity_task_status.return_value = cancellation_status()
    assert await asyncio.wait_for(execution, timeout=4.0) == "claim_aborted"
    await wait_for_exit(int(marker.read_text()))
    client.acknowledge_activity_cancellation.assert_awaited_once()
    client.complete_activity_task.assert_not_awaited()
    client.fail_activity_task.assert_not_awaited()


async def test_dead_supervisor_retains_capacity_and_never_reports_stop(owner: Any, tmp_path: Path) -> None:
    worker, client = owner
    marker = tmp_path / "callback"
    worker.activities["remote"] = released_sync
    execution = asyncio.create_task(worker._run_activity_task(task(str(marker))))
    await wait_for_file(marker)
    callback = next(iter(worker._remote_activity_processes))
    pid = int(marker.read_text())
    try:
        callback._supervisor.kill()
        assert await asyncio.wait_for(execution, timeout=4.0) == "claim_aborted"
        os.kill(pid, 0)
        assert callback.stopped is False
        assert worker._current_task_slots()["activity_available"] == 0
        poller = asyncio.create_task(worker._poll_activity_tasks())
        try:
            await asyncio.sleep(0.1)
            client.poll_activity_task.assert_not_awaited()
        finally:
            poller.cancel()
            with pytest.raises(asyncio.CancelledError):
                await poller
        client.acknowledge_activity_cancellation.assert_not_awaited()
        client.complete_activity_task.assert_not_awaited()
        client.fail_activity_task.assert_not_awaited()
        with pytest.raises(RuntimeError, match="unconfirmed remote callback stop.*registration remains active"):
            await worker._shutdown()
        assert worker._registered is True
        client.deregister_worker_registration.assert_not_awaited()
    finally:
        Path(str(marker) + ".release").touch()
        await wait_for_exit(pid)
        shutil.rmtree(callback.directory, ignore_errors=True)
        # The test performs external recovery after proving the worker cannot
        # confirm stop. Release this synthetic state only after the process dies.
        worker._remote_activity_processes.discard(callback)


async def test_registration_refuses_nonimportable_cooperative_handler_before_claiming(owner: Any) -> None:
    _, client = owner

    @activity.defn(name="not-importable")
    async def callback() -> None:
        pass

    client.register_worker.reset_mock()
    worker = Worker(client, task_queue="queue", worker_id="unsupported-owner", activities=[callback],
                    capabilities=["cooperative_cancellation"])
    with pytest.raises(RuntimeError, match="unsupported-owner.*not-importable.*spawn-compatible"):
        await worker._register()
    client.register_worker.assert_not_awaited()
    client.poll_activity_task.assert_not_awaited()


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
