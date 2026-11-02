"""Worker readiness and liveness status (#168)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import threading
import time
from collections.abc import Callable
from unittest.mock import AsyncMock

import httpx
import pytest

import durable_workflow.worker as worker_module
from durable_workflow import Worker, WorkerState, WorkerStatus, activity
from durable_workflow.client import Client
from durable_workflow.errors import ServerError
from tests.test_worker import TestWorkflow, compatible_cluster_info, echo_activity

pytestmark = pytest.mark.asyncio


async def eventually(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition was not reached")
        await asyncio.sleep(0.005)


def roles(status: WorkerStatus) -> dict[str, object]:
    return {role.role: role for role in status.roles}


@pytest.fixture
def client() -> AsyncMock:
    client = AsyncMock(spec=Client)
    client.get_cluster_info = AsyncMock(return_value=compatible_cluster_info())
    client.register_worker = AsyncMock(return_value={"worker_id": "w1", "registered": True})
    client.deregister_worker_registration = AsyncMock(
        return_value={"worker_id": "w1", "outcome": "deregistered", "recovered_workflow_task_count": 0}
    )
    client.heartbeat_worker = AsyncMock(return_value={"worker_id": "w1", "acknowledged": True})
    client.poll_workflow_task = AsyncMock(return_value=None)
    client.poll_activity_task = AsyncMock(return_value=None)
    client.poll_query_task = AsyncMock(return_value=None)
    client.complete_activity_task = AsyncMock(return_value={"outcome": "completed"})
    client.fail_activity_task = AsyncMock(return_value={"outcome": "failed"})
    return client


@pytest.fixture(autouse=True)
def fast_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker_module, "EVENT_LOOP_TICK_SECONDS", 0.01)
    monkeypatch.setattr(worker_module._PollFailureBackoff, "retry_delay", lambda *_args, **_kwargs: 0.01)


def make_worker(client: AsyncMock, **kwargs: object) -> Worker:
    options: dict[str, object] = {
        "task_queue": "q1",
        "workflows": [TestWorkflow],
        "activities": [echo_activity],
        "poll_timeout": 0.01,
        "shutdown_timeout": 1.0,
    }
    options.update(kwargs)
    return Worker(client, **options)  # type: ignore[arg-type]


async def stop(worker: Worker, run_task: asyncio.Task[object]) -> None:
    await worker.stop()
    with contextlib.suppress(asyncio.CancelledError):
        await run_task


async def test_unstarted_worker_is_neither_ready_nor_live(client: AsyncMock) -> None:
    worker = make_worker(client, worker_id="w-status")

    status = worker.status()

    assert status.state is WorkerState.STARTING
    assert (status.ready, status.ready_reason) == (False, "not_started")
    assert (status.live, status.live_reason) == (False, "not_started")
    assert status.worker_id == "w-status"
    assert status.task_queue == "q1"
    assert status.roles == ()
    assert status.event_loop_progress_age_seconds is None
    assert status.registered_age_seconds is None


async def test_registering_worker_is_live_but_not_ready(client: AsyncMock) -> None:
    release = asyncio.Event()

    async def slow_registration(**_: object) -> dict[str, object]:
        await release.wait()
        return {"worker_id": "w1", "registered": True}

    client.register_worker = AsyncMock(side_effect=slow_registration)
    worker = make_worker(client)
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: client.register_worker.await_count == 1)
        status = worker.status()
        assert status.state is WorkerState.STARTING
        assert (status.ready, status.ready_reason) == (False, "registering")
        assert (status.live, status.live_reason) == (True, "live")

        release.set()
        await eventually(lambda: worker.status().ready)
        status = worker.status()
        assert status.state is WorkerState.REGISTERED
        assert status.registered_age_seconds is not None
    finally:
        release.set()
        await stop(worker, run_task)


async def test_registered_worker_reports_every_configured_role_ready(client: AsyncMock) -> None:
    worker = make_worker(client)
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: client.poll_activity_task.await_count >= 3)
        status = worker.status()

        assert status.state is WorkerState.REGISTERED
        assert (status.ready, status.ready_reason) == (True, "ready")
        assert (status.live, status.live_reason) == (True, "live")
        assert set(roles(status)) == {"heartbeat", "workflow_poll", "activity_poll", "query_poll"}
        for role in status.roles:
            assert role.running is True
            assert role.consecutive_failures == 0
            assert role.stalled is False
        assert roles(status)["workflow_poll"].last_success_age_seconds is not None
        assert roles(status)["heartbeat"].last_success_age_seconds is None
    finally:
        await stop(worker, run_task)


async def test_only_configured_roles_are_reported(client: AsyncMock) -> None:
    worker = make_worker(client, workflows=[])
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: worker.status().ready)
        assert set(roles(worker.status())) == {"heartbeat", "activity_poll"}
    finally:
        await stop(worker, run_task)


async def test_empty_and_in_flight_long_polls_stay_ready(client: AsyncMock) -> None:
    hold = asyncio.Event()

    async def long_poll(**_: object) -> None:
        await hold.wait()
        return None

    client.poll_workflow_task = AsyncMock(side_effect=long_poll)
    worker = make_worker(client)
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: client.poll_workflow_task.await_count == 1)
        await asyncio.sleep(0.05)
        status = worker.status()
        workflow_poll = roles(status)["workflow_poll"]

        assert status.ready and status.live
        assert workflow_poll.in_flight_seconds is not None and workflow_poll.in_flight_seconds >= 0.04
        assert workflow_poll.overdue is False
        assert workflow_poll.stalled is False

        overdue = worker.status(request_overdue_seconds=0.01)
        assert (overdue.ready, overdue.ready_reason) == (False, "server_unavailable:workflow_poll")
        assert (overdue.live, overdue.live_reason) == (True, "live")
        assert roles(overdue)["workflow_poll"].overdue is True

        stalled = worker.status(request_stall_seconds=0.01)
        assert (stalled.live, stalled.live_reason) == (False, "role_stalled:workflow_poll")
        assert (stalled.ready, stalled.ready_reason) == (False, "role_stalled:workflow_poll")
        assert roles(stalled)["workflow_poll"].stalled is True
    finally:
        hold.set()
        await stop(worker, run_task)


async def test_transient_server_loss_is_unready_but_live_and_recovers_without_restart(
    client: AsyncMock,
) -> None:
    outage = threading.Event()
    outage.set()

    async def poll(**_: object) -> None:
        await asyncio.sleep(0.001)
        if outage.is_set():
            raise httpx.ConnectError("connection refused")
        return None

    client.poll_activity_task = AsyncMock(side_effect=poll)
    worker = make_worker(client, workflows=[])
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: client.poll_activity_task.await_count >= 2)
        status = worker.status()
        activity_poll = roles(status)["activity_poll"]
        assert status.state is WorkerState.REGISTERED
        assert (status.ready, status.ready_reason) == (False, "server_unavailable:activity_poll")
        assert (status.live, status.live_reason) == (True, "live")
        assert activity_poll.consecutive_failures >= 1
        assert activity_poll.last_failure_type == "ConnectError"
        assert "connection refused" not in json.dumps(status.as_dict())

        outage.clear()
        await eventually(lambda: worker.status().ready)
        recovered = roles(worker.status())["activity_poll"]
        assert recovered.consecutive_failures == 0
        assert recovered.last_failure_age_seconds is not None
        assert client.register_worker.await_count == 1
    finally:
        await stop(worker, run_task)


async def test_poll_capacity_backpressure_keeps_the_worker_ready(client: AsyncMock) -> None:
    refusal = ServerError(
        429,
        {
            "reason": "long_poll_capacity_exhausted",
            "poll_status": "long_poll_capacity_exhausted",
            "task": None,
            "task_kind": "activity_task",
            "task_queue": "q1",
            "retryable": True,
            "retry_after_seconds": 1,
        },
    )
    client.poll_activity_task = AsyncMock(side_effect=refusal)
    worker = make_worker(client, workflows=[])
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: client.poll_activity_task.await_count >= 1)
        status = worker.status()
        assert status.ready, status.ready_reason
        assert roles(status)["activity_poll"].consecutive_failures == 0
    finally:
        await stop(worker, run_task)


async def test_heartbeat_failure_is_unready_until_the_next_heartbeat_succeeds(client: AsyncMock) -> None:
    failing = threading.Event()
    failing.set()

    async def heartbeat(**_: object) -> dict[str, object]:
        if failing.is_set():
            raise httpx.ReadTimeout("timed out")
        return {"worker_id": "w1", "acknowledged": True}

    client.heartbeat_worker = AsyncMock(side_effect=heartbeat)
    worker = make_worker(client, heartbeat_interval=0.02)
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: client.heartbeat_worker.await_count >= 1)
        await eventually(lambda: worker.status().ready_reason == "server_unavailable:heartbeat")
        assert worker.status().live

        failing.clear()
        await eventually(lambda: worker.status().ready)
        assert roles(worker.status())["heartbeat"].last_success_age_seconds is not None
    finally:
        await stop(worker, run_task)


async def test_registration_refusal_reports_failed(client: AsyncMock) -> None:
    client.register_worker = AsyncMock(side_effect=ServerError(403, {"reason": "forbidden"}))
    worker = make_worker(client)

    with pytest.raises(ServerError):
        await worker.run()

    status = worker.status()
    assert status.state is WorkerState.FAILED
    assert (status.ready, status.ready_reason) == (False, "failed:registration")
    assert (status.live, status.live_reason) == (False, "failed:registration")
    assert worker._status_tick_task is not None
    await asyncio.sleep(0)
    assert worker._status_tick_task.done()


async def test_poller_failure_reports_failed(client: AsyncMock, monkeypatch: pytest.MonkeyPatch) -> None:
    async def broken_poller() -> None:
        raise RuntimeError("poller bug")

    worker = make_worker(client)
    monkeypatch.setattr(worker, "_poll_activity_tasks", broken_poller)

    with pytest.raises(RuntimeError, match="poller bug"):
        await worker.run()

    status = worker.status()
    assert status.state is WorkerState.FAILED
    assert (status.ready, status.live, status.live_reason) == (False, False, "failed:runtime")


async def test_dead_query_thread_is_neither_live_nor_ready(client: AsyncMock) -> None:
    class BrokenQueryClient:
        async def __aenter__(self) -> BrokenQueryClient:
            raise RuntimeError("query client unavailable")

        async def __aexit__(self, *_: object) -> None:
            return None

    worker = make_worker(client)
    worker._can_clone_client_for_query_tasks = lambda: True  # type: ignore[method-assign]
    worker._clone_client_for_query_tasks = lambda: BrokenQueryClient()  # type: ignore[method-assign,return-value]
    run_task = asyncio.create_task(worker.run())
    try:
        def query_role_exited() -> bool:
            query_poll = roles(worker.status()).get("query_poll")
            return query_poll is not None and not query_poll.running  # type: ignore[attr-defined]

        await eventually(query_role_exited)
        status = worker.status()
        assert status.state is WorkerState.REGISTERED
        assert (status.live, status.live_reason) == (False, "role_stopped:query_poll")
        assert (status.ready, status.ready_reason) == (False, "role_stopped:query_poll")
    finally:
        await stop(worker, run_task)


async def test_stalled_event_loop_is_reported_from_another_thread(client: AsyncMock) -> None:
    worker = make_worker(client)
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: worker.status().ready)
        observed: list[WorkerStatus] = []

        def probe() -> None:
            time.sleep(0.3)
            observed.append(worker.status(event_loop_stall_seconds=0.15))

        prober = threading.Thread(target=probe)
        prober.start()
        time.sleep(0.5)  # Block the worker's event loop.
        prober.join()

        assert (observed[0].live, observed[0].live_reason) == (False, "event_loop_stalled")
        assert observed[0].ready is False
        await eventually(lambda: worker.status(event_loop_stall_seconds=0.15).live)
    finally:
        await stop(worker, run_task)


async def test_long_running_activity_and_drain_states(client: AsyncMock) -> None:
    started = threading.Event()
    release = threading.Event()

    @activity.defn(name="status-slow-activity")
    def slow_activity() -> str:
        started.set()
        release.wait(5)
        return "done"

    delivered = False

    async def poll_activity(**_: object) -> dict[str, object] | None:
        nonlocal delivered
        await asyncio.sleep(0.001)
        if delivered:
            return None
        delivered = True
        return {
            "task_id": "at-1",
            "activity_attempt_id": "attempt-1",
            "activity_type": "status-slow-activity",
            "attempt_number": 1,
            "lease_owner": "w1",
            "payload_codec": "avro",
            "arguments": None,
        }

    client.poll_activity_task = AsyncMock(side_effect=poll_activity)
    worker = make_worker(client, workflows=[], activities=[slow_activity], shutdown_timeout=5.0)
    run_task = asyncio.create_task(worker.run())
    stop_task: asyncio.Task[None] | None = None
    try:
        await eventually(started.is_set)
        await asyncio.sleep(0.05)
        status = worker.status(request_stall_seconds=0.02)
        assert status.ready and status.live, (status.ready_reason, status.live_reason)

        stop_task = asyncio.create_task(worker.stop())
        await eventually(lambda: worker.status().state is WorkerState.DRAINING)
        draining = worker.status()
        assert (draining.ready, draining.ready_reason) == (False, "draining")
        assert (draining.live, draining.live_reason) == (True, "live")

        release.set()
        await stop_task
        await run_task
        stopped = worker.status()
        assert stopped.state is WorkerState.STOPPED
        assert (stopped.ready, stopped.ready_reason) == (False, "stopped")
        assert (stopped.live, stopped.live_reason) == (False, "stopped")
        assert worker._status_tick_task is not None and worker._status_tick_task.done()
    finally:
        release.set()
        if stop_task is not None:
            await stop_task
        if not run_task.done():
            await stop(worker, run_task)


async def test_shutdown_that_cannot_confirm_cleanup_reports_failed(
    client: AsyncMock, monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = make_worker(client)
    run_task = asyncio.create_task(worker.run())
    await eventually(lambda: worker.status().ready)

    async def unconfirmed_shutdown() -> None:
        raise RuntimeError("the worker registration remains active")

    monkeypatch.setattr(worker, "_shutdown", unconfirmed_shutdown)
    with pytest.raises(RuntimeError):
        await worker.stop()
    with contextlib.suppress(RuntimeError, asyncio.CancelledError):
        await run_task

    status = worker.status()
    assert status.state is WorkerState.FAILED
    assert (status.ready, status.live, status.live_reason) == (False, False, "failed:shutdown")


async def test_status_snapshot_performs_no_server_io(client: AsyncMock) -> None:
    worker = make_worker(client, workflows=[], activities=[])
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: worker.status().ready)
        calls = [(name, getattr(client, name).await_count) for name in (
            "register_worker", "heartbeat_worker", "poll_workflow_task", "poll_activity_task", "poll_query_task",
            "get_cluster_info",
        )]
        for _ in range(50):
            worker.status()
        await asyncio.to_thread(worker.status)
        assert calls == [(name, getattr(client, name).await_count) for name, _ in calls]
    finally:
        await stop(worker, run_task)


async def test_status_is_json_serializable(client: AsyncMock) -> None:
    worker = make_worker(client)
    run_task = asyncio.create_task(worker.run())
    try:
        await eventually(lambda: worker.status().ready)
        payload = json.loads(json.dumps(worker.status().as_dict()))
        assert payload["state"] == "registered"
        assert payload["ready"] is True and payload["live"] is True
        assert {role["role"] for role in payload["roles"]} == {
            "heartbeat", "workflow_poll", "activity_poll", "query_poll",
        }
    finally:
        await stop(worker, run_task)


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), True, "5"])
async def test_status_rejects_invalid_bounds(client: AsyncMock, value: object) -> None:
    worker = make_worker(client)
    with pytest.raises(ValueError, match="event_loop_stall_seconds"):
        worker.status(event_loop_stall_seconds=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="request_overdue_seconds"):
        worker.status(request_overdue_seconds=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="request_stall_seconds"):
        worker.status(request_stall_seconds=value)  # type: ignore[arg-type]


async def test_default_request_bounds_follow_the_attempt_deadline_and_retry_budget(client: AsyncMock) -> None:
    from durable_workflow.retry_policy import TransportRetryPolicy

    client.timeout = 60.0
    client.retry_policy = TransportRetryPolicy()
    worker = make_worker(client, poll_timeout=35.0)
    assert worker._request_attempt_deadline_seconds() == 60.0
    assert worker._default_request_stall_seconds() == 2 * (3 * 60.0 + 2 * 5.0)
    client.timeout = 10.0
    assert worker._request_attempt_deadline_seconds() == 40.0
    client.retry_policy = TransportRetryPolicy(max_attempts=1)
    assert worker._default_request_stall_seconds() == 2 * 40.0


async def test_run_until_reports_registration_and_stop(client: AsyncMock) -> None:
    from durable_workflow.client import WorkflowExecution

    client.describe_workflow = AsyncMock(return_value=WorkflowExecution(
        workflow_id="wf-1", run_id="run-1", workflow_type="test-wf", status="completed",
    ))
    worker = make_worker(client)
    await worker.run_until(workflow_id="wf-1", timeout=2.0)

    status = worker.status()
    assert status.state is WorkerState.STOPPED
    assert set(roles(status)) == {"heartbeat", "query_poll"}
    assert (status.ready, status.live) == (False, False)
