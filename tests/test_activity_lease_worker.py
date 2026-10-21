from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from durable_workflow import Client, Worker, activity, serializer
from durable_workflow._activity_lease import ActivityLeaseAuthority, current_activity_authority


@pytest.mark.parametrize("capability", [True, False, None, "true"])
async def test_worker_uses_client_discovery_for_original_authority(capability: Any) -> None:
    client = Client("http://runtime.invalid")
    client._cluster_info = {"worker_protocol": {"server_capabilities": {"activity_lease_renewal": capability}}}
    worker = Worker(client, task_queue="queue-1", worker_id="worker-1")
    worker._run_activity_task_body = AsyncMock(return_value="completed")
    context = current_activity_authority.set(None)
    try:
        outcome = await worker._run_activity_task({"task_id": "original-task"})
        if capability is True:
            assert outcome == "claim_aborted"
            worker._run_activity_task_body.assert_not_awaited()
        else:
            assert outcome == "completed"
            worker._run_activity_task_body.assert_awaited_once()
    finally:
        current_activity_authority.reset(context)
        await client.aclose()


def timestamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


class LeaseServer:
    def __init__(self, *, lease: float = 0.18, deadline: float | None = None) -> None:
        self.lease = lease
        self.started = datetime.now(timezone.utc)
        self.monotonic_started = time.monotonic()
        self.task = {
            "task_id": "task/17", "activity_attempt_id": "attempt-017", "attempt_number": 1,
            "activity_execution_id": "execution-1", "workflow_id": "workflow-1", "run_id": "run-1",
            "task_queue": "queue-1", "activity_type": "maintained", "lease_owner": "worker-1",
            "lease_expires_at": timestamp(self.started + timedelta(seconds=lease)),
            "retry_policy": {"heartbeat_timeout": None}, "deadlines": {},
            "arguments": serializer.envelope([], codec="avro"), "payload_codec": "avro",
        }
        if deadline is not None:
            self.task["deadlines"] = {"start_to_close": timestamp(self.started + timedelta(seconds=deadline))}
        self.paths: list[str] = []
        self.renewals = 0
        self.deny = False
        self.delay_poll = 0.0
        self.delay_before_claim = 0.0
        self.delay_renewal = 0.0
        self.fail_completion = False
        self.publication_times: list[float] = []

    async def respond(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        if request.url.path.endswith("/poll"):
            if self.delay_before_claim:
                await asyncio.sleep(self.delay_before_claim)
            claimed_at = datetime.now(timezone.utc)
            # Start-to-close and heartbeat begin when this synthetic server
            # claims the attempt, after client bootstrap and any empty wait.
            offset = claimed_at - self.started
            for key in ("start_to_close", "heartbeat"):
                if key in self.task["deadlines"]:
                    deadline = datetime.fromisoformat(self.task["deadlines"][key].replace("Z", "+00:00"))
                    self.task["deadlines"][key] = timestamp(deadline + offset)
            self.started = claimed_at
            self.monotonic_started = time.monotonic()
            self.task["lease_expires_at"] = timestamp(claimed_at + timedelta(seconds=self.lease))
            response_clock = datetime.now(timezone.utc)
            await asyncio.sleep(self.delay_poll)
            return httpx.Response(200, json={
                "task": self.task, "server_time": timestamp(response_clock),
                "server_capabilities": {"activity_lease_renewal": True},
            })
        if request.url.path.endswith("/renew-lease"):
            self.renewals += 1
            await asyncio.sleep(self.delay_renewal if self.renewals > 1 else 0)
            now = datetime.now(timezone.utc)
            denied = (self.deny and self.renewals > 1
                      or now >= datetime.fromisoformat(self.task["lease_expires_at"].replace("Z", "+00:00")))
            if not denied:
                self.task["lease_expires_at"] = timestamp(now + timedelta(seconds=self.lease))
            return httpx.Response(409 if denied else 200, json={
                "task_id": self.task["task_id"], "workflow_task_id": self.task["task_id"],
                "workflow_instance_id": self.task["workflow_id"], "workflow_run_id": self.task["run_id"],
                "activity_execution_id": self.task["activity_execution_id"],
                "activity_attempt_id": self.task["activity_attempt_id"], "attempt_number": 1,
                "lease_owner": self.task["lease_owner"], "lease_renewed": not denied,
                "can_continue": not denied, "cancel_requested": False, "heartbeat_recorded": False,
                "server_time": timestamp(now), "lease_expires_at": self.task["lease_expires_at"],
                "reason": "lease_expired" if denied else None,
            })
        if request.url.path.endswith("/complete"):
            self.publication_times.append(time.monotonic() - self.monotonic_started)
            if self.fail_completion:
                return httpx.Response(503, json={"reason": "temporary_failure"})
            return httpx.Response(200, json={"outcome": "completed", "recorded": True})
        if request.url.path.endswith("/heartbeat"):
            now = datetime.now(timezone.utc)
            return httpx.Response(200, json={
                "task_id": self.task["task_id"], "activity_attempt_id": self.task["activity_attempt_id"],
                "lease_owner": self.task["lease_owner"], "heartbeat_recorded": True,
                "can_continue": True, "cancel_requested": False, "reason": None,
                "last_heartbeat_at": timestamp(now),
                "lease_expires_at": timestamp(now + timedelta(seconds=self.lease)),
            })
        if request.url.path.endswith("/fail"):
            return httpx.Response(200, json={"outcome": "failed", "recorded": True})
        raise AssertionError(f"Unexpected request {request.method} {request.url.path}")

    def publications(self) -> list[str]:
        return [path for path in self.paths if path.endswith(("/complete", "/fail"))]


async def execute(server: LeaseServer, handler: Any) -> str:
    async with Client("http://lease.invalid", namespace="17", worker_token="synthetic-worker-token") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://lease.invalid", transport=httpx.MockTransport(server.respond))
        client._cluster_info = {"worker_protocol": {"server_capabilities": {"activity_lease_renewal": True}}}
        worker = Worker(client, worker_id="worker-1", task_queue="queue-1", workflows=[], activities=[handler])
        task = await client.poll_activity_task(worker_id="worker-1", task_queue="queue-1")
        return await worker._run_activity_task(task)


async def test_healthy_callback_runs_beyond_original_lease_without_application_heartbeats() -> None:
    server = LeaseServer()

    @activity.defn(name="maintained")
    async def handler() -> str:
        await asyncio.sleep(0.55)
        return "completed once"

    assert await execute(server, handler) == "completed"
    assert server.renewals >= 2
    assert len(server.publications()) == 1
    assert not any(path.endswith("/heartbeat") for path in server.paths)


async def test_settled_callback_stops_maintenance_when_inflight_renewal_swallows_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = LeaseServer(lease=1)
    authority = ActivityLeaseAuthority.delivery(object(), "17", server.task, owner="worker-1",
        task_queue="queue-1", server_time=timestamp(server.started), request_started=server.monotonic_started)
    renewal_started = asyncio.Event()
    calls = 0
    swallowed = False

    async def renewal(_self: ActivityLeaseAuthority) -> bool:
        nonlocal calls, swallowed
        calls += 1
        renewal_started.set()
        try:
            await asyncio.get_running_loop().create_future()
        except asyncio.CancelledError:
            if not swallowed:
                # Model an I/O wrapper completing concurrently with cancel().
                swallowed = True
                return True
            raise
        raise AssertionError("The controlled renewal should remain pending until cancellation")

    async def operation() -> str:
        await renewal_started.wait()
        return "completed once"

    monkeypatch.setattr(ActivityLeaseAuthority, "renew", renewal)
    running = asyncio.create_task(authority.run(operation))
    try:
        done, _ = await asyncio.wait({running}, timeout=0.8)
        assert running in done, "Settled callbacks must not await an indefinitely renewing helper"
        assert await running == "completed once"
        assert swallowed is True
        assert calls == 1, "A settled operation must not start a subsequent renewal"
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)


async def test_loading_supervision_can_settle_before_the_same_claim_runs_its_callback() -> None:
    server = LeaseServer(lease=1)
    authority = ActivityLeaseAuthority.delivery(object(), "17", server.task, owner="worker-1",
        task_queue="queue-1", server_time=timestamp(server.started), request_started=server.monotonic_started)

    async def loading() -> str:
        await asyncio.sleep(0)
        return "loaded"

    assert await authority.run(loading) == "loaded"
    original_deadline = authority.lease_until
    assert await authority.run(loading) == "loaded"
    assert authority.lease_until == original_deadline
    assert authority.remaining() > 0


@pytest.mark.parametrize("loss", ["denied", "slow-renewal", "business-deadline"])
async def test_lost_claim_cannot_publish_a_late_result_or_failure(loss: str) -> None:
    server = LeaseServer(deadline=0.13 if loss == "business-deadline" else None)
    server.deny = loss == "denied"
    server.delay_renewal = 0.3 if loss == "slow-renewal" else 0.0
    called = False
    cancelled = False

    @activity.defn(name="maintained")
    async def handler() -> str:
        nonlocal called, cancelled
        called = True
        try:
            await asyncio.sleep(0.45)
        except asyncio.CancelledError:
            # A callback can swallow cancellation. Publication still needs its
            # original live authority, independently of callback cooperation.
            cancelled = True
            await asyncio.sleep(0.02)
        return "late result"

    assert await execute(server, handler) == "claim_aborted"
    assert called is True
    assert cancelled is True
    assert server.publications() == []


async def test_original_deadline_fences_transport_retries_during_terminal_publication() -> None:
    server = LeaseServer(lease=1, deadline=0.12)
    server.fail_completion = True

    @activity.defn(name="maintained")
    async def handler() -> str:
        return "ready before the deadline"

    assert await execute(server, handler) == "claim_aborted"
    assert server.publications()
    assert all(elapsed < 0.12 for elapsed in server.publication_times)


async def test_cancelled_thread_keeps_its_real_slot_until_the_callback_exits() -> None:
    server = LeaseServer()
    server.deny = True
    started, release = threading.Event(), threading.Event()

    @activity.defn(name="maintained")
    def handler() -> str:
        started.set()
        release.wait(timeout=2)
        return "late thread result"

    async with Client("http://lease.invalid", namespace="17", worker_token="synthetic-worker-token") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://lease.invalid", transport=httpx.MockTransport(server.respond))
        client._cluster_info = {"worker_protocol": {"server_capabilities": {"activity_lease_renewal": True}}}
        worker = Worker(client, worker_id="worker-1", task_queue="queue-1", workflows=[], activities=[handler],
                        max_concurrent_activity_tasks=1)
        try:
            task = await client.poll_activity_task(worker_id="worker-1", task_queue="queue-1")
            assert await worker._run_activity_task(task) == "claim_aborted"
            assert started.is_set()
            assert len(worker._remote_activity_threads) == 1
            assert worker._remote_activity_thread_slots.locked()
            assert server.publications() == []
        finally:
            release.set()
            if worker._remote_activity_executor is not None:
                worker._remote_activity_executor.shutdown(wait=True)
        await asyncio.sleep(0)
        assert worker._remote_activity_threads == set()


async def test_actual_application_heartbeat_acknowledges_recorded_progress_before_original_expiry() -> None:
    server = LeaseServer(lease=3)
    server.task["retry_policy"] = {"heartbeat_timeout": 1}
    server.task["deadlines"] = {"heartbeat": timestamp(server.started + timedelta(seconds=0.15))}

    @activity.defn(name="maintained")
    async def handler() -> str:
        await asyncio.sleep(0.04)
        await activity.context().heartbeat({"step": 1})
        await asyncio.sleep(0.28)
        return "progress acknowledged"

    assert await execute(server, handler) == "completed"
    assert len(server.publications()) == 1
    assert len([path for path in server.paths if path.endswith("/heartbeat")]) == 1


async def test_poll_round_trip_consumes_the_original_delivery_budget() -> None:
    server = LeaseServer(lease=0.06)
    server.delay_poll = 0.1
    called = False

    @activity.defn(name="maintained")
    async def handler() -> str:
        nonlocal called
        called = True
        return "must not run"

    with pytest.raises(Exception, match="activity.*(expired|budget)"):
        await execute(server, handler)
    assert called is False
    assert server.publications() == []


async def test_waiting_for_a_new_task_does_not_consume_its_not_yet_created_lease() -> None:
    server = LeaseServer()
    server.delay_before_claim = 0.25

    @activity.defn(name="maintained")
    async def handler() -> str:
        return "newly claimed after long poll"

    assert await execute(server, handler) == "completed"
    assert len(server.publications()) == 1
