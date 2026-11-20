from __future__ import annotations

import asyncio
import json
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
        # Transient faults applied to successive renew-lease exchanges:
        # "timeout" stalls past the caller's budget, "reset" drops the
        # connection, "503" answers a temporary server error.
        self.renewal_faults: list[str | None] = []
        self.fail_bodies: list[dict[str, Any]] = []
        # When False, an application heartbeat is acknowledged without
        # extending the lease, so it cannot stand in for failed renewals.
        self.heartbeat_extends = True

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
            fault = self.renewal_faults.pop(0) if self.renewal_faults else None
            if fault == "timeout":
                await asyncio.sleep(30)
            if fault == "reset":
                raise httpx.ConnectError("connection reset by peer", request=request)
            if fault == "503":
                return httpx.Response(503, json={"reason": "temporary_failure"})
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
                "lease_expires_at": (timestamp(now + timedelta(seconds=self.lease)) if self.heartbeat_extends
                                     else self.task["lease_expires_at"]),
            })
        if request.url.path.endswith("/fail"):
            self.publication_times.append(time.monotonic() - self.monotonic_started)
            self.fail_bodies.append(json.loads(request.content))
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


@pytest.mark.parametrize("loss", ["denied", "business-deadline"])
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


def assert_retryable_renewal_failure(server: LeaseServer) -> None:
    assert server.publications() == ["/api/worker/activity-tasks/task/17/fail"]
    assert len(server.fail_bodies) == 1
    body = server.fail_bodies[0]
    assert body["activity_attempt_id"] == "attempt-017"
    assert body["lease_owner"] == "worker-1"
    # Cross-SDK released-claim contract (PHP, Python, Rust).
    assert body["failure"]["type"] == "worker_lost"
    assert body["failure"]["class"] == "ActivityWorkerLost"
    assert "activity_lease_renewal_unavailable" in body["failure"]["message"]
    assert "non_retryable" not in body["failure"]
    assert serializer.decode_envelope(body["failure"]["details"]) == {
        "reason": "worker_lost", "cause": "activity_lease_renewal_unavailable",
    }
    # Reported inside the claimed lease, so the Server can still accept it.
    assert all(elapsed < server.lease for elapsed in server.publication_times)


def fast_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("durable_workflow._activity_lease._RENEWAL_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr("durable_workflow._activity_lease._RETRY_BASE_SECONDS", 0.02)


async def test_slow_renewal_releases_a_cooperatively_stopped_claim() -> None:
    server = LeaseServer(lease=0.5)
    server.delay_renewal = 0.6
    cancelled = False

    @activity.defn(name="maintained")
    async def handler() -> str:
        nonlocal cancelled
        try:
            await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            cancelled = True
            raise
        return "late result"

    assert await execute(server, handler) == "failed"
    assert cancelled is True
    assert_retryable_renewal_failure(server)


async def test_coroutine_that_outlives_the_stop_window_is_not_released() -> None:
    # A coroutine may swallow cancellation. Until it has actually finished the
    # claim is not released; past the deadline the lease is left to expire.
    server = LeaseServer(lease=0.5)
    server.delay_renewal = 0.6

    @activity.defn(name="maintained")
    async def handler() -> str:
        try:
            await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            await asyncio.sleep(0.3)
        return "late result"

    assert await execute(server, handler) == "claim_aborted"
    assert server.publications() == []


async def test_thread_that_stops_at_its_next_heartbeat_is_released(monkeypatch: pytest.MonkeyPatch) -> None:
    fast_retries(monkeypatch)
    server = LeaseServer(lease=0.6)
    server.renewal_faults = [None] + ["503"] * 100
    server.heartbeat_extends = False
    stopped_at: list[float] = []

    @activity.defn(name="maintained")
    def handler() -> str:
        try:
            while True:
                activity.context().heartbeat_sync({"step": 1})
                time.sleep(0.01)
        finally:
            stopped_at.append(time.monotonic() - server.monotonic_started)

    assert await execute(server, handler) == "failed"
    assert_retryable_renewal_failure(server)
    assert len(stopped_at) == 1
    assert stopped_at[0] <= server.publication_times[0], "release only after the thread stopped"


async def test_thread_that_will_not_stop_is_left_to_lease_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    fast_retries(monkeypatch)
    server = LeaseServer(lease=0.6)
    server.renewal_faults = [None] + ["503"] * 100
    started, release = threading.Event(), threading.Event()

    @activity.defn(name="maintained")
    def handler() -> str:
        started.set()
        release.wait(timeout=5)
        return "late thread result"

    async with Client("http://lease.invalid", namespace="17", worker_token="synthetic-worker-token") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://lease.invalid", transport=httpx.MockTransport(server.respond))
        client._cluster_info = {"worker_protocol": {"server_capabilities": {"activity_lease_renewal": True}}}
        worker = Worker(client, worker_id="worker-1", task_queue="queue-1", workflows=[], activities=[handler])
        try:
            task = await client.poll_activity_task(worker_id="worker-1", task_queue="queue-1")
            assert await worker._run_activity_task(task) == "claim_aborted"
            assert started.is_set()
            # Still running: a release now would let a second attempt overlap.
            assert len(worker._remote_activity_threads) == 1
            assert server.publications() == []
        finally:
            release.set()
            if worker._remote_activity_executor is not None:
                worker._remote_activity_executor.shutdown(wait=True)
    assert server.publications() == []


async def test_unconfirmed_grant_is_measured_from_the_poll_start(monkeypatch: pytest.MonkeyPatch) -> None:
    fast_retries(monkeypatch)
    server = LeaseServer(lease=1.0)
    server.delay_poll = 0.2
    server.renewal_faults = ["timeout"]
    async with Client("http://lease.invalid", namespace="17", worker_token="synthetic-worker-token") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://lease.invalid", transport=httpx.MockTransport(server.respond))
        client._cluster_info = {"worker_protocol": {"server_capabilities": {"activity_lease_renewal": True}}}
        poll_started = time.monotonic()
        await client.poll_activity_task(worker_id="worker-1", task_queue="queue-1")
        authority = current_activity_authority.get()
        assert authority is not None and authority.renewal_failures == 1
        # Charged from the poll's start, not from the response's arrival 0.2 s later.
        assert poll_started + 0.9 < authority.lease_until < poll_started + 1.1
        current_activity_authority.set(None)


async def test_unstarted_claim_with_a_spent_conservative_grant_is_released(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The long poll waited longer than the lease, then confirmation failed.
    # Nothing ran, so the claim is released rather than dropped.
    fast_retries(monkeypatch)
    server = LeaseServer(lease=0.25)
    server.delay_before_claim = 0.3
    server.renewal_faults = ["timeout"]
    called = False

    @activity.defn(name="maintained")
    async def handler() -> str:
        nonlocal called
        called = True
        return "must not run"

    with pytest.raises(Exception, match="activity_lease_renewal_unavailable"):
        await execute(server, handler)
    assert called is False
    assert_retryable_renewal_failure(server)


@pytest.mark.parametrize("faults", [
    ["timeout"], ["reset"], ["503"], ["timeout", "reset", "503", "timeout"],
])
async def test_transient_initial_confirmation_failure_keeps_the_claim(
    monkeypatch: pytest.MonkeyPatch, faults: list[str],
) -> None:
    # sdk-php#445: one stalled renewal at activity start must not drop the
    # claim. The activity runs under the claimed lease while confirmation is
    # retried, and its completion is accepted.
    fast_retries(monkeypatch)
    server = LeaseServer(lease=1.0)
    server.renewal_faults = list(faults)
    started = False

    @activity.defn(name="maintained")
    async def handler() -> str:
        nonlocal started
        started = True
        await asyncio.sleep(0.6)
        return "completed despite a flaky start"

    assert await execute(server, handler) == "completed"
    assert started is True
    assert server.renewals > len(faults), "confirmation must be retried until it succeeds"
    assert server.publications() == ["/api/worker/activity-tasks/task/17/complete"]


async def test_flaky_renewal_mid_execution_keeps_the_claim_beyond_the_original_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fast_retries(monkeypatch)
    server = LeaseServer(lease=0.6)
    server.renewal_faults = [None, "503", "timeout", "reset"]

    @activity.defn(name="maintained")
    async def handler() -> str:
        await asyncio.sleep(1.0)
        return "outlived the original lease"

    assert await execute(server, handler) == "completed"
    assert server.renewals > 4
    assert server.publications() == ["/api/worker/activity-tasks/task/17/complete"]


@pytest.mark.parametrize("fault", ["timeout", "reset", "503"])
async def test_renewal_that_never_recovers_fails_the_claim_through_the_server(
    monkeypatch: pytest.MonkeyPatch, fault: str,
) -> None:
    fast_retries(monkeypatch)
    server = LeaseServer(lease=0.5)
    server.renewal_faults = [fault] * 100

    @activity.defn(name="maintained")
    async def handler() -> str:
        await asyncio.sleep(2.0)
        return "must not be published"

    assert await execute(server, handler) == "failed"
    assert server.renewals >= 2, "renewal must be retried before the claim is given up"
    assert_retryable_renewal_failure(server)


async def test_initial_confirmation_refused_by_the_server_still_raises() -> None:
    # A typed refusal is the Server's own decision about the claim, not a
    # transient failure, so the claim is not kept.
    server = LeaseServer(lease=0.06)
    server.delay_poll = 0.1

    @activity.defn(name="maintained")
    async def handler() -> str:
        return "must not run"

    with pytest.raises(Exception, match="lease_expired"):
        await execute(server, handler)
    assert server.publications() == []
