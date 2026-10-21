from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest

from durable_workflow import Client, Worker, activity, serializer


def timestamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


class LeaseServer:
    def __init__(self, *, lease: float = 0.18, deadline: float | None = None) -> None:
        self.lease = lease
        self.started = datetime.now(timezone.utc)
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
        self.delay_renewal = 0.0
        self.fail_completion = False

    async def respond(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        if request.url.path.endswith("/poll"):
            await asyncio.sleep(self.delay_poll)
            return httpx.Response(200, json={
                "task": self.task, "server_time": timestamp(self.started),
                "server_capabilities": {"activity_lease_renewal": True},
            })
        if request.url.path.endswith("/renew-lease"):
            self.renewals += 1
            await asyncio.sleep(self.delay_renewal)
            now = datetime.now(timezone.utc)
            return httpx.Response(409 if self.deny else 200, json={
                "task_id": self.task["task_id"], "workflow_task_id": self.task["task_id"],
                "workflow_instance_id": self.task["workflow_id"], "workflow_run_id": self.task["run_id"],
                "activity_execution_id": self.task["activity_execution_id"],
                "activity_attempt_id": self.task["activity_attempt_id"], "attempt_number": 1,
                "lease_owner": self.task["lease_owner"], "lease_renewed": not self.deny,
                "can_continue": not self.deny, "cancel_requested": False, "heartbeat_recorded": False,
                "server_time": timestamp(now), "lease_expires_at": timestamp(now + timedelta(seconds=self.lease)),
                "reason": "lease_expired" if self.deny else None,
            })
        if request.url.path.endswith("/complete"):
            if self.fail_completion:
                return httpx.Response(503, json={"reason": "temporary_failure"})
            return httpx.Response(200, json={"outcome": "completed", "recorded": True})
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
        worker._activity_lease_renewal_supported = True
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
    assert called is True and cancelled is True
    assert server.publications() == []


async def test_original_deadline_fences_transport_retries_during_terminal_publication() -> None:
    server = LeaseServer(lease=1, deadline=0.12)
    server.fail_completion = True

    @activity.defn(name="maintained")
    async def handler() -> str:
        return "ready before the deadline"

    assert await execute(server, handler) == "claim_aborted"
    assert len(server.publications()) == 1


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
        worker._activity_lease_renewal_supported = True
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
