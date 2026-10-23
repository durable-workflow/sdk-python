from __future__ import annotations

import asyncio
import logging
import random
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

import durable_workflow.worker as worker_module
from durable_workflow.client import Client
from durable_workflow.errors import ServerError, Unauthorized
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.worker import Worker

POLL_KINDS = ["workflow", "activity", "query"]


@pytest.mark.parametrize("kind", POLL_KINDS)
@pytest.mark.parametrize("successful_task", [False, True], ids=["empty", "task"])
@pytest.mark.parametrize("error", [
    Unauthorized("Invalid or missing authentication token"),
    ServerError(401, {"message": "private response"}),
    ServerError(403, {"message": "private response"}),
    ServerError(503, {}),
    httpx.ConnectError("connection refused"),
])
async def test_poll_failure_growth_cap_and_empty_success_reset(
    kind: str, successful_task: bool, error: Exception, monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="orders")
    poll = getattr(client, f"poll_{kind}_task")
    calls = 0
    delays: list[float] = []

    async def respond(**_: object) -> dict[str, object] | None:
        nonlocal calls
        calls += 1
        if calls == 10:
            return {"task_id": "recovered"} if successful_task else None
        if calls == 12:
            worker._stop.set()
            return None
        raise error

    async def sleep(delay: float) -> None:
        if delay:
            delays.append(delay)

    dispatched: list[object] = []

    async def dispatch(task: object, **_: object) -> None:
        dispatched.append(task)
        if kind == "activity":
            worker._act_semaphore.release()

    poll.side_effect = respond
    monkeypatch.setattr(worker, f"_dispatch_{kind}_task", dispatch)
    monkeypatch.setattr(worker_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(random, "uniform", lambda low, high: high)
    await getattr(worker, f"_poll_{kind}_tasks")()
    await asyncio.gather(*worker._in_flight)

    assert delays == [1, 2, 4, 8, 16, 30, 30, 30, 30, 1]
    assert calls == 12
    assert len(dispatched) == int(successful_task)
    # Failed/empty polls must release their reservations before retrying.
    assert worker._act_semaphore._value == worker.max_concurrent_activity_tasks
    assert worker._workflow_reserved == 0


def test_jitter_uses_a_fresh_sample_with_bounded_delays(monkeypatch: pytest.MonkeyPatch) -> None:
    bounds: list[tuple[float, float]] = []

    def uniform(low: float, high: float) -> float:
        bounds.append((low, high))
        return low if len(bounds) % 2 else high

    monkeypatch.setattr(random, "uniform", uniform)
    backoff = worker_module._PollFailureBackoff()
    delays = [backoff.retry_delay(Unauthorized(), "workflow") for _ in range(8)]
    assert delays == [0.5, 2, 2, 8, 8, 30, 15, 30]
    assert bounds == [(0.5, 1), (1, 2), (2, 4), (4, 8), (8, 16), (15, 30), (15, 30), (15, 30)]


@pytest.mark.parametrize("kind", POLL_KINDS)
async def test_changed_auth_failure_is_reported_immediately(
    kind: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="orders")
    getattr(client, f"poll_{kind}_task").side_effect = [
        Unauthorized(), Unauthorized(), ServerError(403, {}), ServerError(403, {}),
    ]
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 4:
            worker._stop.set()

    monkeypatch.setattr(worker_module.asyncio, "sleep", sleep)
    with caplog.at_level(logging.WARNING, logger="durable_workflow.worker"):
        await getattr(worker, f"_poll_{kind}_tasks")()
    warnings = [r.getMessage() for r in caplog.records if r.name == "durable_workflow.worker"]
    assert len(warnings) == 2
    assert "HTTP 401" in warnings[0]
    assert "HTTP 403" in warnings[1]
    assert delays[2] >= 2  # A changed failure does not reset the request backoff.


@pytest.mark.parametrize("kind", POLL_KINDS)
@pytest.mark.parametrize("error,diagnostic", [
    (Unauthorized("private response"), "authentication failed (HTTP 401)"),
    (ServerError(403, {"message": "private response"}), "authorization failed (HTTP 403)"),
])
async def test_identical_auth_warnings_are_rate_limited_and_reset_after_success(
    kind: str, error: Exception, diagnostic: str,
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="orders")
    now = 0.0
    calls = 0
    outcomes: list[str] = []

    async def respond(**_: object) -> None:
        nonlocal calls
        calls += 1
        if calls in (12, 14):
            if calls == 14:
                worker._stop.set()
            return None
        raise error

    async def sleep(delay: float) -> None:
        nonlocal now
        now += delay

    getattr(client, f"poll_{kind}_task").side_effect = respond
    monkeypatch.setattr(worker_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(random, "uniform", lambda low, high: high)
    monkeypatch.setattr(worker_module, "time", SimpleNamespace(
        monotonic=lambda: now, perf_counter=time.perf_counter,
    ))
    monkeypatch.setattr(worker, "_record_poll_metrics", lambda _kind, outcome, _duration: outcomes.append(outcome))
    with caplog.at_level(logging.WARNING, logger="durable_workflow.worker"):
        await getattr(worker, f"_poll_{kind}_tasks")()

    warnings = [r.getMessage() for r in caplog.records if r.name == "durable_workflow.worker"]
    # First refusal at 0, reminders at 61, 121 and 181, then the post-success streak.
    assert len(warnings) == 5
    assert all(diagnostic in message and "credentials" in message for message in warnings)
    assert "private response" not in caplog.text
    assert outcomes.count("error") == 12


@pytest.mark.parametrize("kind", POLL_KINDS)
async def test_backoff_does_not_absorb_storage_admission(
    kind: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="orders")
    refusal = ServerError(503, {"reason": "storage_admission_unavailable"})
    getattr(client, f"poll_{kind}_task").side_effect = refusal
    sleep = AsyncMock()
    monkeypatch.setattr(worker_module.asyncio, "sleep", sleep)
    polling = getattr(worker, f"_poll_{kind}_tasks")()
    with pytest.raises(ServerError) as caught:
        await polling
    assert caught.value is refusal
    sleep.assert_not_awaited()


@pytest.mark.parametrize("kind", POLL_KINDS)
async def test_shutdown_interrupts_a_capped_backoff(kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="orders", shutdown_timeout=0.5)
    getattr(client, f"poll_{kind}_task").side_effect = Unauthorized()
    waiting = asyncio.Event()
    real_sleep = asyncio.sleep

    async def sleep(delay: float) -> None:
        if delay == 30:
            waiting.set()
            await real_sleep(delay)

    monkeypatch.setattr(worker_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(random, "uniform", lambda low, high: high)
    task = asyncio.create_task(getattr(worker, f"_poll_{kind}_tasks")())
    worker._poller_tasks.add(task)
    try:
        await asyncio.wait_for(waiting.wait(), timeout=1)
        await asyncio.wait_for(worker.stop(), timeout=1)
        assert task.cancelled()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("kind", POLL_KINDS)
@pytest.mark.parametrize("status", [401, 403, 503, None], ids=["401", "403", "503", "transport"])
async def test_actual_client_poll_errors_back_off_then_recover(
    kind: str, status: int | None, monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client(
        "https://runtime.example", worker_token="test-worker-token", namespace="orders",
        retry_policy=TransportRetryPolicy(max_attempts=1),
    )
    worker = Worker(client, task_queue="orders")
    calls = 0
    delays: list[float] = []

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url.path == f"/api/worker/{kind}-tasks/poll"
        assert request.headers["Authorization"] == "Bearer test-worker-token"
        assert request.headers["X-Namespace"] == "orders"
        if calls in (4, 6):
            if calls == 6:
                worker._stop.set()
            return httpx.Response(200, json={"task": None})
        if status is None:
            raise httpx.ConnectError("runtime unavailable", request=request)
        return httpx.Response(status, json={"message": "poll refused"})

    async def sleep(delay: float) -> None:
        if delay:
            delays.append(delay)

    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(respond))
    client._runtime_external_payload_transport_resolved = True
    monkeypatch.setattr(worker_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(random, "uniform", lambda low, high: high)
    async with client:
        await getattr(worker, f"_poll_{kind}_tasks")()
    assert calls == 6
    assert delays == [1, 2, 4, 1]
