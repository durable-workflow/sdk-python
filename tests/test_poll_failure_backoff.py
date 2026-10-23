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
from durable_workflow.worker import Worker

POLL_KINDS = ["workflow", "activity", "query"]


@pytest.mark.parametrize("kind", POLL_KINDS)
@pytest.mark.parametrize("error", [
    Unauthorized("Invalid or missing authentication token"),
    ServerError(401, {"message": "private response"}),
    ServerError(403, {"message": "private response"}),
    ServerError(503, {}),
    httpx.ConnectError("connection refused"),
])
async def test_poll_failure_growth_cap_and_empty_success_reset(
    kind: str, error: Exception, monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="orders")
    poll = getattr(client, f"poll_{kind}_task")
    calls = 0
    delays: list[float] = []

    async def respond(**_: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 10:
            return None
        if calls == 12:
            worker._stop.set()
            return None
        raise error

    async def sleep(delay: float) -> None:
        if delay:
            delays.append(delay)

    poll.side_effect = respond
    monkeypatch.setattr(worker_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(random, "uniform", lambda low, high: high)
    await getattr(worker, f"_poll_{kind}_tasks")()

    assert delays == [1, 2, 4, 8, 16, 30, 30, 30, 30, 1]
    assert calls == 12
    # Failed/empty polls must release their reservations before retrying.
    assert worker._act_semaphore._value == worker.max_concurrent_activity_tasks
    assert worker._workflow_reserved == 0


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
    # First refusal at 0, reminders at 61 and 121, and the new streak after success.
    assert len(warnings) == 4
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
    with pytest.raises(ServerError) as caught:
        await getattr(worker, f"_poll_{kind}_tasks")()
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
