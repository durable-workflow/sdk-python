"""A runtime that is warming up or briefly unavailable does not end the worker (sdk-php#448)."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
import pytest

from durable_workflow import Client, Worker, activity
from durable_workflow.errors import ServerError, Unauthorized
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.worker import _transient_retry_delay, _transient_runtime_error

from .test_worker import compatible_cluster_info

STORAGE_UNAVAILABLE = {"reason": "storage_unavailable", "message": "storage_unavailable"}


@pytest.mark.parametrize(("error", "transient"), [
    (httpx.ConnectError("refused"), True),
    (httpx.ReadTimeout("timed out"), True),
    (httpx.RemoteProtocolError("reset"), True),
    (ServerError(503, STORAGE_UNAVAILABLE), True),
    (ServerError(503, {"reason": "backend_unavailable"}), True),
    (ServerError(502, "<html>bad gateway</html>"), True),
    (ServerError(504, {}), True),
    (ServerError(500, {"reason": "x", "retryable": True}), True),
    (ServerError(429, {"reason": "rate_limited", "retryable": True}), True),
    (ServerError(503, {"reason": "storage_unavailable", "retryable": False}), False),
    (ServerError(503, {"reason": "storage_pressure"}), False),
    (ServerError(500, {"reason": "internal"}), False),
    (ServerError(500, "boom"), False),
    (ServerError(429, {"reason": "rate_limited"}), False),
    (ServerError(401, {"reason": "unauthorized", "retryable": True}), False),
    (ServerError(403, {}), False),
    (ServerError(409, {"reason": "conflict"}), False),
    (Unauthorized(), False),
    (RuntimeError("incompatible"), False),
])
def test_transient_classification(error: Exception, transient: bool) -> None:
    assert _transient_runtime_error(error) is transient


@pytest.mark.parametrize(("failures", "ceiling"), [(1, 0.25), (2, 0.5), (3, 1.0), (4, 2.0), (5, 4.0), (12, 4.0)])
def test_transient_retry_uses_the_shared_full_jitter_schedule(
    monkeypatch: pytest.MonkeyPatch, failures: int, ceiling: float,
) -> None:
    calls: list[tuple[float, float]] = []
    monkeypatch.setattr("durable_workflow.worker.random.uniform", lambda low, high: calls.append((low, high)) or high)
    assert _transient_retry_delay(failures) == ceiling
    assert calls == [(0.0, ceiling)]


@activity.defn(name="transient.noop")
def noop() -> None:
    return None


class FlakyRuntime:
    def __init__(self, path: str, failures: int, response: Any = None) -> None:
        self.path, self.failures = path, failures
        self.response = response if response is not None else (503, STORAGE_UNAVAILABLE)
        self.served = 0
        self.polls = 0
        info = compatible_cluster_info()
        info["worker_protocol"]["server_capabilities"].pop("worker_deregistration_fencing", None)  # type: ignore[index]
        self.info = info

    async def respond(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith(self.path) and self.served < self.failures:
            self.served += 1
            if self.response == "refused":
                raise httpx.ConnectError("connection refused", request=request)
            status, body = self.response
            return httpx.Response(status, json=body)
        if path.endswith("/cluster/info"):
            return httpx.Response(200, json=self.info)
        if path.endswith("/register"):
            return httpx.Response(200, json={"worker_id": "worker-1", "registered": True})
        if path.endswith("/poll"):
            self.polls += 1
            await asyncio.sleep(0.01)
            return httpx.Response(200, json={"task": None})
        if path.endswith("/registrations/worker-1"):
            return httpx.Response(200, json={"worker_id": "worker-1", "outcome": "deregistered",
                                             "recovered_workflow_task_count": 0})
        return httpx.Response(200, json={"worker_id": "worker-1", "acknowledged": True})


def worker_for(runtime: FlakyRuntime) -> tuple[Client, Worker]:
    client = Client("http://runtime.invalid", token="token", retry_policy=TransportRetryPolicy(max_attempts=1))
    client._http = httpx.AsyncClient(base_url="http://runtime.invalid", transport=httpx.MockTransport(runtime.respond))
    return client, Worker(client, task_queue="q", workflows=[], activities=[noop], worker_id="worker-1")


@pytest.fixture
def fast_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("durable_workflow.worker._TRANSIENT_RETRY_BASE_SECONDS", 0.005)
    monkeypatch.setattr("durable_workflow.worker._TRANSIENT_RETRY_MAX_SECONDS", 0.02)


@pytest.mark.usefixtures("fast_retries")
@pytest.mark.parametrize(("path", "response", "failures"), [
    ("/cluster/info", None, 8),
    ("/register", None, 8),
    ("/register", "refused", 8),
    ("/register", (502, None), 8),
    ("/register", (503, {"reason": "backend_unavailable"}), 8),
    # Pollers already ride out failures with their own backoff (1 s, doubling).
    ("/activity-tasks/poll", None, 2),
])
async def test_transient_failures_then_recovery_leave_the_worker_running(
    path: str, response: Any, failures: int, caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = FlakyRuntime(path, failures=failures, response=response)
    client, worker = worker_for(runtime)
    running = asyncio.create_task(worker.run())
    try:
        for _ in range(500):
            if runtime.served == runtime.failures and runtime.polls >= 3:
                break
            await asyncio.sleep(0.01)
        assert not running.done(), running.exception() if running.done() else None
        assert runtime.served == runtime.failures
        assert runtime.polls >= 3, "the worker polls once the runtime recovers"
        assert worker._registered is True
        assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
    finally:
        await worker.stop()
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await client.aclose()


@pytest.mark.usefixtures("fast_retries")
@pytest.mark.parametrize(("path", "response", "expected"), [
    ("/register", (401, {"reason": "unauthorized", "message": "bad token"}), Unauthorized),
    ("/register", (403, {"reason": "forbidden"}), ServerError),
    ("/register", (503, {"reason": "storage_unavailable", "retryable": False}), ServerError),
    ("/cluster/info", (401, {"message": "bad token"}), RuntimeError),
])
async def test_fatal_failures_stop_the_worker_with_a_clear_log(
    path: str, response: Any, expected: type[Exception], caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = FlakyRuntime(path, failures=100, response=response)
    client, worker = worker_for(runtime)
    try:
        with caplog.at_level(logging.ERROR, logger="durable_workflow.worker"), pytest.raises(expected):
            await asyncio.wait_for(worker.run(), timeout=5)
        assert runtime.served == 1, "a fatal failure is not retried"
        assert runtime.polls == 0
        errors = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
        assert any("stopping: registration failed with a non-retryable error" in message for message in errors)
    finally:
        await client.aclose()


@pytest.mark.usefixtures("fast_retries")
async def test_shutdown_interrupts_a_transient_retry() -> None:
    runtime = FlakyRuntime("/cluster/info", failures=10_000)
    client, worker = worker_for(runtime)
    running = asyncio.create_task(worker.run())
    try:
        while runtime.served < 3:
            await asyncio.sleep(0.005)
        worker._stop.set()
        with pytest.raises(RuntimeError, match="unable to read /api/cluster/info"):
            await asyncio.wait_for(running, timeout=2)
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await client.aclose()
