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


# ── Activity outcomes through transients ──────────────────────────────────

def activity_task(lease_seconds: float = 300.0) -> dict[str, Any]:
    from datetime import datetime, timedelta, timezone

    from durable_workflow import serializer

    expires = datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
    return {
        "task_id": "task-1", "activity_attempt_id": "attempt-1", "attempt_number": 1,
        "activity_type": "transient.echo", "workflow_id": "wf-1", "run_id": "run-1",
        "lease_owner": "worker-1", "task_queue": "q",
        "lease_expires_at": expires.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "arguments": serializer.envelope(["value"], codec="avro"), "payload_codec": "avro",
    }


@activity.defn(name="transient.echo")
def echo(value: str) -> str:
    return value


@activity.defn(name="transient.boom")
def boom(value: str) -> str:
    raise ValueError("application failure")


def committed_duplicate(**changes: Any) -> ServerError:
    body = {
        "task_id": "task-1", "activity_attempt_id": "attempt-1", "outcome": "completed", "recorded": False,
        "reason": "stale_attempt", "next_task_id": None, "lease_owner": "worker-1",
        "activity_status": "completed", "attempt_status": "completed", "task_status": "completed",
    }
    return ServerError(409, {**body, **changes})


def outcome_worker(monkeypatch: pytest.MonkeyPatch, *activities: Any) -> tuple[Any, Worker, list[float]]:
    from unittest.mock import AsyncMock

    client = AsyncMock(spec=Client)
    client.namespace = "default"
    client.external_storage = None
    client._cluster_info = {}
    worker = Worker(client, task_queue="q", workflows=[], activities=list(activities) or [echo], worker_id="worker-1")
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr("durable_workflow.worker.asyncio.sleep", sleep)
    monkeypatch.setattr("durable_workflow.worker.random.uniform", lambda low, high: high)
    return client, worker, delays


async def run_activity(worker: Worker, task: dict[str, Any]) -> str:
    from durable_workflow.worker import _current_activity_claim

    claim = _current_activity_claim.set(task)
    try:
        return await worker._run_activity_task(task)
    finally:
        _current_activity_claim.reset(claim)
        if worker._remote_activity_executor is not None:
            worker._remote_activity_executor.shutdown(wait=True)


async def test_completion_is_repeated_through_transients_with_the_shared_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, worker, delays = outcome_worker(monkeypatch)
    client.complete_activity_task.side_effect = [
        ServerError(503, STORAGE_UNAVAILABLE), httpx.ConnectError("refused"), ServerError(502, None),
        {"outcome": "completed", "recorded": True},
    ]
    assert await run_activity(worker, activity_task()) == "completed"
    assert client.complete_activity_task.await_count == 4
    assert delays == [0.25, 0.5, 1.0]
    client.fail_activity_task.assert_not_awaited()  # a transient never becomes a failure report


async def test_a_resend_answered_as_our_committed_completion_is_success(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    client, worker, _ = outcome_worker(monkeypatch)
    client.complete_activity_task.side_effect = [ServerError(503, STORAGE_UNAVAILABLE), committed_duplicate()]
    with caplog.at_level(logging.INFO, logger="durable_workflow.worker"):
        assert await run_activity(worker, activity_task()) == "completed"
    assert "already committed this worker's completion" in caplog.text
    client.fail_activity_task.assert_not_awaited()


@pytest.mark.parametrize("changes", [
    {"lease_owner": "worker-2"},
    {"attempt_status": "expired", "activity_status": "pending"},
    {"task_status": "failed"},
    {"activity_attempt_id": "attempt-0"},
    {"outcome": "failed"},
])
async def test_any_other_stale_attempt_is_a_closed_attempt_not_success(
    monkeypatch: pytest.MonkeyPatch, changes: dict[str, Any],
) -> None:
    client, worker, delays = outcome_worker(monkeypatch)
    client.complete_activity_task.side_effect = [ServerError(503, STORAGE_UNAVAILABLE), committed_duplicate(**changes)]
    assert await run_activity(worker, activity_task()) == "complete_error"
    assert client.complete_activity_task.await_count == 2, "a closed attempt is not resent again"
    client.fail_activity_task.assert_not_awaited()


async def test_completion_retries_stop_and_log_when_the_lease_runs_out(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    client, worker, delays = outcome_worker(monkeypatch)
    client.complete_activity_task.side_effect = ServerError(503, STORAGE_UNAVAILABLE)
    with caplog.at_level(logging.ERROR, logger="durable_workflow.worker"):
        assert await run_activity(worker, activity_task(lease_seconds=0.9)) == "complete_error"
    assert delays == [0.25, 0.5], "retried only while the lease covered the next wait"
    assert client.complete_activity_task.await_count == 3
    assert "completed outcome not delivered after 3 transient failures" in caplog.text
    assert "lease ran out" in caplog.text
    client.fail_activity_task.assert_not_awaited()


async def test_completion_is_not_retried_on_a_protocol_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    client, worker, delays = outcome_worker(monkeypatch)
    client.complete_activity_task.side_effect = ServerError(422, {"reason": "invalid_result"})
    assert await run_activity(worker, activity_task()) == "complete_error"
    assert client.complete_activity_task.await_count == 1
    assert delays == []


async def test_failure_report_is_repeated_through_transients(monkeypatch: pytest.MonkeyPatch) -> None:
    client, worker, delays = outcome_worker(monkeypatch, boom)
    task = {**activity_task(), "activity_type": "transient.boom"}
    client.fail_activity_task.side_effect = [
        ServerError(503, {"reason": "backend_unavailable"}), httpx.ReadTimeout("slow"),
        {"outcome": "failed", "recorded": True},
    ]
    assert await run_activity(worker, task) == "failed"
    assert client.fail_activity_task.await_count == 3
    assert delays == [0.25, 0.5]
    client.complete_activity_task.assert_not_awaited()
