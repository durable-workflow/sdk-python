"""A completion refused as 413 payload_too_large rediscovers a stale payload policy and resends once."""
from __future__ import annotations

import logging
from typing import Any

import httpx
import pytest

from durable_workflow import Worker, activity, serializer
from durable_workflow.client import Client
from durable_workflow.errors import WorkflowTaskCompletionTooLarge
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.worker import _current_activity_claim

from .test_runtime_external_payload_transport import FakeRuntimePayloadServer

MIB = 1024 * 1024
RESULT = "r" * (300 * 1024)
ran: list[str] = []


@activity.defn(name="policy.big")
def big_result() -> str:
    ran.append("ran")
    return RESULT


class LimitServer(FakeRuntimePayloadServer):
    """Refuses requests over its request limit; the limit (and threshold) can change mid-run."""

    def __init__(self) -> None:
        super().__init__(threshold_bytes=2 * MIB, max_payload_bytes=8 * MIB)
        self.request_limit = 2 * MIB
        self.completions: list[int] = []
        self.failures = 0
        self.discovery_failures = 0
        self.discovery_reads = 0

    def lower_limit(self, limit: int, *, threshold: int | None = None) -> None:
        # Native: an absent namespace threshold is the payload ceiling.
        self.request_limit = limit
        self.threshold_bytes = threshold if threshold is not None else limit

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/cluster/info":
            self.discovery_reads += 1
            if self.discovery_failures:
                self.discovery_failures -= 1
                return httpx.Response(503, json={"reason": "storage_unavailable"})
        if path.startswith("/api/worker/") and len(request.content) > self.request_limit:
            if path.endswith("/complete"):
                self.completions.append(len(request.content))
            return httpx.Response(413, json={
                "message": f"Request payload exceeds the maximum allowed size of {self.request_limit} bytes.",
                "reason": "payload_too_large", "limit": self.request_limit,
            })
        if path.endswith("/activity-tasks/task-1/complete"):
            self.completions.append(len(request.content))
            return httpx.Response(200, json={"outcome": "completed", "recorded": True})
        if path.endswith("/activity-tasks/task-1/fail"):
            self.failures += 1
            return httpx.Response(200, json={"outcome": "failed", "recorded": True})
        return super().handler(request)


def task() -> dict[str, Any]:
    return {
        "task_id": "task-1", "activity_attempt_id": "attempt-1", "attempt_number": 1,
        "activity_type": "policy.big", "workflow_id": "wf-1", "run_id": "run-1",
        "lease_owner": "worker-1", "task_queue": "q",
        "arguments": serializer.envelope([], codec="avro"), "payload_codec": "avro",
    }


async def run_activity(server: LimitServer, *, lower: dict[str, Any] | None,
                       outage: int = 0) -> tuple[str, Client]:
    client = Client("https://runtime.example", token="runtime-role-token", namespace="billing",
                    retry_policy=TransportRetryPolicy(max_attempts=1))
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(server.handler))
    # The worker discovered the 2 MiB policy earlier in its run.
    await client.get_cluster_info()
    assert await client._payload_policy_resolved_snapshot() == (2 * MIB, 8 * MIB)
    if lower is not None:
        server.lower_limit(**lower)  # the Server's limit changes mid-run
    server.discovery_failures = outage
    worker = Worker(client, task_queue="q", workflows=[], activities=[big_result], worker_id="worker-1")
    claim = _current_activity_claim.set(task())
    try:
        return await worker._run_activity_task(task()), client
    finally:
        _current_activity_claim.reset(claim)
        if worker._remote_activity_executor is not None:
            worker._remote_activity_executor.shutdown(wait=True)


@pytest.fixture(autouse=True)
def fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    ran.clear()
    monkeypatch.setattr("durable_workflow.worker._TRANSIENT_RETRY_BASE_SECONDS", 0.001)


async def test_a_lowered_limit_is_rediscovered_and_the_result_resent_externalised() -> None:
    server = LimitServer()
    outcome, client = await run_activity(server, lower={"limit": 100 * 1024})
    try:
        assert outcome == "completed"
        assert ran == ["ran"], "the activity ran once; its computed result was kept"
        assert len(server.completions) == 2, "refused once inline, then resent once"
        assert server.completions[0] > 100 * 1024 >= server.completions[1]
        assert server.upload_count == 1, "the resend externalised the result under the new policy"
        assert server.failures == 0
        assert client._payload_policy_snapshot() == (100 * 1024, 8 * MIB)
    finally:
        await client.aclose()


async def test_rediscovery_rides_out_a_discovery_outage() -> None:
    server = LimitServer()
    outcome, client = await run_activity(server, lower={"limit": 100 * 1024}, outage=3)
    try:
        assert outcome == "completed"
        assert server.upload_count == 1 and len(server.completions) == 2
        assert server.discovery_reads == 1 + 4, "three failed rediscovery reads, then one that answered"
    finally:
        await client.aclose()


async def test_a_genuinely_too_large_result_keeps_todays_handling(caplog: pytest.LogCaptureFixture) -> None:
    # The limit drops but the namespace threshold stays at 2 MiB: rediscovery
    # finds nothing to change, so the refusal stands after a single send.
    server = LimitServer()
    with caplog.at_level(logging.WARNING, logger="durable_workflow.worker"):
        outcome, client = await run_activity(server, lower={"limit": 100 * 1024, "threshold": 2 * MIB})
    try:
        assert outcome == "complete_error"
        assert ran == ["ran"]
        assert len(server.completions) == 1, "no resend when the policy did not change"
        assert server.failures == 0, "a size refusal never becomes a failure report"
        assert "payload policy unchanged" in caplog.text
    finally:
        await client.aclose()


def oversized() -> WorkflowTaskCompletionTooLarge:
    return WorkflowTaskCompletionTooLarge(413, {
        "reason": "payload_too_large", "limit_kind": "workflow_task_completion_bytes", "limit": 32 * MIB,
    }, size=40 * MIB)


@pytest.mark.parametrize("changed", [True, False])
async def test_a_workflow_task_completion_refusal_resends_once_after_a_policy_change(
    monkeypatch: pytest.MonkeyPatch, changed: bool,
) -> None:
    from unittest.mock import AsyncMock

    worker = Worker(AsyncMock(spec=Client), task_queue="q", workflows=[], activities=[big_result])
    sends: list[int] = []

    async def send(**_: Any) -> None:
        sends.append(1)
        if len(sends) == 1:
            raise oversized()

    async def refresh(context: str, window: Any = None) -> bool:
        return changed

    failed = AsyncMock()
    monkeypatch.setattr(worker, "_send_workflow_task_completion", send)
    monkeypatch.setattr(worker, "_refresh_payload_policy", refresh)
    monkeypatch.setattr(worker, "_fail_run_for_oversized_completion", failed)
    if changed:
        await worker._complete_workflow_task_with_retry(task_id="wt-1", attempt=1, commands=[])
        assert len(sends) == 2
        failed.assert_not_awaited()
    else:
        with pytest.raises(WorkflowTaskCompletionTooLarge):
            await worker._complete_workflow_task_with_retry(task_id="wt-1", attempt=1, commands=[])
        assert len(sends) == 1
        failed.assert_awaited_once()
