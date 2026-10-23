from __future__ import annotations

import asyncio
import json
import logging
from unittest.mock import AsyncMock

import httpx
import pytest

import durable_workflow.worker as worker_module
from durable_workflow.client import Client
from durable_workflow.errors import ServerError
from durable_workflow.metrics import CLIENT_REQUEST_DURATION_SECONDS, CLIENT_REQUESTS, InMemoryMetrics
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.worker import Worker


@pytest.mark.parametrize("kind", ["workflow", "activity", "query"])
@pytest.mark.parametrize("error_type", [httpx.ReadTimeout, httpx.ReadError])
async def test_message_less_poll_warning_names_exception_and_route(
    kind: str, error_type: type[httpx.RequestError], monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = AsyncMock(spec=Client)
    client.namespace = "diagnostics"
    worker = Worker(client, task_queue="orders")
    getattr(client, f"poll_{kind}_task").side_effect = error_type("")

    async def stop_after_backoff(_delay: float) -> None:
        worker._stop.set()

    monkeypatch.setattr(worker_module.asyncio, "sleep", stop_after_backoff)
    with caplog.at_level(logging.WARNING, logger="durable_workflow.worker"):
        await getattr(worker, f"_poll_{kind}_tasks")()
    assert error_type.__name__ in caplog.text
    assert f"/worker/{kind}-tasks/poll" in caplog.text


def test_server_summary_keeps_class_status_and_bounded_reason() -> None:
    summary = worker_module._poll_error_summary(ServerError(503, {"reason": "backend_lock_pressure"}))
    assert "ServerError" in summary
    assert "503" in summary
    assert "backend_lock_pressure" in summary
    assert "synthetic-private-message" not in worker_module._poll_error_summary(
        httpx.ReadError("synthetic-private-message")
    )


@pytest.mark.parametrize("bridge", [False, True], ids=["ordinary", "bridge"])
@pytest.mark.parametrize("phase", ["in_flight", "retry_backoff"])
async def test_local_request_cancellation_emits_terminal_class_and_propagates(
    bridge: bool, phase: str,
) -> None:
    entered = asyncio.Event()
    never = asyncio.Event()
    metrics = InMemoryMetrics()
    client = Client(
        "https://runtime.example", metrics=metrics,
        retry_policy=TransportRetryPolicy(max_attempts=3, initial_backoff_seconds=10, jitter=False),
    )
    calls = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        entered.set()
        if phase == "retry_backoff":
            raise httpx.ReadError("", request=request)
        await never.wait()
        return httpx.Response(200, json={})

    await client._http.aclose()
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(respond))
    async with client:
        request = (
            client.send_webhook_bridge_event(
                "synthetic-adapter", action="signal", idempotency_key="event-167", target={"workflow_id": "wf"},
            ) if bridge else client.health()
        )
        pending = asyncio.create_task(request)
        await asyncio.wait_for(entered.wait(), timeout=2)
        pending.cancel("synthetic-private-cancellation")
        # Python 3.11 added propagation of Task.cancel(msg) to the awaiter.
        # Across supported versions, preserve cancellation and its task state.
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert pending.cancelled()
    assert calls == 1
    assert len(metrics.counters) == len(metrics.histograms) == 1
    tags = dict(next(iter(metrics.counters))[1])
    assert tags["outcome"] == "CancelledError"
    assert tags["status_code"] == "none"
    assert metrics.counter_value(CLIENT_REQUESTS, tags) == 1
    assert len(metrics.observations(CLIENT_REQUEST_DURATION_SECONDS, tags)) == 1
    assert "synthetic-private-cancellation" not in repr(metrics.counters)


@pytest.mark.parametrize("failure", ["remote_recovered", "remote_exhausted", "local"])
async def test_protocol_retry_preserves_the_same_worker_request_identity(failure: str) -> None:
    requests: list[httpx.Request] = []
    client = Client(
        "https://runtime.example", namespace="diagnostics", worker_token="synthetic-worker-secret",
        retry_policy=TransportRetryPolicy(max_attempts=2, initial_backoff_seconds=0, jitter=False),
    )

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if failure == "local":
            raise httpx.LocalProtocolError("malformed request", request=request)
        if len(requests) == 1 or failure == "remote_exhausted":
            raise httpx.RemoteProtocolError("response lost", request=request)
        return httpx.Response(200, json={"task": None})

    await client._http.aclose()
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(respond))
    async with client:
        if failure == "remote_recovered":
            assert await client.poll_workflow_task(worker_id="worker-167", task_queue="orders", timeout=1) is None
        else:
            error = httpx.LocalProtocolError if failure == "local" else httpx.RemoteProtocolError
            with pytest.raises(error):
                await client.poll_workflow_task(worker_id="worker-167", task_queue="orders", timeout=1)
    assert len(requests) == (1 if failure == "local" else 2)
    body = json.loads(requests[0].content)
    assert body["poll_request_id"].startswith("wf-poll-")
    assert body["worker_id"] == "worker-167"
    assert body["timeout_seconds"] == 1
    for request in requests:
        assert request.content == requests[0].content
        assert request.url.path == "/api/worker/workflow-tasks/poll"
        assert request.headers["X-Namespace"] == "diagnostics"
        assert request.headers["Authorization"] == "Bearer synthetic-worker-secret"
        assert request.headers["X-Durable-Workflow-Protocol-Version"] == requests[0].headers[
            "X-Durable-Workflow-Protocol-Version"
        ]
