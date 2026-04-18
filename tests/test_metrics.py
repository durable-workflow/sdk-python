from __future__ import annotations

import json
from unittest.mock import AsyncMock

import httpx
import pytest

from durable_workflow import activity
from durable_workflow.client import Client
from durable_workflow.errors import WorkflowNotFound
from durable_workflow.metrics import (
    CLIENT_REQUEST_DURATION_SECONDS,
    CLIENT_REQUESTS,
    WORKER_POLL_DURATION_SECONDS,
    WORKER_POLLS,
    WORKER_TASK_DURATION_SECONDS,
    WORKER_TASKS,
    InMemoryMetrics,
)
from durable_workflow.worker import Worker


def _mock_response(status: int = 200, json_data: dict | None = None) -> httpx.Response:
    return httpx.Response(
        status_code=status,
        content=json.dumps(json_data or {}).encode(),
        headers={"content-type": "application/json"},
        request=httpx.Request("GET", "http://test"),
    )


@activity.defn(name="metrics-act")
def metrics_activity(value: str) -> str:
    return f"ok-{value}"


@pytest.mark.asyncio
async def test_client_records_successful_request_metrics() -> None:
    metrics = InMemoryMetrics()
    client = Client("http://localhost:8080", metrics=metrics)
    try:
        client._http.request = AsyncMock(return_value=_mock_response(200, {"status": "serving"}))  # type: ignore[method-assign]

        assert await client.health() == {"status": "serving"}

        tags = {
            "method": "GET",
            "route": "/health",
            "plane": "control",
            "status_code": "200",
            "outcome": "ok",
        }
        assert metrics.counter_value(CLIENT_REQUESTS, tags) == 1
        assert len(metrics.observations(CLIENT_REQUEST_DURATION_SECONDS, tags)) == 1
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_client_records_normalized_error_route_metrics() -> None:
    metrics = InMemoryMetrics()
    client = Client("http://localhost:8080", metrics=metrics)
    try:
        client._http.request = AsyncMock(  # type: ignore[method-assign]
            return_value=_mock_response(404, {"reason": "workflow_not_found", "message": "missing"})
        )

        with pytest.raises(WorkflowNotFound):
            await client.signal_workflow("wf-123", "approval.received")

        tags = {
            "method": "POST",
            "route": "/workflows/{workflow_id}/signal/{name}",
            "plane": "control",
            "status_code": "404",
            "outcome": "http_error",
        }
        assert metrics.counter_value(CLIENT_REQUESTS, tags) == 1
        assert len(metrics.observations(CLIENT_REQUEST_DURATION_SECONDS, tags)) == 1
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_worker_records_activity_task_metrics() -> None:
    metrics = InMemoryMetrics()
    mock_client = AsyncMock(spec=Client)
    mock_client.complete_activity_task = AsyncMock(return_value={"outcome": "completed"})
    worker = Worker(mock_client, task_queue="q1", activities=[metrics_activity], metrics=metrics)
    task = {
        "task_id": "at1",
        "activity_attempt_id": "aa1",
        "activity_type": "metrics-act",
        "arguments": '["value"]',
        "payload_codec": "json",
    }

    await worker._act_semaphore.acquire()
    await worker._dispatch_activity_task(task)

    tags = {"task_kind": "activity", "task_queue": "q1", "outcome": "completed"}
    assert metrics.counter_value(WORKER_TASKS, tags) == 1
    assert len(metrics.observations(WORKER_TASK_DURATION_SECONDS, tags)) == 1


@pytest.mark.asyncio
async def test_worker_poll_loop_records_empty_poll_metrics() -> None:
    metrics = InMemoryMetrics()
    mock_client = AsyncMock(spec=Client)
    worker = Worker(mock_client, task_queue="q1", metrics=metrics)

    async def stop_after_poll(**_kwargs: object) -> None:
        worker._stop.set()
        return None

    mock_client.poll_workflow_task = AsyncMock(side_effect=stop_after_poll)

    await worker._poll_workflow_tasks()

    tags = {"task_kind": "workflow", "task_queue": "q1", "outcome": "empty"}
    assert metrics.counter_value(WORKER_POLLS, tags) == 1
    assert len(metrics.observations(WORKER_POLL_DURATION_SECONDS, tags)) == 1
