from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from durable_workflow.client import Client
from durable_workflow.errors import ServerError
from durable_workflow.metrics import CLIENT_REQUEST_DURATION_SECONDS, CLIENT_REQUESTS, InMemoryMetrics
from durable_workflow.retry_policy import TransportRetryPolicy

ROUTES = [
    ("/workflows/wf-{id}/signal/signal-{id}", "/workflows/{workflow_id}/signal/{name}"),
    ("/workflows/wf-{id}/runs/run-{id}/history/export", "/workflows/{workflow_id}/runs/{run_id}/history/export"),
    ("/worker/workflow-tasks/task-{id}/complete", "/worker/workflow-tasks/{task_id}/complete"),
    ("/worker/activity-tasks/task-{id}/heartbeat", "/worker/activity-tasks/{task_id}/heartbeat"),
    ("/worker/query-tasks/task-{id}/status", "/worker/query-tasks/{task_id}/status"),
    ("/worker/update-validation-tasks/task-{id}/approve", "/worker/update-validation-tasks/{task_id}/approve"),
    ("/worker/update-validation-tasks/task-{id}/reject", "/worker/update-validation-tasks/{task_id}/reject"),
    ("/worker/registrations/worker-{id}", "/worker/registrations/{worker_id}"),
    ("/worker/sessions/session-{id}/heartbeat", "/worker/sessions/{session_id}/heartbeat"),
    ("/worker/sessions/session-{id}", "/worker/sessions/{session_id}"),
    ("/activities/activity-{id}", "/activities/{activity_id}"),
    ("/workers/worker-{id}", "/workers/{worker_id}"),
    ("/schedules/schedule-{id}/history", "/schedules/{schedule_id}/history"),
    ("/namespaces/namespace-{id}/external-storage", "/namespaces/{name}/external-storage"),
    ("/task-queues/queue-{id}/build-ids/drain", "/task-queues/{task_queue}/build-ids/drain"),
    ("/workflows/wf-{id}/message-streams/stream-{id}/messages", "/workflows/{workflow_id}/message-streams/{name}/messages"),
    ("/workflows/wf-{id}/runs/run-{id}/streams/stream-{id}/items", "/workflows/{workflow_id}/runs/{run_id}/streams/{name}/items"),
    ("/service-endpoints/endpoint-{id}/services/service-{id}/operations/operation-{id}/execute",
     "/service-endpoints/{endpoint_name}/services/{service_name}/operations/{operation_name}/execute"),
    ("/worker/workflow-tasks/task-{id}/local-activities/attempt-{id}/heartbeat",
     "/worker/workflow-tasks/{task_id}/local-activities/{activity_attempt_id}/heartbeat"),
    ("/unrecognized/private-{id}", "unknown"),
    ("/worker/sessions/session-{id}/unrecognized/private-{id}", "unknown"),
    ("/workflows/wf-{id}/unrecognized/private-{id}", "unknown"),
]


@pytest.fixture
async def instrumented_client() -> AsyncIterator[tuple[Client, InMemoryMetrics]]:
    metrics = InMemoryMetrics()
    client = Client(
        "https://runtime.example", token="synthetic-credential", metrics=metrics,
        retry_policy=TransportRetryPolicy(max_attempts=1),
    )
    client._http = httpx.AsyncClient(
        base_url=client.base_url, transport=httpx.MockTransport(lambda _: httpx.Response(200, json={})),
    )
    client._runtime_external_payload_transport_resolved = True
    async with client:
        yield client, metrics


@pytest.mark.parametrize("path,expected", ROUTES)
async def test_request_emissions_have_bounded_routes(
    path: str, expected: str, instrumented_client: tuple[Client, InMemoryMetrics],
) -> None:
    client, metrics = instrumented_client
    for identifier in ("synthetic-a", "synthetic-b"):
        actual = path.replace("{id}", identifier) + "?token=synthetic-query-secret&cursor=" + identifier
        await client._request("POST", actual, json={})
    tags = {"method": "POST", "route": expected, "plane": "control", "status_code": "200", "outcome": "ok"}
    assert metrics.counter_value(CLIENT_REQUESTS, tags) == 2
    assert len(metrics.observations(CLIENT_REQUEST_DURATION_SECONDS, tags)) == 2
    assert len(metrics.counters) == len(metrics.histograms) == 1
    assert "synthetic-" not in repr(metrics.counters)


@pytest.mark.parametrize("kind", ["workflow", "activity", "query", "update-validation"])
async def test_static_poll_route_is_distinct_from_a_task(
    kind: str, instrumented_client: tuple[Client, InMemoryMetrics],
) -> None:
    client, metrics = instrumented_client
    await client._request("POST", f"/worker/{kind}-tasks/poll", worker=True, json={})
    tags = {"method": "POST", "route": f"/worker/{kind}-tasks/poll", "plane": "worker",
            "status_code": "200", "outcome": "ok"}
    assert metrics.counter_value(CLIENT_REQUESTS, tags) == 1


@pytest.mark.parametrize("failure", [False, True], ids=["success", "http-failure"])
async def test_public_worker_apis_do_not_emit_task_or_session_ids(
    failure: bool, instrumented_client: tuple[Client, InMemoryMetrics],
) -> None:
    client, metrics = instrumented_client
    if failure:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(
            lambda _: httpx.Response(503, json={"reason": "unavailable"}),
        ))
    for identifier in ("synthetic-a", "synthetic-b"):
        calls = [
            client.approve_update_validation_task(
                update_validation_task_id=identifier, lease_owner="synthetic-worker", update_validation_attempt=1,
            ),
            client.reject_update_validation_task(
                update_validation_task_id=identifier, lease_owner="synthetic-worker", update_validation_attempt=1,
                message="synthetic-payload-secret", reason="invalid",
            ),
            client.renew_worker_session("synthetic-worker", identifier, 120),
            client.close_worker_session("synthetic-worker", identifier),
        ]
        for call in calls:
            if failure:
                with pytest.raises(ServerError):
                    await call
            else:
                await call
    routes = {dict(tags)["route"] for _, tags in metrics.counters}
    assert routes == {
        "/worker/update-validation-tasks/{task_id}/approve", "/worker/update-validation-tasks/{task_id}/reject",
        "/worker/sessions/{session_id}/heartbeat", "/worker/sessions/{session_id}",
    }
    assert all(count == 2 for count in metrics.counters.values())
    assert "synthetic-" not in repr(metrics.counters)


async def test_transport_failure_uses_the_same_bounded_route(
    instrumented_client: tuple[Client, InMemoryMetrics],
) -> None:
    client, metrics = instrumented_client

    def unavailable(request: httpx.Request) -> Any:
        raise httpx.ConnectError("synthetic-transport-secret", request=request)

    await client._http.aclose()
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(unavailable))
    for identifier in ("synthetic-a", "synthetic-b"):
        polling = client.renew_worker_session("worker", identifier, 120)
        with pytest.raises(httpx.ConnectError):
            await polling
    assert len(metrics.counters) == 1
    assert "synthetic-" not in repr(metrics.counters)
