from __future__ import annotations

import httpx
import pytest

from durable_workflow import Client
from durable_workflow.errors import QueryFailed
from durable_workflow.retry_policy import TransportRetryPolicy


@pytest.mark.parametrize("protocol", ["1.20", "1.19"])
@pytest.mark.parametrize("reason", [
    "query_worker_execution_timeout", "query_task_not_claimed", "query_worker_timeout",
])
@pytest.mark.parametrize("prefix", ["", "/api/runtime/v1/namespaces/example"])
async def test_query_exposes_original_canonical_timeout_without_another_admission(
    monkeypatch: pytest.MonkeyPatch, protocol: str, reason: str, prefix: str,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", protocol)
    original = {"reason": reason, "message": "Original query ended.",
                "control_plane": {"operation": "query", "reason": reason}}
    admissions = 0
    discovery_protocols = []

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal admissions
        if request.url.path.endswith("/api/cluster/info"):
            discovery_protocols.append(request.headers["X-Durable-Workflow-Protocol-Version"])
            return httpx.Response(200, json={
                "worker_protocol": {"server_capabilities": {"query_tasks": True}},
            })
        assert request.url.path == prefix + "/api/workflows/original/query/total"
        assert request.headers["X-Durable-Workflow-Control-Plane-Version"] == "2"
        admissions += 1
        if admissions == 1:
            return httpx.Response(504, json=original)
        # The old policy hides the original failure behind a distinct admission.
        return httpx.Response(200, json={"result": "replacement-query"})

    async with Client("https://runtime.example" + prefix, worker_token="worker", control_token="control",
                      retry_policy=TransportRetryPolicy(initial_backoff_seconds=0, jitter=False)) as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(handle))
        with pytest.raises(QueryFailed) as failure:
            await client.query_workflow("original", "total")
    assert admissions == 1
    assert discovery_protocols == [protocol]
    assert failure.value.status == 504 and failure.value.reason == reason
    assert failure.value.body == original


@pytest.mark.parametrize(("method", "path", "status", "body", "headers"), [
    ("POST", "/api/workflows/w/query/q", 504, {"reason": "gateway_timeout"},
     {"X-Durable-Workflow-Control-Plane-Version": "2"}),
    ("POST", "/api/workflows/w/query/q", 503, {"reason": "query_worker_timeout"},
     {"X-Durable-Workflow-Control-Plane-Version": "2"}),
    ("GET", "/api/workflows/w/query/q", 504, {"reason": "query_worker_timeout"},
     {"X-Durable-Workflow-Control-Plane-Version": "2"}),
    ("POST", "/api/workflows/w/signals/q", 504, {"reason": "query_worker_timeout"},
     {"X-Durable-Workflow-Control-Plane-Version": "2"}),
    ("POST", "/api/workflows/w/query/q", 504, {"reason": "query_worker_timeout"}, {}),
    ("POST", "/api/workflows/w/query/q", 504, [],
     {"X-Durable-Workflow-Control-Plane-Version": "2"}),
    ("POST", "/api/workflows/w/query/q", 504, {"reason": []},
     {"X-Durable-Workflow-Control-Plane-Version": "2"}),
])
def test_other_transport_failures_keep_existing_retry_policy(
    method: str, path: str, status: int, body: object, headers: dict[str, str],
) -> None:
    request = httpx.Request(method, "https://runtime.example" + path, headers=headers)
    response = httpx.Response(status, request=request, json=body)
    error = httpx.HTTPStatusError("transport failure", request=request, response=response)
    assert TransportRetryPolicy().should_retry(error, 0) is True
