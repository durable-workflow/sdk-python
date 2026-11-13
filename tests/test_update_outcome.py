"""Read an accepted update's outcome later by update id (durable-workflow/.github#212, Server #1131).

A read is a GET that never admits another update. A pending update raises
``UpdateStillPending`` and is never returned as ``None``; a completed update
returns its decoded result, including ``None``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from durable_workflow import serializer
from durable_workflow.client import Client, WorkflowHandle
from durable_workflow.errors import (
    RuntimeCapabilityUnsupported,
    TypedValueError,
    UpdateFailed,
    UpdateNotFound,
    UpdateRejected,
    UpdateStillPending,
)
from durable_workflow.metrics import CLIENT_REQUESTS, InMemoryMetrics
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.sync import Client as SyncClient
from durable_workflow.sync import SyncWorkflowHandle

CLUSTER = {"version": "3.6.0", "capabilities": {"update_outcome_reads": True}}


def _envelope(value: Any) -> dict[str, str]:
    return serializer.envelope(value, codec="avro")


def _receipt(**fields: Any) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "workflow_id": "order 1", "run_id": "run-9", "update_id": "upd-1", "update_name": "rename",
        "request_id": "rename-1", "update_status": "completed", "result": None, "result_envelope": None,
        "wait_timed_out": False,
    }
    receipt.update(fields)
    return receipt


def _path(request: httpx.Request) -> str:
    """The request path as sent, still percent-encoded."""
    return request.url.raw_path.split(b"?", 1)[0].decode()


class Server:
    """Answers queued (status, body) pairs and records every request."""

    def __init__(self, *responses: tuple[int, Any]) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        status, body = self.responses.pop(0)
        return httpx.Response(status, json=body)

    @property
    def reads(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.url.path != "/api/cluster/info"]


@pytest.fixture
async def connect() -> AsyncIterator[Any]:
    clients: list[Client] = []

    async def make(server: Server, *, metrics: InMemoryMetrics | None = None) -> Client:
        client = Client(
            "http://localhost:8080", token="test-token", namespace="ns1", metrics=metrics,
            retry_policy=TransportRetryPolicy(max_attempts=1),
        )
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(server))
        client._runtime_external_payload_transport_resolved = True
        clients.append(client)
        return client

    yield make
    for client in clients:
        await client.aclose()


async def test_completed_reads_return_the_decoded_result_including_none(connect: Any) -> None:
    server = Server(
        (200, CLUSTER),
        (200, _receipt(result_envelope=_envelope("renamed"))),
        (200, _receipt(update_id="upd-2", result_envelope=_envelope(None))),
    )
    client = await connect(server)

    assert await client.update_outcome("order 1", "upd-1", wait_timeout_seconds=10) == "renamed"
    assert await client.update_outcome("order 1", "upd-2") is None

    # Discovery runs once; each read is a body-less GET on the update, never a POST.
    assert [_path(request) for request in server.requests] == [
        "/api/cluster/info", "/api/workflows/order%201/updates/upd-1", "/api/workflows/order%201/updates/upd-2",
    ]
    assert all(request.method == "GET" and request.content == b"" for request in server.reads)
    assert parse_qs(server.reads[0].url.query.decode()) == {"wait_for": ["completed"], "wait_timeout_seconds": ["10"]}
    assert parse_qs(server.reads[1].url.query.decode()) == {"wait_for": ["completed"]}


async def test_a_result_type_decodes_the_completed_result(connect: Any) -> None:
    server = Server((200, CLUSTER), (200, _receipt(result_envelope=_envelope(7))), (200, _receipt(
        result_envelope=_envelope("seven"),
    )))
    client = await connect(server)

    assert await client.update_outcome("order 1", "upd-1", result_type=int) == 7
    with pytest.raises(TypedValueError):
        await client.update_outcome("order 1", "upd-1", result_type=int)


async def test_a_pending_read_raises_and_points_at_reading_again(connect: Any) -> None:
    pending = _receipt(update_status="accepted", wait_timed_out=True)
    server = Server((200, CLUSTER), (202, pending))
    client = await connect(server)

    with pytest.raises(UpdateStillPending) as excinfo:
        await client.update_outcome("order 1", "upd-1", wait_timeout_seconds=1)

    error = excinfo.value
    assert (error.workflow_id, error.run_id, error.update_name, error.update_id, error.request_id) == (
        "order 1", "run-9", "rename", "upd-1", "rename-1",
    )
    assert error.update_status == "accepted"
    assert error.wait_timed_out is True
    assert error.body is not None and error.body["update_status"] == "accepted"
    assert "update_outcome() again with update id 'upd-1'" in str(error)
    assert "never admits another update" in str(error)


async def test_a_plain_pending_read_is_not_a_none_result(connect: Any) -> None:
    server = Server((200, CLUSTER), (202, _receipt(update_status="accepted")))
    client = await connect(server)

    with pytest.raises(UpdateStillPending) as excinfo:
        await client.update_outcome("order 1", "upd-1", wait_for="accepted")

    assert excinfo.value.wait_timed_out is False
    assert server.reads[0].url.query == b""


async def test_reading_again_after_pending_returns_the_result(connect: Any) -> None:
    server = Server(
        (200, CLUSTER),
        (202, _receipt(update_status="accepted", wait_timed_out=True)),
        (200, _receipt(result_envelope=_envelope({"ok": True}))),
    )
    client = await connect(server)

    with pytest.raises(UpdateStillPending) as excinfo:
        await client.update_outcome("order 1", "upd-1")
    assert await client.update_outcome("order 1", excinfo.value.update_id) == {"ok": True}
    assert len(server.reads) == 2
    assert all(request.method == "GET" for request in server.requests)


async def test_an_accepted_receipt_read_returns_the_receipt_without_waiting(connect: Any) -> None:
    pending = _receipt(update_status="accepted")
    server = Server((200, CLUSTER), (202, pending))
    client = await connect(server)

    assert await client.update_outcome_receipt("order 1", "upd-1") == pending
    assert server.reads[0].method == "GET"
    assert _path(server.reads[0]) == "/api/workflows/order%201/updates/upd-1"
    assert server.reads[0].url.query == b""


async def test_a_completed_receipt_carries_the_decoded_result(connect: Any) -> None:
    server = Server((200, CLUSTER), (200, _receipt(result_envelope=_envelope(None))))
    client = await connect(server)

    receipt = await client.update_outcome_receipt("order 1", "upd-1", wait_for="completed", wait_timeout_seconds=5)

    assert receipt["update_status"] == "completed"
    assert receipt["result"] is None
    assert parse_qs(urlsplit(str(server.reads[0].url)).query) == {
        "wait_for": ["completed"], "wait_timeout_seconds": ["5"],
    }


async def test_a_failed_update_raises_update_failed(connect: Any) -> None:
    body = _receipt(update_status="failed", failure_id="failure-1", failure_message="inventory unavailable")
    server = Server((200, CLUSTER), (422, body))
    client = await connect(server)

    with pytest.raises(UpdateFailed) as excinfo:
        await client.update_outcome("order 1", "upd-1")

    error = excinfo.value
    assert not isinstance(error, UpdateNotFound)
    assert str(error) == "inventory unavailable"
    assert (error.status, error.update_id, error.run_id, error.failure_id) == (422, "upd-1", "run-9", "failure-1")


async def test_an_unknown_or_pruned_update_raises_update_not_found(connect: Any) -> None:
    body = {"reason": "update_not_found", "message": "Update [upd-1] is not retained for workflow [workflow-1].",
            "workflow_id": "workflow-1", "update_id": "upd-1"}
    server = Server((200, CLUSTER), (404, body))
    client = await connect(server)

    # The workflow id contains "workflow"; the read must not be mistaken for WorkflowNotFound.
    with pytest.raises(UpdateNotFound) as excinfo:
        await client.update_outcome("workflow-1", "upd-1")

    error = excinfo.value
    assert isinstance(error, UpdateFailed)
    assert (error.status, error.reason, error.update_id) == (404, "update_not_found", "upd-1")
    assert str(error) == body["message"]


@pytest.mark.parametrize(
    "status,reason",
    [(409, "structural_limit_exceeded"), (404, "unknown_update"), (422, "invalid_arguments")],
)
async def test_a_rejected_update_raises_update_rejected(connect: Any, status: int, reason: str) -> None:
    body = _receipt(update_status="rejected", reason=reason, rejection_reason=reason, accepted=False)
    server = Server((200, CLUSTER), (status, body))
    client = await connect(server)

    with pytest.raises(UpdateRejected) as excinfo:
        await client.update_outcome("order 1", "upd-1")

    assert excinfo.value.body == body
    assert reason in str(excinfo.value)


@pytest.mark.parametrize(
    "cluster",
    [
        {"version": "3.5.0", "capabilities": {}},
        {"version": "3.5.0", "capabilities": {"update_outcome_reads": False}},
        {"version": "2.4.0"},
    ],
)
@pytest.mark.parametrize("method", ["update_outcome", "update_outcome_receipt"])
async def test_a_server_without_the_capability_is_refused_before_any_read(
    connect: Any, cluster: dict[str, Any], method: str,
) -> None:
    server = Server((200, cluster))
    client = await connect(server)

    with pytest.raises(RuntimeCapabilityUnsupported) as excinfo:
        await getattr(client, method)("order 1", "upd-1")

    assert excinfo.value.capability == "capabilities.update_outcome_reads"
    assert excinfo.value.operation == f"Client.{method}"
    assert "update_outcome_reads" in str(excinfo.value)
    # Only discovery ran: nothing was read, and nothing was posted or resubmitted.
    assert [(request.method, request.url.path) for request in server.requests] == [("GET", "/api/cluster/info")]


@pytest.mark.parametrize("wait_for", ["validated", "", "COMPLETED"])
async def test_an_unknown_wait_stage_is_refused_before_any_request(connect: Any, wait_for: str) -> None:
    server = Server()
    client = await connect(server)

    with pytest.raises(ValueError, match="wait_for"):
        await client.update_outcome("order 1", "upd-1", wait_for=wait_for)
    with pytest.raises(ValueError, match="wait_for"):
        await client.update_outcome_receipt("order 1", "upd-1", wait_for=wait_for)
    assert server.requests == []


@pytest.mark.parametrize("update_id", ["", "   "])
async def test_an_empty_update_id_is_refused_before_any_request(connect: Any, update_id: str) -> None:
    server = Server()
    client = await connect(server)

    with pytest.raises(ValueError, match="update id"):
        await client.update_outcome("order 1", update_id)
    assert server.requests == []


async def test_reads_use_a_bounded_metrics_route(connect: Any) -> None:
    metrics = InMemoryMetrics()
    server = Server((200, CLUSTER), (200, _receipt(result_envelope=_envelope(1))))
    client = await connect(server, metrics=metrics)

    await client.update_outcome("order 1", "upd-1")

    tags = {"method": "GET", "route": "/workflows/{workflow_id}/updates/{update_id}", "plane": "control",
            "status_code": "200", "outcome": "ok"}
    assert metrics.counter_value(CLIENT_REQUESTS, tags) == 1


async def test_handle_reads_forward_to_the_client(connect: Any) -> None:
    server = Server(
        (200, CLUSTER),
        (200, _receipt(result_envelope=_envelope("renamed"))),
        (202, _receipt(update_status="accepted")),
    )
    client = await connect(server)
    handle = WorkflowHandle(client, workflow_id="order 1", run_id="run-1")

    assert await handle.update_outcome("upd-1", wait_timeout_seconds=3) == "renamed"
    assert (await handle.update_outcome_receipt("upd-1"))["update_status"] == "accepted"
    assert [_path(request) for request in server.reads] == ["/api/workflows/order%201/updates/upd-1"] * 2


def test_sync_client_and_handle_read_outcomes() -> None:
    server = Server(
        (200, CLUSTER),
        (200, _receipt(result_envelope=_envelope(None))),
        (202, _receipt(update_status="accepted", wait_timed_out=True)),
        (202, _receipt(update_status="accepted")),
    )
    with SyncClient("http://localhost:8080", token="test-token") as client:
        client._async._http = httpx.AsyncClient(
            base_url=client._async.base_url, transport=httpx.MockTransport(server),
        )
        client._async._runtime_external_payload_transport_resolved = True
        assert client.update_outcome("order 1", "upd-1") is None
        handle = SyncWorkflowHandle(WorkflowHandle(client._async, workflow_id="order 1"), client._runner)
        with pytest.raises(UpdateStillPending):
            handle.update_outcome("upd-1", wait_timeout_seconds=1)
        assert client.update_outcome_receipt("order 1", "upd-1")["update_status"] == "accepted"

    assert [request.method for request in server.requests] == ["GET"] * 4
