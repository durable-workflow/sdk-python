"""Atomic signal-with-start intake (durable-workflow/.github#222, Server #1133).

One request starts the workflow or reuses its open run and records one signal
in the same Server transaction. Every refusal is atomic and typed.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow import SignalFailed, SignalWithStartFailed, serializer
from durable_workflow.client import Client
from durable_workflow.errors import InvalidArgument, RuntimeCapabilityUnsupported
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.sync import Client as SyncClient

CLUSTER = {"version": "3.15.0", "capabilities": {"signal_with_start": True}}


def _receipt(**fields: Any) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "workflow_id": "order 1", "run_id": "run-1", "signal_name": "changed", "command_id": "cmd-2",
        "command_sequence": 2, "command_status": "accepted", "outcome": "signal_received",
        "start_command_id": "cmd-1", "start_command_sequence": 1, "start_outcome": "started_new",
        "start_command_status": "accepted", "intake_group_id": "group-1", "request_id": "order-1:rev-1",
        "retained": False, "intent_verified": True,
        "control_plane": {"operation": "signal_with_start"},
    }
    receipt.update(fields)
    return receipt


def _refusal(reason: str, **fields: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "workflow_id": "order 1", "signal_name": "changed", "command_status": "rejected", "reason": reason,
        "outcome": f"rejected_{reason}", "message": f"refused: {reason}",
        "control_plane": {"operation": "signal_with_start"},
    }
    body.update(fields)
    return body


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
    def intakes(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.url.path != "/api/cluster/info"]


@pytest.fixture
async def connect() -> AsyncIterator[Any]:
    clients: list[Client] = []

    async def make(server: Server) -> Client:
        client = Client(
            "http://localhost:8080", token="test-token", namespace="ns1",
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


async def test_one_request_carries_the_start_and_the_signal(connect: Any) -> None:
    server = Server((200, CLUSTER), (201, _receipt()))
    client = await connect(server)

    receipt = await client.signal_with_start(
        "order 1", "changed", workflow_type="orders.entity", task_queue="orders",
        input=["order 1"], signal_args=[1], request_id="order-1:rev-1",
        closed_run_policy="start_new_run", memo={"tenant": "a"}, business_key="order-1",
    )

    assert receipt["start_outcome"] == "started_new"
    assert receipt["command_id"] == "cmd-2"
    (intake,) = server.intakes
    assert intake.method == "POST"
    assert intake.url.raw_path.decode() == "/api/workflows/order%201/signal-with-start/changed"
    body = json.loads(intake.content)
    assert body["workflow_type"] == "orders.entity"
    assert body["task_queue"] == "orders"
    assert body["request_id"] == "order-1:rev-1"
    assert body["closed_run_policy"] == "start_new_run"
    assert body["memo"] == {"tenant": "a"}
    assert body["business_key"] == "order-1"
    assert "duplicate_policy" not in body
    assert serializer.decode_envelope(body["input"]) == ["order 1"]
    assert serializer.decode_envelope(body["signal_input"]) == [1]


async def test_a_generated_key_is_sent_once_and_returned(connect: Any) -> None:
    server = Server((200, CLUSTER), (202, _receipt(request_id=None, start_outcome="returned_existing_active")))
    client = await connect(server)

    receipt = await client.signal_with_start("order 1", "changed", workflow_type="orders.entity", task_queue="orders")

    body = json.loads(server.intakes[0].content)
    assert body["request_id"].startswith("sdk-python-signal-with-start-")
    assert "signal_input" not in body
    assert receipt["start_outcome"] == "returned_existing_active"


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (409, "run_not_active"),
        (409, "instance_already_started"),
        (409, "workflow_type_conflict"),
        (409, "task_queue_conflict"),
        (409, "workflow_id_reserved_in_namespace"),
        (404, "unknown_signal"),
        (422, "invalid_signal_arguments"),
    ],
)
async def test_every_refusal_is_typed_and_atomic(connect: Any, status: int, reason: str) -> None:
    server = Server((200, CLUSTER), (status, _refusal(reason, run_id="run-1")))
    client = await connect(server)

    with pytest.raises(SignalWithStartFailed) as excinfo:
        await client.signal_with_start(
            "order 1", "changed", workflow_type="orders.entity", task_queue="orders", request_id="k-1",
        )

    assert isinstance(excinfo.value, SignalFailed)
    assert (excinfo.value.reason, excinfo.value.status) == (reason, status)
    assert excinfo.value.request_id == "k-1"
    assert len(server.intakes) == 1


async def test_a_key_conflict_carries_the_retained_command(connect: Any) -> None:
    body = _refusal(
        "request_id_conflict", retained=True, intent_verified=True, retained_command_id="cmd-2",
        retained_outcome="signal_received", conflict=["arguments"],
    )
    server = Server((200, CLUSTER), (409, body))
    client = await connect(server)

    with pytest.raises(SignalWithStartFailed) as excinfo:
        await client.signal_with_start(
            "order 1", "changed", workflow_type="orders.entity", task_queue="orders", request_id="k-1",
        )

    assert excinfo.value.reason == "request_id_conflict"
    assert excinfo.value.details["retained_command_id"] == "cmd-2"
    assert excinfo.value.details["conflict"] == ["arguments"]


async def test_a_validation_failure_stays_invalid_argument(connect: Any) -> None:
    body = {
        "reason": "validation_failed", "message": "bad", "errors": {"closed_run_policy": ["bad"]},
        "control_plane": {"operation": "signal_with_start"},
    }
    server = Server((200, CLUSTER), (422, body))
    client = await connect(server)

    with pytest.raises(InvalidArgument):
        await client.signal_with_start("order 1", "changed", workflow_type="orders.entity", task_queue="orders")


@pytest.mark.parametrize(
    "cluster",
    [
        {"version": "3.14.0", "capabilities": {}},
        {"version": "3.14.0", "capabilities": {"signal_with_start": False}},
        {"version": "2.11.8"},
    ],
)
async def test_a_runtime_without_the_capability_is_refused_before_any_write(
    connect: Any, cluster: dict[str, Any],
) -> None:
    server = Server((200, cluster))
    client = await connect(server)

    with pytest.raises(RuntimeCapabilityUnsupported) as excinfo:
        await client.signal_with_start("order 1", "changed", workflow_type="orders.entity", task_queue="orders")

    assert excinfo.value.capability == "capabilities.signal_with_start"
    assert [(request.method, request.url.path) for request in server.requests] == [("GET", "/api/cluster/info")]


async def test_an_unknown_closed_run_policy_is_refused_locally(connect: Any) -> None:
    server = Server()
    client = await connect(server)

    with pytest.raises(ValueError, match="closed_run_policy"):
        await client.signal_with_start(
            "order 1", "changed", workflow_type="orders.entity", task_queue="orders", closed_run_policy="reopen",
        )

    assert server.requests == []


def test_the_sync_client_forwards_every_option() -> None:
    receipt = _receipt()
    client = SyncClient("http://localhost:8080", token="test-token", namespace="ns1")
    with client, patch.object(client._async, "signal_with_start", new_callable=AsyncMock, return_value=receipt) as call:
        answer = client.signal_with_start(
            "order 1", "changed", workflow_type="orders.entity", task_queue="orders",
            signal_args=[2], request_id="sync-key", duplicate_policy="fail", closed_run_policy="reject",
        )

    assert answer == receipt
    call.assert_awaited_once()
    assert call.call_args.args == ("order 1", "changed")
    kwargs = call.call_args.kwargs
    assert (kwargs["signal_args"], kwargs["request_id"], kwargs["duplicate_policy"], kwargs["closed_run_policy"]) == (
        [2], "sync-key", "fail", "reject",
    )
    assert (kwargs["execution_timeout_seconds"], kwargs["run_timeout_seconds"]) == (3600, 600)
