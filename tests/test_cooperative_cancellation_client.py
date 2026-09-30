from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio

from durable_workflow.client import Client, WorkflowHandle
from durable_workflow.errors import RuntimeCapabilityUnsupported, RuntimeDiscoveryUnavailable, ServerError
from durable_workflow.retry_policy import TransportRetryPolicy


def request_response(*, duplicate: bool = False, run_id: str = "run-1") -> dict[str, Any]:
    return {
        "accepted": True, "duplicate": duplicate, "workflow_id": "wf/1", "run_id": run_id,
        "cancellation_request": {
            "request_id": "original-request", "requested_at": "2026-09-30T12:00:00Z",
            "cleanup_deadline_at": "2026-09-30T12:10:00Z", "history_refresh_page_token": "opaque-first-page",
            "delivery_sequence": None, "delivered_at": None,
        },
    }


def delivery_response(**overrides: Any) -> dict[str, Any]:
    return {
        "delivered": True, "task_id": "task/1", "workflow_run_id": "run-1",
        "request_id": "original-request", "sequence": 3, "call_kind": "activity", "sequence_span": 1,
        "operation_sequence": None, "operation_sequence_span": 1, "reason": None, **overrides,
    }


def response(body: Any, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=body, request=httpx.Request("POST", "http://test"))


@pytest_asyncio.fixture
async def client() -> AsyncIterator[Client]:
    async with Client(
        "http://localhost:8080", control_token="control-token", worker_token="worker-token", namespace="ns1",
        retry_policy=TransportRetryPolicy(initial_backoff_seconds=0, jitter=False),
    ) as value:
        value._cluster_info = {"worker_protocol": {
            "version": "1.20", "server_capabilities": {"cooperative_cancellation": True},
        }}
        yield value


@pytest.mark.asyncio
@pytest.mark.parametrize("run_id", [None, "run/1"])
async def test_request_uses_control_credential_and_exact_run_route(client: Client, run_id: str | None) -> None:
    expected = request_response(run_id=run_id or "run-1")
    with patch.object(client._http, "request", new_callable=AsyncMock, return_value=response(expected, 202)) as send:
        result = await client.request_workflow_cancellation(
            "wf/1", run_id=run_id, reason="stop", cleanup_timeout_seconds=30,
        )
    suffix = "/runs/run%2F1" if run_id is not None else ""
    assert send.call_args.args[:2] == ("POST", f"/api/workflows/wf%2F1{suffix}/request-cancellation")
    assert send.call_args.kwargs["headers"]["Authorization"] == "Bearer control-token"
    assert send.call_args.kwargs["headers"]["X-Namespace"] == "ns1"
    assert send.call_args.kwargs["headers"]["X-Durable-Workflow-Control-Plane-Version"] == "2"
    assert send.call_args.kwargs["json"] == {"reason": "stop", "cleanup_timeout_seconds": 30}
    assert result == expected


@pytest.mark.asyncio
async def test_duplicate_returns_original_server_identity_and_deadline(client: Client) -> None:
    first = request_response()
    duplicate = request_response(duplicate=True)
    with patch.object(client._http, "request", new_callable=AsyncMock,
                      side_effect=[response(first, 202), response(duplicate)]) as send:
        accepted = await client.request_workflow_cancellation("wf/1")
        repeated = await client.request_workflow_cancellation("wf/1", cleanup_timeout_seconds=3600)
    assert accepted["cancellation_request"] == repeated["cancellation_request"]
    assert repeated["duplicate"] is True
    assert send.await_args_list[0].kwargs["json"] == {}
    assert send.await_args_list[1].kwargs["json"] == {"cleanup_timeout_seconds": 3600}


@pytest.mark.asyncio
async def test_run_bound_handle_preserves_selection(client: Client) -> None:
    with patch.object(client, "request_workflow_cancellation", new_callable=AsyncMock,
                      return_value=request_response()) as send:
        result = await WorkflowHandle(client, "wf/1", "run-1").request_cancellation(
            reason="stop", cleanup_timeout_seconds=20,
        )
    send.assert_awaited_once_with("wf/1", run_id="run-1", reason="stop", cleanup_timeout_seconds=20)
    assert result["cancellation_request"]["request_id"] == "original-request"


@pytest.mark.asyncio
@pytest.mark.parametrize(("info", "error"), [
    ({"worker_protocol": {"server_capabilities": {"cooperative_cancellation": False}}}, RuntimeCapabilityUnsupported),
    ({}, RuntimeDiscoveryUnavailable),
    ({"worker_protocol": {"version": "1.20", "server_capabilities": {"cooperative_cancellation": 1}}},
     RuntimeDiscoveryUnavailable),
])
async def test_unsupported_or_missing_discovery_sends_no_request(client: Client, info: dict, error: type) -> None:
    client._cluster_info = info
    with patch.object(client._http, "request", new_callable=AsyncMock) as send, pytest.raises(error):
        await client.request_workflow_cancellation("wf/1")
    send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, True, "1.19", "2.20", "one", "1.２０", "1." + "2" * 5000])
async def test_capability_without_compatible_protocol_is_not_admitted(client: Client, version: Any) -> None:
    client._cluster_info["worker_protocol"]["version"] = version
    with (
        patch.object(client._http, "request", new_callable=AsyncMock) as send,
        pytest.raises(RuntimeDiscoveryUnavailable),
    ):
        await client.request_workflow_cancellation("wf/1")
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_request_discovers_before_mutation(client: Client) -> None:
    info = client._cluster_info
    client._cluster_info = None
    with patch.object(client._http, "request", new_callable=AsyncMock,
                      side_effect=[response(info), response(request_response(), 202)]) as send:
        await client.request_workflow_cancellation("wf/1")
    assert send.await_args_list[0].args[:2] == ("GET", "/api/cluster/info")
    assert send.await_args_list[0].kwargs["headers"]["Authorization"] == "Bearer worker-token"
    assert send.await_args_list[1].kwargs["headers"]["Authorization"] == "Bearer control-token"


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [True, 0, -1, 3601, 1.5])
async def test_invalid_cleanup_timeout_is_rejected_before_io(client: Client, timeout: Any) -> None:
    with patch.object(client._http, "request", new_callable=AsyncMock) as send, pytest.raises(ValueError):
        await client.request_workflow_cancellation("wf/1", cleanup_timeout_seconds=timeout)
    send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"workflow_id": "other"}, {"run_id": "other"}, {"accepted": 1}, {"duplicate": None},
    {"cancellation_request": None},
    {"cancellation_request": {"request_id": "original-request"}},
    {"cancellation_request": {
        **request_response()["cancellation_request"], "cleanup_deadline_at": "2026-09-30T11:00:00Z",
    }},
])
async def test_malformed_or_mismatched_request_ack_is_rejected(client: Client, change: dict) -> None:
    with (
        patch.object(client._http, "request", new_callable=AsyncMock,
                     return_value=response({**request_response(), **change}, 202)),
        pytest.raises(ServerError) as error,
    ):
        await client.request_workflow_cancellation("wf/1", run_id="run-1")
    assert error.value.reason() == "invalid_cooperative_cancellation_response"


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", [False, True])
async def test_delivery_sends_owner_attempt_and_authored_boundary(
    client: Client, monkeypatch: pytest.MonkeyPatch, selection: bool,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    options: dict[str, Any] = {"call_kind": "activity"}
    if selection:
        options.update(call_kind="selection_handle", operation_sequence=1, operation_sequence_span=2)
    with patch.object(client._http, "request", new_callable=AsyncMock,
                      return_value=response(delivery_response(**options))) as send:
        result = await client.deliver_workflow_cancellation(
            task_id="task/1", lease_owner="worker-1", workflow_task_attempt=2,
            request_id="original-request", sequence=3, **options,
        )
    assert result["delivered"] is True
    assert send.call_args.args[:2] == ("POST", "/api/worker/workflow-tasks/task%2F1/deliver-cancellation")
    assert send.call_args.kwargs["headers"]["Authorization"] == "Bearer worker-token"
    assert send.call_args.kwargs["headers"]["X-Durable-Workflow-Protocol-Version"] == "1.20"
    assert send.call_args.kwargs["json"] == {
        "lease_owner": "worker-1", "workflow_task_attempt": 2, "request_id": "original-request",
        "sequence": 3, "call_kind": "activity", "sequence_span": 1, **options,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["1.19", "2.20", "malformed"])
async def test_delivery_requires_explicit_compatible_protocol(
    client: Client, monkeypatch: pytest.MonkeyPatch, version: str,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", version)
    with patch.object(client._http, "request", new_callable=AsyncMock) as send, pytest.raises(ValueError):
        await client.deliver_workflow_cancellation(
            task_id="task/1", lease_owner="worker-1", workflow_task_attempt=2,
            request_id="original-request", sequence=3, call_kind="activity",
        )
    send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"workflow_task_attempt": True}, {"sequence": True}, {"call_kind": "made_up"},
    {"sequence_span": 2}, {"call_kind": "selection_handle", "operation_sequence": 3},
    {"operation_sequence": 1}, {"lease_owner": ""},
])
async def test_invalid_delivery_is_rejected_before_io(
    client: Client, monkeypatch: pytest.MonkeyPatch, change: dict,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    options = {"task_id": "task/1", "lease_owner": "worker-1", "workflow_task_attempt": 2,
               "request_id": "original-request", "sequence": 3, "call_kind": "activity", **change}
    with patch.object(client._http, "request", new_callable=AsyncMock) as send, pytest.raises(ValueError):
        await client.deliver_workflow_cancellation(**options)
    send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"task_id": "other"}, {"request_id": "other"}, {"sequence": 4}, {"sequence": True},
    {"sequence_span": True}, {"operation_sequence_span": True}, {"call_kind": "timer"}, {"delivered": 1},
])
async def test_delivery_ack_must_match_exact_canonical_boundary(
    client: Client, monkeypatch: pytest.MonkeyPatch, change: dict,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    with patch.object(client._http, "request", new_callable=AsyncMock,
                      return_value=response(delivery_response(**change))), pytest.raises(ServerError) as error:
        await client.deliver_workflow_cancellation(
            task_id="task/1", lease_owner="worker-1", workflow_task_attempt=2,
            request_id="original-request", sequence=3, call_kind="activity",
        )
    assert error.value.reason() == "invalid_cooperative_cancellation_delivery"


@pytest.mark.asyncio
async def test_delivery_retries_identical_request_after_acknowledgment_loss(
    client: Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    with patch.object(client._http, "request", new_callable=AsyncMock,
                      side_effect=[httpx.ReadTimeout("acknowledgment lost"), response(delivery_response())]) as send:
        await client.deliver_workflow_cancellation(
            task_id="task/1", lease_owner="worker-1", workflow_task_attempt=2,
            request_id="original-request", sequence=3, call_kind="activity",
        )
    assert send.await_count == 2
    assert send.await_args_list[0] == send.await_args_list[1]


@pytest.mark.asyncio
async def test_active_claim_refusal_does_not_fall_back_to_immediate_cancel(client: Client) -> None:
    reason = "active_claim_cancellation_not_supported"
    with (
        patch.object(client._http, "request", new_callable=AsyncMock,
                     return_value=response({"reason": reason}, 409)) as send,
        pytest.raises(ServerError) as error,
    ):
        await client.request_workflow_cancellation("wf/1")
    assert error.value.reason() == reason
    assert send.await_count == 1
    assert send.call_args.args[1].endswith("/request-cancellation")


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["lease_owner_mismatch", "workflow_task_attempt_mismatch", "lease_expired"])
async def test_delivery_refusal_preserves_the_server_lease_reason(
    client: Client, monkeypatch: pytest.MonkeyPatch, reason: str,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    with (
        patch.object(client._http, "request", new_callable=AsyncMock,
                     return_value=response({"delivered": False, "reason": reason}, 409)) as send,
        pytest.raises(ServerError) as error,
    ):
        await client.deliver_workflow_cancellation(
            task_id="task/1", lease_owner="worker-1", workflow_task_attempt=2,
            request_id="original-request", sequence=3, call_kind="activity",
        )
    assert error.value.reason() == reason
    assert send.await_count == 1
