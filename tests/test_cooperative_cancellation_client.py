from __future__ import annotations

import asyncio
import time
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


def pending_delivery_response(**overrides: Any) -> dict[str, Any]:
    return {
        "delivered": False, "task_id": "task/1", "workflow_run_id": "run-1",
        "reason": "cancellation_waiting_for_child", "claim_released": True,
        "request_id": None, "sequence": None, "call_kind": None, "sequence_span": None,
        "operation_sequence": None, "operation_sequence_span": None, **overrides,
    }


def activity_stop_response(**overrides: Any) -> dict[str, Any]:
    return {
        "task_id": "task/1", "activity_attempt_id": "attempt-1", "lease_owner": "worker-1",
        "request_id": "original-request", "acknowledged": True, "duplicate": False,
        "reason": None, "heartbeat_recorded": False, "history_event_id": "original-event", **overrides,
    }


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
@pytest.mark.parametrize("kind,reason", [
    (kind, "cancellation_waiting_for_child") for kind in ("child", "parallel", "selection_handle")
] + [
    (kind, "cancellation_waiting_for_activity") for kind in ("activity", "local_activity", "parallel", "selection_handle")
])
async def test_pending_cancellation_requires_explicit_claim_release(
    client: Client, monkeypatch: pytest.MonkeyPatch, kind: str, reason: str,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    options = {"operation_sequence": 1} if kind == "selection_handle" else {}
    pending = pending_delivery_response(reason=reason)
    with patch.object(client._http, "request", new_callable=AsyncMock, return_value=response(pending)):
        result = await client.deliver_workflow_cancellation(
            task_id="task/1", lease_owner="worker-1", workflow_task_attempt=2,
            request_id="original-request", sequence=3, call_kind=kind, **options,
        )
    assert result == pending


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["child", "activity"])
@pytest.mark.parametrize("change", [
    {"claim_released": False}, {"claim_released": "true"}, {"claim_released": None},
    {"task_id": "other"}, {"reason": "other"}, {"reason": []}, {"request_id": "original-request"},
    {"sequence": 3}, {"call_kind": "child"}, {"sequence_span": 1},
    {"operation_sequence": 1}, {"operation_sequence_span": 1}, {"delivered": 0},
])
async def test_malformed_pending_cancellation_ack_is_rejected(
    client: Client, monkeypatch: pytest.MonkeyPatch, change: dict[str, Any], kind: str,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    with (
        patch.object(client._http, "request", new_callable=AsyncMock,
                     return_value=response(pending_delivery_response(reason="cancellation_waiting_for_" + kind) | change)),
        pytest.raises(ServerError) as error,
    ):
        await client.deliver_workflow_cancellation(
            task_id="task/1", lease_owner="worker-1", workflow_task_attempt=2,
            request_id="original-request", sequence=3, call_kind=kind,
        )
    assert error.value.reason() == "invalid_cooperative_cancellation_delivery"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind,reason", [
    ("timer", "cancellation_waiting_for_child"), ("timer", "cancellation_waiting_for_activity"),
    ("child", "cancellation_waiting_for_activity"), ("activity", "cancellation_waiting_for_child"),
])
async def test_pending_reply_cannot_release_an_unrelated_claim(
    client: Client, monkeypatch: pytest.MonkeyPatch, kind: str, reason: str,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    with (
        patch.object(client._http, "request", new_callable=AsyncMock,
                     return_value=response(pending_delivery_response(reason=reason))),
        pytest.raises(ServerError),
    ):
        await client.deliver_workflow_cancellation(
            task_id="task/1", lease_owner="worker-1", workflow_task_attempt=2,
            request_id="original-request", sequence=3, call_kind=kind,
        )


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
async def test_activity_stop_receipt_uses_original_identity_and_worker_credential(
    client: Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    with patch.object(client._http, "request", new_callable=AsyncMock,
                      side_effect=[response(activity_stop_response()),
                                   response(activity_stop_response(duplicate=True))]) as send:
        original = await client.acknowledge_activity_cancellation(
            task_id="task/1", activity_attempt_id="attempt-1", lease_owner="worker-1", request_id="original-request",
        )
        duplicate = await client.acknowledge_activity_cancellation(
            task_id="task/1", activity_attempt_id="attempt-1", lease_owner="worker-1", request_id="original-request",
        )
    assert original["history_event_id"] == duplicate["history_event_id"] == "original-event"
    assert duplicate["duplicate"] is True
    for call in send.await_args_list:
        assert call.args[:2] == ("POST", "/api/worker/activity-tasks/task%2F1/acknowledge-cancellation")
        assert call.kwargs["headers"]["Authorization"] == "Bearer worker-token"
        assert call.kwargs["headers"]["X-Namespace"] == "ns1"
        assert call.kwargs["headers"]["X-Durable-Workflow-Protocol-Version"] == "1.20"
        assert call.kwargs["json"] == {
            "activity_attempt_id": "attempt-1", "lease_owner": "worker-1", "request_id": "original-request",
        }
        assert call.kwargs["timeout"] == 5.0


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"task_id": "other"}, {"activity_attempt_id": "other"}, {"lease_owner": "other"}, {"request_id": "other"},
    {"acknowledged": 1}, {"acknowledged": False}, {"duplicate": 1}, {"reason": "refused"},
    {"heartbeat_recorded": True}, {"heartbeat_recorded": 0}, {"history_event_id": None}, {"history_event_id": " "},
])
async def test_activity_stop_receipt_rejects_mismatched_or_unproved_response(
    client: Client, monkeypatch: pytest.MonkeyPatch, change: dict[str, Any],
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    with (
        patch.object(client._http, "request", new_callable=AsyncMock,
                     return_value=response(activity_stop_response(**change))),
        pytest.raises(ServerError) as error,
    ):
        await client.acknowledge_activity_cancellation(
            task_id="task/1", activity_attempt_id="attempt-1", lease_owner="worker-1", request_id="original-request",
        )
    assert error.value.reason() == "invalid_activity_cancellation_acknowledgement"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["task_id", "activity_attempt_id", "lease_owner", "request_id"])
@pytest.mark.parametrize("identity", [None, True, "", " ", "x" * 256, "é" * 128])
async def test_activity_stop_receipt_rejects_invalid_identity_before_io(
    client: Client, monkeypatch: pytest.MonkeyPatch, field: str, identity: Any,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    options = {"task_id": "task/1", "activity_attempt_id": "attempt-1", "lease_owner": "worker-1",
               "request_id": "original-request", field: identity}
    with patch.object(client._http, "request", new_callable=AsyncMock) as send, pytest.raises(ValueError):
        await client.acknowledge_activity_cancellation(**options)
    send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [None, "1.19", "2.20", "invalid"])
async def test_activity_stop_receipt_requires_explicit_worker_opt_in(
    client: Client, monkeypatch: pytest.MonkeyPatch, version: str | None,
) -> None:
    if version is None:
        monkeypatch.delenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", raising=False)
    else:
        monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", version)
    with patch.object(client._http, "request", new_callable=AsyncMock) as send, pytest.raises(ValueError):
        await client.acknowledge_activity_cancellation(
            task_id="task/1", activity_attempt_id="attempt-1", lease_owner="worker-1", request_id="original-request",
        )
    send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(("status", "reason"), [(409, "lease_owner_mismatch"), (503, "storage_fenced")])
async def test_activity_stop_receipt_preserves_server_refusal(
    client: Client, monkeypatch: pytest.MonkeyPatch, status: int, reason: str,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    with (
        patch.object(client._http, "request", new_callable=AsyncMock,
                     return_value=response({"reason": reason, "request_admitted": False}, status)) as send,
        pytest.raises(ServerError) as error,
    ):
        await client.acknowledge_activity_cancellation(
            task_id="task/1", activity_attempt_id="attempt-1", lease_owner="worker-1", request_id="original-request",
        )
    assert error.value.reason() == reason
    assert send.await_count == (1 if status == 409 else 3)
    assert all(call == send.await_args_list[0] for call in send.await_args_list)


@pytest.mark.asyncio
async def test_activity_stop_receipt_retries_original_identity_after_response_loss(
    client: Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    with patch.object(client._http, "request", new_callable=AsyncMock,
                      side_effect=[httpx.ReadTimeout("receipt response lost"),
                                   response(activity_stop_response(duplicate=True))]) as send:
        result = await client.acknowledge_activity_cancellation(
            task_id="task/1", activity_attempt_id="attempt-1", lease_owner="worker-1", request_id="original-request",
        )
    assert result["duplicate"] is True
    assert result["history_event_id"] == "original-event"
    assert send.await_count == 2
    assert send.await_args_list[0] == send.await_args_list[1]


@pytest.mark.asyncio
async def test_activity_stop_receipt_transport_has_one_total_budget(
    client: Client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
    stopped = asyncio.Event()

    async def blocked(*args: Any, **kwargs: Any) -> httpx.Response:
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()
        raise AssertionError("blocked transport resumed")

    started = time.monotonic()
    with (
        patch.object(client._http, "request", new_callable=AsyncMock, side_effect=blocked) as send,
        pytest.raises(asyncio.TimeoutError),
    ):
        await client.acknowledge_activity_cancellation(
            task_id="task/1", activity_attempt_id="attempt-1", lease_owner="worker-1", request_id="original-request",
        )
    assert time.monotonic() - started < 6.0
    assert stopped.is_set()
    assert send.await_count == 1


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
