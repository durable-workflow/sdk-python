from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow import Client, ServerError, Worker
from durable_workflow.retry_policy import TransportRetryPolicy
from tests.test_worker import compatible_cluster_info

TOKEN = "a" * 32
FENCING = {
    "schema": "durable-workflow.v2.worker-deregistration.v1",
    "supported": True,
    "receipt_retention_seconds": 600,
    "endpoint": "/worker/registrations/{workerId}/deregister",
}


def completed(worker_id: str = "worker / one", **changes: Any) -> dict[str, Any]:
    return {
        "worker_id": worker_id,
        "registration_token": TOKEN,
        "outcome": "deregistered",
        "recovered_workflow_task_count": 2,
        **changes,
    }


def refusal(**changes: Any) -> ServerError:
    return ServerError(503, {
        "reason": "backend_lock_pressure",
        "operation": "deregister_worker",
        "worker_id": "worker / one",
        "registration_token": TOKEN,
        "outcome": "unknown",
        "retryable": True,
        "retry_after_seconds": 1,
        **changes,
    })


def worker_client(*, fenced: bool = True, token: Any = TOKEN) -> AsyncMock:
    client = AsyncMock(spec=Client)
    info = compatible_cluster_info()
    if fenced:
        info["worker_protocol"]["server_capabilities"]["worker_deregistration_fencing"] = dict(FENCING)  # type: ignore[index]
    client.get_cluster_info.return_value = info
    client.register_worker.return_value = {
        "registered": True, "worker_id": "worker / one", "registration_token": token,
    }
    client.deregister_worker_registration.return_value = completed()
    return client


@pytest.mark.asyncio
async def test_fenced_client_uses_worker_credentials_and_exact_generation() -> None:
    client = Client("http://runtime", namespace="orders", worker_token="worker", control_token="operator")
    response = httpx.Response(200, request=httpx.Request("POST", "http://runtime"), json=completed())
    try:
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=response) as request:
            result = await client.deregister_worker_registration("worker / one", registration_token=TOKEN)
        assert result == completed()
        request.assert_awaited_once()
        assert request.call_args.args[:2] == ("POST", "/api/worker/registrations/worker%20%2F%20one/deregister")
        assert request.call_args.kwargs["json"] == {"registration_token": TOKEN}
        headers = request.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer worker"
        assert headers["X-Namespace"] == "orders"
        assert "X-Durable-Workflow-Control-Plane-Version" not in headers
    finally:
        await client.aclose()


@pytest.mark.parametrize("token", ["", "a" * 31, "a" * 33, "A" * 32, "z" * 32, 1, True])
@pytest.mark.asyncio
async def test_fenced_client_rejects_invalid_token_before_http(token: Any) -> None:
    async with Client("http://runtime") as client:
        with (
            patch.object(client._http, "request", new_callable=AsyncMock) as request,
            pytest.raises(ValueError, match="registration_token"),
        ):
            await client.deregister_worker_registration("worker / one", registration_token=token)
        request.assert_not_awaited()


@pytest.mark.parametrize("changes", [
    {"worker_id": "replacement"}, {"registration_token": "b" * 32},
    {"outcome": "already_deregistered"}, {"outcome": "unknown"},
    {"recovered_workflow_task_count": -1}, {"recovered_workflow_task_count": True},
    {"recovered_workflow_task_count": 1.0},
])
@pytest.mark.asyncio
async def test_fenced_client_refuses_mismatched_or_malformed_receipt(changes: dict[str, Any]) -> None:
    async with Client("http://runtime") as client:
        response = httpx.Response(200, request=httpx.Request("POST", "http://runtime"), json=completed(**changes))
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=response),
            pytest.raises(ServerError) as error,
        ):
            await client.deregister_worker_registration("worker / one", registration_token=TOKEN)
        assert error.value.reason() == "invalid_worker_deregistration_response"


@pytest.mark.parametrize("fenced", [False, True])
@pytest.mark.asyncio
async def test_deregistration_does_not_inherit_generic_503_retries(fenced: bool) -> None:
    async with Client("http://runtime", retry_policy=TransportRetryPolicy(max_attempts=3)) as client:
        response = httpx.Response(503, request=httpx.Request("POST", "http://runtime"), json={"reason": "unrelated"})
        kwargs = {"registration_token": TOKEN} if fenced else {}
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=response) as request,
            pytest.raises(ServerError),
        ):
            await client.deregister_worker_registration("worker / one", **kwargs)
        request.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_keeps_registration_token_and_coalesces_fenced_shutdown() -> None:
    client = worker_client()
    worker = Worker(client, task_queue="orders", worker_id="worker / one")
    await worker._register()
    await asyncio.gather(worker.stop(), worker.stop())
    await worker.stop()
    assert not worker._registered
    client.deregister_worker_registration.assert_awaited_once()
    call = client.deregister_worker_registration.call_args
    assert call.args == ("worker / one",)
    assert call.kwargs["registration_token"] == TOKEN
    assert 0 < call.kwargs["timeout"] <= worker._shutdown_timeout


@pytest.mark.parametrize("token", [None, "", "bad-token", "A" * 32])
@pytest.mark.asyncio
async def test_advertised_fencing_never_falls_back_after_invalid_registration_ack(token: Any) -> None:
    client = worker_client(token=token)
    worker = Worker(client, task_queue="orders", worker_id="worker / one")
    with pytest.raises(RuntimeError, match="registration_token"):
        await worker._register()
    client.deregister_worker_registration.assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_retries_matching_transient_with_same_token_and_one_deadline() -> None:
    client = worker_client()
    client.deregister_worker_registration.side_effect = [refusal(), completed()]
    worker = Worker(client, task_queue="orders", worker_id="worker / one", shutdown_timeout=3)
    await worker._register()
    await worker.stop()
    calls = client.deregister_worker_registration.call_args_list
    assert len(calls) == 2
    assert all(call.args == (worker.worker_id,) and call.kwargs["registration_token"] == TOKEN for call in calls)
    assert 0 < calls[1].kwargs["timeout"] < calls[0].kwargs["timeout"] <= 3
    assert not worker._registered


@pytest.mark.parametrize("changes", [
    {"reason": "unrelated"}, {"operation": "destroy"}, {"worker_id": "replacement"},
    {"registration_token": "b" * 32}, {"outcome": "deferred"}, {"retryable": False},
    {"retry_after_seconds": True}, {"retry_after_seconds": 0}, {"retry_after_seconds": 1.0},
    {"retry_after_seconds": 61}, {"reason": {"unexpected": "object"}}, {"worker_id": None},
])
@pytest.mark.asyncio
async def test_worker_never_retries_incomplete_or_mismatched_transient(changes: dict[str, Any]) -> None:
    client = worker_client()
    failure = refusal(**changes)
    client.deregister_worker_registration.side_effect = failure
    worker = Worker(client, task_queue="orders", worker_id="worker / one")
    await worker._register()
    with pytest.raises(ServerError) as error:
        await worker.stop()
    assert error.value is failure
    assert worker._registered
    client.deregister_worker_registration.assert_awaited_once()


@pytest.mark.parametrize("changes", [
    {"schema": "unknown"}, {"endpoint": "https://other-runtime/deregister"},
    {"receipt_retention_seconds": 0}, {"receipt_retention_seconds": True},
    {"receipt_retention_seconds": 1.0},
])
@pytest.mark.asyncio
async def test_worker_rejects_malformed_advertised_fencing_before_registration(changes: dict[str, Any]) -> None:
    client = worker_client()
    client.get_cluster_info.return_value["worker_protocol"]["server_capabilities"][
        "worker_deregistration_fencing"
    ].update(changes)
    worker = Worker(client, task_queue="orders", worker_id="worker / one")
    with pytest.raises(RuntimeError, match="worker_deregistration_fencing"):
        await worker._register()
    client.register_worker.assert_not_awaited()
    client.deregister_worker_registration.assert_not_awaited()


@pytest.mark.parametrize("changed", ["worker_id", "namespace", "missing_token"])
@pytest.mark.asyncio
async def test_worker_does_not_deregister_after_registration_identity_changes(changed: str) -> None:
    client = worker_client()
    client.namespace = "orders"
    worker = Worker(client, task_queue="orders", worker_id="worker / one")
    await worker._register()
    if changed == "worker_id":
        worker.worker_id = "replacement"
    elif changed == "namespace":
        client.namespace = "another-namespace"
    else:
        worker._registration_token = None
    with pytest.raises(RuntimeError, match="registration.*unconfirmed"):
        await worker.stop()
    assert worker._registered
    client.deregister_worker_registration.assert_not_awaited()


@pytest.mark.asyncio
async def test_original_worker_error_remains_primary_during_fenced_cleanup_failure() -> None:
    client = worker_client()
    cleanup = ServerError(403, {"reason": "worker_registration_forbidden"})
    client.deregister_worker_registration.side_effect = cleanup
    worker = Worker(client, task_queue="orders", worker_id="worker / one")
    original = RuntimeError("original worker failure")
    worker._heartbeat_loop = AsyncMock(side_effect=original)  # type: ignore[method-assign]
    with pytest.raises(RuntimeError) as error:
        await worker.run()
    assert error.value is original
    assert error.value.__cause__ is cleanup
    client.deregister_worker_registration.assert_awaited_once()


@pytest.mark.parametrize("fenced", [False, True])
@pytest.mark.asyncio
async def test_shutdown_bounds_final_http_and_keeps_uncertain_registration(fenced: bool) -> None:
    client = worker_client(fenced=fenced)
    cancelled = asyncio.Event()

    async def stalled(*_: Any, **__: Any) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    client.deregister_worker_registration.side_effect = stalled
    worker = Worker(client, task_queue="orders", worker_id="worker / one", shutdown_timeout=0.01)
    await worker._register()
    # The outer bound diagnoses the original unbounded implementation without
    # leaving its shielded shutdown task alive after the regression fails.
    try:
        stopping = worker.stop()
        with pytest.raises(RuntimeError, match="registration.*unconfirmed"):
            await asyncio.wait_for(stopping, timeout=0.3)
    finally:
        if worker._shutdown_task is not None and not worker._shutdown_task.done():
            worker._shutdown_task.cancel()
            await asyncio.gather(worker._shutdown_task, return_exceptions=True)
    assert cancelled.is_set()
    assert worker._registered
    client.deregister_worker_registration.assert_awaited_once()


@pytest.mark.asyncio
async def test_lost_fenced_response_retries_same_generation_without_legacy_delete() -> None:
    client = worker_client()
    lost = httpx.RemoteProtocolError("lost response", request=httpx.Request("POST", "http://runtime"))
    client.deregister_worker_registration.side_effect = [lost, completed()]
    worker = Worker(client, task_queue="orders", worker_id="worker / one", shutdown_timeout=1)
    await worker._register()
    await worker.stop()
    calls = client.deregister_worker_registration.call_args_list
    assert len(calls) == 2
    assert all(call.kwargs["registration_token"] == TOKEN for call in calls)


@pytest.mark.parametrize("status,reason", [
    (403, "worker_registration_forbidden"), (404, "worker_registration_token_not_found"),
    (409, "worker_registration_lost_authority"), (410, "worker_registration_receipt_expired"),
    (412, "worker_registration_transaction_boundary_unavailable"),
])
@pytest.mark.asyncio
async def test_worker_surfaces_definitive_refusal_without_retry(status: int, reason: str) -> None:
    client = worker_client()
    failure = ServerError(status, {"reason": reason})
    client.deregister_worker_registration.side_effect = failure
    worker = Worker(client, task_queue="orders", worker_id="worker / one")
    await worker._register()
    with pytest.raises(ServerError) as error:
        await worker.stop()
    assert error.value is failure
    assert worker._registered
    client.deregister_worker_registration.assert_awaited_once()
