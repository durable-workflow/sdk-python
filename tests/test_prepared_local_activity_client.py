from __future__ import annotations

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow.client import Client, _payload_completion_context
from durable_workflow.errors import ExternalPayloadError, ServerError
from durable_workflow.retry_policy import TransportRetryPolicy


@pytest.fixture(autouse=True)
def candidate_protocol(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")


@pytest.mark.parametrize("operation", ["checkpoint", "checkpoint-group", "prepare", "recover",
                                     "control", "heartbeat", "outcome", "acknowledge-cancellation"])
async def test_operation_keeps_original_claim_and_encoded_backend_identity(operation: str) -> None:
    attempt = "backend/attempt" if operation in {
        "control", "heartbeat", "outcome", "acknowledge-cancellation",
    } else None
    async with Client("http://server", worker_token="worker", namespace="tenant") as client:
        with patch.object(client, "_request", new_callable=AsyncMock, return_value={"opaque": "receipt"}) as send:
            result = await client.prepared_local_activity_operation(task_id="task/one", lease_owner="original",
                workflow_task_attempt=4, operation=operation, body={"checkpoint_id": "group"},
                activity_attempt_id=attempt, timeout_seconds=0.5)
    assert result == {"opaque": "receipt"}
    suffix = "backend%2Fattempt/" if attempt else ""
    send.assert_awaited_once_with("POST", "/worker/workflow-tasks/task%2Fone/local-activities/" + suffix + operation,
        worker=True, json={"lease_owner": "original", "workflow_task_attempt": 4, "checkpoint_id": "group"},
        timeout=0.5)


async def test_actual_transport_uses_worker_credential_namespace_and_candidate_header() -> None:
    reply = httpx.Response(200, json={"active": False}, request=httpx.Request("POST", "http://server"))
    async with Client("http://server", control_token="control", worker_token="worker", namespace="tenant") as client:
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=reply) as send:
            await client.prepared_local_activity_operation(task_id="task/one", lease_owner="original",
                workflow_task_attempt=4, operation="control", activity_attempt_id="backend/attempt")
    args, kwargs = send.await_args
    assert args == ("POST", "/api/worker/workflow-tasks/task%2Fone/local-activities/backend%2Fattempt/control")
    assert kwargs["headers"]["Authorization"] == "Bearer worker"
    assert kwargs["headers"]["X-Namespace"] == "tenant"
    assert kwargs["headers"]["X-Durable-Workflow-Protocol-Version"] == "1.20"


@pytest.mark.parametrize("change", [
    {"task_id": ""}, {"lease_owner": " "}, {"workflow_task_attempt": True}, {"workflow_task_attempt": 0},
    {"operation": "guess"}, {"activity_attempt_id": "unexpected"}, {"body": {"lease_owner": "replacement"}},
    {"body": {"workflow_task_attempt": 5}}, {"body": {"not_finite": float("nan")}},
    {"timeout_seconds": 0}, {"timeout_seconds": float("inf")}, {"timeout_seconds": 5.1},
])
async def test_invalid_claim_or_authority_never_starts_transport(change: dict[str, Any]) -> None:
    arguments = {"task_id": "task", "lease_owner": "original", "workflow_task_attempt": 4,
                 "operation": "prepare", **change}
    async with Client("http://server") as client:
        with patch.object(client, "_request", new_callable=AsyncMock) as send:
            with pytest.raises((ValueError, TypeError)):
                await client.prepared_local_activity_operation(**arguments)
            send.assert_not_awaited()


async def test_default_protocol_refuses_before_io(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION")
    async with Client("http://server") as client:
        with patch.object(client, "_request", new_callable=AsyncMock) as send:
            with pytest.raises(ValueError, match="1.20"):
                await client.prepared_local_activity_operation(task_id="task", lease_owner="original",
                    workflow_task_attempt=4, operation="prepare")
            send.assert_not_awaited()


async def test_one_total_budget_cancels_pending_payload_and_retry_work() -> None:
    cancelled = False

    async def pending(*args: Any, **kwargs: Any) -> None:
        nonlocal cancelled
        try:
            await asyncio.sleep(60)
        finally:
            cancelled = True

    async with Client("http://server") as client:
        with patch.object(client, "_request", side_effect=pending):
            started = time.monotonic()
            with pytest.raises(asyncio.TimeoutError):
                await client.prepared_local_activity_operation(task_id="task", lease_owner="original",
                    workflow_task_attempt=4, operation="prepare", timeout_seconds=0.08)
            assert time.monotonic() - started < 0.3
            assert cancelled


@pytest.mark.parametrize("receipt", [None, [], "unproved"])
async def test_non_object_receipt_is_explicitly_refused(receipt: Any) -> None:
    async with Client("http://server") as client:
        with patch.object(client, "_request", new_callable=AsyncMock, return_value=receipt):
            with pytest.raises(ServerError) as failure:
                await client.prepared_local_activity_operation(task_id="task", lease_owner="original",
                    workflow_task_attempt=4, operation="prepare")
            assert failure.value.reason() == "invalid_prepared_local_activity_receipt"


def test_payload_admission_identity_requires_negotiation_and_original_epoch() -> None:
    body = {"lease_owner": "original", "workflow_task_attempt": 4, "checkpoint_id": "atomic-group"}
    route = "/worker/workflow-tasks/task%2Fone/local-activities/checkpoint-group"
    assert _payload_completion_context(route, body) is None
    assert _payload_completion_context(route, body, allow_prepared=True) == {
        "schema": "durable-workflow.v2.payload-completion-context.v2", "kind": "workflow",
        "task_id": "task/one", "attempt": 4, "lease_owner": "original",
        "operation": "local_activity_group_checkpoint", "checkpoint_id": "atomic-group",
    }
    for invalid in [
        {**body, "lease_owner": ""}, {**body, "workflow_task_attempt": True}, {**body, "checkpoint_id": ""},
    ]:
        assert _payload_completion_context(route, invalid, allow_prepared=True) is None


@pytest.mark.parametrize("prepared_supported,state", [(True, "draining"), (False, "draining"), (True, "fenced")])
async def test_group_draining_uploads_bind_each_payload_slot_and_original_claim(
    prepared_supported: bool, state: str,
) -> None:
    from durable_workflow import serializer
    from tests.test_runtime_external_payload_transport import CompletionPayloadServer, runtime_client

    class PreparedPayloadServer(CompletionPayloadServer):
        def cluster_info(self) -> dict[str, Any]:
            info = super().cluster_info()
            context = info["namespace"]["external_payload_storage"]["transport"]["upload"]["completion_context"]
            if prepared_supported:
                context["prepared_schema"] = "durable-workflow.v2.payload-completion-context.v2"
            return info

    server = PreparedPayloadServer(state=state)
    async with runtime_client(server, retry_policy=TransportRetryPolicy(max_attempts=1)) as client:
        call = client.prepared_local_activity_operation(task_id="task/one", lease_owner="original",
            workflow_task_attempt=4, operation="checkpoint-group", body={"checkpoint_id": "group", "commands": [
                {"type": "start_child_workflow", "input": serializer.envelope("child" * 100)},
                {"type": "prepare_local_activity", "arguments": serializer.envelope("local" * 100)},
            ]})
        if not prepared_supported or state == "fenced":
            with pytest.raises((ServerError, ExternalPayloadError)):
                await call
            assert len(server.upload_requests) == 1
            assert "X-Durable-Workflow-Payload-Completion" not in server.upload_requests[0].headers
            assert server.requests == []
            return
        await call
    assert len(server.upload_requests) == 4
    for index, slot in [(0, ["commands", 0, "input"]), (2, ["commands", 1, "arguments"])]:
        first, bound = server.upload_requests[index:index + 2]
        assert first.content == bound.content
        assert bound.headers["authorization"] == first.headers["authorization"]
        assert json.loads(bound.headers["X-Durable-Workflow-Payload-Completion"]) == {
            "schema": "durable-workflow.v2.payload-completion-context.v2", "kind": "workflow",
            "task_id": "task/one", "attempt": 4, "lease_owner": "original",
            "operation": "local_activity_group_checkpoint", "checkpoint_id": "group", "slot": slot,
        }
