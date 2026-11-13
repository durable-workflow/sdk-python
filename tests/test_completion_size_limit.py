"""Typed 413 for an oversized workflow-task completion (.github#228).

Server 3.6.0 refuses a workflow-task completion over its aggregate limit with
``413 payload_too_large`` and ``limit_kind: workflow_task_completion_bytes``.
The worker must not resend that completion or report a workflow-task failure
(which would retry the same completion or block replay). It fails the run once
with a ``structural_limit`` failure instead.
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow import Client, Worker, WorkflowTaskCompletionTooLarge, serializer, workflow
from durable_workflow.errors import InvalidArgument, ServerError, _raise_for_status
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.worker import _should_fail_workflow_task_after_completion_error

LIMIT = 4096
TYPED_413 = {
    "message": (
        f"Workflow task completion exceeds the maximum allowed size of {LIMIT} bytes. Each command's "
        "arguments may be up to 256 bytes; offload larger payloads or split the work across workflow tasks."
    ),
    "reason": "payload_too_large",
    "limit": LIMIT,
    "limit_kind": "workflow_task_completion_bytes",
    "protocol_version": "1.0",
    "server_capabilities": {},
}


@workflow.defn(name="completion-fan-out-wf")
class FanOutWorkflow:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        yield [ctx.schedule_activity("act", ["x" * 2048]) for _ in range(4)]


def task() -> dict[str, Any]:
    return {
        "task_id": "task-413",
        "workflow_id": "wf-413",
        "run_id": "run-413",
        "workflow_type": "completion-fan-out-wf",
        "workflow_task_attempt": 3,
        "history_events": [],
        "arguments": serializer.encode([], codec="avro"),
        "payload_codec": "avro",
    }


def worker_for(responses: list[tuple[int, Any]], requests: list[httpx.Request]) -> tuple[Client, Worker]:
    """A real Client over a mocked HTTP transport; completions answer in order."""

    def reply(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/complete") or request.url.path.endswith("/fail"):
            requests.append(request)
            status, body = responses.pop(0) if responses else (200, {"outcome": "completed"})
            return httpx.Response(status, json=body)
        if request.url.path == "/api/cluster/info":
            return httpx.Response(200, json={"worker_protocol": {"server_capabilities": {}}})
        return httpx.Response(404, json={"reason": "not_found"})

    client = Client("https://runtime.example", worker_token="synthetic-worker", namespace="default",
                    retry_policy=TransportRetryPolicy(max_attempts=1))
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(reply))
    worker = Worker(client, task_queue="q", workflows=[FanOutWorkflow], activities=[], worker_id="worker-413")
    return client, worker


def body(request: httpx.Request) -> dict[str, Any]:
    decoded = json.loads(request.content)
    assert isinstance(decoded, dict)
    return decoded


def test_typed_413_maps_to_typed_non_retryable_error_with_limit_and_size() -> None:
    with pytest.raises(WorkflowTaskCompletionTooLarge) as caught:
        _raise_for_status(413, TYPED_413, request_bytes=9000)
    error = caught.value
    assert isinstance(error, ServerError)
    assert (error.status, error.reason(), error.limit, error.size) == (413, "payload_too_large", LIMIT, 9000)
    assert error.limit_kind == "workflow_task_completion_bytes"
    assert error.non_retryable is True
    assert error.server_message == TYPED_413["message"]
    message = str(error)
    assert message.startswith("Structural limit exceeded: workflow_task_completion_bytes:")
    assert "9000 bytes" in message and f"{LIMIT} bytes" in message
    assert "child workflows" in message and "continue-as-new" in message
    assert _should_fail_workflow_task_after_completion_error(error) is False


@pytest.mark.parametrize("payload", [
    {"reason": "payload_too_large", "limit": LIMIT},
    {"reason": "payload_too_large", "limit": LIMIT, "limit_kind": "something_else"},
    {"reason": "other", "limit_kind": "workflow_task_completion_bytes"},
    "Request Entity Too Large",
])
def test_other_413_bodies_stay_generic_server_errors(payload: Any) -> None:
    with pytest.raises(ServerError) as caught:
        _raise_for_status(413, payload, request_bytes=9000)
    assert type(caught.value) is ServerError


@pytest.mark.asyncio
async def test_typed_413_fails_the_run_once_without_resending_the_completion() -> None:
    requests: list[httpx.Request] = []
    client, worker = worker_for([(413, TYPED_413)], requests)
    async with client:
        result = await worker._run_workflow_task(task())

    assert result is None
    assert [request.url.path for request in requests] == [
        "/api/worker/workflow-tasks/task-413/complete",
        "/api/worker/workflow-tasks/task-413/complete",
    ]
    oversized, failure = (body(request) for request in requests)
    assert [command["type"] for command in oversized["commands"]] == ["schedule_activity"] * 4
    assert (failure["lease_owner"], failure["workflow_task_attempt"]) == ("worker-413", 3)
    assert len(failure["commands"]) == 1
    command = failure["commands"][0]
    assert command["type"] == "fail_workflow"
    assert command["non_retryable"] is True
    assert command["exception_type"] == "WorkflowTaskCompletionTooLarge"
    assert command["message"].startswith("Structural limit exceeded: workflow_task_completion_bytes:")
    assert f"is {len(requests[0].content)} bytes" in command["message"]
    assert f"limit of {LIMIT} bytes" in command["message"]
    assert "sticky_cache" not in failure


@pytest.mark.parametrize("status, payload", [
    (409, {"reason": "invalid_commands"}),
    (409, {}),
    (422, {"reason": "validation_failed"}),
    (401, {"reason": "unauthorized"}),
])
@pytest.mark.asyncio
async def test_rejected_structural_limit_completion_never_calls_fail(status: int, payload: Any) -> None:
    requests: list[httpx.Request] = []
    client, worker = worker_for([(413, TYPED_413), (status, payload)], requests)
    async with client:
        result = await worker._run_workflow_task(task())

    assert result is None
    # The refused completion is never resent, and a rejected follow-up is not
    # reported through /fail, which would retry into the same 413 or block
    # replay on the Server.
    assert [request.url.path for request in requests] == [
        "/api/worker/workflow-tasks/task-413/complete",
        "/api/worker/workflow-tasks/task-413/complete",
    ]
    assert body(requests[1])["commands"][0]["type"] == "fail_workflow"


@pytest.mark.asyncio
async def test_rejected_structural_limit_completion_reraises_the_typed_error() -> None:
    requests: list[httpx.Request] = []
    client, worker = worker_for([(413, TYPED_413), (422, {"reason": "validation_failed"})], requests)
    async with client:
        with pytest.raises(WorkflowTaskCompletionTooLarge) as caught:
            await worker._complete_workflow_task_with_retry(
                task_id="task-413", attempt=3, commands=[{"type": "complete_workflow"}],
            )

    assert (caught.value.limit, caught.value.size) == (LIMIT, len(requests[0].content))
    assert isinstance(caught.value.__cause__, InvalidArgument)
    assert [request.url.path for request in requests] == ["/api/worker/workflow-tasks/task-413/complete"] * 2


@pytest.mark.asyncio
async def test_untyped_413_keeps_the_existing_task_failure() -> None:
    requests: list[httpx.Request] = []
    client, worker = worker_for([(413, {"reason": "payload_too_large", "limit": LIMIT})], requests)
    async with client:
        result = await worker._run_workflow_task(task())

    assert result is None
    assert [request.url.path for request in requests] == [
        "/api/worker/workflow-tasks/task-413/complete",
        "/api/worker/workflow-tasks/task-413/fail",
    ]
    failure = body(requests[1])["failure"]
    assert failure["type"] == "ServerError"
    assert failure["message"].startswith("workflow task completion failed after commands were produced")


@pytest.mark.asyncio
async def test_5xx_completion_errors_still_retry_the_same_completion() -> None:
    requests: list[httpx.Request] = []
    client, worker = worker_for([(503, {"reason": "server_busy"}), (200, {"outcome": "completed"})], requests)
    async with client:
        with patch("durable_workflow.worker.asyncio.sleep", new_callable=AsyncMock):
            result = await worker._run_workflow_task(task())

    assert result is not None
    assert [request.url.path for request in requests] == ["/api/worker/workflow-tasks/task-413/complete"] * 2
    assert requests[0].content == requests[1].content
