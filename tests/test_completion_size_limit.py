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
from durable_workflow.errors import DurableWorkflowError, InvalidArgument, ServerError, _raise_for_status
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
    # A new run records its failure-order marker before its first all-group (#307).
    assert [command["type"] for command in oversized["commands"]] == [
        "record_version_marker", *["schedule_activity"] * 4,
    ]
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


# A history-transaction budget refusal (409) is as deterministic as the
# typed 413: the same commands replay into the same refusal (.github#228).
TYPED_409 = {
    "task_id": "task-413",
    "workflow_task_attempt": 3,
    "outcome": "completed",
    "recorded": False,
    "run_id": None,
    "run_status": None,
    "created_task_ids": [],
    "reason": "history_transaction_limit_exceeded",
    "limit_kind": "history_transaction_bytes",
    "current_value": 16_777_300,
    "configured_limit": 16_777_216,
    "message": "Structural limit exceeded: history_transaction_bytes: this workflow task completion would write "
    "16777300 history bytes in one transaction (limit 16777216).",
    "protocol_version": "1.20",
    "server_capabilities": {},
}


def test_typed_409_maps_to_a_history_limit_error_with_the_server_measure() -> None:
    from durable_workflow import WorkflowTaskHistoryLimitExceeded

    with pytest.raises(WorkflowTaskHistoryLimitExceeded) as caught:
        _raise_for_status(409, TYPED_409, context="task_completion", request_bytes=9000)
    error = caught.value
    assert isinstance(error, WorkflowTaskCompletionTooLarge) and isinstance(error, ServerError)
    assert (error.status, error.reason()) == (409, "history_transaction_limit_exceeded")
    assert error.limit_kind == "history_transaction_bytes"
    assert (error.limit, error.size) == (16_777_216, 16_777_300)
    assert error.non_retryable is True
    message = str(error)
    assert message.startswith("Structural limit exceeded: history_transaction_bytes:")
    assert "16777300 history bytes" in message and "limit of 16777216 history bytes" in message
    assert _should_fail_workflow_task_after_completion_error(error) is False


def test_an_untyped_409_budget_refusal_names_its_reason() -> None:
    from durable_workflow import WorkflowTaskHistoryLimitExceeded

    for reason in ("history_transaction_limit_exceeded", "completion_budget_exceeded"):
        with pytest.raises(WorkflowTaskHistoryLimitExceeded) as caught:
            _raise_for_status(409, {"reason": reason, "recorded": False}, context="task_completion")
        assert caught.value.limit_kind == reason
        assert (caught.value.limit, caught.value.size) == (None, None)
        assert str(caught.value).startswith(f"Structural limit exceeded: {reason}:")


@pytest.mark.parametrize("status, payload, context", [
    (409, TYPED_409, ""),
    (409, {"reason": "invalid_commands"}, "task_completion"),
    (409, {**TYPED_409, "recorded": True}, "task_completion"),
    (422, TYPED_409, "task_completion"),
])
def test_other_responses_are_not_history_limit_errors(status: int, payload: Any, context: str) -> None:
    from durable_workflow import WorkflowTaskHistoryLimitExceeded

    with pytest.raises(DurableWorkflowError) as caught:
        _raise_for_status(status, payload, context=context)
    assert not isinstance(caught.value, WorkflowTaskHistoryLimitExceeded)


@pytest.mark.asyncio
async def test_typed_409_fails_the_run_once_without_resending_or_failing_the_task() -> None:
    requests: list[httpx.Request] = []
    client, worker = worker_for([(409, TYPED_409)], requests)
    async with client:
        result = await worker._run_workflow_task(task())

    assert result is None
    assert [request.url.path for request in requests] == [
        "/api/worker/workflow-tasks/task-413/complete",
        "/api/worker/workflow-tasks/task-413/complete",
    ]
    refused, failure = (body(request) for request in requests)
    assert [command["type"] for command in refused["commands"]] == ["record_version_marker", *["schedule_activity"] * 4]
    assert (failure["lease_owner"], failure["workflow_task_attempt"]) == ("worker-413", 3)
    command = failure["commands"][0]
    assert (command["type"], command["non_retryable"]) == ("fail_workflow", True)
    assert command["exception_type"] == "WorkflowTaskHistoryLimitExceeded"
    assert command["message"].startswith("Structural limit exceeded: history_transaction_bytes:")
    assert "16777300 history bytes" in command["message"]
