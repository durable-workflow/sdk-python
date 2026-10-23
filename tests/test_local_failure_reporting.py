from __future__ import annotations

import hashlib
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from durable_workflow import Client, NonRetryableError, Worker, activity, serializer
from durable_workflow.workflow import replay
from tests.failure_reporting_cases import LocalFailureObservation


async def observe_local_failure(non_retryable: bool, limit: int) -> dict[str, object]:
    class OriginalFailure(Exception):
        pass

    error_type = NonRetryableError if non_retryable else OriginalFailure

    @activity.defn(name="tests.local.large-failure")
    async def fail(_non_retryable: bool) -> None:
        raise error_type("🌈" * (3 * 1024 * 1024 // 4))

    requests: list[httpx.Request] = []
    async with Client("http://test", worker_token="worker", failure_report_limit_bytes=limit) as client:
        client._cluster_info = {}
        client._http._transport = httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(200, json={"recorded": True}),
        )
        worker = Worker(
            client, task_queue="queue", worker_id="original-worker",
            workflows=[LocalFailureObservation], activities=[fail],
        )
        worker._renew_local_workflow_lease = AsyncMock()  # type: ignore[method-assign]
        await worker._run_workflow_task({
            "task_id": "original-task", "workflow_task_attempt": 7, "workflow_id": "workflow", "run_id": "run",
            "workflow_type": "tests.local.failure-observation", "history_events": [],
            "arguments": serializer.envelope([]), "payload_codec": "avro",
        })
    complete = next(request for request in requests if request.url.path.endswith("/complete"))
    assert len(complete.content) <= limit
    body = json.loads(complete.content)
    local, terminal = body["commands"]
    assert local["type"] == "record_local_activity"
    assert terminal["type"] == "complete_workflow"
    first = serializer.decode_envelope(terminal["result"])
    assert first["bytes"] <= 62 * 1024
    assert first["marked"] is True
    assert first["type"] == error_type.__name__
    assert first["non_retryable"] is non_retryable
    assert first["digest"] == hashlib.sha256(local["message"].encode()).hexdigest()
    assert local["exception_type"] == error_type.__name__
    assert local["non_retryable"] is non_retryable
    assert body["lease_owner"] == "original-worker"
    assert body["workflow_task_attempt"] == 7
    history = [{"event_type": "ActivityFailed", "payload": {
        "sequence": 1, "activity_type": "tests.local.large-failure", "execution_mode": "local",
        "message": local["message"], "exception_type": local["exception_type"],
        "non_retryable": local["non_retryable"], "attempts": local["attempts"],
    }}]
    cold = replay(
        LocalFailureObservation, history, [], run_id="run",
        local_activity_executor=lambda _command: pytest.fail("cold replay reran the callback"),
    )
    assert len(cold.commands) == 1
    assert serializer.decode_envelope(cold.commands[0].to_server_command("queue")["result"]) == first
    return first


@pytest.mark.parametrize("non_retryable", [False, True])
@pytest.mark.parametrize("limit", [1024 * 1024, 3500])
async def test_local_failure_observation_matches_reported_and_cold_message(non_retryable: bool, limit: int) -> None:
    await observe_local_failure(non_retryable, limit)


def test_final_batch_refuses_to_change_a_workflow_observed_local_message() -> None:
    from durable_workflow import FailureReportTooLarge
    from durable_workflow._failure_reporting import bound_failure_request, normalize_local_failure

    local = normalize_local_failure(
        {"type": "record_local_activity", "activity_type": "original", "outcome": "failed",
         "message": "🌈" * (3 * 1024 * 1024 // 4), "exception_type": "OriginalFailure",
         "non_retryable": False, "attempts": []},
        lease_owner="original-worker", workflow_task_attempt=7,
    )
    observed = local["message"]
    body = {"lease_owner": "original-worker", "workflow_task_attempt": 7, "commands": [local]}
    with pytest.raises(FailureReportTooLarge):
        bound_failure_request("/worker/workflow-tasks/original/complete", body, 2000)
    assert local["message"] == observed
    assert local["exception_type"] == "OriginalFailure"
    assert local["non_retryable"] is False


async def test_unrepresentable_local_failure_type_is_a_reporting_refusal() -> None:
    from durable_workflow.workflow import LocalActivityExecutionAborted, RecordLocalActivity

    original_type = type("OriginalFailure" * 30, (Exception,), {})

    @activity.defn(name="tests.local.large-failure")
    async def fail() -> None:
        raise original_type("small original message")

    async with Client("http://test", worker_token="worker") as client:
        worker = Worker(client, task_queue="queue", activities=[fail])
        worker._renew_local_workflow_lease = AsyncMock()  # type: ignore[method-assign]
        command = RecordLocalActivity("tests.local.large-failure", [])
        with pytest.raises(LocalActivityExecutionAborted, match="failure type cannot cross"):
            await worker._execute_local_activity(
                {"task_id": "original-task", "workflow_task_attempt": 7}, command, payload_codec="avro",
            )
        assert command.outcome is None
