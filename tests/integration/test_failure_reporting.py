"""Deliver oversized failures through an unmodified Server and durable replay."""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any

import httpx
import pytest

from durable_workflow import ActivityFailed, Client, workflow
from durable_workflow.workflow import replay

from .manual_worker import MANUAL_WORKER_CAPABILITY_MANIFEST


@workflow.defn(name="tests.bounded-failure-consumer")
class FailureConsumer:
    def run(self, ctx: Any) -> Any:
        try:
            yield ctx.schedule_activity("tests.bounded-failure", [], retry_policy={"max_attempts": 1})
        except ActivityFailed as error:
            return {
                "type": error.exception_type, "class": error.exception_class,
                "message": str(error), "non_retryable": error.non_retryable, "code": error.code,
                "diagnostics": (error.exception_payload or {}).get("diagnostics"),
            }
        raise AssertionError("activity unexpectedly succeeded")


def _client(server_url: str, server_token: str) -> Client:
    return Client(
        server_url, worker_token=server_token,
        control_token=os.environ.get("DURABLE_WORKFLOW_CONTROL_TOKEN", server_token), timeout=5,
    )


async def _claim(client: Client, identity: str) -> tuple[Any, dict[str, Any]]:
    await client.register_worker(
        worker_id=identity, task_queue=identity,
        supported_workflow_types=["tests.bounded-failure-consumer"],
        supported_activity_types=["tests.bounded-failure"],
        capability_manifest=MANUAL_WORKER_CAPABILITY_MANIFEST,
    )
    handle = await client.start_workflow(
        workflow_type="tests.bounded-failure-consumer", task_queue=identity, workflow_id=identity, input=[],
    )
    task = await client.poll_workflow_task(worker_id=identity, task_queue=identity, timeout=2)
    assert isinstance(task, dict)
    return handle, task


@pytest.mark.parametrize("scenario", ["message", "stack", "combined", "small"])
async def test_failure_reaches_original_workflow(
    server_url: str, server_token: str, scenario: str,
) -> None:
    async def exercise() -> None:
        identity = "py-bounded-failure-" + uuid.uuid4().hex[:12]
        message = "M" * (3 * 1024 * 1024) if scenario == "message" else "original failure"
        stack = "S" * (3 * 1024 * 1024) if scenario == "stack" else "short stack"
        details: Any = {"small": True}
        if scenario == "combined":
            message, stack = "\x00🌈\"\\" * 700000, "\x01" * (3 * 1024 * 1024)
            details = {"cause": {"nested": "D" * (3 * 1024 * 1024)}}
        non_retryable = scenario in {"message", "stack"}
        requests: list[httpx.Request] = []
        async with _client(server_url, server_token) as client:
            async def record(request: httpx.Request) -> None:
                if request.url.path.endswith("/fail"):
                    requests.append(request)

            client._http.event_hooks["request"].append(record)
            handle, task = await _claim(client, identity)
            try:
                outcome = replay(FailureConsumer, task["history_events"], [], run_id=task["run_id"])
                await client.complete_workflow_task(
                    task_id=task["task_id"], lease_owner=identity,
                    workflow_task_attempt=task["workflow_task_attempt"],
                    commands=[command.to_server_command(identity) for command in outcome.commands],
                )
                activity = await client.poll_activity_task(worker_id=identity, task_queue=identity, timeout=2)
                assert isinstance(activity, dict)
                reply = await client.fail_activity_task(
                    task_id=activity["task_id"], activity_attempt_id=activity["activity_attempt_id"],
                    lease_owner=identity, message=message, stack_trace=stack, details=details,
                    failure_type="OriginalFailure", failure_class="app.OriginalFailure", failure_code=42,
                    non_retryable=non_retryable,
                )
                assert reply["recorded"] is True
                assert len(requests) == 1
                assert len(requests[0].content) <= 1024 * 1024
                submitted = json.loads(requests[0].content)
                assert submitted["activity_attempt_id"] == activity["activity_attempt_id"]
                assert submitted["lease_owner"] == identity
                second = await client.poll_workflow_task(worker_id=identity, task_queue=identity, timeout=2)
                assert isinstance(second, dict)
                completed = replay(FailureConsumer, second["history_events"], [], run_id=second["run_id"])
                await client.complete_workflow_task(
                    task_id=second["task_id"], lease_owner=identity,
                    workflow_task_attempt=second["workflow_task_attempt"],
                    commands=[command.to_server_command(identity) for command in completed.commands],
                )
                observed = await handle.result(timeout=3, poll_interval=.05)
                assert observed["type"] == "OriginalFailure"
                assert observed["class"] == "app.OriginalFailure"
                assert observed["code"] == 42
                assert observed["non_retryable"] is non_retryable
                assert ("[truncated" in observed["message"]) is (scenario != "small")
                if scenario != "small":
                    assert observed["diagnostics"]["failure_report"]["omitted_fields"]
                else:
                    assert observed["message"] == message
                    assert observed["diagnostics"] is None
            finally:
                await client.deregister_worker_registration(identity)

    await asyncio.wait_for(exercise(), timeout=30)


@pytest.mark.parametrize("field", ["message", "stack_trace"])
async def test_workflow_task_failure_remains_a_recorded_replay_failure(
    server_url: str, server_token: str, field: str,
) -> None:
    async def exercise() -> None:
        identity = "py-bounded-task-failure-" + uuid.uuid4().hex[:12]
        requests: list[httpx.Request] = []
        async with _client(server_url, server_token) as client:
            async def record(request: httpx.Request) -> None:
                if request.url.path.endswith("/fail"):
                    requests.append(request)

            client._http.event_hooks["request"].append(record)
            _handle, task = await _claim(client, identity)
            try:
                values = {"message": "replay failed", "stack_trace": "short stack"}
                values[field] = "Z" * (3 * 1024 * 1024)
                reply = await client.fail_workflow_task(
                    task_id=task["task_id"], lease_owner=identity,
                    workflow_task_attempt=task["workflow_task_attempt"],
                    failure_type="NonDeterministicReplayError", failure_reason="activity_identity_mismatch", **values,
                )
                assert reply["recorded"] is True
                assert reply["outcome"] == "failed"
                assert reply["next_task_id"] is None
                assert len(requests) == 1
                assert len(requests[0].content) <= 1024 * 1024
                body = json.loads(requests[0].content)
                assert body["lease_owner"] == identity
                assert body["workflow_task_attempt"] == task["workflow_task_attempt"]
                assert body["failure"]["type"] == "NonDeterministicReplayError"
                assert body["failure"]["reason"] == "activity_identity_mismatch"
                assert "[truncated" in body["failure"]["message"]
            finally:
                await client.deregister_worker_registration(identity)

    await asyncio.wait_for(exercise(), timeout=30)


async def test_worker_reports_original_three_mib_exception(server_url: str, server_token: str) -> None:
    from durable_workflow import Worker, activity

    class OriginalFailure(Exception):
        pass

    @activity.defn(name="tests.bounded-failure")
    async def fail() -> None:
        raise OriginalFailure("W" * (3 * 1024 * 1024))

    async def exercise() -> None:
        identity = "py-bounded-worker-failure-" + uuid.uuid4().hex[:12]
        async with _client(server_url, server_token) as client:
            handle = await client.start_workflow(
                workflow_type="tests.bounded-failure-consumer", task_queue=identity, workflow_id=identity, input=[],
            )
            worker = Worker(
                client, task_queue=identity, worker_id=identity, workflows=[FailureConsumer], activities=[fail],
            )
            observed = await worker.run_until(handle, timeout=20)
            assert observed["type"] == "OriginalFailure"
            assert observed["non_retryable"] is False
            assert "[truncated" in observed["message"]
            assert observed["diagnostics"]["failure_report"]["omitted_fields"]["message"] == 3 * 1024 * 1024

    await asyncio.wait_for(exercise(), timeout=30)
