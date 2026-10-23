from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from durable_workflow import serializer
from durable_workflow.client import Client

MIB = 1024 * 1024
MESSAGE_LIMIT = 64 * 1024
STACK_LIMIT = 128 * 1024


async def capture_failure(kind: str, *, client_kwargs: dict[str, Any] | None = None, **kwargs: Any) -> httpx.Request:
    requests: list[httpx.Request] = []

    def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"recorded": True})

    async with Client("http://test", worker_token="worker", **(client_kwargs or {})) as client:
        client._cluster_info = {}
        client._http._transport = httpx.MockTransport(receive)
        if kind == "activity":
            await client.fail_activity_task(
                task_id="task", activity_attempt_id="attempt", lease_owner="owner", **kwargs,
            )
        elif kind == "workflow":
            await client.fail_workflow_task(task_id="task", workflow_task_attempt=7, lease_owner="owner", **kwargs)
        elif kind == "query":
            await client.fail_query_task(query_task_id="task", query_task_attempt=7, lease_owner="owner", **kwargs)
        else:
            await client.reject_update_validation_task(
                update_validation_task_id="task", update_validation_attempt=7, lease_owner="owner",
                reason="validation_failed", **kwargs,
            )
    assert len(requests) == 1
    return requests[0]


@pytest.mark.parametrize("kind", ["activity", "workflow", "query", "validation"])
@pytest.mark.parametrize("oversized", ["message", "stack", "combined"])
async def test_large_reports_keep_semantics_and_bound_actual_body(kind: str, oversized: str) -> None:
    message = "application failure" if oversized == "stack" else "x" * (3 * MIB)
    stack = "short stack" if oversized == "message" else "s" * (3 * MIB)
    kwargs: dict[str, Any] = {"message": message, "stack_trace": stack, "failure_type": "OriginalFailure"}
    if kind == "activity":
        kwargs.update(non_retryable=True, failure_class="app.OriginalFailure", failure_code=42)
    request = await capture_failure(kind, **kwargs)
    body = json.loads(request.content)
    failure = body["failure"]
    assert len(request.content) <= MIB
    assert int(request.headers["content-length"]) == len(request.content)
    assert len(failure["message"].encode("utf-8")) <= MESSAGE_LIMIT
    assert len(failure["stack_trace"].encode("utf-8")) <= STACK_LIMIT
    assert "[truncated" in failure["message"]
    assert failure["type"] == "OriginalFailure"
    assert body["lease_owner"] == "owner"
    if kind == "activity":
        assert body["activity_attempt_id"] == "attempt"
        assert failure["class"] == "app.OriginalFailure"
        assert failure["non_retryable"] is True
        assert failure["code"] == 42


async def test_escaping_after_marker_insertion_fits_complete_request_and_lower_limit() -> None:
    request = await capture_failure(
        "activity", message="\x00\"\\\n🌈" * (MIB // 4), stack_trace="\x00" * (3 * MIB),
        failure_type="EscapedFailure", details={"nested": ["\x00" * (3 * MIB)]},
        client_kwargs={"failure_report_limit_bytes": 2200, "payload_size_warnings": False},
    )
    failure = json.loads(request.content)["failure"]
    assert len(request.content) <= 2200
    assert failure["type"] == "EscapedFailure"
    assert "[truncated" in failure["message"]
    assert "details" not in failure
    assert failure["diagnostics"]["failure_report"]["omitted_fields"]["details"] > 3 * MIB


async def test_small_report_is_unchanged_including_valid_avro_details() -> None:
    request = await capture_failure("activity", message="small", stack_trace="trace", details={"a": [1.0, -0.0]})
    failure = json.loads(request.content)["failure"]
    assert failure == {
        "message": "small", "stack_trace": "trace", "details": serializer.envelope({"a": [1.0, -0.0]}),
    }
    assert serializer.decode_envelope(failure["details"]) == {"a": [1.0, -0.0]}


async def test_oversized_details_are_omitted_whole_and_reported() -> None:
    request = await capture_failure("activity", message="original", details={"cause": {"nested": "z" * (3 * MIB)}})
    failure = json.loads(request.content)["failure"]
    assert "details" not in failure
    assert "[truncated" in failure["message"]
    assert failure["diagnostics"]["failure_report"]["omitted_fields"]["details"] > 3 * MIB


async def test_unfit_claim_identity_is_explicit_refusal_without_http() -> None:
    requests: list[httpx.Request] = []
    async with Client("http://test", failure_report_limit_bytes=512) as client:
        client._cluster_info = {}
        client._http._transport = httpx.MockTransport(lambda request: requests.append(request) or httpx.Response(200))
        with pytest.raises(Exception, match="failure report.*identity|failure report.*semantics") as caught:
            await client.fail_activity_task(
                task_id="task", activity_attempt_id="a" * 700, lease_owner="owner",
                message="x" * (3 * MIB), failure_type="OriginalFailure",
            )
        assert type(caught.value).__name__ == "FailureReportTooLarge"
    assert requests == []


async def test_terminal_workflow_failure_fits_whole_command_batch_without_mutation() -> None:
    commands = [{
        "type": "fail_workflow", "message": "x" * (3 * MIB), "exception_type": "SagaCompensationFailed",
        "exception_class": "app.SagaCompensationFailed", "non_retryable": True,
        "failed_step_sequence": 3, "failed_activity_execution_id": "activity-id",
        "exception": {"type": "SagaCompensationFailed", "class": "app.SagaCompensationFailed",
                      "cause": {"message": "z" * (3 * MIB)}},
    }]
    requests: list[httpx.Request] = []
    async with Client("http://test", failure_report_limit_bytes=4000) as client:
        client._cluster_info = {}
        client._http._transport = httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(200, json={"recorded": True}),
        )
        await client.complete_workflow_task(
            task_id="task", lease_owner="owner", workflow_task_attempt=7, commands=commands,
        )
    request = requests[0]
    cmd = json.loads(request.content)["commands"][0]
    assert len(request.content) <= 4000
    assert cmd["exception_type"] == "SagaCompensationFailed"
    assert cmd["exception"]["type"] == "SagaCompensationFailed"
    assert cmd["exception_class"] == "app.SagaCompensationFailed"
    assert cmd["non_retryable"] is True
    assert cmd["failed_step_sequence"] == 3
    assert cmd["failed_activity_execution_id"] == "activity-id"
    assert "[truncated" in cmd["message"]
    assert "cause" not in cmd["exception"]
    assert len(commands[0]["message"]) == 3 * MIB
    assert "cause" in commands[0]["exception"]


async def test_default_cap_counts_json_escaping_after_all_insertions() -> None:
    request = await capture_failure("activity", message="\x00" * (3 * MIB), stack_trace="\x01" * (3 * MIB))
    body = json.loads(request.content)
    assert len(request.content) <= MIB
    assert "[truncated" in body["failure"]["message"]
    assert len(body["failure"]["message"].encode()) <= MESSAGE_LIMIT
    assert len(body["failure"]["stack_trace"].encode()) <= STACK_LIMIT


async def test_discovered_lower_limit_controls_reporting() -> None:
    requests: list[httpx.Request] = []
    async with Client("http://test") as client:
        client._cluster_info = {"limits": {"max_payload_bytes": 1700}}
        client._http._transport = httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(200, json={"recorded": True}),
        )
        await client.fail_workflow_task(
            task_id="task", lease_owner="owner", workflow_task_attempt=2,
            message="🌈" * MIB, stack_trace="\x00" * MIB,
            failure_type="ReplayFailure", failure_reason="nondeterminism",
        )
    assert len(requests[0].content) <= 1700
    assert json.loads(requests[0].content)["failure"]["reason"] == "nondeterminism"


def test_details_budget_includes_new_diagnostics_and_preserves_retry_delay() -> None:
    from durable_workflow._failure_reporting import bound_failure_request, encoded_json

    failure = {"message": "x" * (3 * MIB), "details": serializer.envelope("z" * 49120),
               "non_retryable": False, "retry_after_seconds": 19, "type": "ApplicationError"}
    original_details = dict(failure["details"])
    result = bound_failure_request("/worker/activity-tasks/task/fail", {"failure": failure})["failure"]
    optional = {key: result[key] for key in ("details", "diagnostics") if key in result}
    assert len(encoded_json(optional)) <= 64 * 1024
    assert result["non_retryable"] is False
    assert result["retry_after_seconds"] == 19
    assert result["type"] == "ApplicationError"
    assert failure["details"] == original_details
    if "details" in result:
        assert serializer.decode_envelope(result["details"]) == "z" * 49120
    else:
        assert result["diagnostics"]["failure_report"]["omitted_fields"]["details"] > 0


def test_local_attempts_and_update_failures_share_complete_body_budget() -> None:
    from durable_workflow._failure_reporting import bound_failure_request, encoded_json

    attempts = [{"attempt_id": "first", "attempt_number": 1, "message": "a" * (3 * MIB),
                 "exception_type": "FirstFailure", "non_retryable": False, "backoff_seconds": 17}]
    body = {"lease_owner": "owner", "workflow_task_attempt": 3, "commands": [
        {"type": "record_local_activity", "outcome": "failed", "message": "x" * (3 * MIB),
         "exception_type": "LocalFailure", "non_retryable": True, "attempts": attempts},
        {"type": "fail_update", "update_id": "exact-update", "exception_type": "UpdateFailure",
         "non_retryable": True, "message": "u" * (3 * MIB)},
    ]}
    result = bound_failure_request("/worker/workflow-tasks/task/complete", body, 3500)
    assert len(encoded_json(result)) <= 3500
    local, update = result["commands"]
    assert local["exception_type"] == "LocalFailure"
    assert "[truncated" in local["message"]
    assert local["attempts"][0]["attempt_id"] == "first"
    assert local["attempts"][0]["backoff_seconds"] == 17
    assert local["attempts"][0]["exception_type"] == "FirstFailure"
    assert "[truncated" in local["attempts"][0]["message"]
    assert update["update_id"] == "exact-update"
    assert update["exception_type"] == "UpdateFailure"
    assert "[truncated" in update["message"]
    assert len(attempts[0]["message"]) == 3 * MIB


def test_prepared_local_outcome_uses_unchanged_attempt_and_bounded_failure() -> None:
    from durable_workflow._failure_reporting import bound_failure_request, encoded_json

    body = {"lease_owner": "owner", "workflow_task_attempt": 5,
            "report": {"outcome": "failed", "message": "\x00" * (3 * MIB),
                       "exception_type": "OriginalFailure", "non_retryable": True}}
    result = bound_failure_request("/worker/workflow-tasks/task/local-activities/exact-attempt/outcome", body, 1800)
    assert len(encoded_json(result)) <= 1800
    assert result["workflow_task_attempt"] == 5
    assert result["report"]["exception_type"] == "OriginalFailure"
    assert result["report"]["non_retryable"] is True
    assert "[truncated" in result["report"]["message"]


async def test_unencodable_optional_details_do_not_replace_application_failure() -> None:
    details: dict[str, Any] = {}
    details["cycle"] = details
    request = await capture_failure("activity", message="original", failure_type="OriginalFailure", details=details)
    failure = json.loads(request.content)["failure"]
    assert failure["type"] == "OriginalFailure"
    assert "details" not in failure
    assert failure["diagnostics"]["failure_report"]["omitted_fields"]["details"] is None
    assert "[truncated" in failure["message"]


async def test_advisory_warning_limit_does_not_become_a_transport_limit() -> None:
    request = await capture_failure(
        "activity", message="small unchanged failure",
        client_kwargs={"payload_size_limit_bytes": 1, "payload_size_warnings": False},
    )
    assert json.loads(request.content)["failure"] == {"message": "small unchanged failure"}


async def test_query_client_clone_retains_explicit_reporting_limit() -> None:
    from durable_workflow import Worker

    async with Client("http://test", failure_report_limit_bytes=1500, payload_size_warnings=False) as client:
        worker = Worker(client, task_queue="q")
        async with worker._clone_client_for_query_tasks() as cloned:
            assert cloned.failure_report_limit_bytes == 1500


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_reporting_limit_is_rejected(limit: Any) -> None:
    with pytest.raises(ValueError, match="failure_report_limit_bytes"):
        Client("http://test", failure_report_limit_bytes=limit)
