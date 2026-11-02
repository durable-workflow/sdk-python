from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import NonRetryableError
from durable_workflow.external_task_result import parse_external_task_result
from durable_workflow.invocable import handle_invocable_activity
from durable_workflow.worker import Worker
from durable_workflow.workflow import _fail_update_from_exception, _fail_workflow_from_exception
from tests.query_task_helpers import leased_query_task
from tests.test_cooperative_remote_worker import make_owner, task
from tests.test_invocable import activity_input
from tests.test_worker import mock_client  # noqa: F401  (shared fixture)


async def application_failure(kind: str, message: str) -> None:
    if kind == "NonRetryableError":
        raise NonRetryableError(message)
    raise AssertionError(message)


@pytest.mark.parametrize("message", ["", " \t\n", "  original failure  "])
@pytest.mark.parametrize("kind", ["AssertionError", "NonRetryableError"])
@pytest.mark.parametrize("supervised", [False, True])
async def test_callback_failure_reports_a_nonempty_message_with_original_authority(
    monkeypatch: pytest.MonkeyPatch, message: str, kind: str, supervised: bool,
) -> None:
    if supervised:
        worker, client = await make_owner(monkeypatch)
    else:
        client = AsyncMock(spec=Client)
        worker = Worker(client, task_queue="queue", worker_id="owner")
    worker.activities["remote"] = application_failure
    try:
        outcome = await worker._run_activity_task(task(kind, message))
        assert outcome == ("failed_non_retryable" if kind == "NonRetryableError" else "failed")
        client.fail_activity_task.assert_awaited_once()
        report = client.fail_activity_task.await_args.kwargs
        assert report["message"] == (message if message.strip() else kind)
        assert report["failure_type"] == kind
        assert report["failure_class"].endswith("." + kind)
        assert report.get("non_retryable", False) is (kind == "NonRetryableError")
        assert report["task_id"] == "remote-task"
        assert report["activity_attempt_id"] == "attempt"
        assert report["lease_owner"] == "owner"
        assert "application_failure" in report["stack_trace"]
        client.complete_activity_task.assert_not_awaited()
    finally:
        await worker.stop()


@pytest.mark.parametrize("message", ["", " \t\n", "  original failure  "])
@pytest.mark.parametrize("kind", ["AssertionError", "NonRetryableError"])
async def test_invocable_failure_keeps_a_valid_typed_envelope(message: str, kind: str) -> None:
    output = await handle_invocable_activity(
        activity_input(kind, message), {"billing.charge-card": application_failure},
    )
    failure: Any = parse_external_task_result(output).failure
    assert failure.message == (message if message.strip() else kind)
    assert failure.type == kind
    assert "application_failure" in failure.stack_trace


@pytest.mark.parametrize("message", ["", " \t\n", "  original failure  "])
def test_workflow_and_update_failures_have_useful_messages(message: str) -> None:
    error = AssertionError(message)
    expected = message if message.strip() else "AssertionError"
    failed = _fail_workflow_from_exception(error)
    assert failed.message == expected
    assert failed.exception_type == "AssertionError"
    update = _fail_update_from_exception("update", "update rejected", error)
    assert update.message == "update rejected: " + expected
    assert update.exception_type == "AssertionError"


@workflow.defn(name="tests.blank-query-and-validator")
class BlankQueryAndValidatorWorkflow:
    message = ""

    @workflow.query("status")
    def status(self) -> str:
        raise AssertionError(type(self).message)

    @workflow.update("approve")
    def approve(self) -> None:
        return None

    @approve.validator  # type: ignore[attr-defined]
    def validate_approve(self) -> None:
        raise AssertionError(type(self).message)

    def run(self, ctx: Any) -> str:
        return "waiting"


def blank_workflow_task(**fields: Any) -> dict[str, Any]:
    return {
        "workflow_type": "tests.blank-query-and-validator",
        "history_events": [],
        "workflow_arguments": serializer.envelope([], codec="avro"),
        "payload_codec": "avro",
        **fields,
    }


@pytest.mark.parametrize("message", ["", " \t\n", "  original failure  "])
async def test_query_failure_reports_a_nonempty_message(
    mock_client: AsyncMock, message: str,  # noqa: F811
) -> None:
    BlankQueryAndValidatorWorkflow.message = message
    worker = Worker(mock_client, task_queue="q1", workflows=[BlankQueryAndValidatorWorkflow])
    query = blank_workflow_task(
        query_task_id="qt-blank", query_task_attempt=3, query_name="status",
        query_arguments=serializer.envelope([], codec="avro"),
    )

    assert await worker._run_query_task(leased_query_task(worker, query)) == "failed"

    report = mock_client.fail_query_task.await_args.kwargs
    assert report["message"] == (message if message.strip() else "query 'status' failed")
    assert report["reason"] == "query_rejected"
    assert report["query_task_id"] == "qt-blank"
    assert report["query_task_attempt"] == 3
    mock_client.complete_query_task.assert_not_awaited()


@pytest.mark.parametrize("message", ["", " \t\n", "  original failure  "])
async def test_update_validator_rejection_reports_a_nonempty_message(
    mock_client: AsyncMock, message: str,  # noqa: F811
) -> None:
    BlankQueryAndValidatorWorkflow.message = message
    worker = Worker(
        mock_client, task_queue="q1", workflows=[BlankQueryAndValidatorWorkflow], worker_id="validator",
    )
    validation = blank_workflow_task(
        task_kind="update_validation", update_validation_task_id="uv-blank", update_validation_attempt=2,
        update_name="approve", update_arguments=serializer.envelope([], codec="avro"),
        workflow_id="wf", run_id="run",
    )

    assert await worker._run_update_validation_task(validation) == "rejected"

    rejection = mock_client.reject_update_validation_task.await_args.kwargs
    expected = message if message.strip() else "update validator 'approve' rejected the request"
    assert expected in rejection["message"]
    assert rejection["message"].strip()
    assert rejection["reason"] == "update_validator_rejected"
    assert rejection["failure_type"] == "AssertionError"
    assert rejection["update_validation_task_id"] == "uv-blank"
    assert rejection["update_validation_attempt"] == 2
    mock_client.approve_update_validation_task.assert_not_awaited()
