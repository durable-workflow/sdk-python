from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow.client import Client
from durable_workflow.errors import NonRetryableError
from durable_workflow.external_task_result import parse_external_task_result
from durable_workflow.invocable import handle_invocable_activity
from durable_workflow.worker import Worker
from durable_workflow.workflow import _fail_update_from_exception, _fail_workflow_from_exception
from tests.test_cooperative_remote_worker import make_owner, task
from tests.test_invocable import activity_input


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
