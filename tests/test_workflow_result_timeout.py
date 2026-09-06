from unittest.mock import AsyncMock

import pytest

from durable_workflow import Client, WorkflowTimedOut
from durable_workflow.client import WorkflowExecution, WorkflowHandle
from durable_workflow.errors import WorkflowCancelled, WorkflowFailed, WorkflowTerminated


def result_client(status: str, event_type: str, payload: dict) -> tuple[Client, WorkflowHandle]:
    client = Client("https://unused.example")
    client.describe_workflow = AsyncMock(
        return_value=WorkflowExecution(
            workflow_id="order", run_id="current-run", workflow_type="order", status=status,
        )
    )
    client.get_history = AsyncMock(
        return_value={"events": [{"event_type": event_type, "payload": payload}]}
    )
    return client, WorkflowHandle(client, workflow_id="order", run_id="selected-run", workflow_type="order")


@pytest.mark.parametrize("kind", ["execution_timeout", "run_timeout"])
async def test_persisted_deadline_raises_typed_timeout_for_selected_history(kind: str) -> None:
    client, handle = result_client(
        "failed", "WorkflowTimedOut", {"timeout_kind": kind, "deadline_at": "2026-01-01T00:00:00Z"},
    )
    try:
        with pytest.raises(WorkflowTimedOut, match="workflow execution timed out"):
            await client.get_result(handle, timeout=0)
        client.get_history.assert_awaited_once_with("order", "selected-run")
    finally:
        await client.aclose()


async def test_polling_timeout_is_not_a_terminal_workflow_timeout() -> None:
    client, handle = result_client("waiting", "ConditionWaitOpened", {})
    try:
        with pytest.raises(TimeoutError, match="not terminal") as caught:
            await client.get_result(handle, timeout=0)
        assert not isinstance(caught.value, WorkflowTimedOut)
        client.get_history.assert_not_awaited()
    finally:
        await client.aclose()


@pytest.mark.parametrize(
    ("status", "event_type", "exception"),
    [
        ("failed", "WorkflowFailed", WorkflowFailed),
        ("cancelled", "WorkflowCancelled", WorkflowCancelled),
        ("terminated", "WorkflowTerminated", WorkflowTerminated),
    ],
)
async def test_other_terminal_failures_preserve_their_exception(status, event_type, exception) -> None:
    client, handle = result_client(status, event_type, {"message": "stopped", "reason": "stopped"})
    try:
        with pytest.raises(exception, match="stopped"):
            await client.get_result(handle, timeout=0)
    finally:
        await client.aclose()
