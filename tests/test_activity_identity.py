from __future__ import annotations

import pickle
from dataclasses import FrozenInstanceError
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import ActivityInfo, Worker, activity, serializer
from durable_workflow.client import Client


@pytest.mark.parametrize(("received", "expected"), [
    ("execution-1", "execution-1"), ("0", "0"), ("  opaque  ", "  opaque  "),
    (None, None), ("", None), (" \t\n", None), (False, None), (0, None), (1, None),
    ([], None), ({"id": "execution-1"}, None),
])
def test_execution_identity_preserves_only_received_nonblank_strings(received: Any, expected: str | None) -> None:
    info = ActivityInfo("task", "activity", "attempt", 1, "queue", "owner", task_metadata={
        "activity_execution_id": received, "worker_session": {"session_id": "not-an-execution"},
    })
    assert info.activity_execution_id == expected


def test_legacy_constructor_does_not_invent_logical_identity() -> None:
    info = ActivityInfo("task", "activity", "attempt", 1, "queue", "owner")
    assert info.activity_execution_id is None
    assert info.workflow_id is None
    assert info.run_id is None


def test_received_identity_is_immutable_and_survives_callback_serialization() -> None:
    task = {"activity_execution_id": "original", "workflow_id": "workflow", "run_id": "run"}
    info = ActivityInfo("task", "activity", "attempt", 1, "queue", "owner", task_metadata=task)
    task["activity_execution_id"] = "replacement"
    restored = pickle.loads(pickle.dumps(info))
    assert restored.activity_execution_id == "original"
    assert restored.workflow_id == "workflow"
    assert restored.run_id == "run"
    with pytest.raises(FrozenInstanceError):
        info.activity_execution_id = "replacement"  # type: ignore[misc]


@pytest.mark.parametrize("session_routed", [False, True])
async def test_received_retry_keeps_execution_identity_and_distinct_attempts(session_routed: bool) -> None:
    client = AsyncMock(spec=Client)
    seen: list[ActivityInfo] = []

    @activity.defn(name="identity.retry")
    async def inspect_identity() -> str | None:
        info = activity.context().info
        seen.append(info)
        if info.activity_execution_id == "execution-1" and info.attempt_number == 1:
            raise ValueError("retry this received attempt")
        return info.activity_execution_id

    worker = Worker(client, task_queue="queue", activities=[inspect_identity])
    identities = [("execution-1", 1), ("execution-1", 2), ("execution-2", 1), (None, 1)]
    try:
        for index, (execution, attempt) in enumerate(identities):
            task = {
                "task_id": f"task-{index}", "activity_attempt_id": f"attempt-{index}",
                "attempt_number": attempt, "activity_type": "identity.retry",
                "arguments": serializer.envelope([]), "payload_codec": "avro",
                "workflow_id": "workflow", "run_id": "run",
                **({"activity_execution_id": execution} if execution is not None else {}),
                **({"worker_session": {"session_id": "session-1"}} if session_routed else {}),
            }
            outcome = await worker._run_activity_task(task)
            assert outcome == ("failed" if index == 0 else "completed")
        assert [info.activity_execution_id for info in seen] == ["execution-1", "execution-1", "execution-2", None]
        assert len({info.activity_attempt_id for info in seen}) == 4
        assert {info.workflow_id for info in seen} == {"workflow"}
        assert {info.run_id for info in seen} == {"run"}
        client.fail_activity_task.assert_awaited_once()
        assert client.complete_activity_task.await_count == 3
    finally:
        await worker.stop()
