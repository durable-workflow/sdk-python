from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from durable_workflow import ActivityRetryPolicy, Client, Worker, activity
from durable_workflow import worker as worker_module
from durable_workflow.workflow import RecordLocalActivity


async def _run_failing_local_activity(
    monkeypatch: pytest.MonkeyPatch, policy: dict[str, object],
) -> tuple[list[float], list[dict[str, object]]]:
    slept: list[float] = []

    async def record_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(worker_module.asyncio, "sleep", record_sleep)

    @activity.defn(name="tests.local.always-fails")
    async def always_fails() -> None:
        raise RuntimeError("still failing")

    async with Client("http://test", worker_token="worker") as client:
        worker = Worker(client, task_queue="queue", activities=[always_fails])
        worker._renew_local_workflow_lease = AsyncMock()  # type: ignore[method-assign]
        command = RecordLocalActivity("tests.local.always-fails", [], retry_policy=policy)
        await worker._execute_local_activity(
            {"task_id": "task", "workflow_task_attempt": 1}, command, payload_codec="avro",
        )
    assert command.outcome is not None
    return slept, list(command.outcome["attempts"])


async def test_local_retries_repeat_the_final_backoff_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    slept, attempts = await _run_failing_local_activity(
        monkeypatch, {"max_attempts": 5, "backoff_seconds": [1, 2]},
    )
    # Before .github#219 the third and fourth retries used a zero delay.
    assert slept == [1, 2, 2, 2]
    assert [attempt.get("backoff_seconds") for attempt in attempts] == [1, 2, 2, 2, None]


async def test_local_retries_without_a_backoff_list_do_not_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    slept, attempts = await _run_failing_local_activity(monkeypatch, {"max_attempts": 3})
    assert slept == []
    assert len(attempts) == 3


async def test_new_generated_cap_reaches_the_existing_local_executor(monkeypatch: pytest.MonkeyPatch) -> None:
    policy = ActivityRetryPolicy.exponential(max_attempts=9).to_dict()
    slept, attempts = await _run_failing_local_activity(monkeypatch, policy)
    assert slept == [1, 2, 4, 8, 16, 32, 60, 60]
    assert [attempt.get("backoff_seconds") for attempt in attempts] == [*slept, None]
