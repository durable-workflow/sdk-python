"""Activity workers treat a ``run_closed`` refusal as final (server#1193).

When a run's own code closes it, Workflow 2.28.0 (workflow#1117/#1121) and
Server cancel each activity it never awaited in the closing transaction, or
leave an abandon-policy activity running, and append nothing after the
terminal event. A late claim, heartbeat or outcome for such an activity is
answered with the typed ``run_closed`` refusal. The worker must stop there:
no retry, no second report, and no poll failure backoff.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, serializer
from durable_workflow.activity import ActivityContext, ActivityInfo
from durable_workflow.client import Client
from durable_workflow.errors import ActivityCancelled, ActivityLeaseAuthorityLost, ServerError
from durable_workflow.worker import Worker

RUN_CLOSED_OUTCOME = {
    "task_id": "late-task",
    "activity_attempt_id": "late-attempt",
    "outcome": "completed",
    "recorded": False,
    "reason": "run_closed",
    "next_task_id": None,
}


def _task(activity_type: str) -> dict[str, Any]:
    return {
        "task_id": "late-task", "activity_attempt_id": "late-attempt",
        "activity_type": activity_type, "attempt_number": 1,
        "arguments": serializer.encode([], codec="avro"), "payload_codec": "avro",
    }


def _worker(client: AsyncMock, handler: Any) -> Worker:
    return Worker(client, task_queue="late-queue", workflows=[], activities=[handler])


@pytest.mark.asyncio
async def test_completion_refused_because_the_run_closed_is_final(caplog: pytest.LogCaptureFixture) -> None:
    """An abandon-policy activity kept running after close reports its outcome;
    the Server accepts it with ``recorded: false, reason: run_closed``."""
    client = AsyncMock(spec=Client)
    client.complete_activity_task.side_effect = ServerError(
        409, {**RUN_CLOSED_OUTCOME, "activity_status": "completed"},
    )

    @activity.defn(name="abandoned-at-close")
    async def handler() -> str:
        return "independent result"

    with caplog.at_level(logging.INFO, logger="durable_workflow.worker"):
        assert await _worker(client, handler)._run_activity_task(_task("abandoned-at-close")) == "completed"

    client.complete_activity_task.assert_awaited_once()
    client.fail_activity_task.assert_not_called()
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]
    assert any(
        record.levelno == logging.INFO and "late-task completed" in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_completion_refused_because_the_close_cancelled_the_activity_is_aborted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A select() loser the close cancelled finishes anyway and reports its
    result; the Server drops it with ``recorded: false, reason: run_closed``
    and ``activity_status: cancelled``. The worker reports an abort, never a
    completion, and never retries or reports again."""
    client = AsyncMock(spec=Client)
    client.complete_activity_task.side_effect = ServerError(
        409, {**RUN_CLOSED_OUTCOME, "activity_status": "cancelled"},
    )

    @activity.defn(name="loser-finished-after-close")
    async def handler() -> str:
        return "late result"

    with caplog.at_level(logging.INFO, logger="durable_workflow.worker"):
        outcome = await _worker(client, handler)._run_activity_task(_task("loser-finished-after-close"))

    assert outcome == "claim_aborted"
    client.complete_activity_task.assert_awaited_once()
    client.fail_activity_task.assert_not_called()
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "closed and cancelled the activity" in warnings[0].getMessage()
    assert not [record for record in caplog.records if "late-task completed" in record.getMessage()]


@pytest.mark.asyncio
async def test_cancellation_at_close_is_reported_once_and_its_refusal_is_final(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A running select() loser cancelled at close: its heartbeat reports the
    cancellation, and the late report is refused with run_closed."""
    client = AsyncMock(spec=Client)
    client.heartbeat_activity_task.return_value = {
        "can_continue": False, "cancel_requested": True, "reason": "activity_cancelled",
    }
    client.fail_activity_task.side_effect = ServerError(
        409, {**RUN_CLOSED_OUTCOME, "outcome": "failed"},
    )
    cleaned_up: list[str] = []

    @activity.defn(name="loser-cancelled-at-close")
    async def handler() -> str:
        try:
            await activity.context().heartbeat()
        except ActivityCancelled:
            cleaned_up.append("cleanup")
            raise
        return "late result"

    with caplog.at_level(logging.INFO, logger="durable_workflow.worker"):
        assert await _worker(client, handler)._run_activity_task(_task("loser-cancelled-at-close")) == "cancelled"

    assert cleaned_up == ["cleanup"]
    client.complete_activity_task.assert_not_called()
    client.fail_activity_task.assert_awaited_once()
    assert client.fail_activity_task.await_args.kwargs["non_retryable"] is True
    assert client.fail_activity_task.await_args.kwargs["failure_type"] == "ActivityCancelled"
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]


@pytest.mark.asyncio
async def test_failure_refused_because_the_run_closed_is_final() -> None:
    client = AsyncMock(spec=Client)
    client.fail_activity_task.side_effect = ServerError(409, {**RUN_CLOSED_OUTCOME, "outcome": "failed"})

    @activity.defn(name="fails-after-close")
    async def handler() -> str:
        raise RuntimeError("late failure")

    assert await _worker(client, handler)._run_activity_task(_task("fails-after-close")) == "failed"

    client.fail_activity_task.assert_awaited_once()
    client.complete_activity_task.assert_not_called()


@pytest.mark.asyncio
async def test_heartbeat_refused_because_the_run_closed_stops_without_any_outcome() -> None:
    client = AsyncMock(spec=Client)
    client.heartbeat_activity_task.return_value = {
        "can_continue": False, "cancel_requested": False, "reason": "run_closed",
    }
    continued: list[str] = []

    @activity.defn(name="heartbeat-after-close")
    async def handler() -> str:
        await activity.context().heartbeat()
        continued.append("after refusal")
        return "late result"

    assert await _worker(client, handler)._run_activity_task(_task("heartbeat-after-close")) == "claim_aborted"

    assert continued == []
    client.heartbeat_activity_task.assert_awaited_once()
    client.complete_activity_task.assert_not_called()
    client.fail_activity_task.assert_not_called()


@pytest.mark.asyncio
async def test_heartbeat_of_an_abandoned_activity_keeps_running() -> None:
    client = AsyncMock(spec=Client)
    client.heartbeat_activity_task.return_value = {"can_continue": True, "cancel_requested": False}
    ctx = ActivityContext(info=ActivityInfo(
        task_id="late-task", activity_type="abandoned-at-close", activity_attempt_id="late-attempt",
        attempt_number=1, task_queue="late-queue", worker_id="late-worker",
    ), client=client)

    await ctx.heartbeat({"message": "still going"})

    assert not ctx.is_cancelled


@pytest.mark.asyncio
async def test_claim_refused_because_the_run_closed_is_an_answered_poll(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    client = AsyncMock(spec=Client)

    @activity.defn(name="claimed-after-close")
    async def handler() -> str:
        return "never runs"

    worker = Worker(client, task_queue="late-queue", workflows=[], activities=[handler], poll_timeout=0.01)
    polls: list[str] = []

    async def poll(**_: object) -> None:
        polls.append("poll")
        if len(polls) == 1:
            raise ActivityLeaseAuthorityLost("late-task", "run_closed")
        await worker.stop()
        return None

    async def no_failure_backoff(*_: object) -> None:
        raise AssertionError("a run_closed claim refusal is not a poll failure")

    client.poll_activity_task.side_effect = poll
    monkeypatch.setattr(asyncio, "sleep", no_failure_backoff)
    dispatched: list[dict[str, Any]] = []
    monkeypatch.setattr(worker, "_dispatch_activity_task", lambda task: dispatched.append(task))

    with caplog.at_level(logging.INFO, logger="durable_workflow.worker"):
        await worker._poll_activity_tasks()

    assert polls == ["poll", "poll"]
    assert dispatched == []
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]
