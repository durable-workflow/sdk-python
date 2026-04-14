from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity
from durable_workflow.activity import ActivityContext, ActivityInfo, _set_context
from durable_workflow.client import Client
from durable_workflow.errors import ActivityCancelled, NonRetryableError
from durable_workflow.worker import Worker


def _make_info(**overrides: object) -> ActivityInfo:
    defaults = {
        "task_id": "t1",
        "activity_type": "my-act",
        "activity_attempt_id": "aa1",
        "attempt_number": 1,
        "task_queue": "q1",
        "worker_id": "w1",
    }
    defaults.update(overrides)
    return ActivityInfo(**defaults)  # type: ignore[arg-type]


class TestActivityInfo:
    def test_frozen(self) -> None:
        info = _make_info()
        with pytest.raises(AttributeError):
            info.task_id = "t2"  # type: ignore[misc]

    def test_fields(self) -> None:
        info = _make_info(attempt_number=3, task_queue="custom-q")
        assert info.attempt_number == 3
        assert info.task_queue == "custom-q"


class TestActivityContext:
    @pytest.mark.asyncio
    async def test_heartbeat_no_cancel(self) -> None:
        mock_client = AsyncMock(spec=Client)
        mock_client.heartbeat_activity_task = AsyncMock(return_value={"cancel_requested": False})
        ctx = ActivityContext(info=_make_info(), client=mock_client)

        await ctx.heartbeat({"progress": 50})

        mock_client.heartbeat_activity_task.assert_called_once_with(
            task_id="t1",
            activity_attempt_id="aa1",
            lease_owner="w1",
            details={"progress": 50},
        )
        assert not ctx.is_cancelled

    @pytest.mark.asyncio
    async def test_heartbeat_cancel_requested(self) -> None:
        mock_client = AsyncMock(spec=Client)
        mock_client.heartbeat_activity_task = AsyncMock(return_value={"cancel_requested": True})
        ctx = ActivityContext(info=_make_info(), client=mock_client)

        with pytest.raises(ActivityCancelled):
            await ctx.heartbeat()

        assert ctx.is_cancelled

    @pytest.mark.asyncio
    async def test_heartbeat_none_details(self) -> None:
        mock_client = AsyncMock(spec=Client)
        mock_client.heartbeat_activity_task = AsyncMock(return_value={})
        ctx = ActivityContext(info=_make_info(), client=mock_client)

        await ctx.heartbeat()

        mock_client.heartbeat_activity_task.assert_called_once_with(
            task_id="t1",
            activity_attempt_id="aa1",
            lease_owner="w1",
            details=None,
        )


class TestContextVar:
    def test_outside_activity_raises(self) -> None:
        _set_context(None)
        with pytest.raises(RuntimeError, match="outside of an activity"):
            activity.context()

    def test_inside_activity(self) -> None:
        mock_client = AsyncMock(spec=Client)
        ctx = ActivityContext(info=_make_info(), client=mock_client)
        _set_context(ctx)
        try:
            assert activity.context() is ctx
            assert activity.context().info.task_id == "t1"
        finally:
            _set_context(None)


@pytest.fixture
def mock_client() -> AsyncMock:
    client = AsyncMock(spec=Client)
    client.register_worker = AsyncMock(return_value={"worker_id": "w1", "registered": True})
    client.poll_workflow_task = AsyncMock(return_value=None)
    client.poll_activity_task = AsyncMock(return_value=None)
    client.complete_activity_task = AsyncMock(return_value={"outcome": "completed"})
    client.fail_activity_task = AsyncMock(return_value={"outcome": "failed"})
    client.heartbeat_activity_task = AsyncMock(return_value={})
    return client


class TestWorkerActivityContext:
    @pytest.mark.asyncio
    async def test_context_available_during_execution(self, mock_client: AsyncMock) -> None:
        captured: list[ActivityInfo] = []

        @activity.defn(name="ctx-act")
        def ctx_activity() -> str:
            captured.append(activity.context().info)
            return "ok"

        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[ctx_activity])
        task = {
            "task_id": "at1",
            "activity_attempt_id": "aa1",
            "activity_type": "ctx-act",
            "attempt_number": 2,
            "arguments": "[]",
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)

        assert len(captured) == 1
        assert captured[0].task_id == "at1"
        assert captured[0].activity_attempt_id == "aa1"
        assert captured[0].attempt_number == 2
        assert captured[0].task_queue == "q1"

    @pytest.mark.asyncio
    async def test_context_cleared_after_execution(self, mock_client: AsyncMock) -> None:
        @activity.defn(name="clean-act")
        def clean_activity() -> str:
            return "done"

        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[clean_activity])
        task = {
            "task_id": "at2",
            "activity_attempt_id": "aa2",
            "activity_type": "clean-act",
            "arguments": "[]",
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)

        with pytest.raises(RuntimeError, match="outside of an activity"):
            activity.context()

    @pytest.mark.asyncio
    async def test_context_cleared_after_failure(self, mock_client: AsyncMock) -> None:
        @activity.defn(name="fail-act")
        def fail_activity() -> None:
            raise ValueError("boom")

        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[fail_activity])
        task = {
            "task_id": "at3",
            "activity_attempt_id": "aa3",
            "activity_type": "fail-act",
            "arguments": "[]",
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)

        with pytest.raises(RuntimeError, match="outside of an activity"):
            activity.context()

    @pytest.mark.asyncio
    async def test_heartbeat_cancels_activity(self, mock_client: AsyncMock) -> None:
        mock_client.heartbeat_activity_task = AsyncMock(return_value={"cancel_requested": True})

        @activity.defn(name="hb-act")
        async def heartbeat_activity() -> str:
            ctx = activity.context()
            await ctx.heartbeat({"progress": 50})
            return "should not reach"

        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[heartbeat_activity])
        task = {
            "task_id": "at4",
            "activity_attempt_id": "aa4",
            "activity_type": "hb-act",
            "arguments": "[]",
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)

        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert call_kwargs["failure_type"] == "ActivityCancelled"
        assert call_kwargs["non_retryable"] is True

    @pytest.mark.asyncio
    async def test_non_retryable_error(self, mock_client: AsyncMock) -> None:
        @activity.defn(name="nr-act")
        def non_retryable_activity() -> None:
            raise NonRetryableError("permanent failure")

        worker = Worker(
            mock_client, task_queue="q1", workflows=[], activities=[non_retryable_activity]
        )
        task = {
            "task_id": "at5",
            "activity_attempt_id": "aa5",
            "activity_type": "nr-act",
            "arguments": "[]",
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)

        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert call_kwargs["non_retryable"] is True
        assert "permanent failure" in call_kwargs["message"]

    @pytest.mark.asyncio
    async def test_attempt_number_defaults_to_1(self, mock_client: AsyncMock) -> None:
        captured: list[int] = []

        @activity.defn(name="default-attempt-act")
        def default_attempt_activity() -> str:
            captured.append(activity.context().info.attempt_number)
            return "ok"

        worker = Worker(
            mock_client, task_queue="q1", workflows=[], activities=[default_attempt_activity]
        )
        task = {
            "task_id": "at6",
            "activity_attempt_id": "aa6",
            "activity_type": "default-attempt-act",
            "arguments": "[]",
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)

        assert captured == [1]
