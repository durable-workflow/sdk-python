from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, workflow
from durable_workflow.client import Client
from durable_workflow.worker import Worker


@workflow.defn(name="test-wf")
class TestWorkflow:
    def run(self, ctx, *args):  # type: ignore[no-untyped-def]
        result = yield ctx.schedule_activity("test-act", list(args))
        return result


@activity.defn(name="test-act")
def echo_activity(val: str) -> str:
    return f"result-{val}"


@activity.defn(name="test-async-act")
async def echo_async_activity(val: str) -> str:
    return f"async-{val}"


@pytest.fixture
def mock_client() -> AsyncMock:
    client = AsyncMock(spec=Client)
    client.register_worker = AsyncMock(return_value={"worker_id": "w1", "registered": True})
    client.poll_workflow_task = AsyncMock(return_value=None)
    client.poll_activity_task = AsyncMock(return_value=None)
    client.complete_workflow_task = AsyncMock(return_value={"outcome": "completed"})
    client.complete_activity_task = AsyncMock(return_value={"outcome": "completed"})
    client.fail_workflow_task = AsyncMock(return_value={"outcome": "failed"})
    client.fail_activity_task = AsyncMock(return_value={"outcome": "failed"})
    return client


class TestWorkerRegistration:
    @pytest.mark.asyncio
    async def test_register(self, mock_client: AsyncMock) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            worker_id="w-test",
        )
        await worker._register()
        mock_client.register_worker.assert_called_once()
        call_kwargs = mock_client.register_worker.call_args.kwargs
        assert call_kwargs["task_queue"] == "q1"
        assert "test-wf" in call_kwargs["supported_workflow_types"]
        assert "test-act" in call_kwargs["supported_activity_types"]


class TestWorkflowTaskExecution:
    @pytest.mark.asyncio
    async def test_schedule_activity_on_first_replay(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t1",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": '["hello"]',
            "payload_codec": "json",
        }
        await worker._run_workflow_task(task)
        mock_client.complete_workflow_task.assert_called_once()
        call_kwargs = mock_client.complete_workflow_task.call_args.kwargs
        commands = call_kwargs["commands"]
        assert len(commands) == 1
        assert commands[0]["type"] == "schedule_activity"
        assert commands[0]["activity_type"] == "test-act"

    @pytest.mark.asyncio
    async def test_complete_on_resolved_activity(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t2",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [
                {"event_type": "ActivityCompleted", "payload": {"result": '"done"'}},
            ],
            "arguments": '["hello"]',
            "payload_codec": "json",
        }
        await worker._run_workflow_task(task)
        mock_client.complete_workflow_task.assert_called_once()
        commands = mock_client.complete_workflow_task.call_args.kwargs["commands"]
        assert commands[0]["type"] == "complete_workflow"

    @pytest.mark.asyncio
    async def test_unknown_workflow_type_fails_task(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[])
        task = {
            "task_id": "t3",
            "workflow_type": "unknown-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
        }
        await worker._run_workflow_task(task)
        mock_client.fail_workflow_task.assert_called_once()
        call_kwargs = mock_client.fail_workflow_task.call_args.kwargs
        assert "unknown-wf" in call_kwargs["message"]

    @pytest.mark.asyncio
    async def test_fail_task_uses_failure_object(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[])
        task = {
            "task_id": "t4",
            "workflow_type": "missing",
            "workflow_task_attempt": 1,
            "history_events": [],
        }
        await worker._run_workflow_task(task)
        call_kwargs = mock_client.fail_workflow_task.call_args.kwargs
        assert "message" in call_kwargs


class TestActivityTaskExecution:
    @pytest.mark.asyncio
    async def test_sync_activity(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at1",
            "activity_attempt_id": "aa1",
            "activity_type": "test-act",
            "arguments": '["hello"]',
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)
        mock_client.complete_activity_task.assert_called_once()
        call_kwargs = mock_client.complete_activity_task.call_args.kwargs
        assert call_kwargs["result"] == "result-hello"

    @pytest.mark.asyncio
    async def test_async_activity(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_async_activity])
        task = {
            "task_id": "at2",
            "activity_attempt_id": "aa2",
            "activity_type": "test-async-act",
            "arguments": '["world"]',
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)
        mock_client.complete_activity_task.assert_called_once()
        call_kwargs = mock_client.complete_activity_task.call_args.kwargs
        assert call_kwargs["result"] == "async-world"

    @pytest.mark.asyncio
    async def test_unknown_activity_fails(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[])
        task = {
            "task_id": "at3",
            "activity_attempt_id": "aa3",
            "activity_type": "unknown-act",
            "arguments": "[]",
        }
        await worker._run_activity_task(task)
        mock_client.fail_activity_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_activity_exception_fails_task(self, mock_client: AsyncMock) -> None:
        @activity.defn(name="failing-act")
        def failing_act() -> None:
            raise RuntimeError("boom")

        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[failing_act])
        task = {
            "task_id": "at4",
            "activity_attempt_id": "aa4",
            "activity_type": "failing-act",
            "arguments": "[]",
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)
        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert "boom" in call_kwargs["message"]
        assert call_kwargs["failure_type"] == "RuntimeError"


class TestWorkerStop:
    @pytest.mark.asyncio
    async def test_stop_sets_event(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[])
        assert not worker._stop.is_set()
        worker.stop()
        assert worker._stop.is_set()


class TestWorkerIdGeneration:
    def test_default_id(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1")
        assert worker.worker_id.startswith("py-worker-")

    def test_custom_id(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", worker_id="custom-1")
        assert worker.worker_id == "custom-1"
