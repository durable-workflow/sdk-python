from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, serializer, workflow
from durable_workflow.client import (
    CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA,
    CONTROL_PLANE_REQUEST_CONTRACT_VERSION,
    CONTROL_PLANE_VERSION,
    PROTOCOL_VERSION,
    Client,
    WorkflowExecution,
)
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
    client.get_cluster_info = AsyncMock(return_value=compatible_cluster_info())
    return client


def compatible_cluster_info(**overrides: object) -> dict[str, object]:
    info: dict[str, object] = {
        "version": "not-authoritative",
        "control_plane": {
            "version": CONTROL_PLANE_VERSION,
            "request_contract": {
                "schema": CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA,
                "version": CONTROL_PLANE_REQUEST_CONTRACT_VERSION,
                "operations": {},
            },
        },
        "worker_protocol": {
            "version": PROTOCOL_VERSION,
        },
    }
    info.update(overrides)
    return info


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

    @pytest.mark.asyncio
    async def test_register_calls_cluster_info(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        await worker._register()
        mock_client.get_cluster_info.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_register_uses_protocol_manifests_not_top_level_app_version(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.get_cluster_info = AsyncMock(return_value=compatible_cluster_info(version="3.0.0"))
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        await worker._register()
        mock_client.register_worker.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_register_rejects_missing_control_plane_manifest(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(return_value=compatible_cluster_info(control_plane=None))
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match="missing control_plane manifest"):
            await worker._register()
        mock_client.register_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_rejects_unsupported_control_plane_version(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(control_plane={"version": "3", "request_contract": {}})
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match="unsupported control_plane.version"):
            await worker._register()
        mock_client.register_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_rejects_missing_request_contract(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(control_plane={"version": CONTROL_PLANE_VERSION})
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match="missing control_plane.request_contract"):
            await worker._register()
        mock_client.register_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_rejects_unsupported_worker_protocol_version(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(worker_protocol={"version": "2.0"})
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match="unsupported worker_protocol.version"):
            await worker._register()
        mock_client.register_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_fails_closed_when_cluster_info_fails(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(side_effect=RuntimeError("network down"))
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match="unable to read /api/cluster/info"):
            await worker._register()
        mock_client.register_worker.assert_not_called()


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
        assert commands[0]["arguments"]["codec"] == "json"
        assert serializer.decode(commands[0]["arguments"]["blob"], codec="json") == ["hello"]

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
        assert commands[0]["result"]["codec"] == "json"
        assert serializer.decode(commands[0]["result"]["blob"], codec="json") == "done"

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
        assert call_kwargs["codec"] == "json"

    @pytest.mark.asyncio
    async def test_activity_echoes_avro_codec(self, mock_client: AsyncMock) -> None:
        avro = pytest.importorskip("avro", reason="avro package not installed")
        del avro
        from durable_workflow import serializer as _ser

        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at-avro",
            "activity_attempt_id": "aa-avro",
            "activity_type": "test-act",
            "arguments": _ser.envelope(["hello"], codec="avro"),
            "payload_codec": "avro",
        }
        await worker._run_activity_task(task)
        mock_client.complete_activity_task.assert_called_once()
        call_kwargs = mock_client.complete_activity_task.call_args.kwargs
        assert call_kwargs["result"] == "result-hello"
        assert call_kwargs["codec"] == "avro"

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


class TestEnvelopeArguments:
    @pytest.mark.asyncio
    async def test_activity_with_envelope_arguments(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at-env",
            "activity_attempt_id": "aa-env",
            "activity_type": "test-act",
            "arguments": {"codec": "json", "blob": '["hello"]'},
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)
        mock_client.complete_activity_task.assert_called_once()
        call_kwargs = mock_client.complete_activity_task.call_args.kwargs
        assert call_kwargs["result"] == "result-hello"

    @pytest.mark.asyncio
    async def test_workflow_with_envelope_arguments(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-env",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": {"codec": "json", "blob": '["hello"]'},
            "payload_codec": "json",
        }
        await worker._run_workflow_task(task)
        mock_client.complete_workflow_task.assert_called_once()


class TestCodecDecodeFailures:
    """TD-P012 / #370 regression: codec decode failures at the task boundary
    must turn into a deterministic fail_{workflow,activity}_task call so the
    lease does not sit until timeout."""

    @pytest.mark.asyncio
    async def test_activity_json_decode_failure_fails_task(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at-bad-json",
            "activity_attempt_id": "aa-bad-json",
            "activity_type": "test-act",
            "arguments": "{not valid json",
            "payload_codec": "json",
        }
        await worker._run_activity_task(task)
        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert "decode" in call_kwargs["message"].lower()
        assert "json" in call_kwargs["message"]
        assert call_kwargs["non_retryable"] is True
        mock_client.complete_activity_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_activity_avro_decode_failure_fails_task(self, mock_client: AsyncMock) -> None:
        pytest.importorskip("avro", reason="avro package not installed")
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at-bad-avro",
            "activity_attempt_id": "aa-bad-avro",
            "activity_type": "test-act",
            "arguments": "!!!not-valid-base64!!!",
            "payload_codec": "avro",
        }
        await worker._run_activity_task(task)
        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert "decode" in call_kwargs["message"].lower()
        assert "avro" in call_kwargs["message"]
        assert call_kwargs["non_retryable"] is True
        mock_client.complete_activity_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_activity_avro_missing_dependency_fails_task(
        self, mock_client: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from durable_workflow import _avro
        from durable_workflow.errors import AvroNotInstalledError

        def _raise_missing(_blob: str) -> None:
            raise AvroNotInstalledError(
                "The 'avro' package is required to encode/decode payloads with the 'avro' "
                "codec. Reinstall durable-workflow with its runtime dependencies."
            )

        monkeypatch.setattr(_avro, "decode", _raise_missing)

        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at-no-avro",
            "activity_attempt_id": "aa-no-avro",
            "activity_type": "test-act",
            "arguments": "anything",
            "payload_codec": "avro",
        }
        await worker._run_activity_task(task)
        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert "runtime dependencies" in call_kwargs["message"]
        assert call_kwargs["failure_type"] == "AvroNotInstalledError"
        assert call_kwargs["non_retryable"] is True
        mock_client.complete_activity_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_workflow_json_decode_failure_fails_task(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-bad-json",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": "{not valid json",
            "payload_codec": "json",
        }
        await worker._run_workflow_task(task)
        mock_client.fail_workflow_task.assert_called_once()
        call_kwargs = mock_client.fail_workflow_task.call_args.kwargs
        assert "decode" in call_kwargs["message"].lower()
        assert "json" in call_kwargs["message"]
        mock_client.complete_workflow_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_workflow_avro_missing_dependency_fails_task(
        self, mock_client: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from durable_workflow import _avro
        from durable_workflow.errors import AvroNotInstalledError

        def _raise_missing(_blob: str) -> None:
            raise AvroNotInstalledError(
                "The 'avro' package is required to encode/decode payloads with the 'avro' "
                "codec. Reinstall durable-workflow with its runtime dependencies."
            )

        monkeypatch.setattr(_avro, "decode", _raise_missing)

        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-no-avro",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": "anything",
            "payload_codec": "avro",
        }
        await worker._run_workflow_task(task)
        mock_client.fail_workflow_task.assert_called_once()
        call_kwargs = mock_client.fail_workflow_task.call_args.kwargs
        assert "runtime dependencies" in call_kwargs["message"]
        assert call_kwargs["failure_type"] == "AvroNotInstalledError"
        mock_client.complete_workflow_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_workflow_replay_avro_missing_dependency_fails_task(
        self, mock_client: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Avro-encoded history result that cannot be decoded (dependency missing)
        surfaces as fail_workflow_task, not an unhandled dispatcher exception."""
        from durable_workflow import _avro
        from durable_workflow.errors import AvroNotInstalledError

        def _raise_missing(_blob: str) -> None:
            raise AvroNotInstalledError(
                "The 'avro' package is required to encode/decode payloads with the 'avro' "
                "codec. Reinstall durable-workflow with its runtime dependencies."
            )

        monkeypatch.setattr(_avro, "decode", _raise_missing)

        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        # JSON envelope for start args bypasses the Avro path so the replay
        # decode of history result (under the run's avro codec) is the site
        # that triggers AvroNotInstalledError.
        task = {
            "task_id": "t-replay-no-avro",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [
                {"event_type": "ActivityCompleted", "payload": {"result": "anything"}},
            ],
            "arguments": {"codec": "json", "blob": '["hello"]'},
            "payload_codec": "avro",
        }
        await worker._run_workflow_task(task)
        mock_client.fail_workflow_task.assert_called_once()
        call_kwargs = mock_client.fail_workflow_task.call_args.kwargs
        assert call_kwargs["failure_type"] == "AvroNotInstalledError"
        mock_client.complete_workflow_task.assert_not_called()


class TestWorkerStop:
    @pytest.mark.asyncio
    async def test_stop_sets_event(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[])
        assert not worker._stop.is_set()
        await worker.stop()
        assert worker._stop.is_set()

    @pytest.mark.asyncio
    async def test_stop_drains_in_flight(self, mock_client: AsyncMock) -> None:
        completed = asyncio.Event()

        @activity.defn(name="slow-act")
        async def slow_activity() -> str:
            completed.set()
            await asyncio.sleep(0.1)
            return "done"

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[],
            activities=[slow_activity],
            max_concurrent_activity_tasks=5,
        )
        task = {
            "task_id": "at-slow",
            "activity_attempt_id": "aa-slow",
            "activity_type": "slow-act",
            "arguments": "[]",
            "payload_codec": "json",
        }
        worker._track(worker._dispatch_activity_task(task))
        await completed.wait()
        assert len(worker._in_flight) == 1
        await worker.stop()
        assert len(worker._in_flight) == 0
        mock_client.complete_activity_task.assert_called_once()


class TestWorkerIdGeneration:
    def test_default_id(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1")
        assert worker.worker_id.startswith("py-worker-")

    def test_custom_id(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", worker_id="custom-1")
        assert worker.worker_id == "custom-1"


class TestConcurrencyLimits:
    def test_default_concurrency(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1")
        assert worker._wf_semaphore._value == 10
        assert worker._act_semaphore._value == 10

    def test_custom_concurrency(self, mock_client: AsyncMock) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            max_concurrent_workflow_tasks=3,
            max_concurrent_activity_tasks=7,
        )
        assert worker._wf_semaphore._value == 3
        assert worker._act_semaphore._value == 7

    @pytest.mark.asyncio
    async def test_concurrent_activity_dispatch(self, mock_client: AsyncMock) -> None:
        running = 0
        max_running = 0
        gate = asyncio.Event()

        @activity.defn(name="conc-act")
        async def concurrent_activity() -> str:
            nonlocal running, max_running
            running += 1
            max_running = max(max_running, running)
            await gate.wait()
            running -= 1
            return "ok"

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[],
            activities=[concurrent_activity],
            max_concurrent_activity_tasks=5,
        )

        tasks = []
        for i in range(3):
            task = {
                "task_id": f"at-{i}",
                "activity_attempt_id": f"aa-{i}",
                "activity_type": "conc-act",
                "arguments": "[]",
                "payload_codec": "json",
            }
            tasks.append(worker._track(worker._dispatch_activity_task(task)))

        await asyncio.sleep(0.01)
        assert max_running == 3
        gate.set()
        await asyncio.gather(*tasks)
        assert mock_client.complete_activity_task.call_count == 3

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrency(self, mock_client: AsyncMock) -> None:
        running = 0
        max_running = 0
        gate = asyncio.Event()

        @activity.defn(name="limited-act")
        async def limited_activity() -> str:
            nonlocal running, max_running
            running += 1
            max_running = max(max_running, running)
            await gate.wait()
            running -= 1
            return "ok"

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[],
            activities=[limited_activity],
            max_concurrent_activity_tasks=2,
        )

        async def _acquire_and_dispatch(t: dict[str, object]) -> None:
            await worker._act_semaphore.acquire()
            await worker._dispatch_activity_task(t)

        tasks = []
        for i in range(4):
            task = {
                "task_id": f"at-lim-{i}",
                "activity_attempt_id": f"aa-lim-{i}",
                "activity_type": "limited-act",
                "arguments": "[]",
                "payload_codec": "json",
            }
            tasks.append(worker._track(_acquire_and_dispatch(task)))

        await asyncio.sleep(0.01)
        assert max_running == 2
        gate.set()
        await asyncio.gather(*tasks)
        assert mock_client.complete_activity_task.call_count == 4


class TestPollLoops:
    @pytest.mark.asyncio
    async def test_run_starts_both_loops(self, mock_client: AsyncMock) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            poll_timeout=0.01,
        )
        run_task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.05)
        await worker.stop()
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
        assert mock_client.register_worker.call_count == 1
        assert mock_client.poll_workflow_task.call_count >= 1
        assert mock_client.poll_activity_task.call_count >= 1


class TestRunUntil:
    @pytest.mark.asyncio
    async def test_run_until_returns_terminal_description(self, mock_client: AsyncMock) -> None:
        mock_client.describe_workflow = AsyncMock(
            side_effect=[
                WorkflowExecution(workflow_id="wf-1", run_id="run-1", workflow_type="test-wf", status="running"),
                WorkflowExecution(workflow_id="wf-1", run_id="run-1", workflow_type="test-wf", status="completed"),
            ]
        )
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            poll_timeout=0.01,
        )

        desc = await worker.run_until(workflow_id="wf-1", timeout=1.0, poll_interval=0.01)

        assert desc.status == "completed"
        assert worker._stop.is_set()
        mock_client.register_worker.assert_awaited_once()
        assert mock_client.describe_workflow.await_count == 2

    @pytest.mark.asyncio
    async def test_run_until_times_out_and_stops_worker(self, mock_client: AsyncMock) -> None:
        mock_client.describe_workflow = AsyncMock(
            return_value=WorkflowExecution(
                workflow_id="wf-timeout",
                run_id="run-1",
                workflow_type="test-wf",
                status="running",
            )
        )
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            poll_timeout=0.01,
        )

        with pytest.raises(TimeoutError, match="wf-timeout"):
            await worker.run_until(workflow_id="wf-timeout", timeout=0.02, poll_interval=0.01)

        assert worker._stop.is_set()
