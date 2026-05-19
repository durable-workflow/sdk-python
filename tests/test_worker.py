from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import threading
from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, serializer, workflow
from durable_workflow.auth_composition import (
    AUTH_COMPOSITION_CONTRACT_SCHEMA,
    AUTH_COMPOSITION_CONTRACT_VERSION,
)
from durable_workflow.client import (
    CONTROL_PLANE_REQUEST_CONTRACT_SCHEMA,
    CONTROL_PLANE_REQUEST_CONTRACT_VERSION,
    CONTROL_PLANE_VERSION,
    PROTOCOL_VERSION,
    Client,
    WorkflowExecution,
)
from durable_workflow.errors import InvalidArgument, ServerError, Unauthorized, WorkflowNotFound
from durable_workflow.interceptors import (
    ActivityHandler,
    ActivityInterceptorContext,
    PassthroughWorkerInterceptor,
    QueryTaskHandler,
    QueryTaskInterceptorContext,
    WorkflowTaskHandler,
    WorkflowTaskInterceptorContext,
)
from durable_workflow.worker import (
    Worker,
    _query_history_with_export_signal_arguments,
    _should_fail_workflow_task_after_completion_error,
)


@workflow.defn(name="test-wf")
class TestWorkflow:
    def run(self, ctx, *args):  # type: ignore[no-untyped-def]
        result = yield ctx.schedule_activity("test-act", list(args))
        return result


@workflow.defn(name="fanout-wf")
class FanOutWorkflow:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        yield [
            ctx.schedule_activity("first", ["a"]),
            ctx.schedule_activity("second", ["b"]),
        ]


@workflow.defn(name="two-cross-queue-wf")
class TwoCrossQueueWorkflow:
    def run(self, ctx, request):  # type: ignore[no-untyped-def]
        marker = yield ctx.schedule_activity(
            "external.marker",
            [request],
            queue="external-queue",
        )
        description = yield ctx.schedule_activity(
            "external.describe",
            [marker],
            queue="external-queue",
        )
        return {
            "marker": marker,
            "description": description,
        }


@workflow.defn(name="unserializable-result-wf")
class UnserializableResultWorkflow:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        return object()


@workflow.defn(name="update-wf")
class UpdateWorkflow:
    def __init__(self) -> None:
        self.count = 0

    @workflow.update("increment")
    def increment(self, amount: int) -> dict[str, int]:
        self.count += amount
        return {"count": self.count}

    def run(self, ctx):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("wait", [])
        return self.count


@workflow.defn(name="query-wf")
class QueryWorkflow:
    def __init__(self) -> None:
        self.status = "ready"

    @workflow.query("status")
    def current_status(self) -> dict[str, str]:
        return {"status": self.status}

    def run(self, ctx):  # type: ignore[no-untyped-def]
        return self.status


@workflow.defn(name="query-state-unavailable-wf")
class QueryStateUnavailableWorkflow:
    @workflow.query("status")
    def status_query(self) -> dict[str, str]:
        return {"status": "ready"}

    def run(self, ctx):  # type: ignore[no-untyped-def]
        raise RuntimeError("state not ready")


@workflow.defn(name="counter-query-wf")
class CounterQueryWorkflow:
    def __init__(self) -> None:
        self.count = 0

    @workflow.signal("increment")
    def increment(self, amount: int) -> None:
        self.count += amount

    @workflow.query("current")
    def current(self) -> int:
        return self.count

    def run(self, ctx):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: False, key="done")


@workflow.defn(name="async-query-wf")
class AsyncQueryWorkflow:
    @workflow.query("current")
    async def current(self) -> int:
        await asyncio.sleep(0)
        return 0

    def run(self, ctx):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: False)


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
    client.heartbeat_worker = AsyncMock(
        return_value={"worker_id": "w1", "acknowledged": True, "heartbeat_interval_seconds": 60}
    )
    client.poll_workflow_task = AsyncMock(return_value=None)
    client.poll_activity_task = AsyncMock(return_value=None)
    client.poll_query_task = AsyncMock(return_value=None)
    client.complete_workflow_task = AsyncMock(return_value={"outcome": "completed"})
    client.complete_activity_task = AsyncMock(return_value={"outcome": "completed"})
    client.complete_query_task = AsyncMock(return_value={"outcome": "completed"})
    client.fail_workflow_task = AsyncMock(return_value={"outcome": "failed"})
    client.fail_activity_task = AsyncMock(return_value={"outcome": "failed"})
    client.fail_query_task = AsyncMock(return_value={"outcome": "failed"})
    client.get_cluster_info = AsyncMock(return_value=compatible_cluster_info())
    return client


def compatible_cluster_info(**overrides: object) -> dict[str, object]:
    info: dict[str, object] = {
        "version": "not-authoritative",
        "auth_composition_contract": {
            "schema": AUTH_COMPOSITION_CONTRACT_SCHEMA,
            "version": AUTH_COMPOSITION_CONTRACT_VERSION,
            "precedence": {
                "connection_values": ["flag", "environment", "selected_profile", "default"],
                "profile_selection": ["flag_env", "DW_ENV", "current_profile", "default_profile"],
            },
            "canonical_environment": {
                "server_url": "DURABLE_WORKFLOW_SERVER_URL",
                "namespace": "DURABLE_WORKFLOW_NAMESPACE",
                "auth_token": "DURABLE_WORKFLOW_AUTH_TOKEN",
                "tls_verify": "DURABLE_WORKFLOW_TLS_VERIFY",
                "profile": "DW_ENV",
            },
            "auth_material": {
                "token": {"status": "supported", "effective_config_value": "redacted"},
                "mtls": {"status": "reserved"},
                "signed_headers": {"status": "reserved"},
            },
            "effective_config": {
                "required_fields": ["server_url", "namespace", "profile", "auth", "tls", "identity"],
            },
            "redaction": {
                "never_echo": ["bearer_tokens", "private_keys", "raw_authorization_headers"],
            },
        },
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
            "server_capabilities": {
                "query_tasks": True,
            },
        },
    }
    info.update(overrides)
    return info


class TestWorkflowTaskCompletionErrorClassification:
    @pytest.mark.parametrize(
        ("error", "should_fail"),
        [
            (TimeoutError("completion timed out"), False),
            (RuntimeError("connection reset"), False),
            (ServerError(409, {"reason": "lease_expired"}), False),
            (ServerError(409, {"reason": "workflow_task_attempt_mismatch"}), False),
            (ServerError(429, {"reason": "rate_limited"}), False),
            (ServerError(503, {"reason": "server_busy"}), False),
            (ServerError(409, {"reason": "invalid_commands"}), True),
            (InvalidArgument("invalid command payload"), True),
            (Unauthorized("missing bearer token"), True),
            (WorkflowNotFound("wf-missing"), True),
        ],
    )
    def test_classifies_definite_and_ambiguous_completion_errors(
        self, error: BaseException, should_fail: bool
    ) -> None:
        assert _should_fail_workflow_task_after_completion_error(error) is should_fail


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
        assert call_kwargs["workflow_definition_fingerprints"]["test-wf"].startswith("sha256:")
        assert "test-act" in call_kwargs["supported_activity_types"]
        assert call_kwargs["max_concurrent_workflow_tasks"] == 10
        assert call_kwargs["max_concurrent_activity_tasks"] == 10
        assert call_kwargs["capabilities"] == ["query_tasks"]
        assert call_kwargs["task_slots"] == {
            "workflow_available": 10,
            "activity_available": 10,
        }
        process_metrics = call_kwargs["process_metrics"]
        assert process_metrics["process_id"] > 0
        assert "process_started_at" in process_metrics

    @pytest.mark.asyncio
    async def test_register_omits_query_task_capability_when_server_does_not_support_it(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(worker_protocol={"version": PROTOCOL_VERSION})
        )
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            worker_id="w-without-query-capability",
        )

        await worker._register()

        call_kwargs = mock_client.register_worker.call_args.kwargs
        assert call_kwargs["capabilities"] is None

    @pytest.mark.asyncio
    async def test_register_advertises_custom_concurrency_limits(self, mock_client: AsyncMock) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            worker_id="w-capacity",
            max_concurrent_workflow_tasks=3,
            max_concurrent_activity_tasks=7,
        )
        await worker._register()
        call_kwargs = mock_client.register_worker.call_args.kwargs
        assert call_kwargs["max_concurrent_workflow_tasks"] == 3
        assert call_kwargs["max_concurrent_activity_tasks"] == 7

    @pytest.mark.asyncio
    async def test_register_forwards_build_id_when_configured(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            worker_id="w-build",
            build_id="release-2026.04.22-a1",
        )
        assert worker.build_id == "release-2026.04.22-a1"

        await worker._register()

        call_kwargs = mock_client.register_worker.call_args.kwargs
        assert call_kwargs["build_id"] == "release-2026.04.22-a1"

    @pytest.mark.asyncio
    async def test_register_omits_build_id_when_not_configured(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            worker_id="w-no-build",
        )
        assert worker.build_id is None

        await worker._register()

        call_kwargs = mock_client.register_worker.call_args.kwargs
        assert call_kwargs["build_id"] is None

    def test_constructor_rejects_empty_build_id(self, mock_client: AsyncMock) -> None:
        with pytest.raises(ValueError, match="build_id"):
            Worker(mock_client, task_queue="q1", build_id="")

        with pytest.raises(ValueError, match="build_id"):
            Worker(mock_client, task_queue="q1", build_id="   ")

    def test_constructor_rejects_non_positive_concurrency_limits(
        self, mock_client: AsyncMock
    ) -> None:
        with pytest.raises(ValueError, match="max_concurrent_workflow_tasks"):
            Worker(mock_client, task_queue="q1", max_concurrent_workflow_tasks=0)

        with pytest.raises(ValueError, match="max_concurrent_activity_tasks"):
            Worker(mock_client, task_queue="q1", max_concurrent_activity_tasks=0)

    def test_constructor_rejects_changed_workflow_definition_for_same_worker_id(
        self, mock_client: AsyncMock
    ) -> None:
        @workflow.defn(name="reloadable-wf")
        class ReloadableWorkflowV1:
            def run(self, ctx):  # type: ignore[no-untyped-def]
                return "v1"

        @workflow.defn(name="reloadable-wf")
        class ReloadableWorkflowV2:
            def run(self, ctx):  # type: ignore[no-untyped-def]
                return "v2"

        Worker(
            mock_client,
            task_queue="q1",
            workflows=[ReloadableWorkflowV1],
            activities=[],
            worker_id="reload-worker",
        )

        with pytest.raises(RuntimeError, match="Workflow definition changed"):
            Worker(
                mock_client,
                task_queue="q1",
                workflows=[ReloadableWorkflowV2],
                activities=[],
                worker_id="reload-worker",
            )

    def test_constructor_allows_same_workflow_definition_for_same_worker_id(
        self, mock_client: AsyncMock
    ) -> None:
        Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[],
            worker_id="stable-worker",
        )
        Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[],
            worker_id="stable-worker",
        )

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
    async def test_register_rejects_worker_protocol_major_mismatch(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(worker_protocol={"version": "2.0"})
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match="incompatible worker_protocol.version"):
            await worker._register()
        mock_client.register_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_accepts_higher_compatible_minor_protocol(self, mock_client: AsyncMock) -> None:
        # Server is one minor ahead of the SDK. MINOR bumps in workflow:v2's
        # WorkerProtocolVersion are documented as additive (new optional
        # fields, new non-terminal command types) so the SDK must talk to a
        # newer server happily — the test pins that contract.
        major, minor = (int(p) for p in PROTOCOL_VERSION.split("."))
        future_version = f"{major}.{minor + 1}"
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(worker_protocol={"version": future_version})
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        await worker._register()
        mock_client.register_worker.assert_called_once()

    @pytest.mark.asyncio
    async def test_register_rejects_worker_protocol_below_payload_codec_floor(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(worker_protocol={"version": "1.0"})
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match=r"minor>='1\.1'"):
            await worker._register()
        mock_client.register_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_accepts_protocol_1_1_when_newer_feature_floor_is_unavailable(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(
                worker_protocol={
                    "version": PROTOCOL_VERSION,
                    "server_capabilities": {
                        "query_tasks": True,
                        "worker_session_verbs": [],
                        "worker_sessions": {
                            "feature": "worker_sessions",
                            "supported": False,
                            "minimum_protocol_version": "1.2",
                            "unavailable_reason": "worker_protocol_version_below_worker_session_minimum",
                        },
                    },
                }
            )
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])

        await worker._register()

        mock_client.register_worker.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_register_rejects_missing_auth_composition_contract(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(return_value=compatible_cluster_info(auth_composition_contract=None))
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match="missing auth_composition_contract"):
            await worker._register()
        mock_client.register_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_rejects_unsupported_auth_composition_contract(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(
                auth_composition_contract={"schema": AUTH_COMPOSITION_CONTRACT_SCHEMA, "version": 2}
            )
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match="unsupported auth_composition_contract"):
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
    async def test_workflow_task_ambiguous_completion_error_preserves_commands(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.complete_workflow_task.side_effect = ServerError(409, {"reason": "task_not_leased"})
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-complete-not-leased",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 2,
            "history_events": [],
            "arguments": '["hello"]',
            "payload_codec": "json",
        }

        result = await worker._run_workflow_task(task)

        assert result is not None
        assert result[0]["type"] == "schedule_activity"
        mock_client.complete_workflow_task.assert_awaited_once()
        mock_client.fail_workflow_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_workflow_task_retries_transient_completion_error(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.complete_workflow_task.side_effect = [
            TimeoutError("completion timed out"),
            {"outcome": "completed"},
        ]
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-complete-retry",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 2,
            "history_events": [],
            "arguments": '["hello"]',
            "payload_codec": "json",
        }

        result = await worker._run_workflow_task(task)

        assert result is not None
        assert result[0]["type"] == "schedule_activity"
        assert mock_client.complete_workflow_task.await_count == 2
        mock_client.fail_workflow_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_workflow_task_definite_completion_rejection_fails_task(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.complete_workflow_task.side_effect = ServerError(409, {"reason": "invalid_commands"})
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-complete-invalid",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 2,
            "history_events": [],
            "arguments": '["hello"]',
            "payload_codec": "json",
        }

        result = await worker._run_workflow_task(task)

        assert result is None
        mock_client.fail_workflow_task.assert_awaited_once()
        call_kwargs = mock_client.fail_workflow_task.await_args.kwargs
        assert call_kwargs["task_id"] == "t-complete-invalid"
        assert call_kwargs["workflow_task_attempt"] == 2
        assert call_kwargs["lease_owner"] == worker.worker_id
        assert call_kwargs["failure_type"] == "ServerError"
        assert "invalid_commands" in call_kwargs["message"]

    @pytest.mark.parametrize(
        ("completion_error", "failure_type", "message_fragment"),
        [
            (Unauthorized("missing bearer token"), "Unauthorized", "missing bearer token"),
            (WorkflowNotFound("wf-typed-missing"), "WorkflowNotFound", "wf-typed-missing"),
        ],
    )
    @pytest.mark.asyncio
    async def test_workflow_task_typed_completion_rejection_fails_task(
        self,
        mock_client: AsyncMock,
        completion_error: Exception,
        failure_type: str,
        message_fragment: str,
    ) -> None:
        mock_client.complete_workflow_task.side_effect = completion_error
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-complete-typed-rejection",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 2,
            "history_events": [],
            "arguments": '["hello"]',
            "payload_codec": "json",
        }

        result = await worker._run_workflow_task(task)

        assert result is None
        mock_client.complete_workflow_task.assert_awaited_once()
        mock_client.fail_workflow_task.assert_awaited_once()
        call_kwargs = mock_client.fail_workflow_task.await_args.kwargs
        assert call_kwargs["task_id"] == "t-complete-typed-rejection"
        assert call_kwargs["workflow_task_attempt"] == 2
        assert call_kwargs["lease_owner"] == worker.worker_id
        assert call_kwargs["failure_type"] == failure_type
        assert message_fragment in call_kwargs["message"]

    @pytest.mark.asyncio
    async def test_workflow_task_definite_completion_rejection_stays_failed_when_report_fails(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.complete_workflow_task.side_effect = ServerError(409, {"reason": "invalid_commands"})
        mock_client.fail_workflow_task.side_effect = RuntimeError("failure report unavailable")
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-complete-invalid-report-fails",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 2,
            "history_events": [],
            "arguments": '["hello"]',
            "payload_codec": "json",
        }

        result = await worker._run_workflow_task(task)

        assert result is None
        mock_client.complete_workflow_task.assert_awaited_once()
        mock_client.fail_workflow_task.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_workflow_command_payload_warning_uses_client_policy(
        self, mock_client: AsyncMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_client.namespace = "ns1"
        mock_client.payload_size_warning_config = serializer.PayloadSizeWarningConfig(
            limit_bytes=10,
            threshold_percent=50,
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-large",
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": '["this payload is intentionally large"]',
            "payload_codec": "json",
        }

        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            await worker._run_workflow_task(task)

        payload = caplog.records[0].durable_workflow_payload
        assert payload["kind"] == "activity_input"
        assert payload["workflow_id"] == "wf-1"
        assert payload["run_id"] == "run-1"
        assert payload["activity_name"] == "test-act"
        assert payload["task_queue"] == "q1"
        assert payload["namespace"] == "ns1"
        assert payload["threshold_bytes"] == 5

    @pytest.mark.asyncio
    async def test_workflow_command_payload_warning_can_be_disabled(
        self, mock_client: AsyncMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_client.payload_size_warning_config = None
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-large-disabled",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": '["this payload is intentionally large"]',
            "payload_codec": "json",
        }

        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            await worker._run_workflow_task(task)

        assert caplog.records == []

    @pytest.mark.asyncio
    async def test_fanout_workflow_commands_use_batch_payload_envelopes(
        self, mock_client: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[list[object]] = []
        original = serializer.envelope_many

        def spy_envelope_many(values, *args, **kwargs):  # type: ignore[no-untyped-def]
            captured = list(values)
            calls.append(captured)
            return original(captured, *args, **kwargs)

        monkeypatch.setattr(serializer, "envelope_many", spy_envelope_many)

        worker = Worker(mock_client, task_queue="q1", workflows=[FanOutWorkflow], activities=[])
        task = {
            "task_id": "t-fanout",
            "workflow_type": "fanout-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": "[]",
            "payload_codec": "json",
        }

        await worker._run_workflow_task(task)

        commands = mock_client.complete_workflow_task.call_args.kwargs["commands"]
        assert [command["activity_type"] for command in commands] == ["first", "second"]
        assert calls == [[["a"], ["b"]]]

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
    async def test_cross_queue_second_activity_uses_completed_first_result(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="workflow-queue", workflows=[TwoCrossQueueWorkflow], activities=[])
        marker = {"runtime": "external", "name": "Grace", "message": "hello"}
        task = {
            "task_id": "t-cross-queue-second",
            "workflow_type": "two-cross-queue-wf",
            "workflow_task_attempt": 2,
            "history_events": [
                {
                    "event_type": "ActivityCompleted",
                    "payload": {
                        "sequence": 1,
                        "activity_type": "external.marker",
                        "payload_codec": "json",
                        "result": serializer.envelope(marker, codec="json"),
                    },
                },
            ],
            "arguments": serializer.encode([{"name": "Grace"}], codec="json"),
            "payload_codec": "json",
        }

        await worker._run_workflow_task(task)

        mock_client.complete_workflow_task.assert_called_once()
        commands = mock_client.complete_workflow_task.call_args.kwargs["commands"]
        assert commands == [
            {
                "type": "schedule_activity",
                "activity_type": "external.describe",
                "queue": "external-queue",
                "arguments": commands[0]["arguments"],
            }
        ]
        assert commands[0]["arguments"]["codec"] == "json"
        assert serializer.decode(commands[0]["arguments"]["blob"], codec="json") == [marker]

    @pytest.mark.asyncio
    async def test_cross_queue_workflow_completes_after_second_activity(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="workflow-queue", workflows=[TwoCrossQueueWorkflow], activities=[])
        marker = {"runtime": "external", "name": "Grace", "message": "hello"}
        description = {"runtime": "external", "description": "Grace handled by external activity"}
        task = {
            "task_id": "t-cross-queue-complete",
            "workflow_type": "two-cross-queue-wf",
            "workflow_task_attempt": 3,
            "history_events": [
                {
                    "event_type": "ActivityCompleted",
                    "payload": {
                        "sequence": 1,
                        "activity_type": "external.marker",
                        "payload_codec": "json",
                        "result": serializer.envelope(marker, codec="json"),
                    },
                },
                {
                    "event_type": "ActivityCompleted",
                    "payload": {
                        "sequence": 2,
                        "activity_type": "external.describe",
                        "payload_codec": "json",
                        "result": serializer.envelope(description, codec="json"),
                    },
                },
            ],
            "arguments": serializer.encode([{"name": "Grace"}], codec="json"),
            "payload_codec": "json",
        }

        await worker._run_workflow_task(task)

        mock_client.complete_workflow_task.assert_called_once()
        commands = mock_client.complete_workflow_task.call_args.kwargs["commands"]
        assert commands[0]["type"] == "complete_workflow"
        assert commands[0]["result"]["codec"] == "json"
        assert serializer.decode(commands[0]["result"]["blob"], codec="json") == {
            "marker": marker,
            "description": description,
        }

    @pytest.mark.asyncio
    async def test_dispatch_reports_unhandled_workflow_task_error(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[UnserializableResultWorkflow],
            activities=[],
            worker_id="w-unserializable",
        )
        task = {
            "task_id": "t-unserializable",
            "workflow_type": "unserializable-result-wf",
            "workflow_task_attempt": 4,
            "history_events": [],
            "arguments": "[]",
            "payload_codec": "json",
        }

        await worker._dispatch_workflow_task(task)

        mock_client.complete_workflow_task.assert_not_called()
        mock_client.fail_workflow_task.assert_awaited_once()
        call_kwargs = mock_client.fail_workflow_task.await_args.kwargs
        assert call_kwargs["task_id"] == "t-unserializable"
        assert call_kwargs["workflow_task_attempt"] == 4
        assert call_kwargs["lease_owner"] == worker.worker_id
        assert call_kwargs["failure_type"] == "TypeError"
        assert "unhandled workflow task execution error" in call_kwargs["message"]
        assert "Object of type object" in call_kwargs["message"]

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

    @pytest.mark.asyncio
    async def test_update_backed_workflow_task_completes_update_command(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[UpdateWorkflow], activities=[])
        task = {
            "task_id": "t-update",
            "workflow_type": "update-wf",
            "workflow_task_attempt": 1,
            "workflow_update_id": "upd-worker-1",
            "workflow_wait_kind": "update",
            "history_events": [
                {
                    "event_type": "UpdateAccepted",
                    "payload": {
                        "update_id": "upd-worker-1",
                        "update_name": "increment",
                        "arguments": serializer.encode([6], codec="json"),
                        "payload_codec": "json",
                    },
                },
            ],
            "arguments": "[]",
            "payload_codec": "json",
        }

        await worker._run_workflow_task(task)

        mock_client.complete_workflow_task.assert_called_once()
        commands = mock_client.complete_workflow_task.call_args.kwargs["commands"]
        assert commands == [
            {
                "type": "complete_update",
                "update_id": "upd-worker-1",
                "result": {
                    "codec": "json",
                    "blob": '{"count":6}',
                },
            },
        ]
        mock_client.fail_workflow_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_task_ambiguous_completion_error_preserves_command(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.complete_workflow_task.side_effect = ServerError(409, {"reason": "task_not_leased"})
        worker = Worker(mock_client, task_queue="q1", workflows=[UpdateWorkflow], activities=[])
        task = {
            "task_id": "t-update-not-leased",
            "workflow_type": "update-wf",
            "workflow_task_attempt": 3,
            "workflow_update_id": "upd-worker-1",
            "workflow_wait_kind": "update",
            "history_events": [
                {
                    "event_type": "UpdateAccepted",
                    "payload": {
                        "update_id": "upd-worker-1",
                        "update_name": "increment",
                        "arguments": serializer.encode([6], codec="json"),
                        "payload_codec": "json",
                    },
                },
            ],
            "arguments": "[]",
            "payload_codec": "json",
        }

        result = await worker._run_workflow_task(task)

        assert result is not None
        assert result[0]["type"] == "complete_update"
        mock_client.complete_workflow_task.assert_awaited_once()
        mock_client.fail_workflow_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_task_retries_transient_completion_error(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.complete_workflow_task.side_effect = [
            ServerError(503, {"reason": "server_busy"}),
            {"outcome": "completed"},
        ]
        worker = Worker(mock_client, task_queue="q1", workflows=[UpdateWorkflow], activities=[])
        task = {
            "task_id": "t-update-retry",
            "workflow_type": "update-wf",
            "workflow_task_attempt": 3,
            "workflow_update_id": "upd-worker-1",
            "workflow_wait_kind": "update",
            "history_events": [
                {
                    "event_type": "UpdateAccepted",
                    "payload": {
                        "update_id": "upd-worker-1",
                        "update_name": "increment",
                        "arguments": serializer.encode([6], codec="json"),
                        "payload_codec": "json",
                    },
                },
            ],
            "arguments": "[]",
            "payload_codec": "json",
        }

        result = await worker._run_workflow_task(task)

        assert result is not None
        assert result[0]["type"] == "complete_update"
        assert mock_client.complete_workflow_task.await_count == 2
        mock_client.fail_workflow_task.assert_not_called()

    def test_query_history_enrichment_copies_signal_workflow_sequence_from_export(self) -> None:
        history = [
            {
                "event_type": "SignalReceived",
                "workflow_command_id": "cmd-finish",
                "payload": {
                    "signal_id": "sig-finish",
                    "workflow_command_id": "cmd-finish",
                    "signal_name": "finish",
                },
            },
        ]
        export = {
            "payloads": {"codec": "json"},
            "signals": [
                {
                    "id": "sig-finish",
                    "command_id": "cmd-finish",
                    "name": "finish",
                    "workflow_sequence": 2,
                    "payload_codec": "json",
                    "arguments": serializer.encode([], codec="json"),
                },
            ],
        }

        enriched = _query_history_with_export_signal_arguments(history, export, default_codec="json")

        assert isinstance(enriched, list)
        payload = enriched[0]["payload"]
        assert payload["workflow_sequence"] == 2
        assert payload["arguments"]["codec"] == "json"

    @pytest.mark.asyncio
    async def test_query_task_executes_registered_query(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[QueryWorkflow], activities=[])
        task = {
            "query_task_id": "qt1",
            "query_task_attempt": 1,
            "workflow_type": "query-wf",
            "query_name": "status",
            "history_events": [],
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "payload_codec": "json",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt1",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result={"status": "ready"},
            codec="json",
            workflow_id=None,
            run_id=None,
            query_name="status",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_replays_signal_arguments_from_history_export(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[CounterQueryWorkflow], activities=[])
        signal_arguments = serializer.encode([3], codec="json")
        task = {
            "query_task_id": "qt-signal-export",
            "query_task_attempt": 1,
            "workflow_type": "counter-query-wf",
            "workflow_id": "wf-counter",
            "run_id": "run-counter",
            "query_name": "current",
            "history_events": [
                {
                    "event_type": "SignalReceived",
                    "workflow_command_id": "cmd-increment",
                    "payload": {
                        "signal_id": "sig-increment",
                        "workflow_command_id": "cmd-increment",
                        "signal_name": "increment",
                    },
                },
            ],
            "history_export": {
                "payloads": {"codec": "json"},
                "signals": [
                    {
                        "id": "sig-increment",
                        "command_id": "cmd-increment",
                        "name": "increment",
                        "payload_codec": "json",
                        "arguments": signal_arguments,
                    },
                ],
            },
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "payload_codec": "json",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-signal-export",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result=3,
            codec="json",
            workflow_id="wf-counter",
            run_id="run-counter",
            query_name="current",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_replays_repeated_condition_wait_signal_arguments(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[CounterQueryWorkflow], activities=[])
        first_signal_arguments = serializer.encode([3], codec="json")
        second_signal_arguments = serializer.encode([5], codec="json")
        task = {
            "query_task_id": "qt-repeated-wait-signals",
            "query_task_attempt": 1,
            "workflow_type": "counter-query-wf",
            "workflow_id": "wf-counter",
            "run_id": "run-counter",
            "query_name": "current",
            "history_events": [
                {
                    "event_type": "ConditionWaitOpened",
                    "payload": {
                        "condition_wait_id": "wait-count-3",
                        "condition_key": "done",
                    },
                },
                {
                    "event_type": "SignalReceived",
                    "workflow_command_id": "cmd-increment-3",
                    "payload": {
                        "signal_id": "sig-increment-3",
                        "workflow_command_id": "cmd-increment-3",
                        "signal_name": "increment",
                    },
                },
                {
                    "event_type": "ConditionWaitOpened",
                    "payload": {
                        "condition_wait_id": "wait-count-8",
                        "condition_key": "done",
                    },
                },
                {
                    "event_type": "SignalReceived",
                    "workflow_command_id": "cmd-increment-5",
                    "payload": {
                        "signal_id": "sig-increment-5",
                        "workflow_command_id": "cmd-increment-5",
                        "signal_name": "increment",
                    },
                },
            ],
            "history_export": {
                "payloads": {"codec": "json"},
                "signals": [
                    {
                        "id": "sig-increment-3",
                        "command_id": "cmd-increment-3",
                        "name": "increment",
                        "payload_codec": "json",
                        "arguments": first_signal_arguments,
                    },
                    {
                        "id": "sig-increment-5",
                        "command_id": "cmd-increment-5",
                        "name": "increment",
                        "payload_codec": "json",
                        "arguments": second_signal_arguments,
                    },
                ],
            },
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "payload_codec": "json",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-repeated-wait-signals",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result=8,
            codec="json",
            workflow_id="wf-counter",
            run_id="run-counter",
            query_name="current",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_awaits_async_query_result(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[AsyncQueryWorkflow], activities=[])
        task = {
            "query_task_id": "qt-async",
            "query_task_attempt": 1,
            "workflow_type": "async-query-wf",
            "query_name": "current",
            "history_events": [],
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "payload_codec": "json",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-async",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result=0,
            codec="json",
            workflow_id=None,
            run_id=None,
            query_name="current",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_reports_unknown_query(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[QueryWorkflow], activities=[])
        task = {
            "query_task_id": "qt-missing",
            "query_task_attempt": 2,
            "workflow_type": "query-wf",
            "query_name": "missing",
            "history_events": [],
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "payload_codec": "json",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "failed"
        mock_client.fail_query_task.assert_awaited_once()
        call_kwargs = mock_client.fail_query_task.call_args.kwargs
        assert call_kwargs["query_task_id"] == "qt-missing"
        assert call_kwargs["query_task_attempt"] == 2
        assert call_kwargs["reason"] == "rejected_unknown_query"

    @pytest.mark.asyncio
    async def test_query_task_reports_state_unavailable_when_replay_fails(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[QueryStateUnavailableWorkflow], activities=[])
        task = {
            "query_task_id": "qt-state-unavailable",
            "query_task_attempt": 1,
            "workflow_type": "query-state-unavailable-wf",
            "query_name": "status",
            "history_events": [],
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "payload_codec": "json",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "failed"
        mock_client.fail_query_task.assert_awaited_once()
        call_kwargs = mock_client.fail_query_task.call_args.kwargs
        assert call_kwargs["query_task_id"] == "qt-state-unavailable"
        assert call_kwargs["reason"] == "query_workflow_state_unavailable"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reason",
        ["lease_expired", "query_task_not_leased", "query_task_timed_out"],
    )
    async def test_query_task_completion_rejection_after_server_timeout_is_handled(
        self, mock_client: AsyncMock, reason: str
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[QueryWorkflow], activities=[])
        mock_client.complete_query_task.side_effect = ServerError(409, {"reason": reason})
        task = {
            "query_task_id": "qt-late",
            "query_task_attempt": 1,
            "workflow_type": "query-wf",
            "query_name": "status",
            "history_events": [],
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "payload_codec": "json",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "expired"
        mock_client.complete_query_task.assert_awaited_once()
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_reports_query_result_completion_failure(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[QueryWorkflow], activities=[])
        mock_client.complete_query_task.side_effect = TypeError("Object is not payload safe")
        task = {
            "query_task_id": "qt-result-encode-failure",
            "query_task_attempt": 1,
            "workflow_type": "query-wf",
            "query_name": "status",
            "history_events": [],
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "payload_codec": "json",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "failed"
        mock_client.fail_query_task.assert_awaited_once()
        call_kwargs = mock_client.fail_query_task.call_args.kwargs
        assert call_kwargs["query_task_id"] == "qt-result-encode-failure"
        assert call_kwargs["query_task_attempt"] == 1
        assert call_kwargs["reason"] == "query_result_encode_failed"
        assert call_kwargs["failure_type"] == "TypeError"


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


class TestWorkerInterceptors:
    @pytest.mark.asyncio
    async def test_workflow_task_interceptors_wrap_in_order(self, mock_client: AsyncMock) -> None:
        events: list[str] = []

        class Recorder(PassthroughWorkerInterceptor):
            def __init__(self, name: str) -> None:
                self.name = name

            async def execute_workflow_task(
                self,
                context: WorkflowTaskInterceptorContext,
                next: WorkflowTaskHandler,
            ) -> list[dict[str, object]] | None:
                events.append(f"{self.name}:before:{context.task['task_id']}")
                result = await next(context)
                events.append(f"{self.name}:after:{len(result or [])}")
                return result

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[],
            interceptors=[Recorder("outer"), Recorder("inner")],
        )
        task = {
            "task_id": "t-intercept",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": '["hello"]',
            "payload_codec": "json",
        }

        await worker._run_workflow_task(task)

        assert events == [
            "outer:before:t-intercept",
            "inner:before:t-intercept",
            "inner:after:1",
            "outer:after:1",
        ]

    @pytest.mark.asyncio
    async def test_activity_interceptor_observes_result_and_exception(
        self, mock_client: AsyncMock
    ) -> None:
        events: list[str] = []

        @activity.defn(name="boom-act")
        def boom_activity() -> None:
            raise RuntimeError("boom")

        class Recorder(PassthroughWorkerInterceptor):
            async def execute_activity(
                self,
                context: ActivityInterceptorContext,
                next: ActivityHandler,
            ) -> object:
                events.append(f"before:{context.activity_type}:{context.args!r}")
                try:
                    result = await next(context)
                except Exception as e:
                    events.append(f"exception:{type(e).__name__}:{e}")
                    raise
                events.append(f"after:{result}")
                return result

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[],
            activities=[echo_activity, boom_activity],
            interceptors=[Recorder()],
        )

        await worker._run_activity_task(
            {
                "task_id": "at-intercept-ok",
                "activity_attempt_id": "aa-intercept-ok",
                "activity_type": "test-act",
                "arguments": '["hello"]',
                "payload_codec": "json",
            }
        )
        await worker._run_activity_task(
            {
                "task_id": "at-intercept-boom",
                "activity_attempt_id": "aa-intercept-boom",
                "activity_type": "boom-act",
                "arguments": "[]",
                "payload_codec": "json",
            }
        )

        assert events == [
            "before:test-act:('hello',)",
            "after:result-hello",
            "before:boom-act:()",
            "exception:RuntimeError:boom",
        ]
        assert mock_client.complete_activity_task.call_count == 1
        assert mock_client.fail_activity_task.call_count == 1

    @pytest.mark.asyncio
    async def test_query_task_interceptor_can_wrap_query_execution(self, mock_client: AsyncMock) -> None:
        events: list[str] = []

        class Recorder(PassthroughWorkerInterceptor):
            async def execute_query_task(
                self,
                context: QueryTaskInterceptorContext,
                next: QueryTaskHandler,
            ) -> str:
                events.append(f"before:{context.task['query_task_id']}")
                outcome = await next(context)
                events.append(f"after:{outcome}")
                return outcome

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[QueryWorkflow],
            activities=[],
            interceptors=[Recorder()],
        )

        await worker._run_query_task(
            {
                "query_task_id": "qt-intercept",
                "query_task_attempt": 1,
                "workflow_type": "query-wf",
                "query_name": "status",
                "history_events": [],
                "workflow_arguments": serializer.envelope([], codec="json"),
                "query_arguments": serializer.envelope([], codec="json"),
                "payload_codec": "json",
            }
        )

        assert events == ["before:qt-intercept", "after:completed"]


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
    """Codec decode failures at the task boundary must fail tasks deterministically."""

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
    async def test_activity_unsupported_payload_codec_fails_before_handler(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at-unsupported-codec",
            "activity_attempt_id": "aa-unsupported-codec",
            "activity_type": "test-act",
            "arguments": {"codec": "json", "blob": '["hello"]'},
            "payload_codec": "zstd",
        }

        outcome = await worker._run_activity_task(task)

        assert outcome == "decode_error"
        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert "Unsupported payload codec 'zstd'" in call_kwargs["message"]
        assert call_kwargs["failure_type"] == "ValueError"
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
    async def test_workflow_unsupported_payload_codec_fails_before_replay(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t-unsupported-codec",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": {"codec": "json", "blob": '["hello"]'},
            "payload_codec": "zstd",
        }

        commands = await worker._run_workflow_task(task)

        assert commands is None
        mock_client.fail_workflow_task.assert_called_once()
        call_kwargs = mock_client.fail_workflow_task.call_args.kwargs
        assert "Unsupported payload codec 'zstd'" in call_kwargs["message"]
        assert call_kwargs["failure_type"] == "ValueError"
        mock_client.complete_workflow_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_unsupported_payload_codec_fails_before_query_handler(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[QueryWorkflow], activities=[])
        task = {
            "query_task_id": "qt-unsupported-codec",
            "query_task_attempt": 1,
            "workflow_type": "query-wf",
            "query_name": "status",
            "history_events": [],
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "payload_codec": "zstd",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "failed"
        mock_client.fail_query_task.assert_called_once()
        call_kwargs = mock_client.fail_query_task.call_args.kwargs
        assert call_kwargs["reason"] == "query_payload_decode_failed"
        assert "Unsupported payload codec 'zstd'" in call_kwargs["message"]
        assert call_kwargs["failure_type"] == "ValueError"
        mock_client.complete_query_task.assert_not_called()

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
        assert mock_client.poll_query_task.call_count >= 1

    @pytest.mark.asyncio
    async def test_run_skips_query_loop_without_query_task_capability(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(worker_protocol={"version": PROTOCOL_VERSION})
        )
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
        mock_client.poll_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_thread_processes_tasks_while_event_loop_is_blocked(
        self, mock_client: AsyncMock
    ) -> None:
        completed = threading.Event()
        query_task = {
            "query_task_id": "qt-thread",
            "query_task_attempt": 1,
            "workflow_type": "query-wf",
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "query_name": "status",
            "payload_codec": "json",
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "history_events": [],
        }

        class QueryThreadClient:
            def __init__(self) -> None:
                self.polled = False
                self.completed_kwargs: dict[str, object] | None = None

            async def __aenter__(self) -> "QueryThreadClient":
                return self

            async def __aexit__(self, *_: object) -> None:
                return None

            async def poll_query_task(self, **_: object) -> dict[str, object] | None:
                if not self.polled:
                    self.polled = True
                    return query_task
                await asyncio.sleep(0.01)
                return None

            async def complete_query_task(self, **kwargs: object) -> dict[str, str]:
                self.completed_kwargs = kwargs
                completed.set()
                return {"outcome": "completed"}

            async def fail_query_task(self, **_: object) -> dict[str, str]:
                raise AssertionError("query task should complete")

        query_client = QueryThreadClient()
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[QueryWorkflow],
            activities=[],
            poll_timeout=0.01,
            shutdown_timeout=0.2,
        )
        worker._clone_client_for_query_tasks = lambda: query_client  # type: ignore[method-assign]

        worker._start_query_task_thread()

        assert completed.wait(timeout=1.0)
        await worker.stop()

        assert query_client.completed_kwargs is not None
        assert query_client.completed_kwargs["query_task_id"] == "qt-thread"
        assert query_client.completed_kwargs["result"] == {"status": "ready"}


class TestWorkerHeartbeats:
    @pytest.mark.asyncio
    async def test_run_drives_periodic_heartbeats_with_slot_state(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            max_concurrent_workflow_tasks=4,
            max_concurrent_activity_tasks=2,
            poll_timeout=0.01,
            heartbeat_interval=0.05,
        )
        run_task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.2)
        await worker.stop()
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task

        assert mock_client.heartbeat_worker.call_count >= 1
        kwargs = mock_client.heartbeat_worker.call_args.kwargs
        assert kwargs["worker_id"] == worker.worker_id
        assert kwargs["task_slots"]["workflow_available"] == 4
        assert kwargs["task_slots"]["activity_available"] == 2
        process_metrics = kwargs["process_metrics"]
        assert "process_id" in process_metrics
        assert process_metrics["process_id"] > 0
        assert "process_uptime_seconds" in process_metrics
        assert "process_started_at" in process_metrics

    @pytest.mark.asyncio
    async def test_register_adopts_server_advertised_heartbeat_cadence(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.register_worker = AsyncMock(
            return_value={
                "worker_id": "w1",
                "registered": True,
                "heartbeat_interval_seconds": 7,
            }
        )
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            heartbeat_interval=120.0,
        )
        await worker._register()
        assert worker._heartbeat_interval == 7.0

    @pytest.mark.asyncio
    async def test_heartbeat_loop_survives_transient_errors(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.heartbeat_worker = AsyncMock(
            side_effect=[RuntimeError("temporary"), {"acknowledged": True}]
        )
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            poll_timeout=0.01,
            heartbeat_interval=0.02,
        )
        run_task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.15)
        await worker.stop()
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
        assert mock_client.heartbeat_worker.call_count >= 2

    def test_process_metrics_cpu_percent_is_instantaneous_not_lifetime(
        self, mock_client: AsyncMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``cpu_percent`` reflects only the interval since the previous
        heartbeat. A worker that was busy at startup and idle ever since
        used to keep reporting the lifetime average forever, hiding the
        fact that it is no longer doing CPU work."""

        import resource

        from durable_workflow import worker as worker_mod

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
        )
        worker._process_started_at = 1000.0

        class FakeUsage:
            def __init__(self, utime: float, stime: float) -> None:
                self.ru_utime = utime
                self.ru_stime = stime
                self.ru_maxrss = 0

        fake_now = {"value": 1010.0}
        fake_usage = {"value": FakeUsage(7.0, 1.0)}

        monkeypatch.setattr(worker_mod.time, "time", lambda: fake_now["value"])
        monkeypatch.setattr(resource, "getrusage", lambda _who: fake_usage["value"])

        # First sample: 8s of CPU over 10s of wall time since process start = 80%.
        first = worker._current_process_metrics()
        assert first["cpu_percent"] == 80.0

        # Ten more wall seconds with only 0.5s of additional CPU (the
        # worker went idle). Lifetime average would still be 8.5/20 =
        # 42.5%, but the instantaneous reading is 5%.
        fake_now["value"] = 1020.0
        fake_usage["value"] = FakeUsage(7.3, 1.2)
        second = worker._current_process_metrics()
        assert second["cpu_percent"] == 5.0

        # Fully idle for ten more seconds. Used to be ~32% (lifetime
        # average), should now be 0.
        fake_now["value"] = 1030.0
        third = worker._current_process_metrics()
        assert third["cpu_percent"] == 0.0
        assert third["process_uptime_seconds"] == 30

    def test_process_metrics_memory_bytes_is_current_resident_set(
        self, mock_client: AsyncMock
    ) -> None:
        """``memory_bytes`` is the current resident set size on Linux —
        read from ``/proc/self/statm`` — not ``ru_maxrss``, which is the
        process-lifetime high-water mark and never decreases after a
        startup spike."""

        if not sys.platform.startswith("linux"):
            pytest.skip("memory_bytes is only sampled on Linux")

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
        )

        metrics = worker._current_process_metrics()
        assert "memory_bytes" in metrics
        assert isinstance(metrics["memory_bytes"], int)
        assert metrics["memory_bytes"] > 0


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

    @pytest.mark.asyncio
    async def test_run_until_processes_query_tasks_while_waiting(self, mock_client: AsyncMock) -> None:
        query_completed = asyncio.Event()
        query_task = {
            "query_task_id": "qt-run-until",
            "query_task_attempt": 1,
            "workflow_type": "query-wf",
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "query_name": "status",
            "payload_codec": "json",
            "workflow_arguments": serializer.envelope([], codec="json"),
            "query_arguments": serializer.envelope([], codec="json"),
            "history_events": [],
        }
        poll_count = 0

        async def poll_query_task(**_: object) -> dict[str, object] | None:
            nonlocal poll_count
            poll_count += 1
            if poll_count == 1:
                return query_task
            await asyncio.sleep(0)
            return None

        async def complete_query_task(**_: object) -> dict[str, str]:
            query_completed.set()
            return {"outcome": "completed"}

        async def describe_workflow(_: str) -> WorkflowExecution:
            return WorkflowExecution(
                workflow_id="wf-1",
                run_id="run-1",
                workflow_type="query-wf",
                status="completed" if query_completed.is_set() else "running",
            )

        mock_client.poll_query_task.side_effect = poll_query_task
        mock_client.complete_query_task.side_effect = complete_query_task
        mock_client.describe_workflow.side_effect = describe_workflow

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[QueryWorkflow],
            activities=[],
            poll_timeout=0.01,
        )

        desc = await worker.run_until(workflow_id="wf-1", timeout=1.0, poll_interval=0.01)

        assert desc.status == "completed"
        mock_client.complete_query_task.assert_awaited_once()
        complete_kwargs = mock_client.complete_query_task.await_args.kwargs
        assert complete_kwargs["query_task_id"] == "qt-run-until"
        assert complete_kwargs["result"] == {"status": "ready"}
