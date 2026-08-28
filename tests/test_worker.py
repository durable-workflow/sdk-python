from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import threading
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

import durable_workflow.worker as worker_module
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
from durable_workflow.nexus import NEXUS_OPERATION_RESULT_SCHEMA, NexusOperationResult
from durable_workflow.worker import (
    MEMO_UPSERTS_CAPABILITY,
    MESSAGE_STREAMS_CAPABILITY,
    TYPED_SEARCH_ATTRIBUTES_CAPABILITY,
    UPDATE_VALIDATION_TASKS_CAPABILITY,
    WORKFLOW_UPDATES_CAPABILITY,
    Worker,
    _query_history_with_export_signal_arguments,
    _should_fail_workflow_task_after_completion_error,
)


class TypedCancelFlightError(Exception):
    code = 712


@workflow.defn(name="test-wf")
class TestWorkflow:
    def run(self, ctx, *args):  # type: ignore[no-untyped-def]
        result = yield ctx.schedule_activity("test-act", list(args))
        return result


@workflow.defn(name="memo-wf")
class MemoWorkflow:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        yield ctx.upsert_memo({"stage": "processing"})
        return "done"


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


@workflow.defn(name="nexus-worker-wf")
class NexusWorkerWorkflow:
    def run(self, ctx):  # type: ignore[no-untyped-def]
        result = yield ctx.call_nexus_service(
            "greeter",
            "shared",
            "greet",
            ["Ada"],
            service_sdk_language="workflow-php",
            artifact_tuple={"sdk-python": "0.4.95"},
            published_artifact_worker_execution=True,
        )
        return {"service_call_id": result.service_call_id}


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


@workflow.defn(name="validated-update-wf")
class ValidatedUpdateWorkflow:
    validator_calls = 0
    handler_calls = 0

    @workflow.update("approve")
    def approve(self, approved: bool) -> bool:
        type(self).handler_calls += 1
        return approved

    @approve.validator  # type: ignore[attr-defined]
    def validate_approve(self, approved: bool) -> None:
        type(self).validator_calls += 1
        if not approved:
            raise ValueError("approval required")

    def run(self, ctx):  # type: ignore[no-untyped-def]
        return "waiting"


@workflow.defn(name="query-wf")
class QueryWorkflow:
    def __init__(self) -> None:
        self.status = "ready"

    @workflow.query("status")
    def current_status(self) -> dict[str, str]:
        return {"status": self.status}

    def run(self, ctx):  # type: ignore[no-untyped-def]
        return self.status


@workflow.defn(name="activity-query-wf")
class ActivityQueryWorkflow:
    def __init__(self) -> None:
        self.activity_result: str | None = None

    @workflow.query("state")
    def state(self) -> dict[str, str | None]:
        return {"activity_result": self.activity_result}

    def run(self, ctx):  # type: ignore[no-untyped-def]
        self.activity_result = yield ctx.schedule_activity("load", [])
        return self.activity_result


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


@workflow.defn(name="replay-query-snapshot-wf")
class ReplayQuerySnapshotWorkflow:
    def __init__(self) -> None:
        self.activity_result: str | None = None
        self.approved_by: str | None = None
        self.finished = False

    @workflow.signal("approve")
    def approve(self, approved_by: str) -> None:
        self.approved_by = approved_by

    @workflow.query("state")
    def state(self) -> dict[str, object]:
        return {
            "activity_result": self.activity_result,
            "approved_by": self.approved_by,
            "finished": self.finished,
        }

    def run(self, ctx):  # type: ignore[no-untyped-def]
        self.activity_result = yield ctx.schedule_activity("load-state", [])
        yield ctx.wait_condition(lambda: self.approved_by is not None, key="approval")
        self.finished = True
        yield ctx.schedule_activity("after-signal", [self.approved_by])


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


_MISSING_TASK_CODEC = object()


@pytest.fixture
def mock_client() -> AsyncMock:
    client = AsyncMock(spec=Client)
    client.register_worker = AsyncMock(return_value={"worker_id": "w1", "registered": True})
    client.deregister_worker_registration = AsyncMock(
        return_value={
            "worker_id": "w1",
            "outcome": "deregistered",
            "recovered_workflow_task_count": 0,
        }
    )
    client.heartbeat_worker = AsyncMock(
        return_value={"worker_id": "w1", "acknowledged": True, "heartbeat_interval_seconds": 60}
    )
    client.poll_workflow_task = AsyncMock(return_value=None)
    client.poll_activity_task = AsyncMock(return_value=None)
    client.poll_query_task = AsyncMock(return_value=None)
    client.poll_update_validation_task = AsyncMock(return_value=None)
    client.complete_workflow_task = AsyncMock(return_value={"outcome": "completed"})
    client.complete_activity_task = AsyncMock(return_value={"outcome": "completed"})
    client.complete_query_task = AsyncMock(return_value={"outcome": "completed"})
    client.approve_update_validation_task = AsyncMock(return_value={"outcome": "approved"})
    client.reject_update_validation_task = AsyncMock(return_value={"outcome": "rejected"})
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
                "long_poll_timeout": 30,
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
        process_metrics = {
            "host": "worker-host",
            "process_id": 1234,
            "process_started_at": "2026-08-25T18:00:00Z",
        }
        worker._current_process_metrics = Mock(return_value=process_metrics)  # type: ignore[method-assign]

        await worker._register()
        mock_client.register_worker.assert_awaited_once()
        call_kwargs = mock_client.register_worker.call_args.kwargs
        assert call_kwargs == {
            "worker_id": "w-test",
            "task_queue": "q1",
            "supported_workflow_types": ["test-wf"],
            "workflow_definition_fingerprints": worker.workflow_definition_fingerprints,
            "workflow_command_contracts": {
                "test-wf": {
                    "queries": [],
                    "query_contracts": [],
                    "signals": [],
                    "signal_contracts": [],
                    "updates": [],
                    "update_contracts": [],
                    "update_validators": [],
                }
            },
            "supported_activity_types": ["test-act"],
            "max_concurrent_workflow_tasks": 10,
            "max_concurrent_activity_tasks": 10,
            "build_id": None,
            "capabilities": [
                "memo_upserts",
                "typed_search_attributes",
                "query_tasks",
                MESSAGE_STREAMS_CAPABILITY,
            ],
            "capability_manifest": {
                "local_activities": {
                    "supported": False,
                    "minimum_protocol_version": "1.18",
                    "reason": "python_worker_does_not_execute_record_local_activity",
                },
                "worker_sessions": {
                    "supported": False,
                    "minimum_protocol_version": "1.18",
                    "reason": "python_worker_has_no_typed_session_lifecycle",
                },
                "sticky_execution": {
                    "supported": False,
                    "minimum_protocol_version": "1.18",
                    "reason": "python_worker_uses_complete_durable_history_replay",
                },
            },
            "task_slots": {
                "workflow_available": 10,
                "activity_available": 10,
            },
            "process_metrics": process_metrics,
        }

        await worker.stop()

        assert worker._registered is False
        mock_client.deregister_worker_registration.assert_awaited_once_with("w-test")

    @pytest.mark.asyncio
    async def test_register_advertises_typed_workflow_command_contracts(
        self, mock_client: AsyncMock
    ) -> None:
        @workflow.defn(name="typed-command-contract-wf")
        class TypedCommandContractWorkflow:
            @workflow.signal("finish")
            def finish(self) -> None:
                return None

            @workflow.query("state")
            def state(self, verbose: bool = False) -> dict[str, object]:
                return {"verbose": verbose}

            @workflow.update("replace")
            def replace(
                self,
                value: int,
                note: str | None = None,
                *tags: str,
            ) -> dict[str, object]:
                return {"value": value, "note": note, "tags": tags}

            def run(self, ctx):  # type: ignore[no-untyped-def]
                yield ctx.wait_condition(lambda: False)

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TypedCommandContractWorkflow],
            worker_id="w-command-contract",
            build_id="release-a",
        )

        await worker._register()
        await worker._register()

        first_contract = mock_client.register_worker.await_args_list[0].kwargs[
            "workflow_command_contracts"
        ]
        second_contract = mock_client.register_worker.await_args_list[1].kwargs[
            "workflow_command_contracts"
        ]
        assert first_contract == second_contract
        assert first_contract == {
            "typed-command-contract-wf": {
                "queries": ["state"],
                "query_contracts": [
                    {
                        "name": "state",
                        "parameters": [
                            {
                                "name": "verbose",
                                "position": 0,
                                "required": False,
                                "variadic": False,
                                "default_available": True,
                                "default": False,
                                "type": "bool",
                                "allows_null": False,
                            }
                        ],
                    }
                ],
                "signals": ["finish"],
                "signal_contracts": [{"name": "finish", "parameters": []}],
                "updates": ["replace"],
                "update_contracts": [
                    {
                        "name": "replace",
                        "parameters": [
                            {
                                "name": "value",
                                "position": 0,
                                "required": True,
                                "variadic": False,
                                "default_available": False,
                                "default": None,
                                "type": "int",
                                "allows_null": False,
                            },
                            {
                                "name": "note",
                                "position": 1,
                                "required": False,
                                "variadic": False,
                                "default_available": True,
                                "default": None,
                                "type": "string|null",
                                "allows_null": True,
                            },
                            {
                                "name": "tags",
                                "position": 2,
                                "required": False,
                                "variadic": True,
                                "default_available": False,
                                "default": None,
                                "type": "string",
                                "allows_null": False,
                            },
                        ],
                    }
                ],
                "update_validators": [],
            }
        }
        assert mock_client.register_worker.await_args_list[0].kwargs["build_id"] == "release-a"
        assert mock_client.register_worker.await_args_list[0].kwargs["capabilities"] == [
            MEMO_UPSERTS_CAPABILITY,
            TYPED_SEARCH_ATTRIBUTES_CAPABILITY,
            "query_tasks",
            WORKFLOW_UPDATES_CAPABILITY,
            MESSAGE_STREAMS_CAPABILITY,
        ]

    @pytest.mark.asyncio
    async def test_validator_worker_requires_and_advertises_preaccept_capability(self, mock_client: AsyncMock) -> None:
        @workflow.defn(name="validated-registration-wf")
        class ValidatedWorkflow:
            @workflow.update("approve")
            def approve(self, approved: bool) -> bool:
                return approved

            @approve.validator  # type: ignore[attr-defined]
            def validate_approve(self, approved: bool) -> None:
                if not approved:
                    raise ValueError("approval required")

            def run(self, ctx):  # type: ignore[no-untyped-def]
                yield ctx.wait_condition(lambda: False)

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[ValidatedWorkflow],
            worker_id="w-validated-contract",
        )

        with pytest.raises(RuntimeError, match="synchronous pre-accept update validation"):
            await worker._register()
        mock_client.register_worker.assert_not_awaited()

        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(
                worker_protocol={
                    "version": PROTOCOL_VERSION,
                    "server_capabilities": {
                        "query_tasks": True,
                        "update_validation_tasks": True,
                        "synchronous_update_validation": {
                            "supported": True,
                            "acceptance_boundary": "validator_approved",
                            "worker_capability": UPDATE_VALIDATION_TASKS_CAPABILITY,
                            "workflow_contract_field": "update_validators",
                        },
                    },
                }
            )
        )
        with pytest.raises(RuntimeError, match="synchronous pre-accept update validation"):
            await worker._register()
        mock_client.register_worker.assert_not_awaited()

        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(
                worker_protocol={
                    "version": PROTOCOL_VERSION,
                    "server_capabilities": {
                        "query_tasks": True,
                        "update_validation_tasks": True,
                        "synchronous_update_validation": {
                            "supported": True,
                            "acceptance_boundary": "validator_approved",
                            "worker_capability": UPDATE_VALIDATION_TASKS_CAPABILITY,
                            "workflow_contract_field": "update_validators",
                            "task_poll": {
                                "strategy": "multiplexed",
                                "endpoint": "/worker/workflow-tasks/poll",
                                "request_field": "task_kinds",
                                "task_kinds": ["workflow", "update_validation"],
                                "response_discriminator": "task.task_kind",
                            },
                        },
                    },
                }
            )
        )
        await worker._register()

        registered = mock_client.register_worker.await_args.kwargs
        assert registered["workflow_command_contracts"]["validated-registration-wf"]["update_validators"] == ["approve"]
        assert registered["capabilities"] == [
            MEMO_UPSERTS_CAPABILITY,
            TYPED_SEARCH_ATTRIBUTES_CAPABILITY,
            "query_tasks",
            UPDATE_VALIDATION_TASKS_CAPABILITY,
            WORKFLOW_UPDATES_CAPABILITY,
            MESSAGE_STREAMS_CAPABILITY,
        ]

    @pytest.mark.asyncio
    async def test_worker_without_validators_does_not_poll_validation_queue(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(
                worker_protocol={
                    "version": PROTOCOL_VERSION,
                    "server_capabilities": {
                        "query_tasks": True,
                        "update_validation_tasks": True,
                        "synchronous_update_validation": {
                            "supported": True,
                            "acceptance_boundary": "validator_approved",
                            "worker_capability": UPDATE_VALIDATION_TASKS_CAPABILITY,
                            "workflow_contract_field": "update_validators",
                            "task_poll": {
                                "strategy": "multiplexed",
                                "endpoint": "/worker/workflow-tasks/poll",
                                "request_field": "task_kinds",
                                "task_kinds": ["workflow", "update_validation"],
                                "response_discriminator": "task.task_kind",
                            },
                        },
                    },
                }
            )
        )
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[UpdateWorkflow],
            worker_id="w-without-validators",
        )

        await worker._register()

        assert worker._update_validation_tasks_supported is False
        assert UPDATE_VALIDATION_TASKS_CAPABILITY not in (
            mock_client.register_worker.await_args.kwargs["capabilities"]
        )

    @pytest.mark.asyncio
    async def test_register_keeps_http_timeout_above_server_long_poll(self, mock_client: AsyncMock) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(
                worker_protocol={
                    "version": PROTOCOL_VERSION,
                    "server_capabilities": {
                        "query_tasks": True,
                        "long_poll_timeout": 12,
                    },
                }
            )
        )
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            worker_id="w-short-poll",
            poll_timeout=0.01,
        )

        async def poll_once(**_: object) -> None:
            worker._stop.set()
            return None

        mock_client.poll_workflow_task.side_effect = poll_once

        await worker._register()
        await worker._poll_workflow_tasks()

        assert mock_client.poll_workflow_task.call_args.kwargs["timeout"] == 17.0

    @pytest.mark.asyncio
    async def test_register_keeps_baseline_capabilities_when_server_does_not_support_query_tasks(
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
        assert call_kwargs["capabilities"] == [
            MEMO_UPSERTS_CAPABILITY,
            TYPED_SEARCH_ATTRIBUTES_CAPABILITY,
            MESSAGE_STREAMS_CAPABILITY,
        ]

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
    async def test_poll_loops_forward_build_id_when_configured(
        self, mock_client: AsyncMock
    ) -> None:
        workflow_called = asyncio.Event()
        activity_called = asyncio.Event()
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            worker_id="w-build-poll",
            build_id="release-2026.04.22-a1",
            poll_timeout=0.01,
        )

        async def workflow_poll_once(**_: object) -> None:
            workflow_called.set()
            await worker.stop()
            return None

        async def activity_poll_once(**_: object) -> None:
            activity_called.set()
            await worker.stop()
            return None

        mock_client.poll_workflow_task.side_effect = workflow_poll_once
        await worker._poll_workflow_tasks()
        workflow_kwargs = mock_client.poll_workflow_task.call_args.kwargs
        assert workflow_called.is_set()
        assert workflow_kwargs["build_id"] == "release-2026.04.22-a1"

        worker._stop.clear()
        mock_client.poll_activity_task.side_effect = activity_poll_once
        await worker._poll_activity_tasks()
        activity_kwargs = mock_client.poll_activity_task.call_args.kwargs
        assert activity_called.is_set()
        assert activity_kwargs["build_id"] == "release-2026.04.22-a1"

        worker._stop.clear()
        query_called = asyncio.Event()

        async def query_poll_once(**_: object) -> None:
            query_called.set()
            await worker.stop()
            return None

        mock_client.poll_query_task.side_effect = query_poll_once
        await worker._poll_query_tasks()
        query_kwargs = mock_client.poll_query_task.call_args.kwargs
        assert query_called.is_set()
        assert query_kwargs["build_id"] == "release-2026.04.22-a1"

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
    async def test_register_rejects_worker_protocol_below_current_command_floor(
        self, mock_client: AsyncMock
    ) -> None:
        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(worker_protocol={"version": "1.0"})
        )
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        with pytest.raises(RuntimeError, match=rf"minor>='{PROTOCOL_VERSION}'"):
            await worker._register()
        mock_client.register_worker.assert_not_called()

    @pytest.mark.asyncio
    async def test_register_accepts_current_protocol_when_optional_feature_is_unavailable(
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
    async def test_memo_command_uses_discovered_runtime_capability(self, mock_client: AsyncMock) -> None:
        info = compatible_cluster_info()
        worker_protocol = dict(info["worker_protocol"])  # type: ignore[arg-type]
        capabilities = dict(worker_protocol["server_capabilities"])
        capabilities.update({
            "workflow_memo_updates": {"supported": True, "minimum_protocol_version": "1.14"},
            "supported_workflow_task_commands": ["upsert_memo", "complete_workflow"],
        })
        worker_protocol["server_capabilities"] = capabilities
        info["worker_protocol"] = worker_protocol
        mock_client.get_cluster_info = AsyncMock(return_value=info)
        worker = Worker(mock_client, task_queue="q1", workflows=[MemoWorkflow], activities=[])
        await worker._register()
        mock_client.complete_workflow_task.reset_mock()

        await worker._run_workflow_task({
            "task_id": "memo-task",
            "workflow_type": "memo-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
        })

        commands = mock_client.complete_workflow_task.await_args.kwargs["commands"]
        assert commands[0]["type"] == "upsert_memo"
        assert set(commands[0]["entries"]) == {"codec", "blob"}
        assert serializer.decode_envelope(commands[0]["entries"]) == {"stage": "processing"}

    @pytest.mark.asyncio
    async def test_memo_command_fails_before_completion_without_runtime_capability(
        self, mock_client: AsyncMock
    ) -> None:
        info = compatible_cluster_info()
        worker_protocol = dict(info["worker_protocol"])  # type: ignore[arg-type]
        capabilities = dict(worker_protocol["server_capabilities"])
        capabilities.update({
            "workflow_memo_updates": {"supported": False, "minimum_protocol_version": "1.14"},
            "supported_workflow_task_commands": ["complete_workflow"],
        })
        worker_protocol["server_capabilities"] = capabilities
        info["worker_protocol"] = worker_protocol
        mock_client.get_cluster_info = AsyncMock(return_value=info)
        worker = Worker(mock_client, task_queue="q1", workflows=[MemoWorkflow], activities=[])
        await worker._register()
        mock_client.complete_workflow_task.reset_mock()
        mock_client.fail_workflow_task.reset_mock()

        await worker._run_workflow_task({
            "task_id": "memo-task-unsupported",
            "workflow_type": "memo-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
        })

        mock_client.complete_workflow_task.assert_not_called()
        assert "workflow_memo_updates_unavailable" in (
            mock_client.fail_workflow_task.await_args.kwargs["message"]
        )

    @pytest.mark.asyncio
    async def test_schedule_activity_on_first_replay(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[TestWorkflow], activities=[])
        task = {
            "task_id": "t1",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
        }
        await worker._run_workflow_task(task)
        mock_client.complete_workflow_task.assert_called_once()
        call_kwargs = mock_client.complete_workflow_task.call_args.kwargs
        commands = call_kwargs["commands"]
        assert len(commands) == 1
        assert commands[0]["type"] == "schedule_activity"
        assert commands[0]["activity_type"] == "test-act"
        assert commands[0]["arguments"]["codec"] == "avro"
        assert serializer.decode(commands[0]["arguments"]["blob"], codec="avro") == ["hello"]

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
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
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
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
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
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
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
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
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
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
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
            "arguments": serializer.encode(["this payload is intentionally large"], codec="avro"),
            "payload_codec": "avro",
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
            "arguments": serializer.encode(["this payload is intentionally large"], codec="avro"),
            "payload_codec": "avro",
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
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
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
                {
                    "event_type": "ActivityCompleted",
                    "payload": {
                        "payload_codec": "avro",
                        "result": serializer.envelope("done", codec="avro"),
                    },
                },
            ],
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
        }
        await worker._run_workflow_task(task)
        mock_client.complete_workflow_task.assert_called_once()
        commands = mock_client.complete_workflow_task.call_args.kwargs["commands"]
        assert commands[0]["type"] == "complete_workflow"
        assert commands[0]["result"]["codec"] == "avro"
        assert serializer.decode(commands[0]["result"]["blob"], codec="avro") == "done"

    @pytest.mark.asyncio
    async def test_workflow_nexus_service_call_records_side_effect_and_resumes(
        self, mock_client: AsyncMock
    ) -> None:
        async def fake_execute(endpoint_name: str, service_name: str, operation_name: str, arguments: list, **kwargs):
            return NexusOperationResult(
                accepted=True,
                service_call_id="svc-call-1",
                endpoint_name=endpoint_name,
                service_name=service_name,
                operation_name=operation_name,
                caller_workflow_instance_id=kwargs["caller_workflow_instance_id"],
                caller_workflow_run_id=kwargs["caller_workflow_run_id"],
                service_sdk_language=kwargs["service_sdk_language"],
                request_payload={
                    "arguments": list(arguments),
                    "idempotency_key": kwargs["idempotency_key"],
                    "caller_workflow_instance_id": kwargs["caller_workflow_instance_id"],
                    "caller_workflow_run_id": kwargs["caller_workflow_run_id"],
                },
                response_or_failure_surface={"status": "completed", "result": {"greeting": "hello Ada"}},
                artifact_tuple=kwargs["artifact_tuple"],
                published_artifact_worker_execution=kwargs["published_artifact_worker_execution"],
                status="completed",
                result={"greeting": "hello Ada"},
            )

        mock_client.execute_nexus_operation = AsyncMock(side_effect=fake_execute)
        worker = Worker(mock_client, task_queue="q1", workflows=[NexusWorkerWorkflow], activities=[])
        task = {
            "task_id": "t-nexus",
            "workflow_id": "wf-caller",
            "run_id": "run-caller",
            "workflow_type": "nexus-worker-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
        }

        await worker._run_workflow_task(task)

        mock_client.execute_nexus_operation.assert_awaited_once()
        execute_call = mock_client.execute_nexus_operation.await_args
        assert execute_call.args[:4] == ("greeter", "shared", "greet", ["Ada"])
        assert execute_call.kwargs["caller_workflow_instance_id"] == "wf-caller"
        assert execute_call.kwargs["caller_workflow_run_id"] == "run-caller"
        assert execute_call.kwargs["service_sdk_language"] == "workflow-php"
        assert execute_call.kwargs["raise_on_failure"] is False
        assert execute_call.kwargs["idempotency_key"].startswith("dw-py-nexus-")

        mock_client.complete_workflow_task.assert_called_once()
        commands = mock_client.complete_workflow_task.call_args.kwargs["commands"]
        assert [command["type"] for command in commands] == ["record_side_effect", "complete_workflow"]
        recorded = serializer.decode(commands[0]["result"], codec="avro")
        assert recorded["schema"] == NEXUS_OPERATION_RESULT_SCHEMA
        assert recorded["caller_workflow_instance_id"] == "wf-caller"
        assert recorded["caller_workflow_run_id"] == "run-caller"
        assert recorded["caller_sdk_language"] == "sdk-python"
        assert recorded["service_sdk_language"] == "workflow-php"
        assert recorded["operation_name"] == "greet"
        assert recorded["request_payload"]["arguments"] == ["Ada"]
        assert recorded["response_or_failure_surface"]["result"] == {"greeting": "hello Ada"}
        assert recorded["service_call_id"] == "svc-call-1"
        assert recorded["artifact_tuple"]["sdk-python"] == "0.4.95"
        assert recorded["published_artifact_worker_execution"] is True
        assert serializer.decode(commands[1]["result"]["blob"], codec="avro") == {
            "service_call_id": "svc-call-1",
        }

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
                        "payload_codec": "avro",
                        "result": serializer.envelope(marker, codec="avro"),
                    },
                },
            ],
            "arguments": serializer.encode([{"name": "Grace"}], codec="avro"),
            "payload_codec": "avro",
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
        assert commands[0]["arguments"]["codec"] == "avro"
        assert serializer.decode(commands[0]["arguments"]["blob"], codec="avro") == [marker]

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
                        "payload_codec": "avro",
                        "result": serializer.envelope(marker, codec="avro"),
                    },
                },
                {
                    "event_type": "ActivityCompleted",
                    "payload": {
                        "sequence": 2,
                        "activity_type": "external.describe",
                        "payload_codec": "avro",
                        "result": serializer.envelope(description, codec="avro"),
                    },
                },
            ],
            "arguments": serializer.encode([{"name": "Grace"}], codec="avro"),
            "payload_codec": "avro",
        }

        await worker._run_workflow_task(task)

        mock_client.complete_workflow_task.assert_called_once()
        commands = mock_client.complete_workflow_task.call_args.kwargs["commands"]
        assert commands[0]["type"] == "complete_workflow"
        assert commands[0]["result"]["codec"] == "avro"
        assert serializer.decode(commands[0]["result"]["blob"], codec="avro") == {
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
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
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
        assert "unsupported_value_type" in call_kwargs["message"]

    @pytest.mark.asyncio
    async def test_unknown_workflow_type_fails_task(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[])
        task = {
            "task_id": "t3",
            "workflow_type": "unknown-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "payload_codec": "avro",
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
                        "arguments": serializer.encode([6], codec="avro"),
                        "payload_codec": "avro",
                    },
                },
            ],
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
        }

        await worker._run_workflow_task(task)

        mock_client.complete_workflow_task.assert_called_once()
        commands = mock_client.complete_workflow_task.call_args.kwargs["commands"]
        assert commands == [
            {
                "type": "complete_update",
                "update_id": "upd-worker-1",
                "result": {
                    "codec": "avro",
                    "blob": serializer.encode({"count": 6}, codec="avro"),
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
                        "arguments": serializer.encode([6], codec="avro"),
                        "payload_codec": "avro",
                    },
                },
            ],
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
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
                        "arguments": serializer.encode([6], codec="avro"),
                        "payload_codec": "avro",
                    },
                },
            ],
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
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
            "payloads": {"codec": "avro"},
            "signals": [
                {
                    "id": "sig-finish",
                    "command_id": "cmd-finish",
                    "name": "finish",
                    "workflow_sequence": 2,
                    "payload_codec": "avro",
                    "arguments": serializer.encode([], codec="avro"),
                },
            ],
        }

        enriched = _query_history_with_export_signal_arguments(history, export, default_codec="avro")

        assert isinstance(enriched, list)
        payload = enriched[0]["payload"]
        assert payload["workflow_sequence"] == 2
        assert payload["arguments"]["codec"] == "avro"

    @pytest.mark.asyncio
    async def test_query_task_executes_registered_query(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[QueryWorkflow], activities=[])
        task = {
            "query_task_id": "qt1",
            "query_task_attempt": 1,
            "workflow_type": "query-wf",
            "query_name": "status",
            "history_events": [],
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt1",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result={"status": "ready"},
            codec="avro",
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
        signal_arguments = serializer.encode([3], codec="avro")
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
                "payloads": {"codec": "avro"},
                "signals": [
                    {
                        "id": "sig-increment",
                        "command_id": "cmd-increment",
                        "name": "increment",
                        "payload_codec": "avro",
                        "arguments": signal_arguments,
                    },
                ],
            },
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-signal-export",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result=3,
            codec="avro",
            workflow_id="wf-counter",
            run_id="run-counter",
            query_name="current",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_uses_export_history_when_inline_history_is_empty(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[ActivityQueryWorkflow], activities=[])
        activity_result = serializer.envelope("loaded", codec="avro")
        task = {
            "query_task_id": "qt-export-history",
            "query_task_attempt": 1,
            "workflow_type": "activity-query-wf",
            "workflow_id": "wf-export-history",
            "run_id": "run-export-history",
            "query_name": "state",
            "history_events": [],
            "history_export": {
                "payloads": {"codec": "avro"},
                "history_events": [
                    {
                        "type": "ActivityCompleted",
                        "payload": {
                            "sequence": 1,
                            "activity_type": "load",
                            "payload_codec": "avro",
                            "result": activity_result,
                        },
                    }
                ],
            },
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-export-history",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result={"activity_result": "loaded"},
            codec="avro",
            workflow_id="wf-export-history",
            run_id="run-export-history",
            query_name="state",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_replays_repeated_condition_wait_signal_arguments(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[CounterQueryWorkflow], activities=[])
        first_signal_arguments = serializer.encode([3], codec="avro")
        second_signal_arguments = serializer.encode([5], codec="avro")
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
                "payloads": {"codec": "avro"},
                "signals": [
                    {
                        "id": "sig-increment-3",
                        "command_id": "cmd-increment-3",
                        "name": "increment",
                        "payload_codec": "avro",
                        "arguments": first_signal_arguments,
                    },
                    {
                        "id": "sig-increment-5",
                        "command_id": "cmd-increment-5",
                        "name": "increment",
                        "payload_codec": "avro",
                        "arguments": second_signal_arguments,
                    },
                ],
            },
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-repeated-wait-signals",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result=8,
            codec="avro",
            workflow_id="wf-counter",
            run_id="run-counter",
            query_name="current",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_replays_signal_woken_false_wait_reopens(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[CounterQueryWorkflow], activities=[])
        first_signal_arguments = serializer.encode([3], codec="avro")
        second_signal_arguments = serializer.encode([5], codec="avro")
        task = {
            "query_task_id": "qt-false-wait-reopens",
            "query_task_attempt": 1,
            "workflow_type": "counter-query-wf",
            "workflow_id": "wf-counter",
            "run_id": "run-counter",
            "query_name": "current",
            "history_events": [
                {
                    "event_type": "ConditionWaitOpened",
                    "payload": {
                        "condition_wait_id": "wait-count-0",
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
                    "event_type": "ConditionWaitSatisfied",
                    "payload": {
                        "condition_wait_id": "wait-count-0",
                        "condition_key": "done",
                        "workflow_signal_id": "sig-increment-3",
                        "signal_name": "increment",
                    },
                },
                {
                    "event_type": "ConditionWaitOpened",
                    "payload": {
                        "condition_wait_id": "wait-count-3",
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
                {
                    "event_type": "ConditionWaitSatisfied",
                    "payload": {
                        "condition_wait_id": "wait-count-3",
                        "condition_key": "done",
                        "workflow_signal_id": "sig-increment-5",
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
            ],
            "history_export": {
                "payloads": {"codec": "avro"},
                "signals": [
                    {
                        "id": "sig-increment-3",
                        "command_id": "cmd-increment-3",
                        "name": "increment",
                        "payload_codec": "avro",
                        "arguments": first_signal_arguments,
                    },
                    {
                        "id": "sig-increment-5",
                        "command_id": "cmd-increment-5",
                        "name": "increment",
                        "payload_codec": "avro",
                        "arguments": second_signal_arguments,
                    },
                ],
            },
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-false-wait-reopens",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result=8,
            codec="avro",
            workflow_id="wf-counter",
            run_id="run-counter",
            query_name="current",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_replays_history_from_export_after_worker_restart(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[ReplayQuerySnapshotWorkflow], activities=[])
        approval_arguments = serializer.encode(["alice"], codec="avro")
        task = {
            "query_task_id": "qt-export-history",
            "query_task_attempt": 1,
            "workflow_type": "replay-query-snapshot-wf",
            "workflow_id": "wf-replay-query",
            "run_id": "run-replay-query",
            "query_name": "state",
            "history_events": [],
            "history_export": {
                "payloads": {"codec": "avro"},
                "history_events": [
                    {
                        "type": "ActivityCompleted",
                        "payload": {
                            "sequence": 1,
                            "activity_type": "load-state",
                            "payload_codec": "avro",
                            "result": serializer.encode("loaded", codec="avro"),
                        },
                    },
                    {
                        "type": "ConditionWaitOpened",
                        "payload": {
                            "sequence": 2,
                            "condition_wait_id": "wait-approval",
                            "condition_key": "approval",
                        },
                    },
                    {
                        "type": "SignalReceived",
                        "payload": {
                            "signal_id": "sig-approve",
                            "workflow_command_id": "cmd-approve",
                            "signal_name": "approve",
                        },
                    },
                ],
                "signals": [
                    {
                        "id": "sig-approve",
                        "command_id": "cmd-approve",
                        "name": "approve",
                        "payload_codec": "avro",
                        "arguments": approval_arguments,
                    },
                ],
            },
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-export-history",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result={
                "activity_result": "loaded",
                "approved_by": "alice",
                "finished": True,
            },
            codec="avro",
            workflow_id="wf-replay-query",
            run_id="run-replay-query",
            query_name="state",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("placeholder", "query_task_id", "workflow_id", "run_id"),
        [
            (None, "qt-null-placeholders", "wf-null-placeholders", "run-null-placeholders"),
            ("", "qt-empty-placeholders", "wf-empty-placeholders", "run-empty-placeholders"),
        ],
    )
    async def test_query_task_replaces_missing_history_payload_placeholders_from_export(
        self,
        mock_client: AsyncMock,
        placeholder: object,
        query_task_id: str,
        workflow_id: str,
        run_id: str,
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[ReplayQuerySnapshotWorkflow], activities=[])
        approval_arguments = serializer.encode(["alice"], codec="avro")
        task = {
            "query_task_id": query_task_id,
            "query_task_attempt": 1,
            "workflow_type": "replay-query-snapshot-wf",
            "workflow_id": workflow_id,
            "run_id": run_id,
            "query_name": "state",
            "history_events": [
                {
                    "event_type": "ActivityCompleted",
                    "payload": {
                        "sequence": 1,
                        "activity_type": "",
                        "payload_codec": None,
                        "result": placeholder,
                    },
                },
                {
                    "event_type": "ConditionWaitOpened",
                    "payload": {
                        "sequence": 2,
                        "condition_wait_id": "wait-approval",
                        "condition_key": "approval",
                    },
                },
                {
                    "event_type": "SignalReceived",
                    "payload": {
                        "signal_id": "sig-approve",
                        "workflow_command_id": "cmd-approve",
                        "signal_name": "approve",
                        "workflow_sequence": None,
                        "payload_codec": "",
                        "arguments": placeholder,
                    },
                },
            ],
            "history_export": {
                "payloads": {"codec": "avro"},
                "activities": [
                    {
                        "sequence": 1,
                        "activity_type": "load-state",
                        "payload_codec": "avro",
                        "result": serializer.encode("loaded", codec="avro"),
                    },
                ],
                "signals": [
                    {
                        "id": "sig-approve",
                        "command_id": "cmd-approve",
                        "name": "approve",
                        "workflow_sequence": 2,
                        "payload_codec": "avro",
                        "arguments": approval_arguments,
                    },
                ],
            },
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id=query_task_id,
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result={
                "activity_result": "loaded",
                "approved_by": "alice",
                "finished": True,
            },
            codec="avro",
            workflow_id=workflow_id,
            run_id=run_id,
            query_name="state",
        )
        mock_client.fail_query_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_task_enriches_compact_activity_completion_from_export(
        self, mock_client: AsyncMock
    ) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[ReplayQuerySnapshotWorkflow], activities=[])
        task = {
            "query_task_id": "qt-compact-activity",
            "query_task_attempt": 1,
            "workflow_type": "replay-query-snapshot-wf",
            "workflow_id": "wf-compact-activity",
            "run_id": "run-compact-activity",
            "query_name": "state",
            "history_events": [
                {
                    "event_type": "ActivityCompleted",
                    "payload": {
                        "sequence": 1,
                        "activity_type": "load-state",
                    },
                },
            ],
            "history_export": {
                "payloads": {"codec": "avro"},
                "activities": [
                    {
                        "sequence": 1,
                        "activity_type": "load-state",
                        "payload_codec": "avro",
                        "result": serializer.encode("loaded", codec="avro"),
                    },
                ],
            },
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-compact-activity",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result={
                "activity_result": "loaded",
                "approved_by": None,
                "finished": False,
            },
            codec="avro",
            workflow_id="wf-compact-activity",
            run_id="run-compact-activity",
            query_name="state",
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
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "completed"
        mock_client.complete_query_task.assert_awaited_once_with(
            query_task_id="qt-async",
            lease_owner=worker.worker_id,
            query_task_attempt=1,
            result=0,
            codec="avro",
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
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
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
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
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
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
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
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "failed"
        mock_client.fail_query_task.assert_awaited_once()
        call_kwargs = mock_client.fail_query_task.call_args.kwargs
        assert call_kwargs["query_task_id"] == "qt-result-encode-failure"
        assert call_kwargs["query_task_attempt"] == 1
        assert call_kwargs["reason"] == "query_result_encode_failed"
        assert call_kwargs["failure_type"] == "TypeError"


class TestUpdateValidationTaskExecution:
    def task(self, approved: bool) -> dict[str, object]:
        return {
            "task_kind": "update_validation",
            "update_validation_task_id": "uv1",
            "update_validation_attempt": 1,
            "workflow_type": "validated-update-wf",
            "update_name": "approve",
            "history_events": [],
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "update_arguments": serializer.envelope([approved], codec="avro"),
            "payload_codec": "avro",
            "workflow_id": "wf1",
            "run_id": "run1",
        }

    @pytest.mark.asyncio
    async def test_approval_runs_validator_without_handler(self, mock_client: AsyncMock) -> None:
        ValidatedUpdateWorkflow.validator_calls = 0
        ValidatedUpdateWorkflow.handler_calls = 0
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[ValidatedUpdateWorkflow],
            worker_id="validator-worker",
        )

        outcome = await worker._run_update_validation_task(self.task(True))

        assert outcome == "approved"
        assert ValidatedUpdateWorkflow.validator_calls == 1
        assert ValidatedUpdateWorkflow.handler_calls == 0
        mock_client.approve_update_validation_task.assert_awaited_once_with(
            update_validation_task_id="uv1",
            lease_owner="validator-worker",
            update_validation_attempt=1,
        )
        mock_client.reject_update_validation_task.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rejection_is_reported_with_typed_reason(self, mock_client: AsyncMock) -> None:
        ValidatedUpdateWorkflow.validator_calls = 0
        ValidatedUpdateWorkflow.handler_calls = 0
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[ValidatedUpdateWorkflow],
            worker_id="validator-worker",
        )

        outcome = await worker._run_update_validation_task(self.task(False))

        assert outcome == "rejected"
        assert ValidatedUpdateWorkflow.validator_calls == 1
        assert ValidatedUpdateWorkflow.handler_calls == 0
        rejection = mock_client.reject_update_validation_task.await_args.kwargs
        assert rejection["reason"] == "update_validator_rejected"
        assert rejection["failure_type"] == "ValueError"
        mock_client.approve_update_validation_task.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("task_kind", ["workflow", "update_validation"])
    async def test_multiplexed_poll_promptly_dispatches_either_kind_with_one_slot(
        self,
        mock_client: AsyncMock,
        task_kind: str,
    ) -> None:
        dispatch_started = asyncio.Event()
        release_dispatch = asyncio.Event()
        next_poll_started = asyncio.Event()

        workflow_task: dict[str, object] = {
            "task_kind": "workflow",
            "task_id": "wf-task-1",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }
        ready_task = workflow_task if task_kind == "workflow" else self.task(True)

        async def poll_workflow_work(**_: object) -> dict[str, object] | None:
            if mock_client.poll_workflow_task.await_count == 1:
                return ready_task
            next_poll_started.set()
            await asyncio.Event().wait()
            return None

        async def run_work(_: dict[str, object]) -> object:
            dispatch_started.set()
            await release_dispatch.wait()
            return [] if task_kind == "workflow" else "approved"

        mock_client.poll_workflow_task.side_effect = poll_workflow_work
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[ValidatedUpdateWorkflow],
            max_concurrent_workflow_tasks=1,
        )
        worker._update_validation_tasks_supported = True
        if task_kind == "workflow":
            worker._run_workflow_task = run_work  # type: ignore[method-assign]
        else:
            worker._run_update_validation_task = run_work  # type: ignore[method-assign]

        poller = asyncio.create_task(worker._poll_workflow_tasks())
        try:
            await asyncio.wait_for(dispatch_started.wait(), timeout=1.0)

            assert mock_client.poll_workflow_task.await_count == 1
            assert mock_client.poll_workflow_task.await_args.kwargs["task_kinds"] == (
                "workflow",
                "update_validation",
            )
            assert mock_client.poll_update_validation_task.await_count == 0
            assert worker._workflow_reserved == 1
            assert worker._current_task_slots()["workflow_available"] == 0

            release_dispatch.set()
            await asyncio.wait_for(next_poll_started.wait(), timeout=1.0)
        finally:
            release_dispatch.set()
            poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poller
            if worker._in_flight:
                await asyncio.gather(*set(worker._in_flight), return_exceptions=True)

        assert worker._workflow_reserved == 0
        assert worker._workflow_inflight == 0
        assert worker._wf_semaphore._value == 1
        assert worker._current_task_slots()["workflow_available"] == 1

    @pytest.mark.asyncio
    async def test_validator_burst_cannot_lease_beyond_workflow_capacity(
        self, mock_client: AsyncMock
    ) -> None:
        started = [asyncio.Event() for _ in range(3)]
        releases = [asyncio.Event() for _ in range(3)]
        blocked_poll_started = asyncio.Event()
        running = 0
        max_running = 0

        @workflow.defn(name="blocked-validation-wf")
        class BlockedValidationWorkflow:
            @workflow.update("approve")
            def approve(self, index: int) -> bool:
                return index >= 0

            @approve.validator  # type: ignore[attr-defined]
            async def validate_approve(self, index: int) -> None:
                nonlocal running, max_running
                running += 1
                max_running = max(max_running, running)
                started[index].set()
                try:
                    await releases[index].wait()
                finally:
                    running -= 1

            def run(self, ctx):  # type: ignore[no-untyped-def]
                return "waiting"

        validation_tasks = [
            {
                "task_kind": "update_validation",
                "update_validation_task_id": f"uv-{index}",
                "update_validation_attempt": 1,
                "workflow_type": "blocked-validation-wf",
                "update_name": "approve",
                "history_events": [],
                "workflow_arguments": serializer.envelope([], codec="avro"),
                "update_arguments": serializer.envelope([index], codec="avro"),
                "payload_codec": "avro",
                "workflow_id": "wf1",
                "run_id": "run1",
            }
            for index in range(3)
        ]

        async def poll_validation_task(**_: object) -> dict[str, object] | None:
            if validation_tasks:
                return validation_tasks.pop(0)
            blocked_poll_started.set()
            await asyncio.Event().wait()
            return None

        mock_client.poll_workflow_task.side_effect = poll_validation_task
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[BlockedValidationWorkflow],
            max_concurrent_workflow_tasks=2,
        )
        worker._update_validation_tasks_supported = True
        poller = asyncio.create_task(worker._poll_workflow_tasks())

        try:
            await asyncio.wait_for(
                asyncio.gather(started[0].wait(), started[1].wait()),
                timeout=1.0,
            )
            await asyncio.sleep(0)

            assert mock_client.poll_workflow_task.await_count == 2
            assert max_running == 2
            assert worker._workflow_reserved == 2
            assert worker._current_task_slots()["workflow_available"] == 0

            worker._heartbeat_interval = 0.001
            heartbeat_loop = asyncio.create_task(worker._heartbeat_loop())
            try:

                async def wait_for_heartbeat() -> None:
                    while mock_client.heartbeat_worker.await_count == 0:
                        await asyncio.sleep(0)

                await asyncio.wait_for(wait_for_heartbeat(), timeout=1.0)
            finally:
                heartbeat_loop.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_loop
            heartbeat_slots = mock_client.heartbeat_worker.await_args.kwargs["task_slots"]
            assert heartbeat_slots["workflow_available"] == 0

            releases[0].set()
            await asyncio.wait_for(started[2].wait(), timeout=1.0)

            assert mock_client.poll_workflow_task.await_count == 3
            assert max_running == 2
            assert worker._workflow_reserved == 2
            assert worker._current_task_slots()["workflow_available"] == 0

            releases[1].set()
            releases[2].set()
            await asyncio.wait_for(blocked_poll_started.wait(), timeout=1.0)
        finally:
            for release in releases:
                release.set()
            poller.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poller
            if worker._in_flight:
                await asyncio.gather(*set(worker._in_flight), return_exceptions=True)

        assert worker._workflow_inflight == 0
        assert worker._workflow_reserved == 0
        assert worker._wf_semaphore._value == 2
        assert worker._current_task_slots()["workflow_available"] == 2
        assert mock_client.approve_update_validation_task.await_count == 3

    @pytest.mark.asyncio
    async def test_shutdown_cancels_idle_multiplexed_poll_and_releases_reservation(
        self, mock_client: AsyncMock
    ) -> None:
        poll_started = asyncio.Event()
        poll_cancelled = asyncio.Event()

        async def idle_poll(**_: object) -> None:
            poll_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                poll_cancelled.set()

        mock_client.poll_workflow_task.side_effect = idle_poll
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[ValidatedUpdateWorkflow],
            max_concurrent_workflow_tasks=1,
        )
        worker._update_validation_tasks_supported = True
        poller = asyncio.create_task(worker._poll_workflow_tasks())
        worker._poller_tasks.add(poller)

        await asyncio.wait_for(poll_started.wait(), timeout=1.0)

        assert worker._workflow_reserved == 1
        assert worker._workflow_inflight == 0
        assert worker._current_task_slots()["workflow_available"] == 0
        assert mock_client.poll_workflow_task.await_count == 1
        assert mock_client.poll_update_validation_task.await_count == 0

        await worker.stop()

        assert poll_cancelled.is_set()
        assert poller.cancelled()
        assert worker._workflow_reserved == 0
        assert worker._workflow_inflight == 0
        assert worker._wf_semaphore._value == 1
        assert worker._current_task_slots()["workflow_available"] == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("approved", "transport_failure"),
        [(True, False), (False, False), (True, True)],
        ids=["approval", "rejection", "transport-failure"],
    )
    async def test_dispatch_releases_workflow_capacity_after_completion_outcomes(
        self,
        mock_client: AsyncMock,
        *,
        approved: bool,
        transport_failure: bool,
    ) -> None:
        completion_started = asyncio.Event()
        release_completion = asyncio.Event()

        async def complete_validation(**_: object) -> dict[str, str]:
            completion_started.set()
            await release_completion.wait()
            if transport_failure:
                raise RuntimeError("completion transport failed")
            return {"outcome": "completed"}

        if approved:
            mock_client.approve_update_validation_task.side_effect = complete_validation
        else:
            mock_client.reject_update_validation_task.side_effect = complete_validation

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[ValidatedUpdateWorkflow],
            max_concurrent_workflow_tasks=1,
        )
        await worker._reserve_workflow_capacity()
        dispatched = worker._admit_update_validation_task(self.task(approved))

        await asyncio.wait_for(completion_started.wait(), timeout=1.0)
        assert worker._current_task_slots()["workflow_available"] == 0
        assert worker._workflow_reserved == 1
        assert worker._wf_semaphore._value == 0

        release_completion.set()
        await dispatched

        assert worker._workflow_inflight == 0
        assert worker._workflow_reserved == 0
        assert worker._wf_semaphore._value == 1
        assert worker._current_task_slots()["workflow_available"] == 1

    @pytest.mark.asyncio
    async def test_shutdown_cancels_validation_and_deregisters_after_releasing_capacity(
        self, mock_client: AsyncMock
    ) -> None:
        validation_started = asyncio.Event()
        validation_cancelled = asyncio.Event()

        async def blocked_validation(_: dict[str, object]) -> str:
            validation_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                validation_cancelled.set()

        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(
                worker_protocol={
                    "version": PROTOCOL_VERSION,
                    "server_capabilities": {
                        "query_tasks": True,
                        "update_validation_tasks": True,
                        "synchronous_update_validation": {
                            "supported": True,
                            "acceptance_boundary": "validator_approved",
                            "worker_capability": UPDATE_VALIDATION_TASKS_CAPABILITY,
                            "workflow_contract_field": "update_validators",
                            "task_poll": {
                                "strategy": "multiplexed",
                                "endpoint": "/worker/workflow-tasks/poll",
                                "request_field": "task_kinds",
                                "task_kinds": ["workflow", "update_validation"],
                                "response_discriminator": "task.task_kind",
                            },
                        },
                    },
                }
            )
        )
        mock_client.poll_workflow_task.side_effect = [self.task(True)]
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[ValidatedUpdateWorkflow],
            max_concurrent_workflow_tasks=1,
            shutdown_timeout=0.01,
        )
        worker._run_update_validation_task = blocked_validation  # type: ignore[method-assign]
        await worker._register()
        poller = asyncio.create_task(worker._poll_workflow_tasks())
        worker._poller_tasks.add(poller)

        await asyncio.wait_for(validation_started.wait(), timeout=1.0)
        assert worker._current_task_slots()["workflow_available"] == 0

        await worker.stop()

        assert validation_cancelled.is_set()
        assert worker._workflow_inflight == 0
        assert worker._workflow_reserved == 0
        assert worker._wf_semaphore._value == 1
        assert worker._current_task_slots()["workflow_available"] == 1
        assert worker._registered is False
        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

    @pytest.mark.asyncio
    async def test_cancellation_before_dispatch_starts_releases_admitted_capacity(self, mock_client: AsyncMock) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[ValidatedUpdateWorkflow],
            max_concurrent_workflow_tasks=1,
        )
        await worker._reserve_workflow_capacity()

        dispatched = worker._admit_update_validation_task(self.task(True))
        dispatched.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await dispatched
        await asyncio.sleep(0)

        assert worker._workflow_inflight == 0
        assert worker._workflow_reserved == 0
        assert worker._wf_semaphore._value == 1
        assert worker._current_task_slots()["workflow_available"] == 1
        mock_client.approve_update_validation_task.assert_not_awaited()


class TestActivityTaskExecution:
    @pytest.mark.asyncio
    async def test_sync_activity(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at1",
            "activity_attempt_id": "aa1",
            "activity_type": "test-act",
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
        }
        await worker._run_activity_task(task)
        mock_client.complete_activity_task.assert_called_once()
        call_kwargs = mock_client.complete_activity_task.call_args.kwargs
        assert call_kwargs["result"] == "result-hello"
        assert call_kwargs["codec"] == "avro"

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
            "arguments": serializer.encode(["world"], codec="avro"),
            "payload_codec": "avro",
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
            "arguments": serializer.encode([], codec="avro"),
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
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
        }
        await worker._run_activity_task(task)
        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert "boom" in call_kwargs["message"]
        assert call_kwargs["failure_type"] == "RuntimeError"
        assert call_kwargs["failure_class"] == "builtins.RuntimeError"

    @pytest.mark.asyncio
    async def test_activity_exception_reports_typed_failure_metadata(self, mock_client: AsyncMock) -> None:
        @activity.defn(name="cancel-flight")
        def cancel_flight() -> None:
            raise TypedCancelFlightError("cancel_flight typed compensation failure")

        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[cancel_flight])
        task = {
            "task_id": "at-typed",
            "activity_attempt_id": "aa-typed",
            "activity_type": "cancel-flight",
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
        }

        await worker._run_activity_task(task)

        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert call_kwargs["failure_type"] == "TypedCancelFlightError"
        assert call_kwargs["failure_class"] == f"{__name__}.TypedCancelFlightError"
        assert call_kwargs["failure_code"] == 712
        assert "cancel_flight typed compensation failure" in call_kwargs["message"]


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
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
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
                "arguments": serializer.encode(["hello"], codec="avro"),
                "payload_codec": "avro",
            }
        )
        await worker._run_activity_task(
            {
                "task_id": "at-intercept-boom",
                "activity_attempt_id": "aa-intercept-boom",
                "activity_type": "boom-act",
                "arguments": serializer.encode([], codec="avro"),
                "payload_codec": "avro",
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
                "workflow_arguments": serializer.envelope([], codec="avro"),
                "query_arguments": serializer.envelope([], codec="avro"),
                "payload_codec": "avro",
            }
        )

        assert events == ["before:qt-intercept", "after:completed"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("task_kind", ["workflow", "update", "query"])
    @pytest.mark.parametrize(
        "payload_codec",
        [
            pytest.param(_MISSING_TASK_CODEC, id="missing"),
            pytest.param(None, id="null"),
            pytest.param("", id="empty"),
            pytest.param("json", id="json"),
            pytest.param("zstd", id="unknown"),
            pytest.param("Avro", id="wrong-case"),
            pytest.param(0, id="non-string"),
        ],
    )
    async def test_invalid_root_codec_cannot_reach_or_be_suppressed_by_interceptors(
        self,
        mock_client: AsyncMock,
        task_kind: str,
        payload_codec: object,
    ) -> None:
        interceptor_calls: list[str] = []

        class MutatingShortCircuitInterceptor(PassthroughWorkerInterceptor):
            async def execute_workflow_task(
                self,
                context: WorkflowTaskInterceptorContext,
                next: WorkflowTaskHandler,
            ) -> list[dict[str, object]] | None:
                interceptor_calls.append("workflow")
                context.task["payload_codec"] = "avro"
                return []

            async def execute_query_task(
                self,
                context: QueryTaskInterceptorContext,
                next: QueryTaskHandler,
            ) -> str:
                interceptor_calls.append("query")
                context.task["payload_codec"] = "avro"
                return "completed"

        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[UpdateWorkflow] if task_kind == "update" else [QueryWorkflow, TestWorkflow],
            activities=[],
            interceptors=[MutatingShortCircuitInterceptor()],
        )

        if task_kind == "query":
            task: dict[str, object] = {
                "query_task_id": "qt-invalid-interceptor-codec",
                "query_task_attempt": 1,
                "workflow_type": "query-wf",
                "query_name": "status",
                "history_events": [],
                "workflow_arguments": serializer.envelope([], codec="avro"),
                "query_arguments": serializer.envelope([], codec="avro"),
            }
        else:
            task = {
                "task_id": f"{task_kind}-invalid-interceptor-codec",
                "workflow_type": "update-wf" if task_kind == "update" else "test-wf",
                "workflow_task_attempt": 1,
                "history_events": [],
                "arguments": serializer.envelope([], codec="avro"),
            }
            if task_kind == "update":
                task.update(
                    {
                        "workflow_update_id": "upd-invalid-interceptor-codec",
                        "history_events": [
                            {
                                "event_type": "UpdateAccepted",
                                "payload": {
                                    "update_id": "upd-invalid-interceptor-codec",
                                    "update_name": "increment",
                                    "arguments": serializer.envelope([1], codec="avro"),
                                    "payload_codec": "avro",
                                },
                            }
                        ],
                    }
                )

        if payload_codec is not _MISSING_TASK_CODEC:
            task["payload_codec"] = payload_codec

        if task_kind == "query":
            assert await worker._run_query_task(task) == "failed"
            failure = mock_client.fail_query_task
        else:
            assert await worker._run_workflow_task(task) is None
            failure = mock_client.fail_workflow_task

        assert interceptor_calls == []
        failure.assert_awaited_once()
        assert "unsupported_payload_codec" in failure.await_args.kwargs["message"]


class TestEnvelopeArguments:
    @pytest.mark.asyncio
    async def test_activity_with_envelope_arguments(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at-env",
            "activity_attempt_id": "aa-env",
            "activity_type": "test-act",
            "arguments": {"codec": "avro", "blob": serializer.encode(["hello"], codec="avro")},
            "payload_codec": "avro",
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
            "arguments": {"codec": "avro", "blob": serializer.encode(["hello"], codec="avro")},
            "payload_codec": "avro",
        }
        await worker._run_workflow_task(task)
        mock_client.complete_workflow_task.assert_called_once()


class TestCodecDecodeFailures:
    """Codec decode failures at the task boundary must fail tasks deterministically."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload_codec",
        [
            pytest.param(_MISSING_TASK_CODEC, id="missing"),
            pytest.param(None, id="null"),
            pytest.param("", id="empty"),
            pytest.param("json", id="json"),
            pytest.param("zstd", id="unknown"),
            pytest.param("Avro", id="wrong-case"),
            pytest.param(0, id="integer"),
            pytest.param(False, id="boolean"),
            pytest.param([], id="list"),
            pytest.param({}, id="mapping"),
        ],
    )
    @pytest.mark.parametrize(
        "task_kind",
        ["workflow", "activity", "query", "update", "update_validation"],
    )
    async def test_task_root_codec_rejection_precedes_decode_and_user_work(
        self,
        mock_client: AsyncMock,
        monkeypatch: pytest.MonkeyPatch,
        payload_codec: object,
        task_kind: str,
    ) -> None:
        if task_kind == "activity":
            worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
            task: dict[str, object] = {
                "task_id": "at-codec-boundary",
                "activity_attempt_id": "aa-codec-boundary",
                "activity_type": "test-act",
                "arguments": serializer.envelope(["hello"], codec="avro"),
            }
        elif task_kind == "query":
            worker = Worker(mock_client, task_queue="q1", workflows=[QueryWorkflow], activities=[])
            task = {
                "query_task_id": "qt-codec-boundary",
                "query_task_attempt": 1,
                "workflow_type": "query-wf",
                "query_name": "status",
                "history_events": [],
                "workflow_arguments": serializer.envelope([], codec="avro"),
                "query_arguments": serializer.envelope([], codec="avro"),
            }
        elif task_kind == "update_validation":
            worker = Worker(
                mock_client,
                task_queue="q1",
                workflows=[ValidatedUpdateWorkflow],
                activities=[],
            )
            task = {
                "update_validation_task_id": "uv-codec-boundary",
                "update_validation_attempt": 1,
                "workflow_type": "validated-update-wf",
                "update_name": "approve",
                "history_events": [],
                "workflow_arguments": serializer.envelope([], codec="avro"),
                "update_arguments": serializer.envelope([True], codec="avro"),
            }
        else:
            worker = Worker(
                mock_client,
                task_queue="q1",
                workflows=[UpdateWorkflow] if task_kind == "update" else [TestWorkflow],
                activities=[],
            )
            task = {
                "task_id": f"{task_kind}-codec-boundary",
                "workflow_type": "update-wf" if task_kind == "update" else "test-wf",
                "workflow_task_attempt": 1,
                "history_events": [],
                "arguments": serializer.envelope([], codec="avro"),
            }
            if task_kind == "update":
                task.update(
                    {
                        "workflow_update_id": "upd-codec-boundary",
                        "history_events": [
                            {
                                "event_type": "UpdateAccepted",
                                "payload": {
                                    "update_id": "upd-codec-boundary",
                                    "update_name": "increment",
                                    "arguments": serializer.envelope([1], codec="avro"),
                                    "payload_codec": "avro",
                                },
                            },
                        ],
                    }
                )

        if payload_codec is not _MISSING_TASK_CODEC:
            task["payload_codec"] = payload_codec

        decode_spy = Mock(side_effect=AssertionError("task payload decoding must not run"))
        replay_spy = Mock(side_effect=AssertionError("workflow replay must not run"))
        query_spy = Mock(side_effect=AssertionError("query handler must not run"))
        update_spy = Mock(side_effect=AssertionError("update handler must not run"))
        validator_spy = Mock(side_effect=AssertionError("update validator must not run"))
        activity_spy = AsyncMock(side_effect=AssertionError("activity handler must not run"))
        monkeypatch.setattr(serializer, "decode_envelope", decode_spy)
        monkeypatch.setattr(worker_module, "replay", replay_spy)
        monkeypatch.setattr(worker_module, "query_state", query_spy)
        monkeypatch.setattr(worker_module, "apply_update", update_spy)
        monkeypatch.setattr(worker_module, "validate_update", validator_spy)
        monkeypatch.setattr(worker, "_execute_activity_callable", activity_spy)

        if task_kind == "activity":
            outcome = await worker._run_activity_task(task)
            failure = mock_client.fail_activity_task
            assert outcome == "decode_error"
        elif task_kind == "query":
            outcome = await worker._run_query_task(task)
            failure = mock_client.fail_query_task
            assert outcome == "failed"
        elif task_kind == "update_validation":
            outcome = await worker._run_update_validation_task(task)
            failure = mock_client.reject_update_validation_task
            assert outcome == "rejected"
        else:
            outcome = await worker._run_workflow_task(task)
            failure = mock_client.fail_workflow_task
            assert outcome is None

        failure.assert_awaited_once()
        assert "unsupported_payload_codec" in failure.await_args.kwargs["message"]
        assert failure.await_args.kwargs["failure_type"] == "ValueError"
        decode_spy.assert_not_called()
        replay_spy.assert_not_called()
        query_spy.assert_not_called()
        update_spy.assert_not_called()
        validator_spy.assert_not_called()
        activity_spy.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "task_kind",
        ["workflow", "activity", "query", "update", "update_validation"],
    )
    async def test_exact_avro_task_completes_each_path_with_handler_work(
        self,
        mock_client: AsyncMock,
        monkeypatch: pytest.MonkeyPatch,
        task_kind: str,
    ) -> None:
        customer_value = {
            "codec": "customer-codec",
            "payload_codec": None,
            "metadata": {"codec": "json", "payload_codec": "Avro"},
        }
        if task_kind == "activity":
            worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
            task: dict[str, object] = {
                "task_id": "at-exact-avro",
                "activity_attempt_id": "aa-exact-avro",
                "activity_type": "test-act",
                "arguments": serializer.envelope([customer_value], codec="avro"),
                "payload_codec": "avro",
                "metadata": customer_value,
            }
        elif task_kind == "query":
            worker = Worker(mock_client, task_queue="q1", workflows=[QueryWorkflow], activities=[])
            task = {
                "query_task_id": "qt-exact-avro",
                "query_task_attempt": 1,
                "workflow_type": "query-wf",
                "query_name": "status",
                "history_events": [],
                "workflow_arguments": serializer.envelope([], codec="avro"),
                "query_arguments": serializer.envelope([], codec="avro"),
                "payload_codec": "avro",
                "metadata": customer_value,
            }
        elif task_kind == "update_validation":
            ValidatedUpdateWorkflow.validator_calls = 0
            worker = Worker(
                mock_client,
                task_queue="q1",
                workflows=[ValidatedUpdateWorkflow],
                activities=[],
            )
            task = {
                "update_validation_task_id": "uv-exact-avro",
                "update_validation_attempt": 1,
                "workflow_type": "validated-update-wf",
                "update_name": "approve",
                "history_events": [],
                "workflow_arguments": serializer.envelope([], codec="avro"),
                "update_arguments": serializer.envelope([True], codec="avro"),
                "payload_codec": "avro",
                "metadata": customer_value,
            }
        else:
            worker = Worker(
                mock_client,
                task_queue="q1",
                workflows=[UpdateWorkflow] if task_kind == "update" else [TestWorkflow],
                activities=[],
            )
            task = {
                "task_id": f"{task_kind}-exact-avro",
                "workflow_type": "update-wf" if task_kind == "update" else "test-wf",
                "workflow_task_attempt": 1,
                "history_events": [],
                "arguments": serializer.envelope(
                    [] if task_kind == "update" else [customer_value],
                    codec="avro",
                ),
                "payload_codec": "avro",
                "metadata": customer_value,
            }
            if task_kind == "update":
                task.update(
                    {
                        "workflow_update_id": "upd-exact-avro",
                        "history_events": [
                            {
                                "event_type": "UpdateAccepted",
                                "payload": {
                                    "update_id": "upd-exact-avro",
                                    "update_name": "increment",
                                    "arguments": serializer.envelope([6], codec="avro"),
                                    "payload_codec": "avro",
                                },
                            },
                        ],
                    }
                )

        decode_spy = Mock(wraps=serializer.decode_envelope)
        replay_spy = Mock(wraps=worker_module.replay)
        query_spy = Mock(wraps=worker_module.query_state)
        update_spy = Mock(wraps=worker_module.apply_update)
        validator_spy = Mock(wraps=worker_module.validate_update)
        activity_spy = AsyncMock(wraps=worker._execute_activity_callable)
        monkeypatch.setattr(serializer, "decode_envelope", decode_spy)
        monkeypatch.setattr(worker_module, "replay", replay_spy)
        monkeypatch.setattr(worker_module, "query_state", query_spy)
        monkeypatch.setattr(worker_module, "apply_update", update_spy)
        monkeypatch.setattr(worker_module, "validate_update", validator_spy)
        monkeypatch.setattr(worker, "_execute_activity_callable", activity_spy)

        if task_kind == "activity":
            assert await worker._run_activity_task(task) == "completed"
            mock_client.complete_activity_task.assert_awaited_once()
            mock_client.fail_activity_task.assert_not_awaited()
            activity_spy.assert_awaited_once()
        elif task_kind == "query":
            assert await worker._run_query_task(task) == "completed"
            mock_client.complete_query_task.assert_awaited_once()
            mock_client.fail_query_task.assert_not_awaited()
            query_spy.assert_called_once()
        elif task_kind == "update_validation":
            assert await worker._run_update_validation_task(task) == "approved"
            mock_client.approve_update_validation_task.assert_awaited_once()
            mock_client.reject_update_validation_task.assert_not_awaited()
            validator_spy.assert_called_once()
            assert ValidatedUpdateWorkflow.validator_calls == 1
        else:
            assert await worker._run_workflow_task(task) is not None
            mock_client.complete_workflow_task.assert_awaited_once()
            mock_client.fail_workflow_task.assert_not_awaited()
            if task_kind == "update":
                update_spy.assert_called_once()
            else:
                replay_spy.assert_called_once()
                command = mock_client.complete_workflow_task.await_args.kwargs["commands"][0]
                assert serializer.decode(command["arguments"]["blob"], codec="avro") == [customer_value]

        assert decode_spy.call_count > 0

    @pytest.mark.asyncio
    async def test_activity_json_decode_failure_fails_task(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1", workflows=[], activities=[echo_activity])
        task = {
            "task_id": "at-bad-json",
            "activity_attempt_id": "aa-bad-json",
            "activity_type": "test-act",
            "arguments": "{not valid json",
            "payload_codec": "avro",
        }
        await worker._run_activity_task(task)
        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert "decode" in call_kwargs["message"].lower()
        assert "json" in call_kwargs["message"].lower()
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
            "arguments": {"codec": "avro", "blob": serializer.encode(["hello"], codec="avro")},
            "payload_codec": "zstd",
        }

        outcome = await worker._run_activity_task(task)

        assert outcome == "decode_error"
        mock_client.fail_activity_task.assert_called_once()
        call_kwargs = mock_client.fail_activity_task.call_args.kwargs
        assert "unsupported_payload_codec" in call_kwargs["message"]
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
            "payload_codec": "avro",
        }
        await worker._run_workflow_task(task)
        mock_client.fail_workflow_task.assert_called_once()
        call_kwargs = mock_client.fail_workflow_task.call_args.kwargs
        assert "decode" in call_kwargs["message"].lower()
        assert "json" in call_kwargs["message"].lower()
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
            "arguments": {"codec": "avro", "blob": serializer.encode(["hello"], codec="avro")},
            "payload_codec": "zstd",
        }

        commands = await worker._run_workflow_task(task)

        assert commands is None
        mock_client.fail_workflow_task.assert_called_once()
        call_kwargs = mock_client.fail_workflow_task.call_args.kwargs
        assert "unsupported_payload_codec" in call_kwargs["message"]
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
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "zstd",
        }

        outcome = await worker._run_query_task(task)

        assert outcome == "failed"
        mock_client.fail_query_task.assert_called_once()
        call_kwargs = mock_client.fail_query_task.call_args.kwargs
        assert call_kwargs["reason"] == "query_payload_decode_failed"
        assert "unsupported_payload_codec" in call_kwargs["message"]
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
            "arguments": {"codec": "avro", "blob": serializer.encode(["hello"], codec="avro")},
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
            "arguments": serializer.encode([], codec="avro"),
            "payload_codec": "avro",
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
                "arguments": serializer.encode([], codec="avro"),
                "payload_codec": "avro",
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
                "arguments": serializer.encode([], codec="avro"),
                "payload_codec": "avro",
            }
            tasks.append(worker._track(_acquire_and_dispatch(task)))

        await asyncio.sleep(0.01)
        assert max_running == 2
        gate.set()
        await asyncio.gather(*tasks)
        assert mock_client.complete_activity_task.call_count == 4


class TestWorkerShutdown:
    @pytest.mark.asyncio
    async def test_worker_only_token_supports_complete_lifecycle(self) -> None:
        requests: list[tuple[str, str, dict[str, str]]] = []
        active_lifecycle = asyncio.Event()
        active_paths = {
            "/api/cluster/info",
            "/api/worker/register",
            "/api/worker/workflow-tasks/poll",
            "/api/worker/activity-tasks/poll",
            "/api/worker/heartbeat",
        }

        async def request(method: str, path: str, **kwargs: object) -> httpx.Response:
            headers = kwargs["headers"]
            assert isinstance(headers, dict)
            requests.append((method, path, headers))

            if path == "/api/cluster/info":
                payload: dict[str, object] = compatible_cluster_info(worker_protocol={"version": PROTOCOL_VERSION})
            elif path in {"/api/worker/register", "/api/worker/heartbeat"}:
                payload = {"worker_id": "worker-only", "acknowledged": True}
            elif path == "/api/worker/registrations/worker-only":
                payload = {
                    "worker_id": "worker-only",
                    "outcome": "deregistered",
                    "recovered_workflow_task_count": 0,
                }
            else:
                payload = {"task": None, "poll_status": "timeout"}

            if active_paths.issubset({observed_path for _, observed_path, _ in requests}):
                active_lifecycle.set()

            return httpx.Response(
                200,
                json=payload,
                request=httpx.Request(method, f"http://localhost:8080{path}"),
            )

        async with Client(
            "http://localhost:8080",
            worker_token="worker-token",
            namespace="workers",
        ) as client:
            client._http.request = AsyncMock(side_effect=request)  # type: ignore[method-assign]
            worker = Worker(
                client,
                task_queue="orders",
                worker_id="worker-only",
                poll_timeout=0.01,
                heartbeat_interval=0.01,
            )

            run_task = asyncio.create_task(worker.run())
            await asyncio.wait_for(active_lifecycle.wait(), timeout=1.0)
            await worker.stop()
            await run_task

        observed_paths = {path for _, path, _ in requests}
        expected_paths = active_paths | {"/api/worker/registrations/worker-only"}
        assert expected_paths <= observed_paths
        for _, _, headers in requests:
            assert headers["Authorization"] == "Bearer worker-token"

        discovery_headers = next(headers for _, path, headers in requests if path == "/api/cluster/info")
        assert discovery_headers["X-Durable-Workflow-Protocol-Version"] == PROTOCOL_VERSION
        assert "X-Durable-Workflow-Control-Plane-Version" not in discovery_headers

    @pytest.mark.asyncio
    async def test_normal_shutdown_drains_before_deregistering_once(self, mock_client: AsyncMock) -> None:
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            poll_timeout=0.01,
        )
        run_task = asyncio.create_task(worker.run())
        while mock_client.register_worker.await_count == 0:
            await asyncio.sleep(0)

        in_flight_started = asyncio.Event()
        release_in_flight = asyncio.Event()

        async def in_flight_task() -> None:
            in_flight_started.set()
            await release_in_flight.wait()

        tracked_task = worker._track(in_flight_task())
        await in_flight_started.wait()

        stop_task = asyncio.create_task(worker.stop())
        await asyncio.sleep(0)
        mock_client.deregister_worker_registration.assert_not_awaited()

        release_in_flight.set()
        await stop_task
        await run_task

        assert tracked_task.done()
        assert all(task.done() for task in worker._poller_tasks)
        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

    @pytest.mark.asyncio
    async def test_run_drains_accepted_work_before_deregistering(self, mock_client: AsyncMock) -> None:
        events: list[str] = []
        completion_started = asyncio.Event()
        release_completion = asyncio.Event()
        workflow_task = {
            "task_id": "t-shutdown-run",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
        }
        workflow_polled = False

        async def poll_workflow_task(**_: object) -> dict[str, object] | None:
            nonlocal workflow_polled
            events.append("workflow_poll")
            if not workflow_polled:
                workflow_polled = True
                return workflow_task
            await asyncio.Event().wait()
            return None

        async def complete_workflow_task(**_: object) -> dict[str, str]:
            events.append("workflow_completion_started")
            completion_started.set()
            await release_completion.wait()
            events.append("workflow_completion_finished")
            return {"outcome": "completed"}

        async def deregister_worker(_: str) -> dict[str, object]:
            events.append("deregister")
            return {"outcome": "deregistered"}

        mock_client.poll_workflow_task.side_effect = poll_workflow_task
        mock_client.complete_workflow_task.side_effect = complete_workflow_task
        mock_client.deregister_worker_registration.side_effect = deregister_worker
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            poll_timeout=0.01,
        )

        run_task = asyncio.create_task(worker.run())
        await completion_started.wait()
        stop_task = asyncio.create_task(worker.stop())
        await asyncio.sleep(0)

        mock_client.deregister_worker_registration.assert_not_awaited()
        release_completion.set()
        await stop_task
        await run_task

        assert events[-2:] == ["workflow_completion_finished", "deregister"]
        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

    @pytest.mark.asyncio
    async def test_shutdown_deadline_cancels_work_before_deregistering(
        self, mock_client: AsyncMock
    ) -> None:
        events: list[str] = []
        work_started = asyncio.Event()

        async def accepted_work() -> None:
            work_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("work_cancelled")

        async def deregister_worker(_: str) -> dict[str, object]:
            events.append("deregister")
            return {"outcome": "deregistered"}

        mock_client.deregister_worker_registration.side_effect = deregister_worker
        worker = Worker(mock_client, task_queue="q1", shutdown_timeout=0.01)
        await worker._register()
        tracked_task = worker._track(accepted_work())
        await work_started.wait()

        await worker.stop()

        assert tracked_task.cancelled()
        assert events == ["work_cancelled", "deregister"]
        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

    @pytest.mark.asyncio
    async def test_failed_registration_does_not_deregister(self, mock_client: AsyncMock) -> None:
        mock_client.register_worker.side_effect = RuntimeError("registration failed")
        worker = Worker(mock_client, task_queue="q1")

        with pytest.raises(RuntimeError, match="registration failed"):
            await worker.run()

        await worker.stop()
        mock_client.deregister_worker_registration.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_repeated_stop_paths_share_one_deregistration(self, mock_client: AsyncMock) -> None:
        worker = Worker(mock_client, task_queue="q1")
        await worker._register()

        await asyncio.gather(worker.stop(), worker.stop())
        await worker.stop()

        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

    @pytest.mark.asyncio
    async def test_deregistration_403_is_propagated_without_retrying(self, mock_client: AsyncMock) -> None:
        cleanup_error = ServerError(
            403,
            {
                "reason": "worker_registration_forbidden",
                "message": "worker token cannot deregister this registration",
            },
        )
        mock_client.deregister_worker_registration.side_effect = cleanup_error
        worker = Worker(mock_client, task_queue="q1")
        await worker._register()

        for _ in range(2):
            with pytest.raises(ServerError) as exc_info:
                await worker.stop()
            assert exc_info.value.status == 403
            assert exc_info.value.reason() == "worker_registration_forbidden"

        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

    @pytest.mark.asyncio
    async def test_worker_loop_error_remains_primary_when_deregistration_fails(self, mock_client: AsyncMock) -> None:
        cleanup_error = ServerError(
            403,
            {
                "reason": "worker_registration_forbidden",
                "message": "worker token cannot deregister this registration",
            },
        )
        mock_client.deregister_worker_registration.side_effect = cleanup_error
        worker = Worker(mock_client, task_queue="q1")
        worker._poll_workflow_tasks = AsyncMock(  # type: ignore[method-assign]
            side_effect=RuntimeError("worker loop failed")
        )

        with pytest.raises(RuntimeError, match="worker loop failed") as exc_info:
            await worker.run()

        assert exc_info.value.__cause__ is cleanup_error
        assert cleanup_error.reason() == "worker_registration_forbidden"
        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

    @pytest.mark.asyncio
    async def test_query_returned_during_shutdown_is_not_dispatched_before_deregistering(
        self, mock_client: AsyncMock
    ) -> None:
        events: list[str] = []
        poll_started = threading.Event()
        query_task = {
            "query_task_id": "qt-shutdown",
            "query_task_attempt": 1,
            "workflow_type": "query-wf",
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "query_name": "status",
            "payload_codec": "avro",
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "history_events": [],
        }

        class QueryThreadClient:
            async def __aenter__(self) -> QueryThreadClient:
                return self

            async def __aexit__(self, *_: object) -> None:
                events.append("query_thread_exited")

            async def poll_query_task(self, **_: object) -> dict[str, object] | None:
                events.append("query_poll_started")
                poll_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    events.append("query_returned_during_shutdown")
                    return query_task

            async def complete_query_task(self, **_: object) -> dict[str, str]:
                events.append("query_dispatched")
                return {"outcome": "completed"}

            async def fail_query_task(self, **_: object) -> dict[str, str]:
                events.append("query_dispatched")
                return {"outcome": "failed"}

        async def deregister_worker(_: str) -> dict[str, object]:
            assert events[-1] == "query_thread_exited"
            events.append("deregister")
            return {"outcome": "deregistered"}

        query_client = QueryThreadClient()
        mock_client.deregister_worker_registration.side_effect = deregister_worker
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[QueryWorkflow],
            shutdown_timeout=0.2,
        )
        worker._clone_client_for_query_tasks = lambda: query_client  # type: ignore[method-assign]
        await worker._register()
        worker._start_query_task_thread()

        assert await asyncio.to_thread(poll_started.wait, 1.0)
        await worker.stop()

        assert events == [
            "query_poll_started",
            "query_returned_during_shutdown",
            "query_thread_exited",
            "deregister",
        ]
        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

    @pytest.mark.asyncio
    async def test_live_query_thread_blocks_deregistration_with_actionable_failure(
        self, mock_client: AsyncMock
    ) -> None:
        poll_started = threading.Event()
        cancellation_seen = threading.Event()
        release_poll = threading.Event()

        class StuckQueryThreadClient:
            async def __aenter__(self) -> StuckQueryThreadClient:
                return self

            async def __aexit__(self, *_: object) -> None:
                return None

            async def poll_query_task(self, **_: object) -> None:
                poll_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancellation_seen.set()
                    while not release_poll.is_set():
                        await asyncio.sleep(0.01)
                return None

        query_client = StuckQueryThreadClient()
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[QueryWorkflow],
            shutdown_timeout=0.02,
        )
        worker._clone_client_for_query_tasks = lambda: query_client  # type: ignore[method-assign]
        await worker._register()
        worker._start_query_task_thread()

        assert await asyncio.to_thread(poll_started.wait, 1.0)
        with pytest.raises(RuntimeError, match="query poller thread.*registration remains active"):
            await worker.stop()

        assert cancellation_seen.is_set()
        mock_client.deregister_worker_registration.assert_not_awaited()
        release_poll.set()
        assert worker._query_thread is not None
        await asyncio.to_thread(worker._query_thread.join, 1.0)
        assert not worker._query_thread.is_alive()


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
            "payload_codec": "avro",
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
            "history_events": [],
        }

        class QueryThreadClient:
            def __init__(self) -> None:
                self.polled = False
                self.completed_kwargs: dict[str, object] | None = None

            async def __aenter__(self) -> QueryThreadClient:
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
    async def test_external_stop_drains_inline_workflow_completion_before_deregistering(
        self, mock_client: AsyncMock
    ) -> None:
        events: list[str] = []
        completion_started = asyncio.Event()
        release_completion = asyncio.Event()
        workflow_task = {
            "task_id": "t-run-until-shutdown",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
        }

        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(worker_protocol={"version": PROTOCOL_VERSION})
        )
        mock_client.describe_workflow = AsyncMock(
            return_value=WorkflowExecution(
                workflow_id="wf-1",
                run_id="run-1",
                workflow_type="test-wf",
                status="running",
            )
        )
        mock_client.poll_workflow_task = AsyncMock(return_value=workflow_task)

        async def complete_workflow_task(**_: object) -> dict[str, str]:
            events.append("workflow_completion_started")
            completion_started.set()
            await release_completion.wait()
            events.append("workflow_completion_finished")
            return {"outcome": "completed"}

        async def deregister_worker(_: str) -> dict[str, object]:
            events.append("deregister")
            return {"outcome": "deregistered"}

        mock_client.complete_workflow_task.side_effect = complete_workflow_task
        mock_client.deregister_worker_registration.side_effect = deregister_worker
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            poll_timeout=0.01,
        )

        run_until_task = asyncio.create_task(
            worker.run_until(workflow_id="wf-1", timeout=1.0, poll_interval=0.01)
        )
        await completion_started.wait()
        stop_task = asyncio.create_task(worker.stop())
        await asyncio.sleep(0)

        mock_client.deregister_worker_registration.assert_not_awaited()
        release_completion.set()
        await stop_task
        with pytest.raises(asyncio.CancelledError):
            await run_until_task

        assert events == [
            "workflow_completion_started",
            "workflow_completion_finished",
            "deregister",
        ]
        request_counts = (
            mock_client.describe_workflow.await_count,
            mock_client.poll_workflow_task.await_count,
            mock_client.complete_workflow_task.await_count,
        )
        await asyncio.sleep(0)
        assert request_counts == (
            mock_client.describe_workflow.await_count,
            mock_client.poll_workflow_task.await_count,
            mock_client.complete_workflow_task.await_count,
        )
        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

    @pytest.mark.asyncio
    async def test_external_stop_drains_inline_activity_completion_before_deregistering(
        self, mock_client: AsyncMock
    ) -> None:
        events: list[str] = []
        completion_started = asyncio.Event()
        release_completion = asyncio.Event()
        workflow_task = {
            "task_id": "t-run-until-activity-shutdown",
            "workflow_type": "test-wf",
            "workflow_task_attempt": 1,
            "history_events": [],
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
        }
        activity_task = {
            "task_id": "at-run-until-shutdown",
            "activity_attempt_id": "aa-run-until-shutdown",
            "activity_type": "test-act",
            "arguments": serializer.encode(["hello"], codec="avro"),
            "payload_codec": "avro",
        }

        mock_client.get_cluster_info = AsyncMock(
            return_value=compatible_cluster_info(worker_protocol={"version": PROTOCOL_VERSION})
        )
        mock_client.describe_workflow = AsyncMock(
            return_value=WorkflowExecution(
                workflow_id="wf-1",
                run_id="run-1",
                workflow_type="test-wf",
                status="running",
            )
        )
        mock_client.poll_workflow_task = AsyncMock(return_value=workflow_task)
        mock_client.poll_activity_task = AsyncMock(return_value=activity_task)

        async def complete_activity_task(**_: object) -> dict[str, str]:
            events.append("activity_completion_started")
            completion_started.set()
            await release_completion.wait()
            events.append("activity_completion_finished")
            return {"outcome": "completed"}

        async def deregister_worker(_: str) -> dict[str, object]:
            events.append("deregister")
            return {"outcome": "deregistered"}

        mock_client.complete_activity_task.side_effect = complete_activity_task
        mock_client.deregister_worker_registration.side_effect = deregister_worker
        worker = Worker(
            mock_client,
            task_queue="q1",
            workflows=[TestWorkflow],
            activities=[echo_activity],
            poll_timeout=0.01,
        )

        run_until_task = asyncio.create_task(
            worker.run_until(workflow_id="wf-1", timeout=1.0, poll_interval=0.01)
        )
        await completion_started.wait()
        stop_task = asyncio.create_task(worker.stop())
        await asyncio.sleep(0)

        mock_client.deregister_worker_registration.assert_not_awaited()
        release_completion.set()
        await stop_task
        with pytest.raises(asyncio.CancelledError):
            await run_until_task

        assert events == [
            "activity_completion_started",
            "activity_completion_finished",
            "deregister",
        ]
        mock_client.deregister_worker_registration.assert_awaited_once_with(worker.worker_id)

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
            "payload_codec": "avro",
            "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"),
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
