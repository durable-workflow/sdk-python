from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import NexusOperationFailed, NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.external_storage import LocalFilesystemExternalStorage
from durable_workflow.workflow import (
    CompleteWorkflow,
    NexusServiceCall,
    WorkflowContext,
    commands_to_server_commands,
    replay,
)
from tests.test_cooperative_cancellation import marker, request


@workflow.defn(name="tests.nexus.durable-wait")
class DurableNexusCaller:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        result = yield ctx.call_nexus_service(
            "php-service", "greeter", "greet", ["world"],
            target_namespace="shared", idempotency_key="original-request",
            mode="sync", wait_for="completed",
        )
        return result.result


def _service_event(event_type: str, status: str, result: object = None) -> dict[str, Any]:
    response = {"codec": "avro", "blob": serializer.encode(result)} if status == "completed" else None
    return {
        "event_type": event_type,
        "payload": {
            "sequence": 1,
            "service_call_id": "original-service-call",
            "caller_workflow_instance_id": "caller",
            "caller_workflow_run_id": "caller-run",
            "endpoint_name": "php-service",
            "service_name": "greeter",
            "operation_name": "greet",
            "request_payload": {"codec": "avro", "blob": serializer.encode(["world"])},
            "response_payload": response,
            "payload_codec": "avro",
            "operation_mode": "sync",
            "wait_for": "completed",
            "status": status,
            "service_call": {
                "service_call_id": "original-service-call",
                "namespace": "shared",
                "target_namespace": "shared",
                "idempotency_key": "original-request",
                "endpoint_name": "php-service",
                "service_name": "greeter",
                "operation_name": "greet",
                "caller_workflow_instance_id": "caller",
                "caller_workflow_run_id": "caller-run",
                "status": status,
                "operation_mode": "sync",
                "wait_for": "completed",
                "result_envelope": response,
                "metadata": {"target_namespace": "shared"},
            },
        },
    }


def test_new_nexus_call_serializes_the_leased_service_operation() -> None:
    state = replay(DurableNexusCaller, [], [], workflow_id="caller", run_id="caller-run")
    assert len(state.commands) == 1
    assert isinstance(state.commands[0], NexusServiceCall)
    commands = commands_to_server_commands(state.commands, "caller-queue")
    assert len(commands) == 1
    command = commands[0]
    assert command["type"] == "start_service_operation"
    assert command["namespace"] == "shared"
    assert command["idempotency_key"] == "original-request"
    assert command["mode_override"] == "sync"
    assert command["wait_for"] == "completed"
    assert serializer.decode(command["request_payload"], codec="avro") == ["world"]


def test_started_call_stays_pending_and_cold_replay_receives_terminal_result() -> None:
    started = _service_event("ServiceCallStarted", "started")
    pending = replay(DurableNexusCaller, [started], [], workflow_id="caller", run_id="caller-run")
    assert pending.commands == []

    expected = {"greeting": "hello world"}
    completed = _service_event("ServiceCallCompleted", "completed", expected)
    resumed = replay(DurableNexusCaller, [started, completed], [], workflow_id="caller", run_id="caller-run")
    assert len(resumed.commands) == 1
    assert isinstance(resumed.commands[0], CompleteWorkflow)
    assert resumed.commands[0].result == expected


def test_cancellation_request_and_cold_replays_do_not_readmit_pending_call() -> None:
    started = _service_event("ServiceCallStarted", "started")
    requested = {
        "event_type": "CooperativeCancellationRequested",
        "recorded_at": "2026-10-12T07:00:00Z",
        "payload": {
            "workflow_command_id": "original-cancellation",
            "workflow_run_id": "caller-run",
            "cleanup_deadline_at": "2026-10-12T07:00:30Z",
        },
    }
    for history in ([started], [started, requested]):
        for _ in range(3):
            # A fresh workflow instance represents both another task and a
            # replacement worker. Neither may allocate another admission.
            outcome = replay(DurableNexusCaller, history, [], workflow_id="caller", run_id="caller-run")
            assert outcome.commands == []
            if len(history) == 1:
                assert outcome.cancellation_delivery is None
            else:
                assert outcome.cancellation_delivery is not None
                assert outcome.cancellation_delivery.request_id == "original-cancellation"
                assert outcome.cancellation_delivery.sequence == 1
                assert outcome.cancellation_delivery.call_kind == "service_operation"


@workflow.defn(name="tests.nexus.cooperative-cleanup")
class CooperativeNexusCaller:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.call_nexus_service(
                "php-service", "greeter", "greet", ["world"],
                target_namespace="shared", idempotency_key="original-request",
            )
        except WorkflowCancelled as error:
            with ctx.cancellation_shield():
                yield ctx.start_timer(1)
            return error.request_id


def test_canonical_service_delivery_replays_cleanup_at_original_boundary() -> None:
    history = [
        _service_event("ServiceCallStarted", "started"), request(),
        _service_event("ServiceCallCancelled", "cancelled"), marker(1, "service_operation"),
    ]
    for _ in range(3):
        outcome = replay(CooperativeNexusCaller, history, [], run_id="run-1")
        assert outcome.cancellation_delivery is None
        commands = commands_to_server_commands(outcome.commands, "queue")
        assert len(commands) == 1 and commands[0]["type"] == "start_timer"
        assert commands[0]["delay_seconds"] == 1
    history.extend([
        {"event_type": "TimerScheduled", "payload": {"sequence": 2, "delay_seconds": 1}},
        {"event_type": "TimerFired", "payload": {"sequence": 2}},
    ])
    completed = replay(CooperativeNexusCaller, history, [], run_id="run-1")
    assert isinstance(completed.commands[0], CompleteWorkflow)
    assert completed.commands[0].result == "request-1"


@pytest.mark.parametrize("terminal,status", [
    ("ServiceCallCompleted", "completed"), ("ServiceCallFailed", "failed"), ("ServiceCallCancelled", "cancelled"),
])
def test_service_result_committed_before_request_is_not_replaced(terminal: str, status: str) -> None:
    history = [_service_event("ServiceCallStarted", "started"), _service_event(terminal, status, "original"), request()]
    outcome = replay(CatchingNexusCaller, history, [], run_id="run-1")
    assert outcome.cancellation_delivery is None
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    with pytest.raises(NonDeterministicReplayError, match="earlier committed result"):
        replay(CatchingNexusCaller, [*history, marker(1, "service_operation")], [], run_id="run-1")


@pytest.mark.parametrize("canonical", [False, True])
def test_cancellation_preserves_original_service_target(canonical: bool) -> None:
    started = _service_event("ServiceCallStarted", "started")
    started["payload"]["service_call"]["target_namespace"] = "another-tenant"
    history = [started, request()]
    if canonical:
        history.append(marker(1, "service_operation"))
    with pytest.raises(NonDeterministicReplayError, match="nexus_target_namespace_changed"):
        replay(CooperativeNexusCaller, history, [], run_id="run-1")


@workflow.defn(name="tests.nexus.catch-failure")
class CatchingNexusCaller:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.call_nexus_service("php-service", "greeter", "greet", ["world"],
                                         target_namespace="shared", idempotency_key="original-request")
        except NexusOperationFailed as error:
            return {"status": error.status, "type": error.caller_observed_error_type, "call": error.service_call_id}


@pytest.mark.parametrize("status,event_type", [("failed", "ServiceCallFailed"), ("cancelled", "ServiceCallCancelled")])
def test_terminal_failure_is_delivered_at_the_original_yield(status: str, event_type: str) -> None:
    terminal = _service_event(event_type, status)
    terminal["payload"]["service_call"]["caller_observed_error_type"] = None
    terminal["payload"]["exception_type"] = "service_call_" + status
    outcome = replay(CatchingNexusCaller, [_service_event("ServiceCallStarted", "started"), terminal], [],
                     workflow_id="caller", run_id="caller-run")
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == {
        "status": status, "type": "service_call_" + status, "call": "original-service-call",
    }


@workflow.defn(name="tests.nexus.acceptance")
class AcceptanceNexusCaller:
    def run(self, ctx: WorkflowContext, mode: str, wait_for: str):  # type: ignore[no-untyped-def]
        result = yield ctx.call_nexus_service("php-service", "greeter", "greet", ["world"],
                                             target_namespace="shared", idempotency_key="original-request",
                                             mode=mode, wait_for=wait_for)
        return result.service_call_id


@pytest.mark.parametrize("mode,wait_for", [("sync", "accepted"), ("async", "completed")])
def test_explicit_acceptance_resumes_without_terminal_delivery(mode: str, wait_for: str) -> None:
    started = _service_event("ServiceCallStarted", "started")
    started["payload"].update({"operation_mode": mode, "wait_for": wait_for})
    for history in ([started], [started, request()]):
        outcome = replay(AcceptanceNexusCaller, history, [mode, wait_for])
        assert outcome.cancellation_delivery is None
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "original-service-call"


def test_pending_call_refuses_changed_target() -> None:
    started = _service_event("ServiceCallStarted", "started")
    started["payload"]["service_call"]["target_namespace"] = "another-tenant"
    with pytest.raises(NonDeterministicReplayError, match="nexus_target_namespace_changed"):
        replay(DurableNexusCaller, [started], [], workflow_id="caller", run_id="caller-run")


def test_external_result_survives_cold_replay(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    expected = {"greeting": "hello world" * 100}
    terminal = _service_event("ServiceCallCompleted", "completed")
    terminal["payload"]["response_payload"] = serializer.external_storage_envelope(
        expected, external_storage=storage, threshold_bytes=1, codec="avro",
    )
    outcome = replay(DurableNexusCaller, [_service_event("ServiceCallStarted", "started"), terminal], [],
                     workflow_id="caller", run_id="caller-run", external_storage=storage)
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == expected
