"""Workflow code signals another workflow durably (.github#223, server#1134)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import ExternalSignalResult, SignalExternalWorkflow, Worker, serializer, workflow
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.workflow import (
    CompleteWorkflow,
    FailWorkflow,
    WorkflowContext,
    commands_to_server_commands,
    replay,
)
from tests.test_worker import compatible_cluster_info, mock_client  # noqa: F401

SENDER_RUN = "sender-run"
REQUEST_ID = f"workflow-signal:{SENDER_RUN}:1"


class Priority(Enum):
    HIGH = "high"


@dataclass
class Approval:
    approver: str
    priority: Priority


@workflow.defn(name="tests.signal-external.sender")
class SenderWorkflow:
    def run(self, ctx: WorkflowContext, target: str, run_id: str | None = None):  # type: ignore[no-untyped-def]
        result = yield ctx.signal_external_workflow(target, "approve", ["ada", 3], run_id=run_id)
        return {
            "outcome": result.outcome,
            "reason": result.reason,
            "target_run_id": result.target_run_id,
            "target_command_id": result.target_command_id,
            "delivered": result.delivered,
        }


@workflow.defn(name="tests.signal-external.then-timer")
class SignalThenTimerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        result = yield ctx.signal_external_workflow("order-42", "approve", ["ada", 3])
        yield ctx.start_timer(5)
        return result.outcome


@workflow.defn(name="tests.signal-external.parallel")
class ParallelSignalWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield [ctx.signal_external_workflow("order-42", "approve")])


def _started() -> dict[str, Any]:
    return {"event_type": "WorkflowStarted", "payload": {"timestamp": "2026-11-19T00:00:00.000000Z"}}


def _requested(
    *, target: str = "order-42", run_id: str | None = None, name: str = "approve", args: list[Any] | None = None,
) -> dict[str, Any]:
    return {"event_type": "ExternalSignalRequested", "payload": {
        "sequence": 1,
        "target_workflow_instance_id": target,
        "target_workflow_run_id": run_id,
        "signal_name": name,
        "arguments": serializer.envelope(["ada", 3] if args is None else args),
        "payload_codec": "avro",
        "request_id": REQUEST_ID,
    }}


def _delivered(*, target_run: str = "target-run", retained: bool = False) -> dict[str, Any]:
    return {"event_type": "ExternalSignalDelivered", "payload": {
        "sequence": 1,
        "target_workflow_instance_id": "order-42",
        "target_workflow_run_id": target_run,
        "signal_name": "approve",
        "request_id": REQUEST_ID,
        "outcome": "delivered",
        "target_command_id": "target-command",
        "target_signal_id": "target-signal",
        "retained": retained,
    }}


def _failed(outcome: str, reason: str, *, target_run: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "sequence": 1,
        "target_workflow_instance_id": "order-42",
        "target_workflow_run_id": target_run,
        "signal_name": "approve",
        "request_id": REQUEST_ID,
        "outcome": outcome,
        "reason": reason,
        "message": f"signal not delivered: {reason}",
    }
    if outcome == "target_closed":
        payload["target_command_id"] = "rejected-command"
    return {"event_type": "ExternalSignalFailed", "payload": payload}


class TestCommand:
    def test_serializes_the_contract_shape(self) -> None:
        outcome = replay(SenderWorkflow, [], ["order-42"], run_id=SENDER_RUN)
        assert len(outcome.commands) == 1
        command = outcome.commands[0]
        assert isinstance(command, SignalExternalWorkflow)

        [wire] = commands_to_server_commands(outcome.commands, "orders")
        assert set(wire) == {"type", "workflow_id", "run_id", "signal_name", "arguments"}
        assert wire["type"] == "signal_external_workflow"
        assert wire["workflow_id"] == "order-42"
        assert wire["run_id"] is None
        assert wire["signal_name"] == "approve"
        assert set(wire["arguments"]) == {"codec", "blob"}
        assert wire["arguments"]["codec"] == "avro"
        assert serializer.decode_envelope(wire["arguments"]) == ["ada", 3]

    def test_targets_a_named_run(self) -> None:
        outcome = replay(SenderWorkflow, [], ["order-42", "target-run"], run_id=SENDER_RUN)
        [wire] = commands_to_server_commands(outcome.commands, "orders")
        assert wire["run_id"] == "target-run"

    def test_no_arguments_send_an_encoded_empty_array(self) -> None:
        command = WorkflowContext().signal_external_workflow("order-42", "approve")
        wire = command.to_server_command("orders")
        assert serializer.decode_envelope(wire["arguments"]) == []

    def test_arguments_use_avro_even_with_another_command_codec_and_no_offload(self) -> None:
        command = WorkflowContext().signal_external_workflow("order-42", "approve", [Approval("ada", Priority.HIGH)])
        [wire] = commands_to_server_commands([command], "orders", external_storage=object(),  # type: ignore[arg-type]
                                             external_storage_threshold_bytes=0)
        assert wire["arguments"]["codec"] == "avro"
        assert serializer.decode_envelope(wire["arguments"]) == [{"approver": "ada", "priority": "high"}]

    def test_is_not_a_cancellation_scope_or_parallel_member(self) -> None:
        command = WorkflowContext().signal_external_workflow("order-42", "approve")
        [wire] = commands_to_server_commands([command], "orders")
        assert "cancellation_scope_id" not in wire
        assert not any(key.startswith("parallel_group") for key in wire)

    def test_parallel_group_member_is_refused(self) -> None:
        outcome = replay(ParallelSignalWorkflow, [], [], run_id=SENDER_RUN)
        [command] = outcome.commands
        assert isinstance(command, FailWorkflow)
        assert "external workflow signals must be yielded one at a time" in command.message

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"workflow_id": "", "signal_name": "approve"}, "workflow_id"),
            ({"workflow_id": "  ", "signal_name": "approve"}, "workflow_id"),
            ({"workflow_id": "x" * 192, "signal_name": "approve"}, "191"),
            ({"workflow_id": "order-42", "signal_name": " "}, "signal_name"),
            ({"workflow_id": "order-42", "signal_name": "é" * 128}, "255 bytes"),
            ({"workflow_id": "order-42", "signal_name": "approve", "run_id": ""}, "run_id"),
        ],
    )
    def test_refuses_invalid_targets_before_submission(self, kwargs: dict[str, Any], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            SignalExternalWorkflow(**kwargs)


class TestReplay:
    def test_delivered_outcome_resumes_workflow(self) -> None:
        outcome = replay(
            SenderWorkflow, [_started(), _requested(), _delivered()], ["order-42"], run_id=SENDER_RUN,
        )
        assert outcome.commands == [CompleteWorkflow(result={
            "outcome": "delivered", "reason": None, "target_run_id": "target-run",
            "target_command_id": "target-command", "delivered": True,
        })]

    @pytest.mark.parametrize(
        ("outcome", "reason", "target_run"),
        [
            ("target_not_found", "instance_not_found", None),
            ("target_not_found", "run_not_found", None),
            ("target_closed", "run_not_active", "closed-run"),
            ("target_closed", "historical_run", "old-run"),
            ("rejected", "unknown_signal", "target-run"),
            ("rejected", "self_target", None),
        ],
    )
    def test_failed_outcomes_are_values(self, outcome: str, reason: str, target_run: str | None) -> None:
        result = replay(
            SenderWorkflow, [_started(), _requested(), _failed(outcome, reason, target_run=target_run)],
            ["order-42"], run_id=SENDER_RUN,
        )
        [command] = result.commands
        assert isinstance(command, CompleteWorkflow)
        assert command.result["outcome"] == outcome
        assert command.result["reason"] == reason
        assert command.result["target_run_id"] == target_run
        assert command.result["delivered"] is False

    def test_recorded_request_without_outcome_is_never_sent_again(self) -> None:
        outcome = replay(SenderWorkflow, [_started(), _requested()], ["order-42"], run_id=SENDER_RUN)
        assert outcome.commands == []

    def test_resolved_signal_is_not_sent_again_before_the_next_command(self) -> None:
        outcome = replay(SignalThenTimerWorkflow, [_started(), _requested(), _delivered()], [], run_id=SENDER_RUN)
        assert [type(command).__name__ for command in outcome.commands] == ["StartTimer"]

    def test_retained_delivery_is_reported(self) -> None:
        payload = _delivered(retained=True)["payload"]
        result = ExternalSignalResult.from_history_payload("ExternalSignalDelivered", payload)
        assert result.retained is True
        assert result.delivered is True
        assert result.target_signal_id == "target-signal"
        assert result.request_id == REQUEST_ID

    @pytest.mark.parametrize(
        ("requested", "detail"),
        [
            (_requested(target="order-43"), "external_signal_target_changed"),
            (_requested(run_id="other-run"), "external_signal_target_changed"),
            (_requested(name="reject"), "external_signal_name_changed"),
            (_requested(args=["ada", 4]), "external_signal_arguments_changed"),
        ],
    )
    def test_changed_intent_is_nondeterministic(self, requested: dict[str, Any], detail: str) -> None:
        for history in ([_started(), requested, _delivered()], [_started(), requested]):
            with pytest.raises(NonDeterministicReplayError, match=detail) as captured:
                replay(SenderWorkflow, history, ["order-42"], run_id=SENDER_RUN)
            assert captured.value.workflow_sequence == 1

    def test_other_command_at_the_sequence_is_nondeterministic(self) -> None:
        @workflow.defn(name="tests.signal-external.changed-to-timer")
        class ChangedToTimer:
            def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
                yield ctx.start_timer(1)

        with pytest.raises(NonDeterministicReplayError) as captured:
            replay(ChangedToTimer, [_started(), _requested(), _delivered()], [], run_id=SENDER_RUN)
        assert captured.value.workflow_sequence == 1

    def test_unknown_failed_outcome_is_refused(self) -> None:
        with pytest.raises(NonDeterministicReplayError, match="external_signal_history_invalid"):
            replay(
                SenderWorkflow, [_started(), _requested(), _failed("lost", "unknown")], ["order-42"],
                run_id=SENDER_RUN,
            )


def _capabilities(**overrides: Any) -> dict[str, object]:
    info = compatible_cluster_info()
    worker_protocol = dict(info["worker_protocol"])  # type: ignore[call-overload]
    capabilities = dict(worker_protocol["server_capabilities"])
    capabilities.update(overrides)
    worker_protocol["server_capabilities"] = capabilities
    info["worker_protocol"] = worker_protocol
    return info


async def _run_first_task(client: AsyncMock, info: dict[str, object]) -> Worker:
    client.get_cluster_info = AsyncMock(return_value=info)
    worker = Worker(client, task_queue="q1", workflows=[SenderWorkflow], activities=[])
    await worker._register()
    client.complete_workflow_task.reset_mock()
    client.fail_workflow_task.reset_mock()
    await worker._run_workflow_task({
        "task_id": "signal-task",
        "workflow_type": "tests.signal-external.sender",
        "run_id": SENDER_RUN,
        "workflow_task_attempt": 1,
        "history_events": [],
        "arguments": serializer.encode(["order-42"], codec="avro"),
        "payload_codec": "avro",
    })
    return worker


class TestCapabilityGate:
    @pytest.mark.asyncio
    async def test_submits_when_the_server_advertises_the_command(
        self, mock_client: AsyncMock,  # noqa: F811
    ) -> None:
        await _run_first_task(mock_client, _capabilities(
            signal_external_workflow=True,
            supported_workflow_task_commands=["signal_external_workflow", "complete_workflow"],
        ))
        mock_client.fail_workflow_task.assert_not_called()
        [command] = mock_client.complete_workflow_task.await_args.kwargs["commands"]
        assert command["type"] == "signal_external_workflow"
        assert command["workflow_id"] == "order-42"

    @pytest.mark.parametrize(
        "overrides",
        [
            {},
            {"signal_external_workflow": False},
            {"signal_external_workflow": True, "supported_workflow_task_commands": ["complete_workflow"]},
        ],
    )
    @pytest.mark.asyncio
    async def test_refuses_before_submitting_without_the_capability(
        self, mock_client: AsyncMock, overrides: dict[str, Any],  # noqa: F811
    ) -> None:
        await _run_first_task(mock_client, _capabilities(**overrides))
        mock_client.complete_workflow_task.assert_not_called()
        kwargs = mock_client.fail_workflow_task.await_args.kwargs
        assert kwargs["message"].startswith("signal_external_workflow_unavailable:")
        assert kwargs["failure_type"] == "RuntimeCapabilityUnsupported"

    @pytest.mark.asyncio
    async def test_replayed_outcome_needs_no_capability(
        self, mock_client: AsyncMock,  # noqa: F811
    ) -> None:
        mock_client.get_cluster_info = AsyncMock(return_value=_capabilities())
        worker = Worker(mock_client, task_queue="q1", workflows=[SenderWorkflow], activities=[])
        await worker._register()
        mock_client.complete_workflow_task.reset_mock()
        await worker._run_workflow_task({
            "task_id": "signal-task-2",
            "workflow_type": "tests.signal-external.sender",
            "run_id": SENDER_RUN,
            "workflow_task_attempt": 1,
            "history_events": [_started(), _requested(), _delivered()],
            "arguments": serializer.encode(["order-42"], codec="avro"),
            "payload_codec": "avro",
        })
        [command] = mock_client.complete_workflow_task.await_args.kwargs["commands"]
        assert command["type"] == "complete_workflow"
