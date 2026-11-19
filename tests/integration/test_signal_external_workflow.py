"""Workflow code signals another workflow through an installed Server (.github#223).

The delivery scenarios need a Server that advertises ``signal_external_workflow``
(native Server, server#1134) and skip otherwise. The capability-gate scenario
needs a Server that does not advertise it and skips otherwise.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from durable_workflow import Client, Worker, workflow
from durable_workflow.workflow import CompleteWorkflow, SignalExternalWorkflow, WorkflowContext, replay
from tests.worker_observation import stop_and_drain

SELF = "__self__"


@workflow.defn(name="tests.signal-external.receiver")
class ReceiverWorkflow:
    def __init__(self) -> None:
        self.received: list[Any] | None = None

    @workflow.signal("approve")
    def approve(self, approver: str, count: int) -> None:
        self.received = [approver, count]

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.received is not None, key="approved", timeout=120)
        return self.received


@workflow.defn(name="tests.signal-external.closed-target")
class ClosedTargetWorkflow:
    @workflow.signal("approve")
    def approve(self, approver: str, count: int) -> None:
        raise AssertionError("a closed run must not apply a signal")

    def run(self, ctx: WorkflowContext) -> str:
        return "closed"


@workflow.defn(name="tests.signal-external.integration-sender")
class IntegrationSenderWorkflow:
    def __init__(self) -> None:
        self.released = False

    @workflow.signal("release")
    def release(self) -> None:
        self.released = True

    def run(self, ctx: WorkflowContext, target: str, hold: bool):  # type: ignore[no-untyped-def]
        target_id = ctx.workflow_id if target == SELF else target
        result = yield ctx.signal_external_workflow(target_id, "approve", ["ada", 3])
        if hold:
            yield ctx.wait_condition(lambda: self.released, key="released", timeout=120)
        return {
            "outcome": result.outcome,
            "reason": result.reason,
            "target_run_id": result.target_run_id,
            "target_command_id": result.target_command_id,
        }


@workflow.defn(name="tests.signal-external.counting-receiver")
class CountingReceiverWorkflow:
    def __init__(self) -> None:
        self.received: list[list[Any]] = []

    @workflow.signal("approve")
    def approve(self, approver: str, count: int) -> None:
        self.received.append([approver, count])

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: len(self.received) >= 2, key="approved-twice", timeout=120)
        return self.received


@workflow.defn(name="tests.signal-external.twice-then-timer")
class SignalTwiceThenTimerWorkflow:
    """Each signal resolves on its own turn; the timer takes the next sequence."""

    def run(self, ctx: WorkflowContext, target: str):  # type: ignore[no-untyped-def]
        first = yield ctx.signal_external_workflow(target, "approve", ["ada", 1])
        second = yield ctx.signal_external_workflow(target, "approve", ["ada", 2])
        yield ctx.start_timer(1)
        return [first.outcome, second.outcome]


WORKFLOWS = [
    ReceiverWorkflow, ClosedTargetWorkflow, IntegrationSenderWorkflow,
    CountingReceiverWorkflow, SignalTwiceThenTimerWorkflow,
]


async def _advertised(client: Client) -> bool:
    info = await client.get_cluster_info()
    protocol = info.get("worker_protocol") if isinstance(info, dict) else None
    capabilities = protocol.get("server_capabilities") if isinstance(protocol, dict) else None
    return isinstance(capabilities, dict) and capabilities.get("signal_external_workflow") is True


async def _events(client: Client, workflow_id: str, run_id: str) -> list[dict[str, Any]]:
    return [event async for event in client.iter_history(workflow_id, run_id)]


def _of_type(events: list[dict[str, Any]], event_type: str) -> list[dict[str, Any]]:
    return [event for event in events if event.get("event_type") == event_type]


async def _eventually(check: Callable[[], Awaitable[bool]], *, timeout: float = 30.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not await check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached before the deadline")
        await asyncio.sleep(0.1)


async def _run_sender(client: Client, queue: str, target: str) -> tuple[Any, dict[str, Any]]:
    sender_id = f"{queue}-sender"
    handle = await client.start_workflow(
        workflow_type="tests.signal-external.integration-sender", task_queue=queue,
        workflow_id=sender_id, input=[target, False],
    )
    worker = Worker(client, task_queue=queue, workflows=WORKFLOWS, worker_id=f"{queue}-worker",
                    poll_timeout=1.0, shutdown_timeout=5.0)
    completed = await worker.run_until(workflow_id=sender_id, timeout=60.0, poll_interval=0.1)
    assert (completed.status or "").lower() == "completed"
    return handle, completed.output


async def test_signal_is_delivered_once_and_cold_replay_never_resends(server_url: str, server_token: str) -> None:
    queue = f"signal-external-{uuid.uuid4().hex[:10]}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        if not await _advertised(client):
            pytest.skip("Server does not advertise signal_external_workflow")
        receiver = await client.start_workflow(
            workflow_type="tests.signal-external.receiver", task_queue=queue,
            workflow_id=f"{queue}-receiver", input=[],
        )
        sender_id = f"{queue}-sender"
        sender = await client.start_workflow(
            workflow_type="tests.signal-external.integration-sender", task_queue=queue,
            workflow_id=sender_id, input=[receiver.workflow_id, True],
        )
        assert sender.run_id is not None and receiver.run_id is not None

        first = Worker(client, task_queue=queue, workflows=WORKFLOWS, worker_id=f"{queue}-first",
                       poll_timeout=1.0, shutdown_timeout=5.0)
        first_runner = asyncio.create_task(first.run())
        try:
            assert await receiver.result(timeout=30.0, poll_interval=0.1) == ["ada", 3]

            async def sender_holds() -> bool:
                events = await _events(client, sender_id, sender.run_id or "")
                return bool(_of_type(events, "ExternalSignalDelivered")) and bool(
                    _of_type(events, "ConditionWaitOpened"))

            await _eventually(sender_holds)
        finally:
            await stop_and_drain([first], [first_runner])

        # A fresh worker has no cached state: it replays the recorded signal
        # from history, and must resolve it without sending it again.
        replacement = Worker(client, task_queue=queue, workflows=WORKFLOWS, worker_id=f"{queue}-cold",
                             poll_timeout=1.0, shutdown_timeout=5.0)
        replacement_task = asyncio.create_task(
            replacement.run_until(workflow_id=sender_id, timeout=60.0, poll_interval=0.1),
        )
        await asyncio.sleep(0.2)
        await sender.signal("release")
        completed = await replacement_task
        assert (completed.status or "").lower() == "completed"
        result = completed.output
        assert result["outcome"] == "delivered"
        assert result["reason"] is None
        assert result["target_run_id"] == receiver.run_id
        assert result["target_command_id"]

        sender_events = await _events(client, sender_id, sender.run_id)
        requested = _of_type(sender_events, "ExternalSignalRequested")
        delivered = _of_type(sender_events, "ExternalSignalDelivered")
        assert len(requested) == 1 and len(delivered) == 1
        assert requested[0]["payload"]["sequence"] == delivered[0]["payload"]["sequence"]
        assert requested[0]["payload"]["target_workflow_instance_id"] == receiver.workflow_id
        assert requested[0]["payload"]["request_id"] == (
            f"workflow-signal:{sender.run_id}:{requested[0]['payload']['sequence']}"
        )
        assert not _of_type(sender_events, "ExternalSignalFailed")

        receiver_signals = [
            event for event in _of_type(await _events(client, receiver.workflow_id, receiver.run_id), "SignalReceived")
            if event["payload"].get("signal_name") == "approve"
        ]
        assert len(receiver_signals) == 1

        # Offline cold replay of the whole sender history emits no signal.
        outcome = replay(IntegrationSenderWorkflow, sender_events, [receiver.workflow_id, True],
                         workflow_id=sender_id, run_id=sender.run_id)
        assert not any(isinstance(command, SignalExternalWorkflow) for command in outcome.commands)
        assert outcome.commands == [CompleteWorkflow(result=result)]


async def test_later_commands_take_the_sequences_after_each_signal(server_url: str, server_token: str) -> None:
    queue = f"signal-external-twice-{uuid.uuid4().hex[:10]}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        if not await _advertised(client):
            pytest.skip("Server does not advertise signal_external_workflow")
        receiver = await client.start_workflow(
            workflow_type="tests.signal-external.counting-receiver", task_queue=queue,
            workflow_id=f"{queue}-receiver", input=[],
        )
        sender_id = f"{queue}-sender"
        sender = await client.start_workflow(
            workflow_type="tests.signal-external.twice-then-timer", task_queue=queue,
            workflow_id=sender_id, input=[receiver.workflow_id],
        )
        assert sender.run_id is not None and receiver.run_id is not None
        worker = Worker(client, task_queue=queue, workflows=WORKFLOWS, worker_id=f"{queue}-worker",
                        poll_timeout=1.0, shutdown_timeout=5.0)
        runner = asyncio.create_task(worker.run())
        try:
            assert await sender.result(timeout=60.0, poll_interval=0.1) == ["delivered", "delivered"]
            assert await receiver.result(timeout=30.0, poll_interval=0.1) == [["ada", 1], ["ada", 2]]
        finally:
            await stop_and_drain([worker], [runner])

        events = await _events(client, sender_id, sender.run_id)
        requested = _of_type(events, "ExternalSignalRequested")
        delivered = _of_type(events, "ExternalSignalDelivered")
        assert [event["payload"]["sequence"] for event in requested] == [1, 2]
        assert [event["payload"]["sequence"] for event in delivered] == [1, 2]
        assert [event["payload"]["request_id"] for event in requested] == [
            f"workflow-signal:{sender.run_id}:1", f"workflow-signal:{sender.run_id}:2",
        ]
        assert {event["payload"]["sequence"] for event in _of_type(events, "TimerScheduled")} == {3}
        assert {event["payload"]["sequence"] for event in _of_type(events, "TimerFired")} == {3}
        assert len([
            event for event in _of_type(await _events(client, receiver.workflow_id, receiver.run_id), "SignalReceived")
            if event["payload"].get("signal_name") == "approve"
        ]) == 2

        # Offline cold replay of the whole sender history sends nothing new.
        outcome = replay(SignalTwiceThenTimerWorkflow, events, [receiver.workflow_id],
                         workflow_id=sender_id, run_id=sender.run_id)
        assert outcome.commands == [CompleteWorkflow(result=["delivered", "delivered"])]


async def test_missing_target_is_a_recorded_outcome(server_url: str, server_token: str) -> None:
    queue = f"signal-external-missing-{uuid.uuid4().hex[:10]}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        if not await _advertised(client):
            pytest.skip("Server does not advertise signal_external_workflow")
        handle, result = await _run_sender(client, queue, f"{queue}-nobody")
        assert result == {"outcome": "target_not_found", "reason": "instance_not_found",
                          "target_run_id": None, "target_command_id": None}
        events = await _events(client, handle.workflow_id, handle.run_id)
        assert len(_of_type(events, "ExternalSignalRequested")) == 1
        [failed] = _of_type(events, "ExternalSignalFailed")
        assert failed["payload"]["outcome"] == "target_not_found"


async def test_closed_target_is_a_recorded_outcome(server_url: str, server_token: str) -> None:
    queue = f"signal-external-closed-{uuid.uuid4().hex[:10]}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        if not await _advertised(client):
            pytest.skip("Server does not advertise signal_external_workflow")
        closed_id = f"{queue}-closed"
        closed = await client.start_workflow(
            workflow_type="tests.signal-external.closed-target", task_queue=queue,
            workflow_id=closed_id, input=[],
        )
        worker = Worker(client, task_queue=queue, workflows=WORKFLOWS, worker_id=f"{queue}-closer",
                        poll_timeout=1.0, shutdown_timeout=5.0)
        assert (await worker.run_until(workflow_id=closed_id, timeout=60.0, poll_interval=0.1)).output == "closed"

        _, result = await _run_sender(client, queue, closed_id)
        assert result["outcome"] == "target_closed"
        assert result["reason"] == "run_not_active"
        assert result["target_run_id"] == closed.run_id
        closed_signals = [
            event for event in _of_type(await _events(client, closed_id, closed.run_id or ""), "SignalReceived")
            if event["payload"].get("signal_name") == "approve"
        ]
        assert closed_signals == []


async def test_self_target_is_rejected(server_url: str, server_token: str) -> None:
    queue = f"signal-external-self-{uuid.uuid4().hex[:10]}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        if not await _advertised(client):
            pytest.skip("Server does not advertise signal_external_workflow")
        _, result = await _run_sender(client, queue, SELF)
        assert result["outcome"] == "rejected"
        assert result["reason"] == "self_target"


async def test_worker_refuses_before_submitting_without_the_capability(server_url: str, server_token: str) -> None:
    queue = f"signal-external-gate-{uuid.uuid4().hex[:10]}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        if await _advertised(client):
            pytest.skip("Server advertises signal_external_workflow")
        failures: list[dict[str, Any]] = []
        original = client.fail_workflow_task

        async def record(**kwargs: Any) -> Any:
            failures.append(kwargs)
            return await original(**kwargs)

        client.fail_workflow_task = record  # type: ignore[method-assign]
        sender_id = f"{queue}-sender"
        handle = await client.start_workflow(
            workflow_type="tests.signal-external.integration-sender", task_queue=queue,
            workflow_id=sender_id, input=[f"{queue}-target", False],
        )
        worker = Worker(client, task_queue=queue, workflows=WORKFLOWS, worker_id=f"{queue}-worker",
                        poll_timeout=1.0, shutdown_timeout=5.0)
        runner = asyncio.create_task(worker.run())
        try:
            async def refused() -> bool:
                return bool(failures)

            await _eventually(refused)
        finally:
            await stop_and_drain([worker], [runner])
        try:
            assert failures[0]["message"].startswith("signal_external_workflow_unavailable:")
            assert failures[0]["failure_type"] == "RuntimeCapabilityUnsupported"
            events = await _events(client, sender_id, handle.run_id or "")
            assert not _of_type(events, "ExternalSignalRequested")
        finally:
            await handle.terminate(reason="capability gate checked")
