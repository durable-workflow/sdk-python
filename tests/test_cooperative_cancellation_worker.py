from __future__ import annotations

import asyncio
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import ServerError, WorkflowCancelled
from durable_workflow.worker import Worker
from tests.test_cooperative_cancellation import marker, observation, request
from tests.test_worker import compatible_cluster_info


@workflow.defn(name="worker-cancellation")
class CancellationWorkflow:
    def run(self, ctx: workflow.WorkflowContext, kind: str):  # type: ignore[no-untyped-def]
        ctx.throw_if_cancellation_requested()
        try:
            if kind == "local_activity":
                yield ctx.local_activity("work", [])
            elif kind == "activity":
                yield ctx.schedule_activity("work", [])
            elif kind == "parallel":
                yield [ctx.start_timer(20), ctx.schedule_activity("work", [])]
            else:
                yield ctx.start_timer(30)
        except WorkflowCancelled as error:
            with ctx.cancellation_shield():
                yield ctx.start_timer(1)
            return error.request_id
        return "not cancelled"


@workflow.defn(name="worker-cancellation-prior")
class PriorCommandsWorkflow:
    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.upsert_memo({"stage": "before-boundary"})
        yield ctx.start_timer(30)


@workflow.defn(name="worker-cancellation-local-cleanup")
class LocalCleanupWorkflow:
    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_timer(30)
        except WorkflowCancelled as error:
            with ctx.cancellation_shield():
                yield ctx.local_activity("cleanup", [error.request_id])
            return error.request_id


def claimed_task(kind: str = "timer", *, observed: bool = True) -> dict[str, Any]:
    return {
        "task_id": "task-1", "workflow_id": "workflow-1", "run_id": "run-1",
        "workflow_type": "worker-cancellation", "workflow_task_attempt": 4,
        "payload_codec": "avro", "arguments": serializer.envelope([kind], codec="avro"),
        "history_events": [], **({"cancellation_request": observation()} if observed else {}),
    }


def lease_ack(*, observed: bool = False) -> dict[str, Any]:
    return {
        "task_id": "task-1", "lease_owner": "cooperative-worker", "workflow_task_attempt": 4,
        "renewed": True, **({"cancellation_request": observation()} if observed else {}),
    }


class ClaimServer:
    def __init__(self, *, history: list[dict[str, Any]] | None = None) -> None:
        self.client = AsyncMock(spec=Client)
        self.history = list(history if history is not None else [request()])
        self.trace: list[str] = []
        self.delivery_error: Exception | None = None
        self.commit_delivery = True
        self.client.get_cluster_info.return_value = compatible_cluster_info(worker_protocol={
            "version": "1.20", "server_capabilities": {
                "query_tasks": True, "long_poll_timeout": 30, "cooperative_cancellation": True,
                "workflow_memo_updates": True,
            },
        })
        self.client.register_worker.return_value = {"registered": True}
        self.client.heartbeat_workflow_task.return_value = lease_ack()
        self.client.workflow_task_history.side_effect = self.page
        self.client.deliver_workflow_cancellation.side_effect = self.deliver
        self.client.complete_workflow_task.side_effect = self.complete

    async def page(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs == {
            "task_id": "task-1", "next_history_page_token": "opaque-first-page",
            "lease_owner": "cooperative-worker", "workflow_task_attempt": 4,
        }
        self.trace.append("history")
        return {"history_events": deepcopy(self.history), "next_history_page_token": None}

    async def deliver(self, **kwargs: Any) -> dict[str, Any]:
        self.trace.append("delivery")
        if self.commit_delivery:
            self.history.append(marker(
                kwargs["sequence"], kwargs["call_kind"], sequence_span=kwargs["sequence_span"],
                operation_sequence=kwargs["operation_sequence"],
                operation_sequence_span=kwargs["operation_sequence_span"],
            ))
        if self.delivery_error:
            raise self.delivery_error
        return {"delivered": True}

    async def complete(self, **kwargs: Any) -> dict[str, Any]:
        self.trace.append("completion")
        return {"outcome": "completed"}

    async def worker(self, monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> Worker:
        monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
        worker = Worker(
            self.client, task_queue="queue", worker_id="cooperative-worker",
            workflows=kwargs.pop("workflows", [CancellationWorkflow]),
            capabilities=["cooperative_cancellation"], **kwargs,
        )
        await worker._register()
        return worker


@pytest.mark.parametrize("kind,span", [("timer", 1), ("activity", 1), ("local_activity", 1), ("parallel", 2)])
async def test_claim_commits_delivery_and_reloads_before_cleanup(
    monkeypatch: pytest.MonkeyPatch, kind: str, span: int,
) -> None:
    server = ClaimServer()
    worker = await server.worker(monkeypatch)
    commands = await worker._run_workflow_task(claimed_task(kind))
    assert server.trace == ["history", "delivery", "history", "completion"]
    assert commands is not None and len(commands) == 1
    assert commands[0]["type"] == "start_timer"
    assert commands[0]["delay_seconds"] == 1
    assert server.client.deliver_workflow_cancellation.await_args.kwargs == {
        "task_id": "task-1", "lease_owner": "cooperative-worker", "workflow_task_attempt": 4,
        "request_id": "request-1", "sequence": 1, "call_kind": kind, "sequence_span": span,
        "operation_sequence": None, "operation_sequence_span": 1,
    }
    server.client.fail_workflow_task.assert_not_awaited()


async def test_lost_delivery_ack_is_resolved_only_from_canonical_history(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer()
    server.delivery_error = TimeoutError("ack lost after commit")
    worker = await server.worker(monkeypatch)
    commands = await worker._run_workflow_task(claimed_task())
    assert commands is not None and commands[0]["type"] == "start_timer"
    assert server.trace == ["history", "delivery", "history", "completion"]
    assert server.client.deliver_workflow_cancellation.await_count == 1


async def test_legacy_boolean_cannot_replace_canonical_cooperative_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer()
    worker = await server.worker(monkeypatch)
    task = claimed_task()
    task["cancel_requested"] = True
    commands = await worker._run_workflow_task(task)
    assert commands is not None and commands[0]["type"] == "start_timer"
    assert server.trace == ["history", "delivery", "history", "completion"]


@pytest.mark.parametrize("error", [None, TimeoutError("not accepted"), ServerError(409, {"reason": "lease_expired"})])
async def test_ack_without_matching_marker_cannot_run_cleanup_or_complete(
    monkeypatch: pytest.MonkeyPatch, error: Exception | None,
) -> None:
    server = ClaimServer()
    server.commit_delivery = False
    server.delivery_error = error
    worker = await server.worker(monkeypatch)
    assert await worker._run_workflow_task(claimed_task()) is None
    assert server.trace == ["history", "delivery", "history"]
    server.client.complete_workflow_task.assert_not_awaited()
    server.client.fail_workflow_task.assert_not_awaited()


async def test_claim_loss_during_refresh_cannot_complete_or_fail_stale_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer()
    server.client.workflow_task_history.side_effect = [
        {"history_events": [request()], "next_history_page_token": None},
        ServerError(409, {"reason": "workflow_task_attempt_mismatch"}),
    ]
    worker = await server.worker(monkeypatch)
    assert await worker._run_workflow_task(claimed_task()) is None
    server.client.complete_workflow_task.assert_not_awaited()
    server.client.fail_workflow_task.assert_not_awaited()


async def test_prior_commands_commit_before_a_successor_can_deliver(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer()
    worker = await server.worker(monkeypatch, workflows=[PriorCommandsWorkflow])
    worker._workflow_memo_updates_supported = True
    task = claimed_task()
    task.update(workflow_type="worker-cancellation-prior", arguments=serializer.envelope([], codec="avro"))
    commands = await worker._run_workflow_task(task)
    assert commands is not None and [command["type"] for command in commands] == ["upsert_memo"]
    server.client.deliver_workflow_cancellation.assert_not_awaited()
    assert server.trace == ["history", "completion"]


async def test_cold_replacement_replays_marker_without_redelivery(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer(history=[request(), marker(1, "timer")])
    worker = await server.worker(monkeypatch)
    task = claimed_task(observed=False)
    task["history_events"] = deepcopy(server.history)
    commands = await worker._run_workflow_task(task)
    assert commands is not None and commands[0]["delay_seconds"] == 1
    server.client.deliver_workflow_cancellation.assert_not_awaited()
    assert server.trace == ["completion"]


async def test_local_result_after_request_observation_is_not_serialized_or_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = ClaimServer(history=[])

    @activity.defn(name="work")
    async def work() -> object:
        server.trace.append("local-result")
        server.history.append(request())
        return object()  # Would fail serialization if the late value escaped the fence.

    server.client.heartbeat_workflow_task.side_effect = [lease_ack(), lease_ack(observed=True)]
    worker = await server.worker(monkeypatch, activities=[work])
    commands = await worker._run_workflow_task(claimed_task("local_activity", observed=False))
    assert server.trace == ["local-result", "history", "delivery", "history", "completion"]
    assert commands is not None and [command["type"] for command in commands] == ["start_timer"]
    assert server.client.deliver_workflow_cancellation.await_args.kwargs["call_kind"] == "local_activity"
    server.client.fail_workflow_task.assert_not_awaited()


async def test_local_heartbeat_observation_does_not_become_an_activity_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer(history=[])

    @activity.defn(name="work")
    async def work() -> str:
        server.history.append(request())
        await activity.context().heartbeat()
        raise AssertionError("local execution should have returned to canonical replay")

    server.client.heartbeat_workflow_task.side_effect = [lease_ack(), lease_ack(observed=True)]
    worker = await server.worker(monkeypatch, activities=[work])
    commands = await worker._run_workflow_task(claimed_task("local_activity", observed=False))
    assert commands is not None and [command["type"] for command in commands] == ["start_timer"]
    server.client.fail_workflow_task.assert_not_awaited()


async def test_active_local_call_without_user_heartbeat_is_fenced_before_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = ClaimServer(history=[])
    late_result_discarded = asyncio.Event()

    @activity.defn(name="work")
    async def work() -> object:
        server.history.append(request())
        server.client.heartbeat_workflow_task.return_value = lease_ack(observed=True)
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            late_result_discarded.set()
            return object()

    worker = await server.worker(monkeypatch, activities=[work], heartbeat_interval=0.01)
    commands = await asyncio.wait_for(
        worker._run_workflow_task(claimed_task("local_activity", observed=False)), timeout=2,
    )
    await asyncio.wait_for(late_result_discarded.wait(), timeout=1)
    assert commands is not None and [command["type"] for command in commands] == ["start_timer"]
    assert server.client.deliver_workflow_cancellation.await_args.kwargs["call_kind"] == "local_activity"
    server.client.fail_workflow_task.assert_not_awaited()


@pytest.mark.parametrize("field,value", [
    ("renewed", False), ("lease_owner", "replacement-worker"),
    ("workflow_task_attempt", 5), ("task_id", "replacement-task"),
])
async def test_active_local_lease_refusal_drops_work_without_delivery(
    monkeypatch: pytest.MonkeyPatch, field: str, value: Any,
) -> None:
    server = ClaimServer(history=[])
    abandoned = asyncio.Event()

    @activity.defn(name="work")
    async def work() -> None:
        ack = lease_ack(observed=True)
        ack[field] = value
        server.client.heartbeat_workflow_task.return_value = ack
        try:
            await asyncio.Event().wait()
        finally:
            abandoned.set()

    worker = await server.worker(monkeypatch, activities=[work], heartbeat_interval=0.01)
    task = claimed_task("local_activity", observed=False)
    assert await asyncio.wait_for(worker._run_workflow_task(task), timeout=2) is None
    await asyncio.wait_for(abandoned.wait(), timeout=1)
    assert "cancellation_request" not in task
    server.client.deliver_workflow_cancellation.assert_not_awaited()
    server.client.complete_workflow_task.assert_not_awaited()
    server.client.fail_workflow_task.assert_not_awaited()


async def test_shielded_local_cleanup_can_heartbeat_the_delivered_request(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer()
    cleanup: list[str] = []

    @activity.defn(name="cleanup")
    async def compensate(request_id: str) -> str:
        cleanup.append(request_id)
        await activity.context().heartbeat()
        return request_id

    server.client.heartbeat_workflow_task.return_value = lease_ack(observed=True)
    worker = await server.worker(monkeypatch, workflows=[LocalCleanupWorkflow], activities=[compensate])
    task = claimed_task()
    task.update(workflow_type="worker-cancellation-local-cleanup", arguments=serializer.envelope([], codec="avro"))
    commands = await worker._run_workflow_task(task)
    assert cleanup == ["request-1"]
    assert commands is not None
    assert [command["type"] for command in commands] == ["record_local_activity", "complete_workflow"]
    assert server.client.deliver_workflow_cancellation.await_count == 1


@pytest.mark.parametrize("value", ["", None, {}, "not-a-page"])
async def test_invalid_or_missing_refresh_token_never_delivers(monkeypatch: pytest.MonkeyPatch, value: Any) -> None:
    server = ClaimServer()
    worker = await server.worker(monkeypatch)
    task = claimed_task()
    task["cancellation_request"]["history_refresh_page_token"] = value
    assert await worker._run_workflow_task(task) is None
    server.client.deliver_workflow_cancellation.assert_not_awaited()
    server.client.complete_workflow_task.assert_not_awaited()


async def test_canonical_refresh_rejects_repeated_page_token(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer()
    server.client.workflow_task_history.side_effect = [{
        "history_events": [request()], "next_history_page_token": "opaque-first-page",
    }]
    worker = await server.worker(monkeypatch)
    assert await worker._run_workflow_task(claimed_task()) is None
    assert server.client.workflow_task_history.await_count == 1
    server.client.deliver_workflow_cancellation.assert_not_awaited()
    server.client.complete_workflow_task.assert_not_awaited()


@pytest.mark.parametrize("version", ["1.19", "2.20", "1.bad"])
async def test_explicit_cooperative_worker_refuses_incompatible_protocol(
    monkeypatch: pytest.MonkeyPatch, version: str,
) -> None:
    server = ClaimServer()
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", version)
    worker = Worker(server.client, task_queue="queue", capabilities=["cooperative_cancellation"])
    with pytest.raises(RuntimeError, match="protocol 1.20"):
        await worker._register()
    server.client.register_worker.assert_not_awaited()


async def test_current_default_worker_does_not_advertise_cooperative_support(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer()
    monkeypatch.delenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", raising=False)
    worker = Worker(server.client, task_queue="queue")
    await worker._register()
    assert worker._cooperative_cancellation_supported is False
    assert "cooperative_cancellation" not in server.client.register_worker.await_args.kwargs["capabilities"]


async def test_incapable_worker_cannot_execute_a_canonical_cooperative_run(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer(history=[request(), marker(1, "timer")])
    monkeypatch.delenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", raising=False)
    worker = Worker(server.client, task_queue="queue", workflows=[CancellationWorkflow])
    await worker._register()
    task = claimed_task(observed=False)
    task["history_events"] = deepcopy(server.history)
    assert await worker._run_workflow_task(task) is None
    server.client.deliver_workflow_cancellation.assert_not_awaited()
    server.client.complete_workflow_task.assert_not_awaited()
    server.client.fail_workflow_task.assert_not_awaited()
