from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import ServerError, WorkflowCancelled
from durable_workflow.worker import Worker
from durable_workflow.workflow import LocalActivityExecutionAborted
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
    def __init__(
        self, *, history: list[dict[str, Any]] | None = None,
        lease_owner: str = "cooperative-worker", workflow_task_attempt: int = 4,
    ) -> None:
        self.client = AsyncMock(spec=Client)
        self.lease_owner = lease_owner
        self.workflow_task_attempt = workflow_task_attempt
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
        self.client.heartbeat_workflow_task.return_value = {
            **lease_ack(), "lease_owner": lease_owner, "workflow_task_attempt": workflow_task_attempt,
        }
        self.client.workflow_task_history.side_effect = self.page
        self.client.deliver_workflow_cancellation.side_effect = self.deliver
        self.client.complete_workflow_task.side_effect = self.complete

    async def page(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs == {
            "task_id": "task-1", "next_history_page_token": "opaque-first-page",
            "lease_owner": self.lease_owner, "workflow_task_attempt": self.workflow_task_attempt,
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
            self.client, task_queue="queue", worker_id=self.lease_owner,
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


async def test_worker_stop_drains_shielded_local_cleanup_within_its_grace_period(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = ClaimServer()
    entered = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name="cleanup")
    async def compensate(request_id: str) -> str:
        entered.set()
        await release.wait()
        await activity.context().heartbeat()
        return request_id

    worker = await server.worker(
        monkeypatch, workflows=[LocalCleanupWorkflow], activities=[compensate], shutdown_timeout=1,
    )
    task = claimed_task()
    task.update(workflow_type="worker-cancellation-local-cleanup", arguments=serializer.envelope([], codec="avro"))
    execution = worker._track(worker._run_workflow_task(task))
    await asyncio.wait_for(entered.wait(), timeout=1)
    stopping = asyncio.create_task(worker.stop())
    await asyncio.sleep(0)
    assert worker._stop.is_set()
    release.set()
    commands = await asyncio.wait_for(execution, timeout=1)
    await asyncio.wait_for(stopping, timeout=1)
    assert commands is not None
    assert [command["type"] for command in commands] == ["record_local_activity", "complete_workflow"]
    server.client.deliver_workflow_cancellation.assert_awaited_once()
    server.client.fail_workflow_task.assert_not_awaited()
    server.client.deregister_worker_registration.assert_awaited_once_with("cooperative-worker")


@pytest.mark.parametrize("handler_kind", ["async", "sync"])
async def test_shutdown_timeout_abandons_cleanup_and_replacement_replays_original_delivery(
    monkeypatch: pytest.MonkeyPatch, handler_kind: str,
) -> None:
    server = ClaimServer()
    entered = asyncio.Event()
    discarded = asyncio.Event()
    loop = asyncio.get_running_loop()
    release_thread = threading.Event()
    fenced_heartbeats: list[bool] = []

    @activity.defn(name="cleanup")
    async def compensate(request_id: str) -> object:
        assert request_id == "request-1"
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            with pytest.raises(LocalActivityExecutionAborted):
                await activity.context().heartbeat()
            fenced_heartbeats.append(True)
            discarded.set()
            return object()  # A late, unencodable result must never become an activity failure.

    @activity.defn(name="cleanup")
    def synchronous_compensation(request_id: str) -> object:
        assert request_id == "request-1"
        context = activity.context()
        assert context.info.worker_id == "cooperative-worker"
        loop.call_soon_threadsafe(entered.set)
        try:
            assert release_thread.wait(timeout=2)
            heartbeat = asyncio.run_coroutine_threadsafe(context.heartbeat(), loop)
            try:
                heartbeat.result(timeout=1)
            except LocalActivityExecutionAborted:
                fenced_heartbeats.append(True)
            else:
                fenced_heartbeats.append(False)
            return object()
        finally:
            loop.call_soon_threadsafe(discarded.set)

    worker = await server.worker(
        monkeypatch, workflows=[LocalCleanupWorkflow],
        activities=[compensate if handler_kind == "async" else synchronous_compensation], shutdown_timeout=0.01,
    )
    task = claimed_task()
    task.update(workflow_type="worker-cancellation-local-cleanup", arguments=serializer.envelope([], codec="avro"))
    execution = worker._track(worker._run_workflow_task(task))
    await asyncio.wait_for(entered.wait(), timeout=1)
    original_history = deepcopy(server.history)
    heartbeats_before_stop = server.client.heartbeat_workflow_task.await_count
    await asyncio.wait_for(worker.stop(), timeout=1)
    release_thread.set()
    await asyncio.wait_for(discarded.wait(), timeout=1)
    assert fenced_heartbeats == [True]
    assert server.client.heartbeat_workflow_task.await_count == heartbeats_before_stop
    assert execution.cancelled() or execution.result() is None
    assert server.history == original_history
    server.client.complete_workflow_task.assert_not_awaited()
    server.client.fail_workflow_task.assert_not_awaited()
    server.client.deregister_worker_registration.assert_awaited_once_with("cooperative-worker")

    replacement = ClaimServer(history=original_history, lease_owner="replacement-worker", workflow_task_attempt=5)
    cleanup_ids: list[str] = []

    @activity.defn(name="cleanup")
    async def resumed_cleanup(request_id: str) -> str:
        cleanup_ids.append(request_id)
        await activity.context().heartbeat()
        return request_id

    successor = await replacement.worker(monkeypatch, workflows=[LocalCleanupWorkflow], activities=[resumed_cleanup])
    reclaimed = deepcopy(task)
    reclaimed["workflow_task_attempt"] = 5
    reclaimed["history_events"] = deepcopy(original_history)
    commands = await asyncio.wait_for(successor._run_workflow_task(reclaimed), timeout=1)
    assert cleanup_ids == ["request-1"]
    assert commands is not None
    assert [command["type"] for command in commands] == ["record_local_activity", "complete_workflow"]
    assert replacement.history == original_history
    replacement.client.deliver_workflow_cancellation.assert_not_awaited()
    replacement.client.fail_workflow_task.assert_not_awaited()
    assert replacement.client.complete_workflow_task.await_args.kwargs["lease_owner"] == "replacement-worker"
    assert replacement.client.complete_workflow_task.await_args.kwargs["workflow_task_attempt"] == 5


async def test_active_synchronous_local_call_observes_request_without_blocking_the_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = ClaimServer(history=[])
    entered = asyncio.Event()
    discarded = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    fenced: list[bool] = []
    loop.set_default_executor(ThreadPoolExecutor(max_workers=1))

    @activity.defn(name="work")
    def work() -> object:
        context = activity.context()
        loop.call_soon_threadsafe(entered.set)
        try:
            assert release.wait(timeout=2)
            heartbeat = asyncio.run_coroutine_threadsafe(context.heartbeat(), loop)
            try:
                heartbeat.result(timeout=1)
            except LocalActivityExecutionAborted:
                fenced.append(True)
            else:
                fenced.append(False)
            return object()
        finally:
            loop.call_soon_threadsafe(discarded.set)

    worker = await server.worker(monkeypatch, activities=[work], heartbeat_interval=0.01)
    execution = worker._track(worker._run_workflow_task(claimed_task("local_activity", observed=False)))
    await asyncio.wait_for(entered.wait(), timeout=1)
    server.history.append(request())
    server.client.heartbeat_workflow_task.return_value = lease_ack(observed=True)
    try:
        commands = await asyncio.wait_for(execution, timeout=1)
        assert commands is not None and [command["type"] for command in commands] == ["start_timer"]
        heartbeats_after_delivery = server.client.heartbeat_workflow_task.await_count
    finally:
        release.set()
    await asyncio.wait_for(discarded.wait(), timeout=1)
    assert fenced == [True]
    assert server.client.heartbeat_workflow_task.await_count == heartbeats_after_delivery
    assert server.client.deliver_workflow_cancellation.await_args.kwargs["call_kind"] == "local_activity"
    server.client.fail_workflow_task.assert_not_awaited()
    await worker.stop()


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
