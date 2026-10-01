"""Connected cooperative qualification, opt-in until the service tuple is published."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import uuid
from typing import Any

import pytest

from durable_workflow import Client, Worker, activity, workflow
from durable_workflow.client import WorkflowHandle
from durable_workflow.errors import ServerError, WorkflowCancelled
from durable_workflow.worker import _RemoteActivityExecutionAborted
from durable_workflow.workflow import LocalActivityExecutionAborted

pytestmark = pytest.mark.usefixtures("cooperative_runtime")


@pytest.fixture
async def cooperative_runtime(server_url: str, server_token: str, monkeypatch: pytest.MonkeyPatch) -> None:
    if os.environ.get("DURABLE_WORKFLOW_COOPERATIVE_QUALIFICATION") != "1":
        pytest.skip("candidate cooperative Server qualification is opt-in")
    async with Client(server_url, token=server_token, namespace="default") as client:
        info = await client.get_cluster_info()
    assert info["worker_protocol"]["server_capabilities"]["cooperative_cancellation"] is True
    assert info["worker_protocol"]["version"] == "1.20"
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")


@workflow.defn(name="tests.python-cooperative-cleanup")
class CooperativeCleanupWorkflow:
    def run(self, ctx: Any, kind: str) -> Any:
        try:
            if kind == "local":
                yield ctx.local_activity("tests.python-cooperative-work", [])
            elif kind == "remote":
                yield ctx.schedule_activity("tests.python-cooperative-work", [])
            else:
                yield ctx.start_timer(300)
        except WorkflowCancelled as error:
            with ctx.cancellation_shield():
                yield ctx.local_activity("tests.python-cooperative-cleanup", [error.request_id])
            return error.request_id
        return "not cancelled"


@activity.defn(name="tests.python-cooperative-work")
async def cooperative_work() -> str:
    return "ordinary result"


@activity.defn(name="tests.python-cooperative-cleanup")
async def cooperative_cleanup(request_id: str) -> str:
    await activity.context().heartbeat({"request_id": request_id})
    return request_id


def candidate_worker(client: Client, queue: str, **kwargs: Any) -> Worker:
    return Worker(
        client, task_queue=queue, worker_id=kwargs.pop("worker_id", f"{queue}-worker"),
        workflows=[CooperativeCleanupWorkflow], activities=[cooperative_work, cooperative_cleanup],
        capabilities=["cooperative_cancellation"], **kwargs,
    )


async def poll_claim(client: Client, worker: Worker) -> dict[str, Any]:
    async def poll() -> dict[str, Any]:
        while True:
            task = await client.poll_workflow_task(
                worker_id=worker.worker_id, task_queue=worker.task_queue, timeout=worker._poll_http_timeout,
            )
            if task is not None:
                return task
            await asyncio.sleep(0.1)
    return await asyncio.wait_for(poll(), timeout=30)


async def events(handle: WorkflowHandle) -> list[dict[str, Any]]:
    history = await handle.get_history(page_size=1000)
    assert history.get("next_page_token") is None
    return history.get("events", history.get("history_events", []))


async def assert_cancelled_cleanup(handle: WorkflowHandle, request_id: str) -> list[dict[str, Any]]:
    with pytest.raises(WorkflowCancelled):
        await handle.result(timeout=10)
    history = await events(handle)
    kinds = [event["event_type"] for event in history]
    cancellation_kinds = ("CooperativeCancellationRequested", "CooperativeCancellationDelivered", "WorkflowCancelled")
    for kind in (*cancellation_kinds, "ActivityCompleted"):
        assert kinds.count(kind) == 1
    for kind in ("WorkflowCompleted", "WorkflowFailed", "ActivityFailed", "ActivityTimedOut"):
        assert kind not in kinds
    for event in history:
        if event["event_type"] in cancellation_kinds:
            assert event["payload"]["workflow_command_id"] == request_id
    return history


class LostDeliveryAcknowledgmentClient(Client):
    delivery_calls = 0

    async def deliver_workflow_cancellation(self, **kwargs: Any) -> dict[str, Any]:
        result = await super().deliver_workflow_cancellation(**kwargs)
        self.delivery_calls += 1
        if self.delivery_calls == 1:
            raise ServerError(503, {"reason": "qualification_lost_delivery_acknowledgment"})
        return result


class ObservedOwnerClient(Client):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.owner_heartbeat = asyncio.Event()

    async def heartbeat_worker(self, **kwargs: Any) -> Any:
        reply = await super().heartbeat_worker(**kwargs)
        if isinstance(reply, dict) and reply.get("acknowledged") is True:
            self.owner_heartbeat.set()
        return reply


@pytest.mark.parametrize("handler_kind", ["async", "sync"])
@pytest.mark.parametrize("user_heartbeat", [False, True])
async def test_actual_remote_worker_fences_blocked_callbacks_without_manufacturing_progress(
    server_url: str, server_token: str, handler_kind: str, user_heartbeat: bool,
) -> None:
    queue = f"py-cooperative-owner-{uuid.uuid4().hex[:8]}"
    entered, late_fenced = asyncio.Event(), asyncio.Event()
    release_thread = threading.Event()
    loop = asyncio.get_running_loop()
    fences: list[dict[str, str]] = []

    def record_fence() -> None:
        info = activity.context().info
        fences.append({"task_id": info.task_id, "activity_attempt_id": info.activity_attempt_id,
                       "lease_owner": info.worker_id})

    async def asynchronous() -> object:
        record_fence()
        entered.set()
        try:
            while True:
                await asyncio.sleep(0.1)
                if user_heartbeat:
                    await activity.context().heartbeat({"qualification": "remote-in-flight"})
        except (asyncio.CancelledError, _RemoteActivityExecutionAborted):
            with pytest.raises(_RemoteActivityExecutionAborted):
                await activity.context().heartbeat({"late": True})
            late_fenced.set()
            return object()

    def synchronous() -> object:
        record_fence()
        loop.call_soon_threadsafe(entered.set)
        try:
            while not release_thread.wait(timeout=0.1):
                if user_heartbeat:
                    asyncio.run(activity.context().heartbeat({"qualification": "remote-in-flight"}))
        except _RemoteActivityExecutionAborted:
            pass
        with pytest.raises(_RemoteActivityExecutionAborted):
            asyncio.run(activity.context().heartbeat({"late": True}))
        loop.call_soon_threadsafe(late_fenced.set)
        return object()

    async with ObservedOwnerClient(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue, max_concurrent_activity_tasks=1)
        worker.activities["tests.python-cooperative-work"] = asynchronous if handler_kind == "async" else synchronous
        running = asyncio.create_task(worker.run())
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["remote"],
            )
            await asyncio.wait_for(entered.wait(), timeout=15)
            await asyncio.wait_for(client.owner_heartbeat.wait(), timeout=15)
            assert not late_fenced.is_set()
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            history = await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
            release_thread.set()
            await asyncio.wait_for(late_fenced.wait(), timeout=5)
            delivery = [event for event in history if event["event_type"] == "CooperativeCancellationDelivered"][0]
            assert delivery["payload"]["call_kind"] == "activity"
            assert len([event for event in history if event["event_type"] == "ActivityCancelled"]) == 1
            progress = [event for event in history if event["event_type"] == "ActivityHeartbeatRecorded"
                        and event["payload"].get("activity_type") == "tests.python-cooperative-work"]
            assert bool(progress) is user_heartbeat
            assert len(fences) == 1
            with pytest.raises(ServerError) as completion:
                await client.complete_activity_task(**fences[0], result="late")
            assert completion.value.status == 409
            with pytest.raises(ServerError) as failure:
                await client.fail_activity_task(**fences[0], message="late", failure_type="LateQualification")
            assert failure.value.status == 409
            assert await events(handle) == history
        finally:
            release_thread.set()
            await worker.stop()
            await asyncio.wait_for(running, timeout=5)


@pytest.mark.parametrize("handler_kind", ["async", "sync"])
async def test_actual_remote_shutdown_expiry_fences_callback_before_replacement_cleanup(
    server_url: str, server_token: str, handler_kind: str,
) -> None:
    queue = f"py-cooperative-owner-stop-{uuid.uuid4().hex[:8]}"
    entered, late_fenced = asyncio.Event(), asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()

    async def asynchronous() -> object:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            with pytest.raises(_RemoteActivityExecutionAborted):
                await activity.context().heartbeat({"late": True})
            late_fenced.set()
            return object()

    def synchronous() -> object:
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(timeout=20)
        with pytest.raises(_RemoteActivityExecutionAborted):
            asyncio.run(activity.context().heartbeat({"late": True}))
        loop.call_soon_threadsafe(late_fenced.set)
        return object()

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue, shutdown_timeout=0.1)
        worker.activities["tests.python-cooperative-work"] = asynchronous if handler_kind == "async" else synchronous
        running = asyncio.create_task(worker.run())
        replacement = candidate_worker(client, queue, worker_id=f"{queue}-replacement")
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["remote"],
            )
            await asyncio.wait_for(entered.wait(), timeout=15)
            before = await events(handle)
            await asyncio.wait_for(worker.stop(), timeout=5)
            await asyncio.wait_for(running, timeout=5)
            release.set()
            await asyncio.wait_for(late_fenced.wait(), timeout=5)
            assert await events(handle) == before
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            await replacement._register()
            assert await replacement._run_workflow_task(await poll_claim(client, replacement)) is not None
            await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
        finally:
            release.set()
            await replacement.stop()
            await worker.stop()
            await asyncio.wait_for(running, timeout=5)


async def test_waiting_timer_is_cancelled_by_canonical_delivery(
    server_url: str, server_token: str,
) -> None:
    queue = f"py-cooperative-waiting-{uuid.uuid4().hex[:8]}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue)
        await worker._register()
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["timer"],
            )
            task = await poll_claim(client, worker)
            assert [command["type"] for command in await worker._run_workflow_task(task) or []] == ["start_timer"]
            before = await events(handle)
            assert [event["event_type"] for event in before].count("TimerScheduled") == 1
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            resumed = await poll_claim(client, worker)
            assert await worker._run_workflow_task(resumed) is not None
            history = await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
            kinds = [event["event_type"] for event in history]
            assert kinds.count("TimerCancelled") == 1
            assert "TimerFired" not in kinds
        finally:
            await worker.stop()


async def test_leased_remote_activity_cannot_complete_after_delivery(
    server_url: str, server_token: str,
) -> None:
    queue = f"py-cooperative-remote-{uuid.uuid4().hex[:8]}"
    cleanup_entered = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def cleanup(request_id: str) -> str:
        cleanup_entered.set()
        await release_cleanup.wait()
        return await cooperative_cleanup(request_id)

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue)
        worker.activities["tests.python-cooperative-cleanup"] = cleanup
        await worker._register()
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["remote"],
            )
            task = await poll_claim(client, worker)
            assert [command["type"] for command in await worker._run_workflow_task(task) or []] == ["schedule_activity"]
            remote = await client.poll_activity_task(worker_id=worker.worker_id, task_queue=queue, timeout=5)
            assert remote is not None
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            resumed = await poll_claim(client, worker)
            execution = worker._track(worker._run_workflow_task(resumed))
            await asyncio.wait_for(cleanup_entered.wait(), timeout=10)
            late_fence = {
                "task_id": remote["task_id"], "activity_attempt_id": remote["activity_attempt_id"],
                "lease_owner": worker.worker_id,
            }
            heartbeat = await client.heartbeat_activity_task(**late_fence)
            assert heartbeat["cancel_requested"] is True
            assert heartbeat["can_continue"] is False
            assert heartbeat["heartbeat_recorded"] is False
            release_cleanup.set()
            assert await asyncio.wait_for(execution, timeout=10) is not None
            before = await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
            with pytest.raises(ServerError) as late_result:
                await client.complete_activity_task(**late_fence, result="late remote result")
            assert late_result.value.status == 409
            assert late_result.value.reason() == "run_cancelled"
            with pytest.raises(ServerError) as late_failure:
                await client.fail_activity_task(
                    **late_fence, message="qualification late failure", failure_type="qualification-late-failure",
                )
            assert late_failure.value.status == 409
            assert late_failure.value.reason() == "run_cancelled"
            assert await events(handle) == before
        finally:
            release_cleanup.set()
            await worker.stop()


@pytest.mark.parametrize("lost_ack", [False, True])
async def test_request_before_claim_retains_identity_and_proves_delivery(
    server_url: str, server_token: str, lost_ack: bool,
) -> None:
    queue = f"py-cooperative-request-{uuid.uuid4().hex[:8]}"
    client_type = LostDeliveryAcknowledgmentClient if lost_ack else Client
    async with client_type(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue)
        await worker._register()
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["timer"],
            )
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            repeated = await handle.request_cancellation(cleanup_timeout_seconds=300)
            assert accepted["duplicate"] is False
            assert repeated["duplicate"] is True
            assert repeated["cancellation_request"] == accepted["cancellation_request"]
            task = await poll_claim(client, worker)
            assert task["cancellation_request"] == accepted["cancellation_request"]
            commands = await worker._run_workflow_task(task)
            assert commands is not None
            assert [command["type"] for command in commands] == ["record_local_activity", "complete_workflow"]
            await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
            if lost_ack:
                assert isinstance(client, LostDeliveryAcknowledgmentClient)
                assert client.delivery_calls == 1
        finally:
            await worker.stop()


@pytest.mark.parametrize("handler_kind", ["async", "sync"])
async def test_active_local_work_observes_request_and_discards_late_result(
    server_url: str, server_token: str, handler_kind: str,
) -> None:
    queue = f"py-cooperative-active-{uuid.uuid4().hex[:8]}"
    entered = asyncio.Event()
    discarded = asyncio.Event()
    cleanup_entered = asyncio.Event()
    release_thread = threading.Event()
    loop = asyncio.get_running_loop()

    async def blocked_work() -> object:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            discarded.set()
            return object()

    def synchronous_work() -> object:
        loop.call_soon_threadsafe(entered.set)
        assert release_thread.wait(timeout=20)
        loop.call_soon_threadsafe(discarded.set)
        return object()

    async def cleanup(request_id: str) -> str:
        cleanup_entered.set()
        return await cooperative_cleanup(request_id)

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue)
        worker.activities["tests.python-cooperative-work"] = (
            blocked_work if handler_kind == "async" else synchronous_work
        )
        worker.activities["tests.python-cooperative-cleanup"] = cleanup
        await worker._register()
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["local"],
            )
            task = await poll_claim(client, worker)
            execution = worker._track(worker._run_workflow_task(task))
            await asyncio.wait_for(entered.wait(), timeout=10)
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            await asyncio.wait_for(cleanup_entered.wait(), timeout=10)
            release_thread.set()
            await asyncio.wait_for(discarded.wait(), timeout=10)
            assert await asyncio.wait_for(execution, timeout=10) is not None
            history = await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
            completed = [event for event in history if event["event_type"] == "ActivityCompleted"]
            assert completed[0]["payload"]["activity_type"] == "tests.python-cooperative-cleanup"
        finally:
            release_thread.set()
            await worker.stop()


@pytest.mark.parametrize("shutdown_kind", ["drain", "timeout"])
async def test_shutdown_during_shielded_cleanup_reuses_canonical_delivery(
    server_url: str, server_token: str, shutdown_kind: str,
) -> None:
    queue = f"py-cooperative-shutdown-{uuid.uuid4().hex[:8]}"
    entered = asyncio.Event()
    release = asyncio.Event()
    late_fenced = asyncio.Event()

    async def cleanup(request_id: str) -> object:
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            with pytest.raises(LocalActivityExecutionAborted):
                await activity.context().heartbeat()
            late_fenced.set()
            return object()
        return await cooperative_cleanup(request_id)

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue, shutdown_timeout=5 if shutdown_kind == "drain" else 0.1)
        worker.activities["tests.python-cooperative-cleanup"] = cleanup
        await worker._register()
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["timer"],
            )
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            task = await poll_claim(client, worker)
            execution = worker._track(worker._run_workflow_task(task))
            await asyncio.wait_for(entered.wait(), timeout=10)
            before = await events(handle)
            stop = asyncio.create_task(worker.stop())
            if shutdown_kind == "drain":
                await asyncio.sleep(0)
                release.set()
            await asyncio.wait_for(stop, timeout=10)
            if shutdown_kind == "timeout":
                await asyncio.wait_for(late_fenced.wait(), timeout=10)
                assert execution.cancelled() or execution.result() is None
                after = await events(handle)
                assert after[:len(before)] == before
                assert [event["event_type"] for event in after[len(before):]] == ["RepairRequested"]
                assert after[-1]["payload"]["command"]["request_method"] == "DELETE"
                assert after[-1]["payload"]["command"]["request_path"].endswith(worker.worker_id)
                replacement = candidate_worker(client, queue, worker_id=f"{queue}-replacement")
                await replacement._register()
                try:
                    reclaimed = await poll_claim(client, replacement)
                    assert reclaimed["workflow_task_attempt"] > task["workflow_task_attempt"]
                    assert reclaimed["lease_owner"] == replacement.worker_id
                    original = accepted["cancellation_request"]
                    assert reclaimed["cancellation_request"]["request_id"] == original["request_id"]
                    assert reclaimed["cancellation_request"]["cleanup_deadline_at"] == original["cleanup_deadline_at"]
                    assert await replacement._run_workflow_task(reclaimed) is not None
                finally:
                    await replacement.stop()
            else:
                assert await execution is not None
            await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
        finally:
            release.set()
            await worker.stop()


async def native_process(queue: str, worker_id: str, mode: str) -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", "tests.integration.cooperative_worker", queue, worker_id, mode,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )


async def process_event(process: asyncio.subprocess.Process, phase: str) -> dict[str, Any]:
    async def read() -> dict[str, Any]:
        assert process.stdout is not None
        while line := await process.stdout.readline():
            value = json.loads(line)
            if value["phase"] == phase:
                return value
        assert process.stderr is not None
        pytest.fail(f"worker exited before {phase}: {(await process.stderr.read()).decode()}")
    return await asyncio.wait_for(read(), timeout=40)


async def test_killed_remote_owner_cannot_publish_after_cold_workflow_delivery(
    server_url: str, server_token: str,
) -> None:
    queue = f"py-cooperative-remote-process-{uuid.uuid4().hex[:8]}"
    processes: list[asyncio.subprocess.Process] = []
    async with Client(server_url, token=server_token, namespace="default") as client:
        seed = candidate_worker(client, queue, worker_id=f"{queue}-seed")
        await seed._register()
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["remote"],
            )
            owner = await native_process(queue, f"{queue}-killed", "remote")
            processes.append(owner)
            claimed = await process_event(owner, "remote-entered")
            before = await events(handle)
            owner.kill()
            assert await asyncio.wait_for(owner.wait(), timeout=10) == -9
            assert await events(handle) == before
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            replacement = await native_process(queue, f"{queue}-replacement", "finish")
            processes.append(replacement)
            resumed = await process_event(replacement, "cleanup")
            assert resumed["request_id"] == accepted["cancellation_request"]["request_id"]
            assert (await process_event(replacement, "finished"))["committed"] is True
            assert await asyncio.wait_for(replacement.wait(), timeout=10) == 0
            history = await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
            assert len([event for event in history if event["event_type"] == "ActivityCancelled"]) == 1
            fence = {key: claimed[key] for key in ("task_id", "activity_attempt_id", "lease_owner")}
            with pytest.raises(ServerError) as completion:
                await client.complete_activity_task(**fence, result="late")
            assert completion.value.status == 409
            with pytest.raises(ServerError) as failure:
                await client.fail_activity_task(**fence, message="late", failure_type="LateQualification")
            assert failure.value.status == 409
            assert await events(handle) == history
        finally:
            for process in processes:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            await seed.stop()


async def test_killed_process_reclaims_cleanup_in_a_new_process(
    server_url: str, server_token: str,
) -> None:
    queue = f"py-cooperative-process-{uuid.uuid4().hex[:8]}"
    processes: list[asyncio.subprocess.Process] = []
    async with Client(server_url, token=server_token, namespace="default") as client:
        seed = candidate_worker(client, queue, worker_id=f"{queue}-seed")
        await seed._register()
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["timer"],
            )
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=120)
            original = accepted["cancellation_request"]
            first = await native_process(queue, f"{queue}-killed", "hold")
            processes.append(first)
            claim = await process_event(first, "claim")
            cleanup = await process_event(first, "cleanup")
            assert cleanup["request_id"] == original["request_id"]
            before = await events(handle)
            first.kill()
            assert await asyncio.wait_for(first.wait(), timeout=10) == -9
            assert await events(handle) == before
            replacement = await native_process(queue, f"{queue}-replacement", "finish")
            processes.append(replacement)
            reclaimed = await process_event(replacement, "claim")
            assert reclaimed["attempt"] > claim["attempt"]
            assert reclaimed["worker_id"] != claim["worker_id"]
            assert reclaimed["request_id"] == original["request_id"]
            assert reclaimed["cleanup_deadline_at"] == original["cleanup_deadline_at"]
            resumed = await process_event(replacement, "cleanup")
            assert resumed["request_id"] == original["request_id"]
            assert (await process_event(replacement, "finished"))["committed"] is True
            assert await asyncio.wait_for(replacement.wait(), timeout=10) == 0
            await assert_cancelled_cleanup(handle, original["request_id"])
        finally:
            for process in processes:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            await seed.stop()


@pytest.mark.parametrize("close_kind", ["deadline", "terminate"])
async def test_cleanup_deadline_and_termination_fence_in_flight_local_result(
    server_url: str, server_token: str, close_kind: str,
) -> None:
    queue = f"py-cooperative-close-{uuid.uuid4().hex[:8]}"
    entered = asyncio.Event()

    async def cleanup(request_id: str) -> object:
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            return object()

    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue)
        worker.activities["tests.python-cooperative-cleanup"] = cleanup
        await worker._register()
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["timer"],
            )
            await handle.request_cancellation(cleanup_timeout_seconds=2 if close_kind == "deadline" else 60)
            task = await poll_claim(client, worker)
            execution = worker._track(worker._run_workflow_task(task))
            await asyncio.wait_for(entered.wait(), timeout=10)
            if close_kind == "terminate":
                await handle.terminate(reason="qualification termination during cleanup")
            assert await asyncio.wait_for(execution, timeout=15) is None
            history = await events(handle)
            kinds = [event["event_type"] for event in history]
            assert kinds.count("WorkflowCancelled" if close_kind == "deadline" else "WorkflowTerminated") == 1
            assert kinds.count("CooperativeCancellationRequested") == 1
            assert kinds.count("CooperativeCancellationDelivered") == 1
            assert "ActivityCompleted" not in kinds
            assert "ActivityFailed" not in kinds
            assert "WorkflowCompleted" not in kinds
        finally:
            await worker.stop()
