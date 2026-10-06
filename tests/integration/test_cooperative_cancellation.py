"""Connected cooperative qualification, opt-in until the service tuple is published."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import CancellationPolicy, Client, Worker, activity, serializer, workflow
from durable_workflow.cancellation import ScopedCancellationContext
from durable_workflow.client import WorkflowHandle
from durable_workflow.errors import ServerError, WorkflowCancelled
from durable_workflow.worker import _poll_capacity_delay
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
    def run(self, ctx: Any, kind: str, cancellation_policy: str | None = None) -> Any:
        try:
            if kind == "local":
                yield ctx.local_activity("tests.python-cooperative-work", [])
            elif kind == "remote":
                yield ctx.schedule_activity(
                    "tests.python-cooperative-work", [], cancellation_policy=cancellation_policy,
                    schedule_to_close_timeout=60 if cancellation_policy is not None else None,
                )
            else:
                yield ctx.start_timer(300)
        except WorkflowCancelled as error:
            if kind == "clock_timer":
                assert error.context is ctx.cancellation_context and error.context is not None
                print(json.dumps({"phase": "remaining-delivery", "remaining": error.context.remaining(),
                                  "context": error.context.to_dict()}), flush=True)
            with ctx.cancellation_shield():
                yield ctx.local_activity("tests.python-cooperative-cleanup", [error.request_id])
            if kind == "clock_timer":
                assert error.context is not None
                print(json.dumps({"phase": "remaining-cleanup", "remaining": error.context.remaining(),
                                  "context": error.context.to_dict()}), flush=True)
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


@workflow.defn(name="tests.python-run-scope-cleanup")
class RunScopeCleanupWorkflow:
    def run(self, ctx: Any, marker: str) -> Any:
        def scoped() -> Any:
            try:
                yield ctx.start_timer(300)
            except WorkflowCancelled as error:
                assert isinstance(error.context, ScopedCancellationContext)
                context = error.context.to_dict()
                with ctx.cancellation_shield():
                    yield ctx.local_activity(
                        "tests.python-run-scope-cleanup", [marker, "scoped", context],
                        retry_policy={"max_attempts": 2, "backoff_seconds": [0]},
                    )

        yield from ctx.cancellation_scope(scoped)
        try:
            yield ctx.start_timer(300)
        except WorkflowCancelled as error:
            assert error.context is not None and not isinstance(error.context, ScopedCancellationContext)
            context = error.context.to_dict()
            with ctx.cancellation_shield():
                yield ctx.local_activity("tests.python-run-scope-cleanup", [marker, "root", context])
        return "finished"


@activity.defn(name="tests.python-run-scope-cleanup")
async def run_scope_cleanup(marker: str, phase: str, context: dict[str, Any]) -> dict[str, Any]:
    info = activity.context().info
    path = Path(marker + "." + phase + "." + info.worker_id)
    pending = path.with_suffix(path.suffix + ".writing")
    pending.write_text(json.dumps({"callback_pid": os.getpid(), "context": context,
                                   "activity_attempt_id": info.activity_attempt_id}))
    pending.replace(path)
    if phase == "scoped" and os.environ.get("DW_PREPARED_FIXTURE_MODE") == "hold":
        await asyncio.Event().wait()
    return {"phase": phase, "context": context}


def scope_prepared_worker(client: Client, queue: str, **kwargs: Any) -> Worker:
    worker = Worker(
        client, task_queue=queue, worker_id=kwargs.pop("worker_id", queue + "-owner"),
        workflows=[RunScopeCleanupWorkflow], activities=[run_scope_cleanup],
        capabilities=["cooperative_cancellation", "prepared_local_activities"], **kwargs,
    )
    worker._allow_cancellation_scope_authoring = True
    worker._allow_cancellation_scope_delivery = True
    return worker


async def poll_claim(client: Client, worker: Worker) -> dict[str, Any]:
    async def poll() -> dict[str, Any]:
        while True:
            try:
                task = await client.poll_workflow_task(
                    worker_id=worker.worker_id, task_queue=worker.task_queue, timeout=worker._poll_timeout,
                )
            except ServerError as error:
                delay = _poll_capacity_delay(error, "workflow_task", worker.task_queue)
                if delay is None:
                    raise
                print(json.dumps({"phase": "poll-deferral", "delay_seconds": delay}), flush=True)
                await asyncio.sleep(delay)
                continue
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


@dataclass(frozen=True)
class AsyncRemoteQualification:
    marker: str
    user_heartbeat: bool = False
    duration_seconds: int | None = None

    async def __call__(self) -> str | None:
        if self.user_heartbeat:
            await activity.context().heartbeat({"qualification": "remote-in-flight"})
        write_remote_marker(self.marker)
        if self.duration_seconds is not None:
            await asyncio.sleep(self.duration_seconds)
            return "independent-completion"
        await asyncio.Event().wait()
        return None


@dataclass(frozen=True)
class SyncRemoteQualification:
    marker: str
    user_heartbeat: bool = False

    def __call__(self) -> None:
        if self.user_heartbeat:
            asyncio.run(activity.context().heartbeat({"qualification": "remote-in-flight"}))
        write_remote_marker(self.marker)
        while True:
            time.sleep(1)


def write_remote_marker(marker: str) -> None:
    info = activity.context().info
    pending = Path(marker + ".writing")
    pending.write_text(json.dumps({
        "task_id": info.task_id, "activity_attempt_id": info.activity_attempt_id,
        "lease_owner": info.worker_id, "callback_pid": os.getpid(),
    }))
    pending.replace(marker)


async def remote_marker(marker: Path) -> dict[str, Any]:
    async def ready() -> dict[str, Any]:
        while not marker.exists():
            await asyncio.sleep(0.05)
        return json.loads(marker.read_text())
    return await asyncio.wait_for(ready(), timeout=20)


async def callback_gone(pid: int) -> None:
    async def gone() -> None:
        while True:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            await asyncio.sleep(0.05)
    await asyncio.wait_for(gone(), timeout=10)


async def observed_stop_receipt(client: Client, fence: dict[str, str]) -> dict[str, Any]:
    async def receipt() -> dict[str, Any]:
        while True:
            reply = await client.activity_task_status(**fence)
            proof = reply.get("cancellation_acknowledgement")
            if isinstance(proof, dict) and proof.get("callback_state") == "stopped":
                assert reply["heartbeat_recorded"] is False
                assert reply["can_continue"] is False
                return proof
            await asyncio.sleep(0.1)
    return await asyncio.wait_for(receipt(), timeout=15)


@pytest.mark.parametrize("handler_kind", ["async", "sync"])
@pytest.mark.parametrize("user_heartbeat,policy", [
    (False, None), (True, None), (False, CancellationPolicy.TRY_CANCEL),
    (False, CancellationPolicy.WAIT_CANCELLATION_COMPLETED),
])
async def test_actual_remote_worker_stops_callbacks_and_reports_original_cancellation(
    server_url: str, server_token: str, handler_kind: str, user_heartbeat: bool, policy: CancellationPolicy | None,
    tmp_path: Path,
) -> None:
    queue = f"py-cooperative-owner-{uuid.uuid4().hex[:8]}"
    marker = tmp_path / "remote"
    async with ObservedOwnerClient(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue, max_concurrent_activity_tasks=1)
        worker.activities["tests.python-cooperative-work"] = (
            AsyncRemoteQualification(str(marker), user_heartbeat) if handler_kind == "async"
            else SyncRemoteQualification(str(marker), user_heartbeat)
        )
        running = asyncio.create_task(worker.run())
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue,
                input=["remote", policy.value if policy is not None else None],
            )
            entered = await remote_marker(marker)
            await asyncio.wait_for(client.owner_heartbeat.wait(), timeout=15)
            fence = {key: entered[key] for key in ("task_id", "activity_attempt_id", "lease_owner")}
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            original = accepted["cancellation_request"]
            await assert_cancelled_cleanup(handle, original["request_id"])
            await callback_gone(entered["callback_pid"])
            if os.environ.get("DURABLE_WORKFLOW_NATIVE_SOURCE_QUALIFICATION") == "1":
                proof = await observed_stop_receipt(client, fence)
                assert proof["request_id"] == original["request_id"]
                assert proof["root_request_id"] == original["request_id"]
                assert proof["cleanup_deadline_at"] == original["cleanup_deadline_at"]
                assert proof["received_after_deadline"] is False
                duplicate = await client.acknowledge_activity_cancellation(**fence, request_id=original["request_id"])
                assert duplicate["duplicate"] is True
                assert duplicate["history_event_id"] == proof["history_event_id"]
                print(f"Remote callback stop receipt: {json.dumps([entered, original, proof, duplicate])}")
            history = await events(handle)
            if os.environ.get("DURABLE_WORKFLOW_NATIVE_SOURCE_QUALIFICATION") == "1":
                receipt = [event for event in history if event["event_type"] == "ActivityCancellationAcknowledged"]
                assert len(receipt) == 1
                # Public history exposes event sequence/payload, not row IDs.
                # The readonly status and duplicate transport prove receipt ID.
                payload = receipt[0]["payload"]
                assert payload["activity_attempt_id"] == fence["activity_attempt_id"]
                assert payload["lease_owner"] == fence["lease_owner"]
                assert payload["evidence_source"] == "activity_worker"
                for key in ("request_id", "root_request_id", "cleanup_deadline_at", "acknowledged_at"):
                    assert payload[key] == proof[key]
            delivery = [event for event in history if event["event_type"] == "CooperativeCancellationDelivered"][0]
            assert delivery["payload"]["call_kind"] == "activity"
            if policy is not None:
                scheduled = [event for event in history if event["event_type"] == "ActivityScheduled"][0]
                assert scheduled["payload"]["activity"]["cancellation_policy"] == policy.value
            if policy == CancellationPolicy.WAIT_CANCELLATION_COMPLETED:
                kinds = [event["event_type"] for event in history]
                assert kinds.index("ActivityCancellationAcknowledged") < kinds.index("CooperativeCancellationDelivered")
            assert len([event for event in history if event["event_type"] == "ActivityCancelled"]) == 1
            progress = [event for event in history if event["event_type"] == "ActivityHeartbeatRecorded"
                        and event["payload"].get("activity_type") == "tests.python-cooperative-work"]
            assert bool(progress) is user_heartbeat
            with pytest.raises(ServerError) as completion:
                await client.complete_activity_task(**fence, result="late")
            assert completion.value.status == 409
            with pytest.raises(ServerError) as failure:
                await client.fail_activity_task(**fence, message="late", failure_type="LateQualification")
            assert failure.value.status == 409
            assert await events(handle) == history
            print(f"Remote stopped cancellation history: {json.dumps(history)}")
        finally:
            await worker.stop()
            await asyncio.wait_for(running, timeout=10)


async def test_bounded_remote_abandon_completes_after_parent_cancellation(
    server_url: str, server_token: str, tmp_path: Path,
) -> None:
    queue = f"py-cooperative-abandon-{uuid.uuid4().hex[:8]}"
    marker = tmp_path / "remote"
    async with ObservedOwnerClient(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue, max_concurrent_activity_tasks=1)
        worker.activities["tests.python-cooperative-work"] = AsyncRemoteQualification(str(marker), duration_seconds=25)
        running = asyncio.create_task(worker.run())
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue,
                input=["remote", CancellationPolicy.ABANDON.value],
            )
            entered = await remote_marker(marker)
            await asyncio.wait_for(client.owner_heartbeat.wait(), timeout=15)
            fence = {key: entered[key] for key in ("task_id", "activity_attempt_id", "lease_owner")}
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=30)
            duplicate = await handle.request_cancellation(cleanup_timeout_seconds=300)
            assert duplicate["duplicate"] is True
            assert duplicate["cancellation_request"] == accepted["cancellation_request"]
            history = await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
            os.kill(entered["callback_pid"], 0)
            kinds = [event["event_type"] for event in history]
            assert "ActivityCancelled" not in kinds
            assert "ActivityCancellationAcknowledged" not in kinds
            scheduled = [event for event in history if event["event_type"] == "ActivityScheduled"][0]
            assert scheduled["payload"]["activity"]["cancellation_policy"] == "abandon"
            total_deadline = scheduled["payload"]["activity"]["schedule_to_close_deadline_at"]
            assert isinstance(total_deadline, str)
            status = await client.activity_task_status(**fence)
            assert status["can_continue"] is True
            until = time.monotonic() + 35
            while True:
                history = await events(handle)
                completed = [event for event in history if event["event_type"] == "ActivityCompleted"
                             and event["payload"].get("activity_type") == "tests.python-cooperative-work"]
                if completed or time.monotonic() >= until:
                    break
                await asyncio.sleep(0.1)
            assert len(completed) == 1
            assert serializer.decode_envelope(completed[0]["payload"]["result"]) == "independent-completion"
            assert completed[0]["payload"]["activity"]["schedule_to_close_deadline_at"] == total_deadline
            assert completed[0]["payload"]["activity_attempt_id"] == fence["activity_attempt_id"]
            kinds = [event["event_type"] for event in history]
            assert kinds.count("WorkflowCancelled") == 1
            assert "WorkflowCompleted" not in kinds
            assert "ActivityCancellationAcknowledged" not in kinds
            with pytest.raises(ServerError) as completion:
                await client.complete_activity_task(**fence, result="stale")
            assert completion.value.status == 409
            with pytest.raises(ServerError) as failure:
                await client.fail_activity_task(**fence, message="stale", failure_type="LateQualification")
            assert failure.value.status == 409
            assert await events(handle) == history
            print(f"Bounded remote Abandon: {json.dumps([entered, accepted, completed])}")
        finally:
            await worker.stop()
            await asyncio.wait_for(running, timeout=10)


@pytest.mark.parametrize("handler_kind", ["async", "sync"])
async def test_actual_remote_shutdown_joins_callback_before_replacement_cleanup(
    server_url: str, server_token: str, handler_kind: str, tmp_path: Path,
) -> None:
    queue = f"py-cooperative-owner-stop-{uuid.uuid4().hex[:8]}"
    marker = tmp_path / "remote"
    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue, shutdown_timeout=0.1)
        worker.activities["tests.python-cooperative-work"] = (
            AsyncRemoteQualification(str(marker)) if handler_kind == "async" else SyncRemoteQualification(str(marker))
        )
        running = asyncio.create_task(worker.run())
        replacement = candidate_worker(client, queue, worker_id=f"{queue}-replacement")
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["remote"],
            )
            entered = await remote_marker(marker)
            before = await events(handle)
            await asyncio.wait_for(worker.stop(), timeout=10)
            await asyncio.wait_for(running, timeout=10)
            await callback_gone(entered["callback_pid"])
            assert await events(handle) == before
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            await replacement._register()
            assert await replacement._run_workflow_task(await poll_claim(client, replacement)) is not None
            await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
            print(f"Stopped remote callback before replacement: {json.dumps(entered)}")
        finally:
            await replacement.stop()
            await worker.stop()
            await asyncio.wait_for(running, timeout=10)


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


async def test_scope_opening_proves_real_native_tree_on_original_claim(
    server_url: str, server_token: str,
) -> None:
    if os.environ.get("DURABLE_WORKFLOW_NATIVE_SOURCE_QUALIFICATION") != "1":
        pytest.skip("scope opening requires the Native candidate source overlay")
    queue = f"py-scope-opening-{uuid.uuid4().hex[:8]}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = candidate_worker(client, queue)
        await worker._register()
        handle = None
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["timer"],
            )
            task = await poll_claim(client, worker)
            authority = dict(task_id=task["task_id"], run_id=task["run_id"], lease_owner=worker.worker_id,
                             workflow_task_attempt=task["workflow_task_attempt"])
            parent = await client.open_cancellation_scope_on_claim(**authority, sequence=1)
            child = await client.open_cancellation_scope_on_claim(
                **authority, sequence=2, parent_scope_id=parent.scope_id, shield_parent=True,
            )
            duplicate = await client.open_cancellation_scope_on_claim(
                **authority, sequence=2, parent_scope_id=parent.scope_id, shield_parent=True,
            )
            assert parent.parent_scope_id == "root" and not parent.shield_parent and not parent.duplicate
            assert child.parent_scope_id == parent.scope_id and child.shield_parent and not child.duplicate
            assert duplicate.duplicate and duplicate.scope_id == child.scope_id
            assert duplicate.history_event_id == child.history_event_id and duplicate.history == child.history
            openings = [event for event in child.history if event["event_type"] == "CancellationScopeOpened"]
            assert [event["payload"]["scope_id"] for event in openings] == [parent.scope_id, child.scope_id]
            with pytest.raises(ServerError) as stale:
                await client.open_cancellation_scope_on_claim(
                    **{**authority, "workflow_task_attempt": authority["workflow_task_attempt"] + 1}, sequence=3,
                )
            assert stale.value.status == 409
            public = await events(handle)
            assert len([event for event in public if event["event_type"] == "CancellationScopeOpened"]) == 2
            assert all(event["event_type"] not in {"TimerScheduled", "WorkflowFailed", "WorkflowCompleted"}
                       for event in public)
            print(f"Native scope opening proof: {json.dumps(child.history)}")
        finally:
            if handle is not None:
                await handle.terminate(reason="scope opening qualification complete")
            await worker.stop()


@workflow.defn(name="tests.python-candidate-scope-replay")
class CandidateScopeReplayWorkflow:
    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.side_effect(lambda: "original prefix")

        def outer():  # type: ignore[no-untyped-def]
            deferred = yield from ctx.cancellation_scope(lambda: ctx.start_timer(1), shield_parent=True)
            yield deferred
            yield ctx.start_timer(1)

        yield from ctx.cancellation_scope(outer)
        yield ctx.start_timer(1)
        return "replayed original scopes"


async def test_candidate_scope_authoring_replays_nested_tree_and_deferred_membership_on_replacement(
    server_url: str, server_token: str,
) -> None:
    if os.environ.get("DURABLE_WORKFLOW_NATIVE_SOURCE_QUALIFICATION") != "1":
        pytest.skip("scope authoring requires the Native candidate source overlay")
    queue = f"py-scope-replay-{uuid.uuid4().hex[:8]}"
    async with Client(server_url, token=server_token, namespace="default") as client:
        original = Worker(client, task_queue=queue, worker_id=f"{queue}-original",
                          workflows=[CandidateScopeReplayWorkflow], capabilities=["cooperative_cancellation"])
        replacement = Worker(client, task_queue=queue, worker_id=f"{queue}-replacement",
                             workflows=[CandidateScopeReplayWorkflow], capabilities=["cooperative_cancellation"])
        original._allow_cancellation_scope_authoring = replacement._allow_cancellation_scope_authoring = True
        handle = None
        try:
            await original._register()
            handle = await client.start_workflow(
                workflow_type="tests.python-candidate-scope-replay", workflow_id=queue, task_queue=queue, input=[],
            )
            for expected in ("record_side_effect", "start_timer"):
                wire = await original._run_workflow_task(await poll_claim(client, original))
                assert wire is not None and [command["type"] for command in wire] == [expected]
            await original.stop()
            await replacement._register()
            for expected in ("start_timer", "start_timer", "complete_workflow"):
                wire = await replacement._run_workflow_task(await poll_claim(client, replacement))
                assert wire is not None and [command["type"] for command in wire] == [expected]
            assert await handle.result(timeout=10) == "replayed original scopes"
            history = await events(handle)
            scopes = [event["payload"] for event in history if event["event_type"] == "CancellationScopeOpened"]
            assert len(scopes) == 2
            parent, child = scopes
            assert (parent["sequence"], parent["parent_scope_id"], parent["shield_parent"]) == (2, "root", False)
            assert (child["sequence"], child["parent_scope_id"], child["shield_parent"]) == (
                3, parent["scope_id"], True,
            )
            timers = [event["payload"] for event in history if event["event_type"] == "TimerScheduled"]
            assert [timer["sequence"] for timer in timers] == [4, 5, 6]
            assert [timer.get("cancellation_scope_id", "root") for timer in timers] == [
                child["scope_id"], parent["scope_id"], "root",
            ]
            assert len([event for event in history if event["event_type"] == "SideEffectRecorded"]) == 1
            print(f"Native scope authoring and replacement replay: {json.dumps(history)}")
        finally:
            if handle is not None and (await handle.describe()).status not in {
                "completed", "failed", "cancelled", "terminated", "continued_as_new",
            }:
                await handle.terminate(reason="scope authoring qualification complete")
            await replacement.stop()
            await original.stop()


@workflow.defn(name="tests.python-candidate-scope-cleanup")
class CandidateScopeCleanupWorkflow:
    grouped = False
    descendants = False

    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.side_effect(lambda: "original prefix")

        def body():  # type: ignore[no-untyped-def]
            try:
                if self.grouped:
                    yield [ctx.start_timer(300), [ctx.start_timer(600)]]
                else:
                    yield ctx.start_timer(300)
            except WorkflowCancelled as error:
                assert isinstance(error.context, ScopedCancellationContext)
                assert error.context is ctx.cancellation_context
                cancellation = error.context
                with ctx.cancellation_shield():
                    yield ctx.side_effect(lambda: {"entry": cancellation.to_dict()})
                    yield ctx.start_timer(1)
                return {"context": cancellation.to_dict(), "remaining": cancellation.remaining()}

        def parent():  # type: ignore[no-untyped-def]
            result = yield from ctx.cancellation_scope(body)
            try:
                ctx.throw_if_cancellation_requested()
                pytest.fail("the parent must retain its accepted ancestor request")
            except WorkflowCancelled as error:
                assert isinstance(error.context, ScopedCancellationContext)
                assert error.context is ctx.cancellation_context
                assert error.context.root_context.to_dict() == result["context"]["root_context"]
                cancellation = error.context
                with ctx.cancellation_shield():
                    yield ctx.start_timer(1)
                return {"context": cancellation.to_dict(), "remaining": cancellation.remaining(),
                        "descendant_context": result["context"]}

        result = yield from ctx.cancellation_scope(parent if self.descendants else body)
        assert not ctx.is_cancellation_requested and ctx.cancellation_context is None
        yield ctx.start_timer(1)
        return result


@workflow.defn(name="tests.python-candidate-scope-group-cleanup")
class CandidateScopeGroupCleanupWorkflow(CandidateScopeCleanupWorkflow):
    grouped = True


@workflow.defn(name="tests.python-candidate-scope-descendant-cleanup")
class CandidateScopeDescendantCleanupWorkflow(CandidateScopeCleanupWorkflow):
    descendants = True


@workflow.defn(name="tests.python-candidate-scope-descendant-group-cleanup")
class CandidateScopeDescendantGroupCleanupWorkflow(CandidateScopeCleanupWorkflow):
    grouped = True
    descendants = True


async def request_native_scope_fixture(run_id: str, workflow_id: str, scope_id: str) -> dict[str, Any]:
    process = await asyncio.create_subprocess_exec(
        "docker", "compose", "exec", "-T", "--user", "1000:1000", "--env",
        "DURABLE_WORKFLOW_NATIVE_SCOPE_FIXTURE=1", "server", "timeout", "--kill-after=1", "5",
        "php", "/app/sdk-source-fixtures/native-scope-request.php",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(json.dumps({
            "run_id": run_id, "workflow_id": workflow_id, "scope_id": scope_id,
        }).encode()), timeout=10)
    except BaseException:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise
    assert process.returncode == 0, stderr.decode()
    return json.loads(stdout)  # type: ignore[no-any-return]


@workflow.defn(name="tests.python-scoped-remote")
class ScopedRemoteWorkflow:
    def run(self, ctx: Any, policy: str, grouped: bool) -> Any:
        def body() -> Any:
            try:
                operation = ctx.schedule_activity(
                    "tests.python-cooperative-work", [], cancellation_policy=policy,
                    schedule_to_close_timeout=60,
                )
                if grouped:
                    yield [operation, [ctx.start_timer(300)]]
                else:
                    yield operation
            except WorkflowCancelled as error:
                assert isinstance(error.context, ScopedCancellationContext)
                assert error.context is ctx.cancellation_context
                with ctx.cancellation_shield():
                    yield ctx.start_timer(1)
                return error.context.to_dict()
            pytest.fail("blocked scoped callback completed without cancellation")

        result = yield from ctx.cancellation_scope(body)
        assert not ctx.is_cancellation_requested and ctx.cancellation_context is None
        yield ctx.start_timer(1)
        return result


@pytest.mark.parametrize("handler_kind", ["async", "sync"])
@pytest.mark.parametrize("grouped,policy", [
    (False, CancellationPolicy.TRY_CANCEL),
    (False, CancellationPolicy.WAIT_CANCELLATION_COMPLETED),
    (True, CancellationPolicy.WAIT_CANCELLATION_COMPLETED),
])
async def test_scoped_remote_callback_stop_leaves_parent_available(
    server_url: str, server_token: str, handler_kind: str, grouped: bool,
    policy: CancellationPolicy, tmp_path: Path,
) -> None:
    if os.environ.get("DURABLE_WORKFLOW_NATIVE_SOURCE_QUALIFICATION") != "1":
        pytest.skip("scope callback supervision requires the exact Native overlay")
    queue = "py-cooperative-scope-boundary-" + uuid.uuid4().hex[:8]
    marker = tmp_path / "remote"
    async with ObservedOwnerClient(server_url, token=server_token, namespace="default") as client:
        worker = Worker(client, task_queue=queue, worker_id=queue + "-owner",
                        workflows=[ScopedRemoteWorkflow], activities=[cooperative_work],
                        capabilities=["cooperative_cancellation"], max_concurrent_activity_tasks=1)
        worker._allow_cancellation_scope_authoring = True
        worker._allow_cancellation_scope_delivery = True
        worker.activities["tests.python-cooperative-work"] = (
            AsyncRemoteQualification(str(marker)) if handler_kind == "async"
            else SyncRemoteQualification(str(marker))
        )
        running = asyncio.create_task(worker.run())
        handle = None
        try:
            handle = await client.start_workflow(workflow_type="tests.python-scoped-remote",
                                                 workflow_id=queue, task_queue=queue,
                                                 input=[policy.value, grouped])
            entered = await remote_marker(marker)
            await asyncio.wait_for(client.owner_heartbeat.wait(), timeout=15)
            fence = {key: entered[key] for key in ("task_id", "activity_attempt_id", "lease_owner")}
            initial = await events(handle)
            scope = next(row["payload"]["scope_id"] for row in initial
                         if row["event_type"] == "CancellationScopeOpened")
            run_id = (await handle.describe()).run_id
            assert run_id is not None
            accepted = await request_native_scope_fixture(run_id, queue, scope)
            assert accepted == await request_native_scope_fixture(run_id, queue, scope)
            original = accepted["payload"]["cancellation"]["root_context"]
            assert await handle.result(timeout=20) == accepted["payload"]["cancellation"]
            await callback_gone(entered["callback_pid"])
            proof = await observed_stop_receipt(client, fence)
            assert proof["request_id"] == accepted["payload"]["request_id"]
            assert proof["root_request_id"] == original["root_request_id"]
            assert proof["cleanup_deadline_at"] == original["cleanup_deadline_at"]
            assert proof["received_after_deadline"] is False
            duplicate = await client.acknowledge_activity_cancellation(
                **fence, request_id=accepted["payload"]["request_id"],
            )
            assert duplicate["duplicate"] is True
            assert duplicate["history_event_id"] == proof["history_event_id"]
            history = await events(handle)
            kinds = [row["event_type"] for row in history]
            for kind in ("CancellationScopeRequested", "CancellationScopeDeliveryPrepared",
                         "CancellationScopeDelivered", "ActivityCancelled",
                         "ActivityCancellationAcknowledged", "WorkflowCompleted"):
                assert kinds.count(kind) == 1
            for kind in ("CooperativeCancellationRequested", "CooperativeCancellationDelivered",
                         "WorkflowCancelled", "WorkflowFailed", "ActivityHeartbeatRecorded", "ActivityCompleted"):
                assert kind not in kinds
            assert kinds.count("TimerCancelled") == int(grouped)
            assert kinds.count("TimerScheduled") == 2 + int(grouped)
            assert kinds.count("TimerFired") == 2
            if policy == CancellationPolicy.WAIT_CANCELLATION_COMPLETED:
                assert kinds.index("ActivityCancellationAcknowledged") < kinds.index("CancellationScopeDelivered")
            acknowledged = next(row["payload"] for row in history
                                if row["event_type"] == "ActivityCancellationAcknowledged")
            assert acknowledged["activity_attempt_id"] == fence["activity_attempt_id"]
            assert acknowledged["lease_owner"] == fence["lease_owner"]
            assert acknowledged["evidence_source"] == "activity_worker"
            for key in ("request_id", "root_request_id", "cleanup_deadline_at", "acknowledged_at"):
                assert acknowledged[key] == proof[key]
            terminal = next(row for row in history if row["event_type"] == "WorkflowCompleted")
            assert datetime.fromisoformat(terminal["timestamp"].replace("Z", "+00:00")) < (
                datetime.fromisoformat(original["cleanup_deadline_at"].replace("Z", "+00:00"))
            )
            with pytest.raises(ServerError) as completion:
                await client.complete_activity_task(**fence, result="late")
            assert completion.value.status == 409
            with pytest.raises(ServerError) as failure:
                await client.fail_activity_task(**fence, message="late", failure_type="LateQualification")
            assert failure.value.status == 409
            assert await events(handle) == history
            print("Actual scoped callback stop and unchanged parent: " + json.dumps({
                "callback": entered, "receipt": proof, "history": history,
            }))
        finally:
            if handle is not None and (await handle.describe()).status not in {
                "completed", "failed", "cancelled", "terminated", "continued_as_new",
            }:
                await handle.terminate(reason="scoped callback qualification complete")
            await worker.stop()
            await asyncio.wait_for(running, timeout=10)


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("descendants", [False, True])
async def test_candidate_scope_cleanup_keeps_original_delivery_and_budget_after_replacement(
    server_url: str, server_token: str, grouped: bool, descendants: bool,
) -> None:
    if os.environ.get("DURABLE_WORKFLOW_NATIVE_SOURCE_QUALIFICATION") != "1":
        pytest.skip("scope delivery requires the exact Native source overlay")
    queue = f"py-cooperative-scope-boundary-{uuid.uuid4().hex[:8]}"
    cls = {(False, False): CandidateScopeCleanupWorkflow, (True, False): CandidateScopeGroupCleanupWorkflow,
           (False, True): CandidateScopeDescendantCleanupWorkflow,
           (True, True): CandidateScopeDescendantGroupCleanupWorkflow}[grouped, descendants]
    workflow_type = str(cls.__workflow_name__)
    async with Client(server_url, token=server_token, namespace="default") as client:
        original = Worker(client, task_queue=queue, worker_id=f"{queue}-original",
                          workflows=[cls], capabilities=["cooperative_cancellation"])
        replacement = Worker(client, task_queue=queue, worker_id=f"{queue}-replacement",
                             workflows=[cls], capabilities=["cooperative_cancellation"])
        for worker in (original, replacement):
            worker._allow_cancellation_scope_authoring = True
            worker._allow_cancellation_scope_delivery = True
        handle = None
        try:
            await original._register()
            handle = await client.start_workflow(
                workflow_type=workflow_type, workflow_id=queue, task_queue=queue, input=[],
            )
            for expected in (["record_side_effect"], ["start_timer"] * (2 if grouped else 1)):
                wire = await original._run_workflow_task(await poll_claim(client, original))
                assert wire is not None and [command["type"] for command in wire] == expected
            history = await events(handle)
            scope = next(row["payload"]["scope_id"] for row in history
                         if row["event_type"] == "CancellationScopeOpened")
            run_id = (await handle.describe()).run_id
            assert run_id is not None
            accepted = await request_native_scope_fixture(run_id, queue, scope)
            repeated = await request_native_scope_fixture(run_id, queue, scope)
            assert accepted == repeated
            wire = await original._run_workflow_task(await poll_claim(client, original))
            assert wire is not None and [command["type"] for command in wire] == ["record_side_effect", "start_timer"]
            await original.stop()
            await replacement._register()
            for expected in (["start_timer"] * (2 if descendants else 1) + ["complete_workflow"]):
                wire = await replacement._run_workflow_task(await poll_claim(client, replacement))
                assert wire is not None and [command["type"] for command in wire] == [expected]
            result = await handle.result(timeout=10)
            assert result["context"] == accepted["payload"]["cancellation"]
            assert 0 < result["remaining"] < 30
            history = await events(handle)
            kinds = [row["event_type"] for row in history]
            for kind in ("CancellationScopeDeliveryPrepared", "CancellationScopeDelivered", "WorkflowCompleted"):
                assert kinds.count(kind) == 1
            for kind in ("CancellationScopeOpened", "CancellationScopeRequested"):
                assert kinds.count(kind) == (2 if descendants else 1)
            if descendants:
                child_request = next(
                    row["payload"]["cancellation"] for row in history
                    if row["event_type"] == "CancellationScopeRequested" and row["payload"]["scope_id"] != scope
                )
                assert result["descendant_context"] == child_request
                assert child_request["root_context"] == result["context"]["root_context"]
                assert child_request["lineage"][:-1] == result["context"]["lineage"]
            assert kinds.count("TimerCancelled") == (2 if grouped else 1)
            assert kinds.count("SideEffectRecorded") == 2
            assert kinds.count("TimerScheduled") == (4 if grouped else 3) + int(descendants)
            assert kinds.count("TimerFired") == 2 + int(descendants)
            assert "CooperativeCancellationRequested" not in kinds and "WorkflowCancelled" not in kinds
            assert "WorkflowFailed" not in kinds
            print(f"Native scope cleanup and replacement replay: {json.dumps(history)}")
        finally:
            if handle is not None and (await handle.describe()).status not in {
                "completed", "failed", "cancelled", "terminated", "continued_as_new",
            }:
                await handle.terminate(reason="scope cleanup source qualification complete")
            await replacement.stop()
            await original.stop()


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
        await worker._register()
        # This fixture invokes only local cleanup and claims remote work by API.
        # Its in-process closure does not qualify callback process supervision.
        worker.activities["tests.python-cooperative-cleanup"] = cleanup
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
        await worker._register()
        # Exercise the existing local replay fence without polling remote work.
        # Physical local callback supervision remains a separate qualification.
        worker.activities["tests.python-cooperative-work"] = (
            blocked_work if handler_kind == "async" else synchronous_work
        )
        worker.activities["tests.python-cooperative-cleanup"] = cleanup
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
        await worker._register()
        # Local-only replay fixture. It does not poll remote activity callbacks.
        worker.activities["tests.python-cooperative-cleanup"] = cleanup
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


async def process_event(process: asyncio.subprocess.Process, phase: str, timeout: float = 40) -> dict[str, Any]:
    async def read() -> dict[str, Any]:
        assert process.stdout is not None
        while line := await process.stdout.readline():
            value = json.loads(line)
            if value["phase"] == phase:
                return value
        assert process.stderr is not None
        pytest.fail(f"worker exited before {phase}: {(await process.stderr.read()).decode()}")
    return await asyncio.wait_for(read(), timeout=timeout)


async def test_sigkill_activity_owner_reclaims_attempt_before_cooperative_cleanup(
    server_url: str, server_token: str,
) -> None:
    queue = f"py-cooperative-activity-reclaim-{uuid.uuid4().hex[:8]}"
    processes: list[asyncio.subprocess.Process] = []
    async with Client(server_url, token=server_token, namespace="default") as client:
        handle = await client.start_workflow(
            workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["remote"],
        )
        try:
            owner = await native_process(queue, f"{queue}-killed", "remote")
            processes.append(owner)
            original = await process_event(owner, "remote-entered")
            assert original["attempt_number"] == 1
            fence = {key: original[key] for key in ("task_id", "activity_attempt_id", "lease_owner")}
            leased = await client.activity_task_status(**fence)
            assert leased["can_continue"] is True
            assert leased["attempt_status"] == "running"
            expires_at = datetime.fromisoformat(leased["lease_expires_at"].replace("Z", "+00:00")).timestamp()
            assert expires_at > time.time(), "kill an actually current lease"
            print(f"SIGKILL original activity: {json.dumps([original, leased])}")
            owner.kill()
            assert await asyncio.wait_for(owner.wait(), timeout=10) == -9
            await callback_gone(original["callback_pid"])

            successor = await native_process(queue, f"{queue}-successor", "remote")
            processes.append(successor)
            # Wait for Native's actual five-minute lease and normal repair.
            # No clock, storage row or production lease is changed.
            reclaimed = await process_event(successor, "remote-entered", timeout=330)
            assert reclaimed["task_id"] == original["task_id"]
            assert reclaimed["activity_attempt_id"] != original["activity_attempt_id"]
            assert reclaimed["lease_owner"] != original["lease_owner"]
            assert reclaimed["attempt_number"] == 2
            assert time.time() >= expires_at, "reclaim precedes actual lease expiry"
            print(f"SIGKILL successor activity: {json.dumps(reclaimed)}")

            closed = await client.activity_task_status(**fence)
            assert closed["attempt_status"] == "expired"
            assert closed["can_continue"] is False
            before = await events(handle)
            with pytest.raises(ServerError) as completion:
                await client.complete_activity_task(**fence, result="late")
            assert completion.value.status == 409
            with pytest.raises(ServerError) as failure:
                await client.fail_activity_task(**fence, message="late", failure_type="LateQualification")
            assert failure.value.status == 409
            heartbeat = await client.heartbeat_activity_task(**fence)
            assert heartbeat["can_continue"] is False
            assert heartbeat["heartbeat_recorded"] is False
            assert heartbeat["cancel_requested"] is False
            assert heartbeat["reason"] == "attempt_closed"
            assert heartbeat["lease_expires_at"] == closed["lease_expires_at"]
            assert heartbeat["last_heartbeat_at"] == closed["last_heartbeat_at"]
            assert await client.activity_task_status(**fence) == closed
            assert await events(handle) == before, "dead attempt changed canonical history"

            accepted = await handle.request_cancellation(cleanup_timeout_seconds=60)
            history = await assert_cancelled_cleanup(handle, accepted["cancellation_request"]["request_id"])
            await callback_gone(reclaimed["callback_pid"])
            assert len([event for event in history if event["event_type"] == "ActivityCancelled"]) == 1
            print(f"SIGKILL reclaimed cancellation history: {json.dumps(history)}")
        finally:
            for process in processes:
                if process.returncode is None:
                    process.kill()
                    await asyncio.wait_for(process.wait(), timeout=10)


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
            await callback_gone(claimed["callback_pid"])
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


async def test_scoped_cleanup_sigkill_replays_before_root_delivery_with_original_30_second_deadline(
    server_url: str, server_token: str, tmp_path: Path,
) -> None:
    if os.environ.get("DURABLE_WORKFLOW_NATIVE_SOURCE_QUALIFICATION") != "1":
        pytest.skip("scoped prepared cleanup requires the exact Native overlay")
    queue = "py-run-scope-recovery-" + uuid.uuid4().hex[:8]
    marker = str(tmp_path / "cleanup")
    trace = tmp_path / "prepared.jsonl"
    processes: list[asyncio.subprocess.Process] = []
    logs: list[Any] = []

    async def owner(name: str, mode: str) -> asyncio.subprocess.Process:
        log = (tmp_path / (name + ".log")).open("wb")
        logs.append(log)
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "tests.integration.prepared_worker", queue, name, mode, str(trace),
            stdout=log, stderr=log,
        )
        processes.append(process)
        return process

    async with Client(server_url, token=server_token, namespace="default") as client:
        seed = scope_prepared_worker(client, queue, worker_id=queue + "-seed")
        handle = None
        try:
            await seed._register()
            handle = await client.start_workflow(workflow_type="tests.python-run-scope-cleanup",
                                                 workflow_id=queue, task_queue=queue, input=[marker])
            wire = await seed._run_workflow_task(await poll_claim(client, seed))
            assert wire is not None and [command["type"] for command in wire] == ["start_timer"]
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=30)
            original = accepted["cancellation_request"]
            first = await owner(queue + "-first", "scope-hold")
            entered = await remote_marker(Path(marker + ".scoped." + queue + "-first"))
            before = await events(handle)
            assert [row["event_type"] for row in before].count("CancellationScopeDelivered") == 1
            assert "CooperativeCancellationDelivered" not in [row["event_type"] for row in before]
            first.kill()
            assert await asyncio.wait_for(first.wait(), timeout=10) == -9
            await callback_gone(entered["callback_pid"])
            duplicate = await handle.request_cancellation(cleanup_timeout_seconds=90)
            for field in ("request_id", "requested_at", "cleanup_deadline_at"):
                assert duplicate["cancellation_request"][field] == original[field]
            await owner(queue + "-replacement", "scope-finish")
            deadline = datetime.fromisoformat(original["cleanup_deadline_at"].replace("Z", "+00:00"))
            timeout = (deadline - datetime.now(deadline.tzinfo)).total_seconds()
            assert timeout > 0
            with pytest.raises(WorkflowCancelled):
                await handle.result(timeout=timeout)
            history = await events(handle)
            kinds = [row["event_type"] for row in history]
            for kind in ("CancellationScopeDeliveryPrepared", "CancellationScopeDelivered",
                         "CooperativeCancellationDelivered", "WorkflowCancelled"):
                assert kinds.count(kind) == 1
            assert kinds.count("ActivityCompleted") == 2 and "ActivityHeartbeatRecorded" not in kinds
            assert "WorkflowFailed" not in kinds
            resumed = await remote_marker(Path(marker + ".scoped." + queue + "-replacement"))
            assert resumed["context"] == entered["context"]
            assert resumed["activity_attempt_id"] != entered["activity_attempt_id"]
            root = await remote_marker(Path(marker + ".root." + queue + "-replacement"))
            assert resumed["context"]["root_context"] == root["context"]
            assert root["context"]["root_request_id"] == original["request_id"]
            terminal = next(row for row in history if row["event_type"] == "WorkflowCancelled")
            assert datetime.fromisoformat(terminal["timestamp"].replace("Z", "+00:00")) < deadline
            receipts = [json.loads(line) for line in trace.read_text().splitlines()]
            scoped = [row["receipt"] for row in receipts if row["operation"] == "prepare"
                      and row["receipt"].get("cancellation_cleanup", {}).get("scope_id")]
            assert len(scoped) == 2
            assert scoped[0]["cancellation_cleanup"] == scoped[1]["cancellation_cleanup"]
            assert scoped[0]["cancellation_cleanup"]["cleanup_deadline_at"] == original["cleanup_deadline_at"]
            assert any(row["operation"] == "recover" for row in receipts)
            await callback_gone(resumed["callback_pid"])
            await callback_gone(root["callback_pid"])
            print("scoped SIGKILL and root convergence: " + json.dumps({
                "original_cleanup": entered, "replacement_cleanup": resumed, "root_cleanup": root,
                "receipts": receipts, "history": history,
            }))
        finally:
            for process in processes:
                if process.returncode is None:
                    process.kill()
                await asyncio.wait_for(process.wait(), timeout=10)
            for log in logs:
                log.close()
                print(log.name + ": " + Path(log.name).read_text()[-8000:])
            if handle is not None and (await handle.describe()).status not in {
                "completed", "failed", "cancelled", "terminated", "continued_as_new",
            }:
                await handle.terminate(reason="scoped cleanup source qualification complete")
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
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue,
                input=["clock_timer"],
            )
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=120)
            original = accepted["cancellation_request"]
            first = await native_process(queue, f"{queue}-killed", "hold")
            processes.append(first)
            claim = await process_event(first, "claim")
            original_remaining = await process_event(first, "remaining-delivery")
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
            replacement_remaining = await process_event(replacement, "remaining-delivery")
            assert replacement_remaining == original_remaining
            resumed = await process_event(replacement, "cleanup")
            assert resumed["request_id"] == original["request_id"]
            completed_remaining = await process_event(replacement, "remaining-cleanup")
            assert completed_remaining["context"] == original_remaining["context"]
            assert completed_remaining["remaining"] == original_remaining["remaining"] > 0
            assert (await process_event(replacement, "finished"))["committed"] is True
            assert await asyncio.wait_for(replacement.wait(), timeout=10) == 0
            history = await assert_cancelled_cleanup(handle, original["request_id"])
            deadline = datetime.fromisoformat(original["cleanup_deadline_at"].replace("Z", "+00:00"))
            delivered = next(event for event in history if event["event_type"] == "CooperativeCancellationDelivered")
            recorded = datetime.fromisoformat(delivered["timestamp"].replace("Z", "+00:00"))
            assert original_remaining["remaining"] == pytest.approx((deadline - recorded).total_seconds(), abs=1e-6)
            remaining_observations = {
                "original": original_remaining, "replacement": replacement_remaining, "completed": completed_remaining,
            }
            print(f"SIGKILL replay remaining-time observations: {json.dumps(remaining_observations)}")
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
        await worker._register()
        # Local-only replay fixture. It does not poll remote activity callbacks.
        worker.activities["tests.python-cooperative-cleanup"] = cleanup
        try:
            handle = await client.start_workflow(
                workflow_type="tests.python-cooperative-cleanup", workflow_id=queue, task_queue=queue, input=["timer"],
            )
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=2 if close_kind == "deadline" else 60)
            task = await poll_claim(client, worker)
            execution = worker._track(worker._run_workflow_task(task))
            await asyncio.wait_for(entered.wait(), timeout=10)
            if close_kind == "terminate":
                await handle.terminate(reason="qualification termination during cleanup")
            assert await asyncio.wait_for(execution, timeout=15) is None
            history = await events(handle)
            if close_kind == "deadline":
                # Callback fencing ends cleanup authority immediately. Native's
                # configured repair pass records the eventual terminal row.
                # Measure that separately rather than treating callback join
                # as acknowledgement that the repair pass has already run.
                original = accepted["cancellation_request"]
                original_deadline = datetime.fromisoformat(original["cleanup_deadline_at"].replace("Z", "+00:00"))

                async def terminal_history() -> list[dict[str, Any]]:
                    while True:
                        current = await events(handle)
                        requests = [row for row in current if row["event_type"] == "CooperativeCancellationRequested"]
                        assert len(requests) == 1
                        canonical = requests[0]["payload"]
                        assert canonical["workflow_command_id"] == original["request_id"]
                        assert datetime.fromisoformat(canonical["cleanup_deadline_at"].replace("Z", "+00:00")) == (
                            original_deadline
                        )
                        if any(row["event_type"] == "WorkflowCancelled" for row in current):
                            return current
                        await asyncio.sleep(0.1)
                history = await asyncio.wait_for(terminal_history(), timeout=15)
            kinds = [event["event_type"] for event in history]
            assert kinds.count("WorkflowCancelled" if close_kind == "deadline" else "WorkflowTerminated") == 1
            assert kinds.count("CooperativeCancellationRequested") == 1
            assert kinds.count("CooperativeCancellationDelivered") == 1
            assert "ActivityCompleted" not in kinds
            assert "ActivityFailed" not in kinds
            assert "WorkflowCompleted" not in kinds
        finally:
            await worker.stop()
