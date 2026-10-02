"""Opt-in exact-source proof of Python's durable sequential local consumer."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import Client, Worker, activity, workflow
from durable_workflow.errors import ServerError, WorkflowCancelled
from durable_workflow.workflow import replay
from tests.integration.test_cooperative_cancellation import callback_gone, events, poll_claim, remote_marker

pytestmark = pytest.mark.usefixtures("prepared_runtime")


@pytest.fixture
async def prepared_runtime(server_url: str, server_token: str, monkeypatch: pytest.MonkeyPatch) -> None:
    if os.environ.get("DURABLE_WORKFLOW_NATIVE_SOURCE_QUALIFICATION") != "1":
        pytest.skip("prepared local consumer requires exact-source Native qualification")
    async with Client(server_url, token=server_token, namespace="default") as client:
        info = await client.get_cluster_info()
    assert info["worker_protocol"]["server_capabilities"]["prepared_local_activities"] is True
    assert info["worker_protocol"]["server_capabilities"]["prepared_local_activity_groups"] is True
    assert info["worker_protocol"]["version"] == "1.20"
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")


@workflow.defn(name="tests.python-prepared-sequential")
class PreparedSequentialWorkflow:
    def run(self, ctx: Any, marker: str) -> Any:
        yield ctx.upsert_memo({"before": "durable local admission"})
        first = yield ctx.local_activity("tests.python-prepared-local", [marker, "first"], heartbeat_timeout=10)
        second = yield ctx.local_activity("tests.python-prepared-local", [marker, "second"], heartbeat_timeout=10)
        return [first, second]


@activity.defn(name="tests.python-prepared-local")
async def prepared_local(marker: str, phase: str) -> dict[str, Any]:
    info = activity.context().info
    Path(marker + "." + phase).write_text(json.dumps({
        "callback_pid": os.getpid(), "attempt_id": info.activity_attempt_id,
    }))
    await activity.context().heartbeat({"phase": phase})
    return {"phase": phase, "bytes": b"\x00\xff", "attempt": info.activity_attempt_id}


@workflow.defn(name="tests.python-prepared-cancellation")
class PreparedCancellationWorkflow:
    def run(self, ctx: Any, marker: str, group: bool = False) -> Any:
        try:
            if group:
                yield [ctx.local_activity("tests.python-prepared-blocked", [marker, "work-0", None]),
                       ctx.local_activity("tests.python-prepared-blocked", [marker, "work-1", None])]
            else:
                yield ctx.local_activity("tests.python-prepared-blocked", [marker, "work", None])
        except WorkflowCancelled as error:
            with ctx.cancellation_shield():
                if group:
                    yield [ctx.local_activity(
                        "tests.python-prepared-blocked", [marker, "cleanup-" + str(index), error.request_id],
                        retry_policy={"max_attempts": 2, "backoff_seconds": [0]},
                    ) for index in range(2)]
                else:
                    yield ctx.local_activity(
                        "tests.python-prepared-blocked", [marker, "cleanup", error.request_id],
                        retry_policy={"max_attempts": 2, "backoff_seconds": [0]},
                    )
            return error.request_id
        return "not cancelled"


@activity.defn(name="tests.python-prepared-blocked")
async def prepared_blocked(marker: str, phase: str, request_id: str | None) -> dict[str, Any]:
    info = activity.context().info
    path = Path(marker + "." + phase + "." + info.worker_id)
    pending = path.with_suffix(path.suffix + ".writing")
    pending.write_text(json.dumps({"phase": phase, "callback_pid": os.getpid(), "request_id": request_id,
                                   "activity_attempt_id": info.activity_attempt_id}))
    pending.replace(path)
    if phase.startswith("work") or os.environ.get("DW_PREPARED_FIXTURE_MODE") == "hold":
        # Intentionally no application heartbeat.
        await asyncio.Event().wait()
    return {"request_id": request_id, "bytes": b"\x00\xff"}


@workflow.defn(name="tests.python-prepared-group")
class PreparedGroupWorkflow:
    def run(self, ctx: Any, marker: str) -> Any:
        yield ctx.upsert_memo({"before": "atomic local group"})
        first = ctx.local_activity("tests.python-prepared-peer", [marker, "first", "second"], heartbeat_timeout=10)
        second = ctx.local_activity("tests.python-prepared-peer", [marker, "second", "first"], heartbeat_timeout=10)
        return (yield [first, [second, ctx.start_timer(1)]])


@activity.defn(name="tests.python-prepared-peer")
async def prepared_peer(marker: str, phase: str, peer: str) -> dict[str, Any]:
    info = activity.context().info
    Path(marker + "." + phase).write_text(json.dumps({
        "callback_pid": os.getpid(), "attempt_id": info.activity_attempt_id,
    }))
    await remote_marker(Path(marker + "." + peer))
    await activity.context().heartbeat({"phase": phase})
    return {"phase": phase, "bytes": b"\x00\xff", "attempt": info.activity_attempt_id}


def prepared_worker(client: Client, queue: str, **kwargs: Any) -> Worker:
    return Worker(
        client, task_queue=queue, worker_id=kwargs.pop("worker_id", queue + "-owner"),
        workflows=[PreparedSequentialWorkflow, PreparedCancellationWorkflow, PreparedGroupWorkflow],
        activities=[prepared_local, prepared_blocked, prepared_peer],
        capabilities=["cooperative_cancellation", "prepared_local_activities", "prepared_local_activity_groups"],
        **kwargs,
    )


class TrackingPreparedClient(Client):
    def __init__(self, *args: Any, trace_path: Path, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.trace_path = trace_path

    async def prepared_local_activity_operation(self, **kwargs: Any) -> dict[str, Any]:
        try:
            receipt = await super().prepared_local_activity_operation(**kwargs)
        except ServerError as error:
            with self.trace_path.open("a") as stream:
                stream.write(json.dumps({"operation": kwargs["operation"], "sequence": kwargs["body"].get("sequence"),
                                         "refused": error.reason(), "status": error.status,
                                         "response": error.body}) + "\n")
            raise
        if kwargs["operation"] != "control" or receipt.get("active") is False:
            with self.trace_path.open("a") as stream:
                stream.write(json.dumps({"operation": kwargs["operation"], "receipt": receipt}) + "\n")
        return receipt


async def test_prepared_prefix_two_callbacks_and_cold_replay_use_canonical_history(
    server_url: str, server_token: str, tmp_path: Path,
) -> None:
    queue = "py-prepared-sequential-" + uuid.uuid4().hex[:8]
    marker = str(tmp_path / "callback")
    async with Client(server_url, token=server_token, namespace="default") as client:
        worker = prepared_worker(client, queue)
        await worker._register()
        try:
            handle = await client.start_workflow(workflow_type="tests.python-prepared-sequential", task_queue=queue,
                                                 workflow_id=queue, input=[marker])
            task = await poll_claim(client, worker)
            commands = await worker._run_workflow_task(task)
            assert commands is not None and [command["type"] for command in commands] == ["complete_workflow"]
            result = await handle.result(timeout=10)
            assert [item["phase"] for item in result] == ["first", "second"]
            assert all(item["bytes"] == b"\x00\xff" for item in result)
            assert result[0]["attempt"] != result[1]["attempt"]
            history = await events(handle)
            kinds = [event["event_type"] for event in history]
            assert kinds.count("ActivityStarted") == kinds.count("ActivityCompleted") == 2
            assert kinds.count("ActivityHeartbeatRecorded") == 2
            assert sorted(event["payload"]["progress"]["phase"] for event in history
                          if event["event_type"] == "ActivityHeartbeatRecorded") == ["first", "second"]
            assert kinds.count("MemoUpserted") == kinds.count("WorkflowCompleted") == 1
            outcome = replay(PreparedSequentialWorkflow, history, [marker], run_id=handle.run_id or "",
                             prepare_local_activities=True)
            assert outcome.prepared_local_activity is None and outcome.commands[0].result == result  # type: ignore[union-attr]
            for phase in ("first", "second"):
                proof = json.loads(Path(marker + "." + phase).read_text())
                await callback_gone(proof["callback_pid"])
            print("prepared sequential history: " + json.dumps(history))
        finally:
            await worker.stop()


async def test_prepared_nested_mixed_group_starts_peers_concurrently_and_cold_replays_results_in_position(
    server_url: str, server_token: str, tmp_path: Path,
) -> None:
    queue = "py-prepared-group-" + uuid.uuid4().hex[:8]
    marker = str(tmp_path / "callback")
    trace_path = tmp_path / "group-receipts.jsonl"
    async with TrackingPreparedClient(
        server_url, token=server_token, namespace="default", trace_path=trace_path,
    ) as client:
        worker = prepared_worker(client, queue)
        owner = asyncio.create_task(worker.run())
        try:
            handle = await client.start_workflow(workflow_type="tests.python-prepared-group", task_queue=queue,
                                                 workflow_id=queue, input=[marker])
            result = await handle.result(timeout=15)
            assert result[0]["phase"] == "first" and result[1][0]["phase"] == "second"
            assert result[0]["bytes"] == result[1][0]["bytes"] == b"\x00\xff"
            assert result[0]["attempt"] != result[1][0]["attempt"]
            history = await events(handle)
            kinds = [event["event_type"] for event in history]
            assert kinds.count("ActivityScheduled") == kinds.count("ActivityStarted") == 2
            assert kinds.count("ActivityCompleted") == 2
            assert kinds.count("ActivityHeartbeatRecorded") == 2
            assert sorted(event["payload"]["progress"]["phase"] for event in history
                          if event["event_type"] == "ActivityHeartbeatRecorded") == ["first", "second"]
            assert kinds.count("TimerScheduled") == kinds.count("TimerFired") == 1
            assert kinds.count("MemoUpserted") == kinds.count("WorkflowCompleted") == 1
            completed = replay(PreparedGroupWorkflow, history, [marker], run_id=handle.run_id or "",
                               prepare_local_activities=True, prepare_local_activity_groups=True)
            assert completed.prepared_local_activity_group is None
            assert completed.commands[0].result == result  # type: ignore[union-attr]
            for phase in ("first", "second"):
                await callback_gone(json.loads(Path(marker + "." + phase).read_text())["callback_pid"])
            print("prepared nested mixed group history: " + json.dumps(history))
        finally:
            await worker.stop()
            await asyncio.wait_for(owner, timeout=10)
            if trace_path.exists():
                print("prepared nested mixed group receipts: " + trace_path.read_text())
            print("prepared nested mixed group final history: " + json.dumps(await events(handle)))


@pytest.mark.parametrize("group", [False, True], ids=["sequential", "atomic-group"])
async def test_prepared_callback_stop_cleanup_sigkill_and_cold_recovery_keep_original_30_second_deadline(
    server_url: str, server_token: str, tmp_path: Path, group: bool,
) -> None:
    queue = "py-prepared-cancel-" + uuid.uuid4().hex[:8]
    marker = str(tmp_path / "callback")
    trace_path = tmp_path / "receipts.jsonl"
    processes: list[asyncio.subprocess.Process] = []
    logs: list[Any] = []

    async def owner(name: str, mode: str) -> asyncio.subprocess.Process:
        log = (tmp_path / (name + ".log")).open("wb")
        logs.append(log)
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "tests.integration.prepared_worker", queue, name, mode, str(trace_path),
            stdout=log, stderr=log,
        )
        processes.append(process)
        return process

    async with Client(server_url, token=server_token, namespace="default") as client:
        try:
            handle = await client.start_workflow(workflow_type="tests.python-prepared-cancellation", task_queue=queue,
                                                 workflow_id=queue, input=[marker, group])
            first = await owner(queue + "-first", "hold")
            work_phases = ["work-0", "work-1"] if group else ["work"]
            cleanup_phases = ["cleanup-0", "cleanup-1"] if group else ["cleanup"]
            work = [await remote_marker(Path(marker + "." + phase + "." + queue + "-first"))
                    for phase in work_phases]
            accepted = await handle.request_cancellation(cleanup_timeout_seconds=30)
            original = accepted["cancellation_request"]
            requested_history = await events(handle)
            original_context = next(event["payload"]["cancellation"] for event in requested_history
                                    if event["event_type"] == "CooperativeCancellationRequested")
            cleanup = [await remote_marker(Path(marker + "." + phase + "." + queue + "-first"))
                       for phase in cleanup_phases]
            assert all(item["request_id"] == original["request_id"] for item in cleanup)
            for item in work:
                await callback_gone(item["callback_pid"])
            first.kill()
            await asyncio.wait_for(first.wait(), timeout=10)
            for item in cleanup:
                await callback_gone(item["callback_pid"])
            duplicate = await handle.request_cancellation(cleanup_timeout_seconds=90)
            for field in ("request_id", "requested_at", "cleanup_deadline_at"):
                assert duplicate["cancellation_request"][field] == original[field]
            await owner(queue + "-replacement", "finish")
            deadline = datetime.fromisoformat(original["cleanup_deadline_at"].replace("Z", "+00:00"))
            timeout = (deadline - datetime.now(deadline.tzinfo)).total_seconds()
            assert timeout > 0
            with pytest.raises(WorkflowCancelled):
                await handle.result(timeout=timeout)
            history = await events(handle)
            kinds = [event["event_type"] for event in history]
            assert kinds.count("CooperativeCancellationDelivered") == kinds.count("WorkflowCancelled") == 1
            assert kinds.count("ActivityCancellationAcknowledged") == len(work)
            assert kinds.count("ActivityCancelled") == len(work)
            assert "ActivityHeartbeatRecorded" not in kinds
            terminal = next(event for event in history if event["event_type"] == "WorkflowCancelled")
            assert datetime.fromisoformat(terminal["timestamp"].replace("Z", "+00:00")) < deadline
            receipts = [json.loads(line) for line in trace_path.read_text().splitlines()]
            admissions = [entry["receipt"] for entry in receipts
                          if entry["operation"] == "prepare" and entry["receipt"].get("cancellation_cleanup")]
            assert len(admissions) == 2 * len(cleanup)
            assert len({item["activity_attempt_id"] for item in admissions}) == len(admissions)
            assert all(item["cancellation_cleanup"] == admissions[0]["cancellation_cleanup"] for item in admissions)
            authority = admissions[0]["cancellation_cleanup"]
            assert authority["request_id"] == original["request_id"]
            assert authority["root_request_id"] == original_context["root_request_id"]
            assert datetime.fromisoformat(authority["cleanup_deadline_at"].replace("Z", "+00:00")) == deadline
            recoveries = [entry["receipt"] for entry in receipts if entry["operation"] == "recover"]
            assert len(recoveries) == len(cleanup)
            assert all(item["callback_stop_state"] == "unknown" for item in recoveries)
            replacement = [await remote_marker(Path(marker + "." + phase + "." + queue + "-replacement"))
                           for phase in cleanup_phases]
            for item in replacement:
                await callback_gone(item["callback_pid"])
            print("prepared physically joined callbacks: " + json.dumps({
                "work": work, "killed_cleanup": cleanup, "replacement_cleanup": replacement,
            }))
            print("prepared cleanup recovery receipts: " + json.dumps(receipts))
            print("prepared cleanup recovery history: " + json.dumps(history))
        finally:
            for process in processes:
                if process.returncode is None:
                    process.kill()
                await asyncio.wait_for(process.wait(), timeout=10)
            for log in logs:
                log.close()
            for path in tmp_path.glob("*.log"):
                print(path.name + ": " + path.read_text())
