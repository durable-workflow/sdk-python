from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, serializer, workflow
from durable_workflow._prepared_local_activity import PreparedAttempt, PreparedCancellationObserved
from durable_workflow.client import Client
from durable_workflow.errors import WorkflowCancelled
from durable_workflow.worker import Worker
from durable_workflow.workflow import LocalActivityExecutionAborted, replay
from tests.test_activity_process import blocked_without_python_progress, wait_for_exit, wait_for_file, wait_for_release
from tests.test_cancellation_context import delivery, request, snapshot
from tests.test_replay_regression_corpus import PreparedLocalColdResultsWorkflow
from tests.test_worker import compatible_cluster_info


@workflow.defn(name="prepared.sequential")
class SequentialWorkflow:
    def run(self, ctx: workflow.WorkflowContext, marker: str, prefix: bool = False):  # type: ignore[no-untyped-def]
        if prefix:
            yield ctx.upsert_memo({"before": "local"})
        return (yield ctx.local_activity("prepared.callback", [marker]))


@workflow.defn(name="prepared.cleanup")
class CleanupWorkflow:
    def run(self, ctx: workflow.WorkflowContext, shield: bool = True):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_timer(60)
        except WorkflowCancelled:
            if shield:
                with ctx.cancellation_shield():
                    yield ctx.local_activity("prepared.callback", [])
            else:
                yield ctx.local_activity("prepared.callback", [])
            return "cleaned"


@workflow.defn(name="prepared.group")
class GroupWorkflow:
    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield [ctx.local_activity("prepared.callback", []), ctx.start_timer(1)])


@activity.defn(name="prepared.callback")
def typed_callback(marker: str) -> dict[str, Any]:
    Path(marker).write_text(str(os.getpid()))
    return {"value": b"\x00\xff", "attempt": activity.context().info.activity_attempt_id}


def timestamp(seconds: float = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec="microseconds").replace(
        "+00:00", "Z",
    )


def admission(**changes: Any) -> dict[str, Any]:
    return {
        "prepared": True, "duplicate": False, "reason": None,
        "workflow_task_id": "task", "workflow_task_attempt": 3, "lease_owner": "owner",
        "worker_attempt_id": "nonce", "activity_execution_id": "execution", "activity_attempt_id": "attempt",
        "attempt_number": 1, "server_time": timestamp(), "lease_expires_at": timestamp(30),
        "start_to_close_deadline_at": None, "schedule_to_close_deadline_at": None, "heartbeat_deadline_at": None,
        "cancellation_cleanup": None, **changes,
    }


def admitted(receipt: dict[str, Any], **changes: Any) -> PreparedAttempt:
    return PreparedAttempt.admitted(
        receipt, task_id="task", run_id="run-1", owner="owner", epoch=3, nonce="nonce",
        heartbeat_timeout=None, cleanup=None, request_started=time.monotonic(), **changes,
    )


def control(receipt: dict[str, Any], **changes: Any) -> dict[str, Any]:
    return {
        **receipt, "active": True, "renewed": True, "stop_required": False,
        "workflow_lease_expires_at": receipt["lease_expires_at"],
        "heartbeat_recorded": False, "heartbeat_history_event_id": None, **changes,
    }


@pytest.mark.parametrize("changes", [
    {"prepared": False}, {"duplicate": 1}, {"reason": "refused"}, {"workflow_task_id": "other"},
    {"workflow_task_attempt": True}, {"workflow_task_attempt": 4}, {"lease_owner": "other"},
    {"worker_attempt_id": "other"}, {"attempt_number": True}, {"attempt_number": 0},
    {"activity_attempt_id": ""}, {"activity_execution_id": " "}, {"server_time": "2026-02-30T00:00:00Z"},
    {"lease_expires_at": "2026-01-01T00:00:00Z"}, {"heartbeat_deadline_at": "2027-01-01T00:00:00Z"},
    {"cancellation_cleanup": {"request_id": "invented"}},
])
def test_admission_refuses_changed_identity_and_invented_authority(changes: dict[str, Any]) -> None:
    with pytest.raises(LocalActivityExecutionAborted):
        admitted(admission(**changes))


@pytest.mark.parametrize("field", ["reason", "start_to_close_deadline_at", "schedule_to_close_deadline_at",
                                        "heartbeat_deadline_at"])
def test_admission_requires_explicit_receipt_fields(field: str) -> None:
    receipt = admission()
    del receipt[field]
    with pytest.raises(LocalActivityExecutionAborted):
        admitted(receipt)


@pytest.mark.parametrize("changes", [
    {"active": 1}, {"renewed": False}, {"stop_required": True}, {"workflow_task_attempt": True},
    {"activity_attempt_id": "other"}, {"lease_owner": "other"}, {"reason": "refused"},
    {"heartbeat_recorded": True, "heartbeat_history_event_id": "invented-progress"},
    {"heartbeat_deadline_at": "2027-01-01T00:00:00Z"},
    {"start_to_close_deadline_at": "2027-01-01T00:00:00Z"},
    {"workflow_lease_expires_at": "2026-01-01T00:00:00Z"},
    {"cancellation_cleanup": {"request_id": "invented"}},
])
def test_supervisor_control_cannot_change_authority_or_report_application_progress(changes: dict[str, Any]) -> None:
    receipt = admission()
    attempt = admitted(receipt)
    with pytest.raises(LocalActivityExecutionAborted):
        attempt.validate_control(control(receipt, **changes))


def test_application_heartbeat_alone_advances_its_deadline() -> None:
    receipt = admission(heartbeat_deadline_at=timestamp(9))
    attempt = PreparedAttempt.admitted(
        receipt, task_id="task", run_id="run-1", owner="owner", epoch=3, nonce="nonce",
        heartbeat_timeout=10, cleanup=None, request_started=time.monotonic(),
    )
    refreshed = control(receipt, renewed=False, heartbeat_recorded=True, heartbeat_history_event_id="heartbeat",
                        server_time=timestamp(1), heartbeat_deadline_at=timestamp(10))
    attempt.validate_control(refreshed, heartbeat=True)
    assert attempt.deadlines["heartbeat_deadline_at"] == refreshed["heartbeat_deadline_at"]
    attempt.validate_control(control(refreshed, renewed=True, heartbeat_recorded=False,
                                     heartbeat_history_event_id=None))


def test_transport_elapsed_time_cannot_restart_an_authority_budget() -> None:
    receipt = admission(lease_expires_at=timestamp(2))
    with pytest.raises(LocalActivityExecutionAborted, match="budget expired"):
        PreparedAttempt.admitted(
            receipt, task_id="task", run_id="run-1", owner="owner", epoch=3, nonce="nonce",
            heartbeat_timeout=None, cleanup=None, request_started=time.monotonic() - 3,
        )


@pytest.mark.parametrize("field", ["request_id", "root_request_id", "delivery_history_event_id", "cleanup_deadline_at"])
def test_cleanup_admission_and_control_cannot_replace_original_cascade_authority(field: str) -> None:
    cleanup = {"request_id": "request-1", "root_request_id": "root-1", "delivery_history_event_id": "delivery-1",
               "cleanup_deadline_at": timestamp(20)}
    receipt = admission(cancellation_cleanup=cleanup, heartbeat_deadline_at=cleanup["cleanup_deadline_at"],
                        start_to_close_deadline_at=cleanup["cleanup_deadline_at"],
                        schedule_to_close_deadline_at=cleanup["cleanup_deadline_at"])
    attempt = PreparedAttempt.admitted(
        receipt, task_id="task", run_id="run-1", owner="owner", epoch=3, nonce="nonce",
        heartbeat_timeout=None, cleanup=cleanup, request_started=time.monotonic(),
    )
    changed = {**cleanup, field: timestamp(30) if field == "cleanup_deadline_at" else "replacement"}
    with pytest.raises(LocalActivityExecutionAborted, match="cleanup authority"):
        attempt.validate_control(control(receipt, cancellation_cleanup=changed))
    with pytest.raises(LocalActivityExecutionAborted, match="cleanup authority"):
        PreparedAttempt.admitted(
            {**receipt, "cancellation_cleanup": changed}, task_id="task", run_id="run-1", owner="owner",
            epoch=3, nonce="nonce", heartbeat_timeout=None, cleanup=cleanup, request_started=time.monotonic(),
        )


def test_replay_captures_a_fresh_local_call_before_invoking_its_executor() -> None:
    outcome = replay(SequentialWorkflow, [], ["unused", True], prepare_local_activities=True,
                     local_activity_executor=lambda _: pytest.fail("callback ran before admission"))
    assert [type(command).__name__ for command in outcome.commands] == ["UpsertMemo"]
    call = outcome.prepared_local_activity
    assert call is not None and call.sequence == 2 and call.recover is False and call.cleanup is None
    descriptor = call.descriptor("avro")
    assert "outcome" not in descriptor and "result" not in descriptor
    assert serializer.decode_envelope(descriptor["arguments"]) == ["unused"]


@pytest.mark.parametrize("last_event,recover", [("ActivityStarted", True), ("ActivityRetryScheduled", False)])
def test_cold_replay_distinguishes_unknown_started_callback_from_durable_retry(last_event: str, recover: bool) -> None:
    history = [{"event_type": "ActivityScheduled", "payload": {
        "sequence": 1, "activity_type": "prepared.callback", "execution_mode": "local",
    }}, {"event_type": last_event, "payload": {
        "sequence": 1, "activity_type": "prepared.callback", "execution_mode": "local",
    }}]
    outcome = replay(SequentialWorkflow, history, ["unused"], prepare_local_activities=True)
    assert outcome.commands == []
    assert outcome.prepared_local_activity is not None
    assert outcome.prepared_local_activity.sequence == 1 and outcome.prepared_local_activity.recover is recover


def test_immutable_cold_results_skip_completed_callback_and_recover_unfinished_original_attempt() -> None:
    fixture = json.loads((
        Path(__file__).parent / "fixtures/replay_regressions/prepared-local-cold-results.json"
    ).read_text())
    history = fixture["history"]
    outcome = replay(PreparedLocalColdResultsWorkflow, history[:-1], [], prepare_local_activities=True,
                     local_activity_executor=lambda _: pytest.fail("unfinished Started callback was reexecuted"))
    call = outcome.prepared_local_activity
    assert call is not None and call.sequence == 2 and call.recover is True
    assert call.command.activity_type == "prepared.second" and outcome.commands == []
    completed = replay(PreparedLocalColdResultsWorkflow, history, [], prepare_local_activities=True,
                       local_activity_executor=lambda _: pytest.fail("completed callback was reexecuted"))
    assert completed.prepared_local_activity is None
    assert completed.commands[0].result == ["fast-value", "fast-value"]  # type: ignore[union-attr]


def test_cleanup_capture_preserves_the_original_delivery_root_and_deadline() -> None:
    marker = {**delivery(), "id": "canonical-delivery"}
    original = [request(), marker]
    outcome = replay(CleanupWorkflow, original, [], run_id="run-1", prepare_local_activities=True)
    call = outcome.prepared_local_activity
    assert call is not None and call.sequence == 2
    assert call.cleanup == {
        "request_id": "request-1", "root_request_id": "root-1", "delivery_history_event_id": "canonical-delivery",
        "cleanup_deadline_at": snapshot()["cleanup_deadline_at"],
    }
    assert call.descriptor("avro")["cancellation_cleanup"] == {
        "request_id": "request-1", "delivery_history_event_id": "canonical-delivery",
    }
    original[1]["id"] = ""
    with pytest.raises(LocalActivityExecutionAborted, match="canonical cancellation delivery"):
        replay(CleanupWorkflow, original, [], run_id="run-1", prepare_local_activities=True)
    with pytest.raises(LocalActivityExecutionAborted, match="shield"):
        replay(CleanupWorkflow, [request(), marker], [False], run_id="run-1", prepare_local_activities=True)


def test_sequential_consumer_refuses_a_local_parallel_group_before_any_callback() -> None:
    with pytest.raises(LocalActivityExecutionAborted, match="atomic group consumer"):
        replay(GroupWorkflow, [], [], prepare_local_activities=True,
               local_activity_executor=lambda _: pytest.fail("group callback ran before complete admission"))


class PreparedServer:
    def __init__(self) -> None:
        self.client = AsyncMock(spec=Client)
        self.history: list[dict[str, Any]] = []
        self.trace: list[str] = []
        self.receipt: dict[str, Any] = {}
        self.stop = False
        self.bad_admission = False
        self.omit_outcome_history = False
        self.fail_outcome = False
        self.marker: Path | None = None
        self.client.get_cluster_info.return_value = compatible_cluster_info(worker_protocol={
            "version": "1.20", "server_capabilities": {
                "prepared_local_activities": True, "cooperative_cancellation": True,
                "workflow_memo_updates": {"supported": True},
                "supported_workflow_task_commands": ["upsert_memo"],
            },
        })
        self.client.register_worker.return_value = {"registered": True}
        self.client.prepared_local_activity_operation.side_effect = self.operation
        self.client.workflow_task_history.side_effect = self.page
        self.client.complete_workflow_task.return_value = {"outcome": "completed"}

    async def page(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs == {"task_id": "task", "lease_owner": "owner", "workflow_task_attempt": 3,
                          "next_history_page_token": "canonical-page"}
        self.trace.append("history")
        return {"history_events": deepcopy(self.history), "next_history_page_token": None}

    async def operation(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["task_id"] == "task" and kwargs["lease_owner"] == "owner"
        assert kwargs["workflow_task_attempt"] == 3
        name = kwargs["operation"]
        self.trace.append(name)
        body = kwargs["body"]
        if name == "prepare":
            assert "outcome" not in body["descriptor"]
            self.receipt = admission(worker_attempt_id=body["worker_attempt_id"])
            if self.bad_admission:
                self.receipt["activity_attempt_id"] = ""
            payload = {"sequence": body["sequence"], "activity_type": "prepared.callback", "execution_mode": "local",
                       "activity_execution_id": "execution", "activity_attempt_id": "attempt"}
            self.history.extend([{"id": "scheduled", "event_type": "ActivityScheduled", "payload": payload},
                                 {"id": "started", "event_type": "ActivityStarted", "payload": payload}])
            return self.receipt
        assert kwargs["activity_attempt_id"] == "attempt"
        if name == "control":
            assert body == {"renew_lease": True}
            if not self.stop:
                return control(self.receipt)
            context = snapshot()
            context.update(requested_at=timestamp(), cleanup_deadline_at=timestamp(30))
            return control(self.receipt, active=False, stop_required=True, renewed=False,
                           reason="cancellation_requested", fenced=True, cancellation_request=context,
                           cancellation_history_event_id="cancelled", history_refresh_page_token="canonical-page")
        if name == "acknowledge-cancellation":
            assert body == {"request_id": "request-1"}
            assert self.marker is not None
            pid = int(self.marker.read_text())
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
            return {"acknowledged": True, "duplicate": False, "reason": None, "history_event_id": "joined"}
        assert name == "outcome"
        if self.fail_outcome:
            raise TimeoutError("outcome acknowledgment lost")
        if not self.omit_outcome_history:
            payload = {**self.history[-1]["payload"], "result": body["report"]["result"]}
            self.history.append({"id": "completed", "event_type": "ActivityCompleted", "payload": payload})
        return {**self.receipt, "recorded": True, "event_id": "completed", "event_type": "ActivityCompleted",
                "workflow_run_id": "run-1", "recorded_at": timestamp(), "claim_released": False,
                "created_task_ids": [], "history_refresh_page_token": "canonical-page"}

    async def worker(self, monkeypatch: pytest.MonkeyPatch, *, handler: Any = typed_callback) -> Worker:
        monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
        worker = Worker(self.client, task_queue="queue", worker_id="owner", workflows=[SequentialWorkflow],
                        capabilities=["cooperative_cancellation", "prepared_local_activities"])
        await worker._register()
        worker.activities["prepared.callback"] = handler
        return worker

    def task(self, marker: Path) -> dict[str, Any]:
        self.marker = marker
        return {"task_id": "task", "workflow_type": "prepared.sequential", "workflow_id": "workflow-1",
                "run_id": "run-1", "workflow_task_attempt": 3, "payload_codec": "avro",
                "arguments": serializer.envelope([str(marker)]), "history_events": deepcopy(self.history)}


async def test_worker_runs_only_an_admitted_process_then_replays_canonical_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    server = PreparedServer()
    worker = await server.worker(monkeypatch)
    commands = await worker._run_workflow_task(server.task(tmp_path / "callback"))
    assert commands is not None and [command["type"] for command in commands] == ["complete_workflow"]
    assert serializer.decode_envelope(commands[0]["result"]) == {"value": b"\x00\xff", "attempt": "attempt"}
    assert server.trace == ["prepare", "control", "control", "outcome", "history"]
    assert not worker._prepared_local_activity_processes
    server.client.fail_workflow_task.assert_not_awaited()
    await wait_for_exit(int((tmp_path / "callback").read_text()))


async def test_bad_admission_never_spawns_and_never_completes_or_fails_a_claim(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    server = PreparedServer()
    server.bad_admission = True
    worker = await server.worker(monkeypatch)
    assert await worker._run_workflow_task(server.task(tmp_path / "callback")) is None
    assert not (tmp_path / "callback").exists()
    assert server.trace == ["prepare"]
    server.client.complete_workflow_task.assert_not_awaited()
    server.client.fail_workflow_task.assert_not_awaited()


@pytest.mark.parametrize("failure", ["fail_outcome", "omit_outcome_history"])
async def test_lost_or_noncanonical_outcome_abandons_without_reexecuting_callback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str,
) -> None:
    server = PreparedServer()
    setattr(server, failure, True)
    worker = await server.worker(monkeypatch)
    assert await worker._run_workflow_task(server.task(tmp_path / "callback")) is None
    assert server.trace.count("prepare") == 1 and server.trace.count("outcome") == 1
    server.client.complete_workflow_task.assert_not_awaited()
    server.client.fail_workflow_task.assert_not_awaited()
    assert not worker._prepared_local_activity_processes


async def test_control_stops_and_joins_a_gil_blocked_local_callback_without_application_heartbeats(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    server = PreparedServer()
    worker = await server.worker(monkeypatch, handler=blocked_without_python_progress)
    marker = tmp_path / "callback"
    task = server.task(marker)
    outcome = replay(SequentialWorkflow, [], [str(marker)], prepare_local_activities=True)
    pending = asyncio.create_task(worker._execute_prepared_local_activity(task, [], outcome))
    try:
        await wait_for_file(marker)
        pid = int(marker.read_text())
        started = time.monotonic()
        server.stop = True
        with pytest.raises(PreparedCancellationObserved):
            await asyncio.wait_for(pending, timeout=7.0)
        assert time.monotonic() - started < 4.0
        await wait_for_exit(pid)
        assert server.trace.count("acknowledge-cancellation") == 1
        assert "heartbeat" not in server.trace and "outcome" not in server.trace
        assert task["_prepared_cancellation_context"]["request_id"] == "request-1"
        assert not worker._prepared_local_activity_processes
        server.client.complete_workflow_task.assert_not_awaited()
        server.client.fail_workflow_task.assert_not_awaited()
    finally:
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


async def test_prepared_capability_requires_actual_bridge_and_never_advertises_group_support(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = PreparedServer()
    worker = await server.worker(monkeypatch)
    manifest = server.client.register_worker.await_args.kwargs["capability_manifest"]
    assert manifest["prepared_local_activities"]["implementation"] == "durable_sequential_admission"
    assert "prepared_local_activity_groups" not in manifest
    info = server.client.get_cluster_info.return_value
    del info["worker_protocol"]["server_capabilities"]["prepared_local_activities"]
    with pytest.raises(RuntimeError, match="installed admission bridge"):
        await worker._register()
    with pytest.raises(ValueError, match="no prepared local group consumer"):
        Worker(server.client, task_queue="queue", capabilities=["prepared_local_activity_groups"])


async def test_descendant_stop_keeps_local_observation_time_and_canonical_root_time_distinct(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    server = PreparedServer()
    worker = await server.worker(monkeypatch)
    task = server.task(tmp_path / "unused")
    root = snapshot()
    observed = {"request_id": root["request_id"], "requested_at": "2026-10-01T00:00:05Z",
                "cleanup_deadline_at": root["cleanup_deadline_at"], "history_refresh_page_token": "canonical-page"}
    server.client.heartbeat_workflow_task.return_value = {
        "task_id": "task", "lease_owner": "owner", "workflow_task_attempt": 3, "renewed": True,
        "cancellation_request": observed,
    }

    async def stopped(*_: Any) -> list[dict[str, Any]]:
        task["_prepared_cancellation_context"] = root
        server.history = [request()]
        raise PreparedCancellationObserved("original callback joined")

    async def delivered(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["call_kind"] == "local_activity"
        server.history.append({**delivery(), "payload": {**delivery()["payload"], "call_kind": "local_activity"}})
        return {"delivered": True}

    worker._execute_prepared_local_activity = stopped  # type: ignore[method-assign]
    server.client.deliver_workflow_cancellation.side_effect = delivered
    outcome, _ = await worker._replay_workflow_claim(
        SequentialWorkflow, task, [], ["unused"], payload_codec="avro",
        execute_local=lambda _: pytest.fail("callback reexecuted after joined stop"),
    )
    assert outcome.prepared_local_activity is None and len(outcome.commands) == 1
    assert task["cancellation_request"] == observed
    assert task["cancellation_request"]["requested_at"] != root["requested_at"]
    assert server.client.deliver_workflow_cancellation.await_count == 1


async def test_lost_prepared_supervisor_retains_workflow_capacity_and_never_reports_stop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    server = PreparedServer()
    worker = await server.worker(monkeypatch, handler=wait_for_release)
    worker.max_concurrent_workflow_tasks = 1
    worker._wf_semaphore = asyncio.Semaphore(1)
    marker = tmp_path / "callback"
    await worker._reserve_workflow_capacity()
    pending = worker._admit_workflow_work(server.task(marker), "workflow")
    await wait_for_file(marker)
    callback = next(iter(worker._prepared_local_activity_processes))
    pid = int(marker.read_text())
    try:
        callback._supervisor.kill()
        assert await asyncio.wait_for(pending, timeout=4.0) is None
        os.kill(pid, 0)
        assert callback.stopped is False
        assert worker._current_task_slots()["workflow_available"] == 0 and worker._workflow_reserved == 1
        assert "acknowledge-cancellation" not in server.trace and "outcome" not in server.trace
        server.client.complete_workflow_task.assert_not_awaited()
        server.client.fail_workflow_task.assert_not_awaited()
        with pytest.raises(RuntimeError, match="unconfirmed prepared local callback stop.*registration remains active"):
            await worker._shutdown()
        server.client.deregister_worker_registration.assert_not_awaited()
    finally:
        Path(str(marker) + ".release").touch()
        await wait_for_exit(pid)
        shutil.rmtree(callback.directory, ignore_errors=True)
        worker._prepared_local_activity_processes.discard(callback)
        worker._abandoned_prepared_local_claims.discard("task")
        worker._release_workflow_capacity()
