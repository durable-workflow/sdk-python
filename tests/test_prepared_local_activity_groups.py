from __future__ import annotations

import asyncio
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import activity, serializer, workflow
from durable_workflow._prepared_local_activity import PreparedCancellationObserved
from durable_workflow.client import Client
from durable_workflow.errors import NonDeterministicReplayError, ServerError, WorkflowCancelled
from durable_workflow.worker import Worker
from durable_workflow.workflow import LocalActivityExecutionAborted, replay
from tests.test_activity_process import blocked_without_python_progress, wait_for_exit, wait_for_file
from tests.test_cancellation_context import delivery, request, snapshot
from tests.test_prepared_local_activity_worker import admission, control, timestamp
from tests.test_replay_regression_corpus import PreparedLocalGroupColdResultsWorkflow
from tests.test_worker import compatible_cluster_info


@workflow.defn(name="prepared.two-locals")
class TwoLocals:
    def run(self, ctx: workflow.WorkflowContext, marker: str, prefix: bool = False):  # type: ignore[no-untyped-def]
        if prefix:
            yield ctx.upsert_memo({"before": "group"})
        return (yield [ctx.local_activity("prepared.group", [marker + ".0", marker + ".1"]),
                      ctx.local_activity("prepared.group", [marker + ".1", marker + ".0"])])


@workflow.defn(name="prepared.mixed-group")
class MixedGroup:
    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield [ctx.start_child_workflow("python.child", [], task_queue="child"),
                      [ctx.schedule_activity("remote", []), ctx.local_activity("prepared.group", [])]])


@workflow.defn(name="prepared.group-cleanup")
class GroupCleanup:
    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_timer(60)
        except WorkflowCancelled:
            with ctx.cancellation_shield():
                yield [ctx.local_activity("prepared.group", []), ctx.local_activity("prepared.group", [])]
            return "cleaned"


@activity.defn(name="prepared.group")
async def concurrent_callback(marker: str, peer: str) -> dict[str, Any]:
    Path(marker).write_text(str(os.getpid()))
    for _ in range(200):
        if Path(peer).exists():
            return {"bytes": b"\x00\xff", "attempt": activity.context().info.activity_attempt_id}
        await asyncio.sleep(0.01)
    raise RuntimeError("parallel sibling did not start")


def capture(cls: type = TwoLocals, history: list[dict[str, Any]] | None = None, inputs: list[Any] | None = None):  # type: ignore[no-untyped-def]
    return replay(cls, history or [], ["unused"] if inputs is None else inputs,
                  prepare_local_activities=True, prepare_local_activity_groups=True, run_id="run-1",
                  local_activity_executor=lambda _: pytest.fail("callback ran without group admission"))


def local_event(kind: str, sequence: int, *, base: int = 1, **extra: Any) -> dict[str, Any]:
    metadata = {"parallel_group_id": f"parallel-activities:{base}:2", "parallel_group_kind": "activity",
                "parallel_group_base_sequence": base, "parallel_group_size": 2,
                "parallel_group_index": sequence - base}
    return {"id": kind + str(sequence), "event_type": kind, "payload": {
        "sequence": sequence, "activity_type": "prepared.group", "execution_mode": "local",
        "activity_execution_id": f"execution-{sequence}", "activity_attempt_id": f"attempt-{sequence}",
        **metadata, "parallel_group_path": [metadata], **extra,
    }}


def test_complete_nested_mixed_batch_is_captured_without_inventing_local_outcomes() -> None:
    group = capture(MixedGroup, inputs=[]).prepared_local_activity_group
    assert group is not None and not group.committed and group.base_sequence == 1 and group.size == 3
    assert [call.sequence for call in group.calls] == [3]
    descriptor = group.calls[0].descriptor("avro")
    assert "outcome" not in descriptor and "result" not in descriptor and "queue" not in descriptor
    assert len(descriptor["parallel_group_path"]) == 2
    assert descriptor["parallel_group_path"][0]["parallel_group_index"] == 2
    assert descriptor["parallel_group_path"][1]["parallel_group_index"] == 1
    assert group.commands[0].task_queue == "child"  # type: ignore[union-attr]


def test_group_prefix_is_checkpointed_separately_and_preserves_authored_sequences() -> None:
    outcome = capture(inputs=["unused", True])
    assert [type(command).__name__ for command in outcome.commands] == ["UpsertMemo"]
    assert outcome.prepared_local_activity_group is not None
    assert outcome.prepared_local_activity_group.base_sequence == 2
    assert [call.sequence for call in outcome.prepared_local_activity_group.calls] == [2, 3]


@pytest.mark.parametrize("history", [
    [local_event("ActivityScheduled", 1)],
    [local_event("ActivityStarted", 1), local_event("ActivityStarted", 2)],
    [{**local_event("ActivityScheduled", 1), "payload": {
        **local_event("ActivityScheduled", 1)["payload"], "parallel_group_path": []}},
     local_event("ActivityScheduled", 2)],
    [local_event("ActivityScheduled", 1, base=2), local_event("ActivityScheduled", 2, base=2)],
])
def test_partial_or_changed_group_history_cannot_authorize_any_callback(history: list[dict[str, Any]]) -> None:
    with pytest.raises(NonDeterministicReplayError):
        capture(history=history)


def test_partial_cold_replay_skips_a_completed_member_and_recovers_only_the_started_sibling() -> None:
    history = [local_event("ActivityScheduled", 1), local_event("ActivityScheduled", 2),
               local_event("ActivityCompleted", 1, result=serializer.envelope("first")),
               local_event("ActivityStarted", 2)]
    group = capture(history=history).prepared_local_activity_group
    assert group is not None and group.committed and group.commands == ()
    assert [call.sequence for call in group.calls] == [2] and group.calls[0].recover is True
    history.append(local_event("ActivityRetryScheduled", 2))
    assert capture(history=history).prepared_local_activity_group.calls[0].recover is False
    history.append(local_event("ActivityCompleted", 2, result=serializer.envelope("second")))
    outcome = capture(history=history)
    assert outcome.prepared_local_activity_group is None and outcome.commands[0].result == ["first", "second"]


def test_reverse_completion_order_replays_results_at_the_authored_positions() -> None:
    history = [local_event("ActivityScheduled", 1), local_event("ActivityScheduled", 2),
               local_event("ActivityCompleted", 2, result=serializer.envelope("second")),
               local_event("ActivityCompleted", 1, result=serializer.envelope("first"))]
    assert capture(history=history).commands[0].result == ["first", "second"]


def test_cleanup_group_preserves_original_root_delivery_and_deadline_for_every_member() -> None:
    history = [request(), {**delivery(), "id": "canonical-delivery"}]
    group = capture(GroupCleanup, history, []).prepared_local_activity_group
    assert group is not None and group.base_sequence == 2
    for call in group.calls:
        assert call.cleanup == {"request_id": "request-1", "root_request_id": "root-1",
                                "delivery_history_event_id": "canonical-delivery",
                                "cleanup_deadline_at": snapshot()["cleanup_deadline_at"]}
    assert group.calls[0].cleanup == group.calls[1].cleanup


@pytest.mark.parametrize("selection", [False, True])
def test_unsupported_group_shapes_are_refused_before_callbacks(selection: bool) -> None:
    @workflow.defn(name="prepared.unsupported")
    class Unsupported:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            local = ctx.local_activity("prepared.group", [])
            yield ctx.select([local]) if selection else [local] * 101

    with pytest.raises(LocalActivityExecutionAborted):
        capture(Unsupported, inputs=[])


class GroupServer:
    def __init__(self, markers: tuple[Path, Path]) -> None:
        self.client = AsyncMock(spec=Client)
        self.history: list[dict[str, Any]] = []
        self.trace: list[tuple[str, Any]] = []
        self.attempts: dict[str, dict[str, Any]] = {}
        self.markers = markers
        self.stop = False
        self.bad_checkpoint = False
        self.bad_second_admission = False
        self.partial_checkpoint_history = False
        self.lost_outcome = False
        self.admission_cancelled = False
        self.omit_second_outcome = False
        self.client.get_cluster_info.return_value = compatible_cluster_info(worker_protocol={
            "version": "1.20", "server_capabilities": {
                "prepared_local_activities": True, "prepared_local_activity_groups": True,
                "cooperative_cancellation": True,
            },
        })
        self.client.register_worker.return_value = {"registered": True}
        self.client.complete_workflow_task.return_value = {"outcome": "completed"}
        self.client.prepared_local_activity_operation.side_effect = self.operation
        self.client.workflow_task_history.side_effect = self.page

    async def page(self, **_: Any) -> dict[str, Any]:
        self.trace.append(("history", None))
        return {"history_events": deepcopy(self.history), "next_history_page_token": None}

    async def operation(self, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["task_id"] == "task" and kwargs["lease_owner"] == "owner"
        assert kwargs["workflow_task_attempt"] == 3
        name = kwargs["operation"]
        body = kwargs["body"]
        attempt_id = kwargs.get("activity_attempt_id")
        self.trace.append((name, body.get("sequence", attempt_id)))
        if name == "checkpoint-group":
            assert all(not path.exists() for path in self.markers)
            assert [command["type"] for command in body["commands"]] == ["prepare_local_activity"] * 2
            self.history = [local_event("ActivityScheduled", 1), local_event("ActivityScheduled", 2)]
            if self.partial_checkpoint_history:
                self.history.pop()
            locals_ = [{"sequence": index, "activity_execution_id": f"execution-{index}"} for index in (1, 2)]
            if self.bad_checkpoint:
                locals_[1]["activity_execution_id"] = "execution-1"
            return {"checkpointed": True, "duplicate": False, "reason": None,
                    "checkpoint_id": body["checkpoint_id"], "task_id": "task", "workflow_run_id": "run-1",
                    "workflow_task_attempt": 3, "lease_owner": "owner", "start_sequence": 1, "next_sequence": 3,
                    "local_activities": locals_, "history_refresh_page_token": "canonical-page"}
        if name == "prepare":
            assert all(not path.exists() for path in self.markers)
            sequence = body["sequence"]
            if sequence == 2 and self.admission_cancelled:
                self.stop = True
                raise ServerError(409, {"reason": "cancellation_requested"})
            receipt = admission(
                activity_execution_id=f"execution-{sequence}", activity_attempt_id=f"attempt-{sequence}",
                worker_attempt_id=body["worker_attempt_id"],
            )
            if sequence == 2 and self.bad_second_admission:
                receipt["activity_attempt_id"] = ""
            self.attempts[f"attempt-{sequence}"] = receipt
            self.history.append(local_event("ActivityStarted", sequence))
            return receipt
        assert isinstance(attempt_id, str) and attempt_id in self.attempts
        receipt = self.attempts[attempt_id]
        sequence = int(attempt_id[-1])
        if name == "control":
            assert body == {"renew_lease": True}
            if not self.stop:
                return control(receipt)
            context = {**snapshot(), "requested_at": timestamp(), "cleanup_deadline_at": timestamp(30)}
            # One immutable context for the entire group.
            if not hasattr(self, "context"):
                self.context = context
            return control(receipt, active=False, renewed=False, stop_required=True, reason="cancellation_requested",
                           fenced=True, cancellation_request=self.context, cancellation_history_event_id="cancelled",
                           history_refresh_page_token="canonical-page")
        if name == "acknowledge-cancellation":
            assert body == {"request_id": "request-1"}
            for marker in self.markers:
                # This member must have physically exited before its own ACK.
                if marker.exists() and marker == self.markers[sequence - 1]:
                    with pytest.raises(ProcessLookupError):
                        os.kill(int(marker.read_text()), 0)
            return {"acknowledged": True, "duplicate": False, "reason": None,
                    "history_event_id": "joined-" + attempt_id}
        assert name == "outcome"
        assert len(self.attempts) == 2
        if self.lost_outcome:
            raise TimeoutError("canonical acknowledgment lost")
        if sequence != 2 or not self.omit_second_outcome:
            self.history.append(local_event("ActivityCompleted", sequence, result=body["report"]["result"]))
        return {**receipt, "recorded": True, "event_id": "ActivityCompleted" + str(sequence),
                "event_type": "ActivityCompleted", "workflow_run_id": "run-1", "recorded_at": timestamp(),
                "claim_released": False, "created_task_ids": [], "history_refresh_page_token": "canonical-page"}

    async def worker(self, monkeypatch: pytest.MonkeyPatch, *, handler: Any = concurrent_callback) -> Worker:
        monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.20")
        worker = Worker(self.client, task_queue="queue", worker_id="owner", workflows=[TwoLocals],
                        capabilities=["cooperative_cancellation", "prepared_local_activities",
                                      "prepared_local_activity_groups"])
        await worker._register()
        worker.activities["prepared.group"] = handler
        return worker

    def task(self) -> dict[str, Any]:
        return {"task_id": "task", "workflow_type": "prepared.two-locals", "workflow_id": "workflow-1",
                "run_id": "run-1", "workflow_task_attempt": 3, "payload_codec": "avro",
                "arguments": serializer.envelope([str(self.markers[0])[:-2]]), "history_events": deepcopy(self.history)}


def server(tmp_path: Path) -> GroupServer:
    return GroupServer((tmp_path / "callback.0", tmp_path / "callback.1"))


async def test_atomic_batch_then_complete_admission_precedes_concurrent_callbacks_and_canonical_results(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    fixture = server(tmp_path)
    worker = await fixture.worker(monkeypatch)
    commands = await worker._run_workflow_task(fixture.task())
    assert commands is not None and [command["type"] for command in commands] == ["complete_workflow"]
    result = serializer.decode_envelope(commands[0]["result"])
    assert [item["attempt"] for item in result] == ["attempt-1", "attempt-2"]
    assert all(item["bytes"] == b"\x00\xff" for item in result)
    assert fixture.trace[:4] == [("checkpoint-group", None), ("history", None), ("prepare", 1), ("prepare", 2)]
    for marker in fixture.markers:
        await wait_for_exit(int(marker.read_text()))
    assert not worker._prepared_local_activity_processes
    fixture.client.fail_workflow_task.assert_not_awaited()


@pytest.mark.parametrize("failure", ["bad_checkpoint", "partial_checkpoint_history", "bad_second_admission"])
async def test_incomplete_admission_never_spawns_or_publishes_application_work(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str,
) -> None:
    fixture = server(tmp_path)
    setattr(fixture, failure, True)
    worker = await fixture.worker(monkeypatch)
    assert await worker._run_workflow_task(fixture.task()) is None
    assert all(not marker.exists() for marker in fixture.markers)
    assert not any(name == "outcome" for name, _ in fixture.trace)
    fixture.client.complete_workflow_task.assert_not_awaited()
    fixture.client.fail_workflow_task.assert_not_awaited()


@pytest.mark.parametrize("failure", ["lost_outcome", "omit_second_outcome"])
async def test_unknown_or_noncanonical_group_result_joins_siblings_and_never_reinvokes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str,
) -> None:
    fixture = server(tmp_path)
    setattr(fixture, failure, True)
    worker = await fixture.worker(monkeypatch)
    assert await worker._run_workflow_task(fixture.task()) is None
    assert sum(name == "prepare" for name, _ in fixture.trace) == 2
    for marker in fixture.markers:
        await wait_for_exit(int(marker.read_text()))
    assert not worker._prepared_local_activity_processes
    fixture.client.complete_workflow_task.assert_not_awaited()
    fixture.client.fail_workflow_task.assert_not_awaited()


async def test_cancellation_joins_both_blocked_callbacks_without_application_heartbeats_before_replay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    fixture = server(tmp_path)
    worker = await fixture.worker(monkeypatch, handler=blocked_without_python_progress)
    task = fixture.task()
    initial = capture(history=fixture.history, inputs=[str(tmp_path / "callback")])
    fixture.history = await worker._execute_prepared_local_activity_group(task, [], initial)
    outcome = capture(history=fixture.history, inputs=[str(tmp_path / "callback")])
    # The blocked fixture accepts one marker, with no app heartbeat.
    for call in outcome.prepared_local_activity_group.calls:
        call.command.arguments = call.command.arguments[:1]
    pending = asyncio.create_task(worker._execute_prepared_local_activity_group(task, fixture.history, outcome))
    try:
        for marker in fixture.markers:
            await wait_for_file(marker)
        fixture.stop = True
        with pytest.raises(PreparedCancellationObserved):
            await asyncio.wait_for(pending, timeout=8)
        for marker in fixture.markers:
            await wait_for_exit(int(marker.read_text()))
        assert sum(name == "acknowledge-cancellation" for name, _ in fixture.trace) == 2
        assert not any(name in {"heartbeat", "outcome"} for name, _ in fixture.trace)
        assert not worker._prepared_local_activity_processes
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


async def test_cancel_during_second_admission_acks_unstarted_first_attempt_without_spawning(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    fixture = server(tmp_path)
    worker = await fixture.worker(monkeypatch)
    task = fixture.task()
    fixture.history = await worker._execute_prepared_local_activity_group(task, [], capture())
    fixture.admission_cancelled = True
    fixture.client.heartbeat_workflow_task.return_value = {
        "task_id": "task", "lease_owner": "owner", "workflow_task_attempt": 3, "renewed": True,
        "cancellation_request": {"request_id": "request-1", "requested_at": timestamp(),
                                 "cleanup_deadline_at": timestamp(30), "history_refresh_page_token": "canonical-page"},
    }
    with pytest.raises(LocalActivityExecutionAborted):
        await worker._execute_prepared_local_activity_group(task, fixture.history, capture(history=fixture.history))
    assert all(not marker.exists() for marker in fixture.markers)
    assert sum(name == "acknowledge-cancellation" for name, _ in fixture.trace) == 1


async def test_group_capability_is_explicit_and_requires_the_installed_atomic_bridge(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    fixture = server(tmp_path)
    worker = await fixture.worker(monkeypatch)
    manifest = fixture.client.register_worker.await_args.kwargs["capability_manifest"]
    assert manifest["prepared_local_activity_groups"]["implementation"] == "durable_atomic_all_admission"
    capabilities = fixture.client.get_cluster_info.return_value["worker_protocol"]["server_capabilities"]
    del capabilities["prepared_local_activity_groups"]
    with pytest.raises(RuntimeError, match="installed atomic bridge"):
        await worker._register()


def test_immutable_atomic_group_fixture_recovers_only_the_unfinished_original_sibling() -> None:
    fixture = json.loads((Path(__file__).parent / "fixtures/replay_regressions/prepared-local-group-cold-results.json")
                         .read_text())
    outcome = capture(PreparedLocalGroupColdResultsWorkflow, fixture["history"][:-1], [])
    group = outcome.prepared_local_activity_group
    assert group is not None and group.committed and [call.sequence for call in group.calls] == [2]
    assert group.calls[0].recover is True and group.commands == ()
    completed = capture(PreparedLocalGroupColdResultsWorkflow, fixture["history"], [])
    assert completed.prepared_local_activity_group is None
    assert completed.commands[0].result == ["fast-value", "fast-value"]
