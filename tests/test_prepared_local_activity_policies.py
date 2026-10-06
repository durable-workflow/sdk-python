from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.cancellation import CancellationPolicy, ParentClosePolicy
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.worker import Worker
from durable_workflow.workflow import (
    CompleteUpdate,
    LocalActivityExecutionAborted,
    RecordLocalActivity,
    apply_update,
    query_state,
    replay,
    validate_update,
)
from tests.test_cancellation_context import delivery, request
from tests.test_prepared_local_activity_groups import local_event
from tests.test_prepared_local_activity_worker import PreparedServer, SequentialWorkflow
from tests.test_updates import _update_accepted_event

POLICIES = ("try_cancel", "wait_cancellation_completed")


@workflow.defn(name="prepared.policy-group")
class PolicyGroupWorkflow:
    def run(self, ctx: Any, policy: str = "wait_cancellation_completed") -> Any:
        return (yield [ctx.local_activity("prepared.group", []),
                      ctx.local_activity("prepared.group", [], cancellation_policy=policy)])


@workflow.defn(name="prepared.policy")
class PolicyWorkflow:
    def run(self, ctx: Any, policy: Any = None, group: bool = False) -> Any:
        call = ctx.local_activity("prepared.callback", [], cancellation_policy=policy)
        self.result = None
        self.result = yield [ctx.local_activity("prepared.callback", []), [call]] if group else call
        return self.result

    @workflow.query(name="result")
    def result_query(self) -> Any:
        return self.result

    @workflow.update("inspect")
    def inspect(self) -> Any:
        return self.result

    @workflow.update_validator("inspect")
    def validate_inspect(self) -> Any:
        return self.result


def history(policy: str | None, *, complete: bool = False) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {"sequence": 1, "activity_type": "prepared.callback", "execution_mode": "local"}
    if policy is not None:
        payload["activity"] = {"cancellation_policy": policy}
    events = [{"event_type": kind, "payload": dict(payload)} for kind in ("ActivityScheduled", "ActivityStarted")]
    if complete:
        events.append({"event_type": "ActivityCompleted", "payload": {
            **payload, "result": serializer.envelope(b"completed"),
        }})
    return events


@pytest.mark.parametrize("policy", [*POLICIES, CancellationPolicy.TRY_CANCEL,
                                    CancellationPolicy.WAIT_CANCELLATION_COMPLETED])
def test_explicit_policy_is_preserved_before_admission(policy: Any) -> None:
    outcome = replay(PolicyWorkflow, [], [policy], prepare_local_activities=True,
                     local_activity_cancellation_policies=POLICIES,
                     local_activity_executor=lambda _: pytest.fail("callback ran before admission"))
    call = outcome.prepared_local_activity
    assert call is not None and not call.recover
    assert call.descriptor("avro")["cancellation_policy"] == policy
    assert "outcome" not in call.descriptor("avro")


@pytest.mark.parametrize("policy", [None, *POLICIES])
def test_cold_replay_retains_policy_and_skips_completed_callback(policy: str | None) -> None:
    options = {"prepare_local_activities": True, "local_activity_cancellation_policies": POLICIES}
    pending = replay(PolicyWorkflow, history(policy), [policy], **options)
    call = pending.prepared_local_activity
    assert call is not None and call.recover
    assert call.descriptor("avro").get("cancellation_policy") == policy
    completed = replay(PolicyWorkflow, history(policy, complete=True), [policy], **options)
    assert completed.prepared_local_activity is None
    assert completed.commands[0].result == b"completed"  # type: ignore[union-attr]
    assert query_state(PolicyWorkflow, history(policy, complete=True), [policy], "result", **options) == b"completed"


@pytest.mark.parametrize("recorded,authored", [
    (None, "wait_cancellation_completed"), ("wait_cancellation_completed", None),
    ("wait_cancellation_completed", "try_cancel"), ("try_cancel", "wait_cancellation_completed"),
])
@pytest.mark.parametrize("complete", [False, True])
def test_changed_policy_fails_pending_and_completed_replay(recorded: str | None, authored: str | None,
                                                         complete: bool) -> None:
    with pytest.raises(NonDeterministicReplayError, match="local_activity_cancellation_policy_changed"):
        replay(PolicyWorkflow, history(recorded, complete=complete), [authored], prepare_local_activities=True,
               local_activity_cancellation_policies=POLICIES)


@pytest.mark.parametrize("delivered", [False, True])
def test_changed_policy_is_rejected_before_request_or_committed_delivery(delivered: bool) -> None:
    events = [*history("wait_cancellation_completed"), request()]
    if delivered:
        marker = delivery()
        marker["payload"]["call_kind"] = "local_activity"
        events.append(marker)
    with pytest.raises(NonDeterministicReplayError, match="local_activity_cancellation_policy_changed"):
        replay(PolicyWorkflow, events, ["try_cancel"], run_id="run-1", prepare_local_activities=True,
               local_activity_cancellation_policies=POLICIES)


def test_atomic_group_recovery_preserves_policy_from_canonical_nested_snapshot() -> None:
    events = []
    for sequence in (1, 2):
        events.append(local_event("ActivityScheduled", sequence, activity={
            "cancellation_policy": "try_cancel" if sequence == 1 else "wait_cancellation_completed",
        }))
        events.append(local_event("ActivityStarted", sequence))
    options = {"prepare_local_activities": True, "prepare_local_activity_groups": True,
               "local_activity_cancellation_policies": POLICIES}
    outcome = replay(PolicyGroupWorkflow, events, [], **options)
    group = outcome.prepared_local_activity_group
    assert group is not None and group.committed and all(call.recover for call in group.calls)
    assert group.calls[1].descriptor("avro")["cancellation_policy"] == "wait_cancellation_completed"
    with pytest.raises(NonDeterministicReplayError, match="local_activity_cancellation_policy_changed"):
        replay(PolicyGroupWorkflow, events, ["try_cancel"], **options)


@pytest.mark.parametrize("policy", ["abandon", CancellationPolicy.ABANDON, "unknown", 1, False,
                                    ParentClosePolicy.ABANDON])
def test_local_abandon_and_malformed_authoring_are_refused(policy: Any) -> None:
    with pytest.raises(ValueError, match="local activity"):
        RecordLocalActivity("prepared.callback", [], cancellation_policy=policy)


@pytest.mark.parametrize("prepared,group", [(False, False), (False, True), (True, False), (True, True)])
def test_unsupported_policy_refuses_the_entire_call_before_callback_or_checkpoint(prepared: bool, group: bool) -> None:
    with pytest.raises(LocalActivityExecutionAborted, match="installed Server discovery"):
        replay(PolicyWorkflow, [], ["wait_cancellation_completed", group], prepare_local_activities=prepared,
               prepare_local_activity_groups=prepared,
               local_activity_executor=lambda _: pytest.fail("unsupported callback ran"))
    with pytest.raises(LocalActivityExecutionAborted, match="explicit policies require prepared admission"):
        RecordLocalActivity("prepared.callback", [], cancellation_policy="try_cancel").to_server_command("queue")


@pytest.mark.parametrize("advertised,accepted", [(None, ()), (True, ()), ("try_cancel", ()),
                                               (["abandon", "unknown"], ()),
                                               (["try_cancel"], ("try_cancel",)), (list(POLICIES), POLICIES)])
async def test_worker_registers_only_discovered_prepared_policies(monkeypatch: pytest.MonkeyPatch,
                                                               advertised: Any, accepted: tuple[str, ...]) -> None:
    server = PreparedServer()
    capabilities = server.client.get_cluster_info.return_value["worker_protocol"]["server_capabilities"]
    capabilities["prepared_local_activity_cancellation_policies"] = advertised
    worker = await server.worker(monkeypatch)
    assert worker._local_activity_cancellation_policies == accepted
    registration = server.client.register_worker.await_args.kwargs
    key = "prepared_local_activity_cancellation_policies"
    assert (key in registration["capabilities"]) is bool(accepted)
    assert (key in registration["capability_manifest"]) is bool(accepted)
    if not accepted:
        server.client.register_worker.reset_mock()
        worker.capabilities = (*worker.capabilities, key)
        with pytest.raises(RuntimeError, match="installed prepared policies"):
            await worker._register()
        server.client.register_worker.assert_not_awaited()


def test_explicit_worker_policy_capability_requires_prepared_admission() -> None:
    with pytest.raises(ValueError, match="require prepared_local_activities"):
        Worker(PreparedServer().client, task_queue="queue",
               capabilities=["prepared_local_activity_cancellation_policies"])


async def test_worker_refuses_undiscovered_policy_before_prefix_checkpoint_and_callback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    server = PreparedServer()
    worker = await server.worker(monkeypatch)
    marker = tmp_path / "callback"
    task = server.task(marker)
    task["arguments"] = serializer.envelope([str(marker), True, "wait_cancellation_completed"])
    assert await worker._run_workflow_task(task) is None
    assert not marker.exists() and server.trace == []
    server.client.prepared_local_activity_operation.assert_not_awaited()
    server.client.complete_workflow_task.assert_not_awaited()
    server.client.fail_workflow_task.assert_not_awaited()
    assert worker.workflows["prepared.sequential"] is SequentialWorkflow


@pytest.mark.parametrize("complete", [False, True])
def test_queries_updates_and_validators_replay_explicit_local_policy_without_running_callback(complete: bool) -> None:
    policy = "wait_cancellation_completed"
    events = history(policy, complete=complete)
    options = {"prepare_local_activities": True, "local_activity_cancellation_policies": POLICIES}
    expected = b"completed" if complete else None
    assert query_state(PolicyWorkflow, events, [policy], "result", **options) == expected
    assert validate_update(PolicyWorkflow, events, [policy], "inspect", [], **options) == expected
    updated = apply_update(PolicyWorkflow, [*events, _update_accepted_event("inspect-1", "inspect", [])],
                           [policy], "inspect-1", **options)
    assert isinstance(updated, CompleteUpdate) and updated.result == expected
