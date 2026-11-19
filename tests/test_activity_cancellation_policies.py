from __future__ import annotations

from typing import Any

import pytest

from durable_workflow import CancellationPolicy, ParentClosePolicy, serializer
from durable_workflow import workflow as workflow_module
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.worker import Worker
from durable_workflow.workflow import (
    CompleteWorkflow,
    ScheduleActivity,
    WorkflowContext,
    commands_to_server_commands,
    replay,
)
from tests.test_cooperative_cancellation import marker, request
from tests.test_cooperative_cancellation_worker import ClaimServer, claimed_task


def workflow(options: dict[str, Any], mode: str = "sequential") -> type:
    class PolicyWorkflow:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            first = ctx.schedule_activity("work", [], **options)
            try:
                if mode == "parallel":
                    return (yield [first])
                if mode == "selection":
                    return (yield ctx.select({"work": first}))
                return (yield first)
            except WorkflowCancelled as cancelled:
                return cancelled.request_id

    return PolicyWorkflow


def event(kind: str, policy: Any = None) -> dict[str, Any]:
    activity = {"type": "work"}
    if policy is not None:
        activity["cancellation_policy"] = policy
    return {"event_type": kind, "payload": {"sequence": 1, "activity": activity}}


def test_changed_activity_policy_is_rejected_during_replay() -> None:
    with pytest.raises(NonDeterministicReplayError, match="activity_cancellation_policy_changed"):
        replay(workflow({}), [event("ActivityScheduled", "wait_cancellation_completed")], [])


@pytest.mark.parametrize("policy", list(CancellationPolicy))
def test_typed_and_string_policies_match_both_encoders_and_replay(policy: CancellationPolicy) -> None:
    command = ScheduleActivity("work", [], schedule_to_close_timeout=60, cancellation_policy=policy)
    direct = command.to_server_command("queue")
    assert direct == commands_to_server_commands([command], "queue")[0]
    assert direct == ScheduleActivity(
        "work", [], schedule_to_close_timeout=60, cancellation_policy=policy.value,
    ).to_server_command("queue")
    assert type(direct["cancellation_policy"]) is str
    history = [event("ActivityScheduled", policy.value), event("ActivityStarted"), event("ActivityCompleted")]
    history[-1]["payload"]["result"] = serializer.envelope("recorded")
    assert replay(workflow({"cancellation_policy": policy, "schedule_to_close_timeout": 60}), history, []).commands == [
        CompleteWorkflow("recorded"),
    ]


def test_omitted_policy_preserves_wire_and_historical_try_default() -> None:
    assert "cancellation_policy" not in ScheduleActivity("work", []).to_server_command("queue")
    history = [event("ActivityCompleted")]
    history[0]["payload"]["result"] = serializer.envelope("recorded")
    assert replay(workflow({}), history, []).commands == [CompleteWorkflow("recorded")]
    assert replay(workflow({"cancellation_policy": CancellationPolicy.TRY_CANCEL}), history, []).commands == [
        CompleteWorkflow("recorded"),
    ]


@pytest.mark.parametrize("mode", ["sequential", "parallel", "selection"])
@pytest.mark.parametrize("delivered", [False, True])
def test_changed_policy_fails_group_matching_and_cancellation_delivery(mode: str, delivered: bool) -> None:
    options = {"cancellation_policy": CancellationPolicy.WAIT_CANCELLATION_COMPLETED}
    commands = commands_to_server_commands(replay(workflow(options, mode), [], []).commands, "queue")
    history = [{"event_type": "ActivityScheduled", "payload": {"sequence": 1, **command}} for command in commands]
    if delivered:
        history += [request(), marker(1, "activity" if mode == "sequential" else "parallel", sequence_span=1)]
        assert replay(workflow(options, mode), history, [], run_id="run-1").commands == [CompleteWorkflow("request-1")]
    with pytest.raises(NonDeterministicReplayError, match="activity_cancellation_policy_changed"):
        replay(workflow({"cancellation_policy": CancellationPolicy.TRY_CANCEL}, mode), history, [], run_id="run-1")


@pytest.mark.parametrize("value", ["unknown", "", True, 1, [], ParentClosePolicy.ABANDON])
def test_invalid_policy_is_refused_before_suspension(value: Any) -> None:
    with pytest.raises(ValueError, match="cancellation_policy"):
        ScheduleActivity("work", [], cancellation_policy=value)


@pytest.mark.parametrize("timeout", [None, 0, -1, 1.5, "60", True])
def test_abandon_requires_finite_total_timeout(timeout: Any) -> None:
    with pytest.raises(ValueError, match="schedule_to_close_timeout"):
        ScheduleActivity("work", [], cancellation_policy=CancellationPolicy.ABANDON, schedule_to_close_timeout=timeout)


@pytest.mark.parametrize("kind", ["ActivityScheduled", "ActivityCompleted"])
@pytest.mark.parametrize("value", [None, "unknown", []])
def test_malformed_canonical_history_is_rejected(kind: str, value: Any) -> None:
    item = event(kind)
    item["payload"]["activity"]["cancellation_policy"] = value
    with pytest.raises(NonDeterministicReplayError, match="invalid_activity_cancellation_policy_history"):
        replay(workflow({}), [item], [])


def test_conflicting_history_is_rejected() -> None:
    with pytest.raises(NonDeterministicReplayError, match="activity_cancellation_policy_history_conflict"):
        replay(workflow({}), [event("ActivityScheduled", "try_cancel"), event("ActivityStarted", "abandon")], [])


@pytest.mark.parametrize("protocol", ["1.19", "1.20"])
@pytest.mark.parametrize("policy", list(CancellationPolicy))
async def test_worker_without_opt_in_refuses_explicit_policy(
    monkeypatch: pytest.MonkeyPatch, protocol: str, policy: CancellationPolicy,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", protocol)
    cls = workflow_module.defn(name="activity-policy-worker")(workflow({
        "cancellation_policy": policy, "schedule_to_close_timeout": 60,
    }))
    server = ClaimServer(history=[])
    worker = Worker(server.client, task_queue="queue", worker_id="cooperative-worker", workflows=[cls])
    await worker._register()
    result = await worker._run_workflow_task({
        **claimed_task(observed=False), "workflow_type": "activity-policy-worker", "arguments": serializer.envelope([]),
    })
    assert result is None
    server.client.complete_workflow_task.assert_not_awaited()
    failure = server.client.fail_workflow_task.await_args.kwargs
    assert failure["failure_type"] == "RuntimeCapabilityUnsupported"
    assert "activity_cancellation_policy_not_supported" in failure["message"]
    assert "cooperative-worker" in failure["message"]
    assert "1.20" in failure["message"]
