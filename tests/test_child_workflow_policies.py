from __future__ import annotations

from typing import Any

import pytest

from durable_workflow import CancellationPolicy, ParentClosePolicy, serializer
from durable_workflow import workflow as workflow_module
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.worker import Worker
from durable_workflow.workflow import (
    CompleteWorkflow,
    StartChildWorkflow,
    WorkflowContext,
    commands_to_server_commands,
    replay,
)
from tests.test_cooperative_cancellation import marker, request
from tests.test_cooperative_cancellation_worker import ClaimServer, claimed_task


def options() -> dict[str, Any]:
    return {
        "parent_close_policy": ParentClosePolicy.REQUEST_CANCELLATION,
        "cancellation_policy": CancellationPolicy.WAIT_CANCELLATION_COMPLETED,
    }


def workflow(mode: str, policies: dict[str, Any]) -> type:
    class PolicyWorkflow:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            first = ctx.start_child_workflow("child", ["argument"], **policies)
            try:
                if mode == "parallel":
                    return (yield [first])
                if mode == "selection":
                    return (yield ctx.select({
                        "first": first,
                        "second": ctx.start_child_workflow("other-child", [], **policies),
                    }))
                return (yield first)
            except WorkflowCancelled as cancelled:
                return cancelled.request_id

    return PolicyWorkflow


def scheduled(commands: list[Any]) -> list[dict[str, Any]]:
    return [
        {"event_type": "ChildWorkflowScheduled", "payload": {
            "sequence": index + 1, "child_workflow_type": command["workflow_type"],
            "child_workflow_run_id": f"child-run-{index + 1}", **command,
        }}
        for index, command in enumerate(commands_to_server_commands(commands, "queue"))
    ]


@pytest.mark.parametrize("parent", list(ParentClosePolicy))
@pytest.mark.parametrize("operation", list(CancellationPolicy))
def test_typed_and_string_policies_round_trip_through_both_encoders_and_cold_replay(
    parent: ParentClosePolicy, operation: CancellationPolicy,
) -> None:
    child = StartChildWorkflow("child", ["argument"], parent_close_policy=parent, cancellation_policy=operation)
    direct = child.to_server_command("queue")
    assert direct == commands_to_server_commands([child], "queue")[0]
    assert direct == StartChildWorkflow(
        "child", ["argument"], parent_close_policy=parent.value, cancellation_policy=operation.value,
    ).to_server_command("queue")
    assert type(direct["parent_close_policy"]) is str
    assert type(direct["cancellation_policy"]) is str
    assert serializer.decode_envelope(direct["arguments"]) == ["argument"]
    history = scheduled([child]) + [{"event_type": "ChildRunCompleted", "payload": {
        "sequence": 1, "child_workflow_type": "child", "result": serializer.envelope("recorded-result"),
    }}]
    result = replay(workflow("sequential", {
        "parent_close_policy": parent, "cancellation_policy": operation,
    }), history, [])
    assert result.commands == [CompleteWorkflow("recorded-result")]


@pytest.mark.parametrize("field", ["parent_close_policy", "cancellation_policy"])
@pytest.mark.parametrize("value", ["unknown", "", True, 1, [], {"type": "abandon"}])
def test_invalid_options_are_rejected_before_scheduling(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=field):
        StartChildWorkflow("child", **{field: value})


def test_policy_enum_types_are_not_interchangeable() -> None:
    with pytest.raises(ValueError, match="parent_close_policy"):
        StartChildWorkflow("child", parent_close_policy=CancellationPolicy.ABANDON)
    with pytest.raises(ValueError, match="cancellation_policy"):
        StartChildWorkflow("child", cancellation_policy=ParentClosePolicy.ABANDON)


def test_existing_positional_constructor_and_omitted_wire_fields_are_preserved() -> None:
    child = StartChildWorkflow("child", ["argument"], "queue", "request_cancel", None, 60, 30)
    assert child.execution_timeout_seconds == 60
    assert child.run_timeout_seconds == 30
    assert child.cancellation_policy is None
    assert "cancellation_policy" not in child.to_server_command("queue")
    omitted = StartChildWorkflow("child").to_server_command("queue")
    assert "parent_close_policy" not in omitted
    assert "cancellation_policy" not in omitted
    result = replay(workflow("sequential", {
        "parent_close_policy": ParentClosePolicy.ABANDON, "cancellation_policy": CancellationPolicy.ABANDON,
    }), scheduled([StartChildWorkflow("child", ["argument"])]), [])
    # Omitted historical fields retain their defaults, without starting another child.
    assert result.commands == []


@pytest.mark.parametrize("mode", ["sequential", "parallel", "selection"])
@pytest.mark.parametrize("field", ["parent_close_policy", "cancellation_policy"])
@pytest.mark.parametrize("delivered", [False, True])
def test_changed_policies_fail_cold_replay_and_cancellation_delivery(mode: str, field: str, delivered: bool) -> None:
    policies = options()
    history = scheduled(replay(workflow(mode, policies), [], []).commands)
    if delivered:
        history += [request(), marker(1, "child" if mode == "sequential" else "parallel", sequence_span=len(history))]
        assert replay(workflow(mode, policies), history, [], run_id="run-1").commands == [CompleteWorkflow("request-1")]
    else:
        cold = replay(workflow(mode, policies), history, [])
        if mode in ("sequential", "selection"):
            assert cold.commands == []
        else:
            assert scheduled(cold.commands) == history
    policies[field] = "abandon"
    with pytest.raises(NonDeterministicReplayError, match="child_workflow_policy_changed") as captured:
        replay(workflow(mode, policies), history, [], run_id="run-1")
    assert captured.value.workflow_sequence == 1


@pytest.mark.parametrize("mode", ["sequential", "parallel", "selection"])
@pytest.mark.parametrize("field", ["parent_close_policy", "cancellation_policy"])
def test_historical_defaults_cannot_change_to_cooperative_policies(mode: str, field: str) -> None:
    history = scheduled(replay(workflow(mode, {}), [], []).commands)
    with pytest.raises(NonDeterministicReplayError, match="child_workflow_policy_changed"):
        replay(workflow(mode, {field: options()[field]}), history, [])


@pytest.mark.parametrize("event", [
    "ChildRunStarted", "ChildRunCompleted", "ChildRunFailed", "ChildRunCancelled", "ChildRunTerminated",
])
@pytest.mark.parametrize("policy,reason", [
    ("try_cancel", "child_workflow_policy_history_conflict"),
    ({"type": "try_cancel"}, "invalid_child_workflow_policy_history"),
])
def test_invalid_and_conflicting_child_history_is_rejected(event: str, policy: Any, reason: str) -> None:
    history = scheduled(replay(workflow("sequential", options()), [], []).commands)
    history.append({"event_type": event, "payload": {"sequence": 1, "cancellation_policy": policy}})
    with pytest.raises(NonDeterministicReplayError, match=reason):
        replay(workflow("sequential", options()), history, [])


def test_later_start_and_terminal_events_preserve_the_scheduled_snapshot() -> None:
    history = scheduled(replay(workflow("sequential", options()), [], []).commands)
    history += [
        {"event_type": "ChildRunStarted", "payload": {"sequence": 1, "child_workflow_type": "child"}},
        {"event_type": "ChildRunCompleted", "payload": {"sequence": 1, "result": serializer.envelope("done")}},
    ]
    assert replay(workflow("sequential", options()), history, []).commands == [CompleteWorkflow("done")]


def test_selection_handle_keeps_the_loser_policy_after_the_winner_and_during_cancellation() -> None:
    def authoring(second_policy: CancellationPolicy) -> type:
        class AwaitSecond:
            def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
                selected = yield ctx.select({
                    "first": ctx.start_child_workflow("child", ["argument"], **options()),
                    "second": ctx.start_child_workflow("other-child", [], **{
                        **options(), "cancellation_policy": second_policy,
                    }),
                })
                try:
                    return (yield selected.handles["second"].await_result())
                except WorkflowCancelled as cancelled:
                    return cancelled.request_id

        return AwaitSecond

    original = authoring(CancellationPolicy.WAIT_CANCELLATION_COMPLETED)
    history = scheduled(replay(original, [], [], run_id="run-1").commands)
    history += [
        {"id": "child-completed", "event_type": "ChildRunCompleted", "payload": {
            **history[0]["payload"], "result": serializer.envelope("first-result"),
        }},
        {"event_type": "SelectionResolved", "payload": {
            "selection_group_id": "select-calls:1:2", "selection_group_base_sequence": 1, "selection_group_size": 2,
            "member_key": "first", "member_index": 0, "member_base_sequence": 1, "member_size": 1,
            "operation_kind": "child", "operation_identity": "child-run-1", "outcome": "completed",
            "resolution_event_id": "child-completed", "resolution_event_type": "ChildRunCompleted",
        }},
    ]
    assert replay(original, history, [], run_id="run-1").commands == []
    for delivered in (False, True):
        current = list(history)
        if delivered:
            current += [request(), marker(3, "selection_handle", operation_sequence=2, operation_sequence_span=1)]
            assert replay(original, current, [], run_id="run-1").commands == [CompleteWorkflow("request-1")]
        with pytest.raises(NonDeterministicReplayError, match="child_workflow_policy_changed") as captured:
            replay(authoring(CancellationPolicy.TRY_CANCEL), current, [], run_id="run-1")
        assert captured.value.workflow_sequence == 2


@pytest.mark.parametrize("protocol", ["1.19", "1.20"])
@pytest.mark.parametrize("policies", [
    {"parent_close_policy": ParentClosePolicy.REQUEST_CANCELLATION},
    {"cancellation_policy": CancellationPolicy.TRY_CANCEL},
    {"cancellation_policy": CancellationPolicy.WAIT_CANCELLATION_COMPLETED},
])
async def test_worker_without_opt_in_refuses_cooperative_child_commands(
    monkeypatch: pytest.MonkeyPatch, protocol: str, policies: dict[str, Any],
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", protocol)
    cls = workflow_module.defn(name="child-policy-worker")(workflow("sequential", policies))
    server = ClaimServer(history=[])
    worker = Worker(server.client, task_queue="queue", worker_id="cooperative-worker", workflows=[cls])
    await worker._register()
    result = await worker._run_workflow_task({
        **claimed_task(observed=False), "workflow_type": "child-policy-worker", "arguments": serializer.envelope([]),
    })
    assert result is None
    server.client.complete_workflow_task.assert_not_awaited()
    failure = server.client.fail_workflow_task.await_args.kwargs
    assert failure["failure_type"] == "RuntimeCapabilityUnsupported"
    assert "child_cancellation_policy_not_supported" in failure["message"]
    assert "cooperative-worker" in failure["message"]
    assert "1.20" in failure["message"]


@pytest.mark.parametrize("parent", [
    ParentClosePolicy.ABANDON, ParentClosePolicy.REQUEST_CANCEL, ParentClosePolicy.TERMINATE,
])
async def test_legacy_child_policies_remain_available_without_cooperation(
    monkeypatch: pytest.MonkeyPatch, parent: ParentClosePolicy,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.19")
    cls = workflow_module.defn(name="child-policy-worker")(workflow("sequential", {
        "parent_close_policy": parent, "cancellation_policy": CancellationPolicy.ABANDON,
    }))
    server = ClaimServer(history=[])
    worker = Worker(server.client, task_queue="queue", worker_id="cooperative-worker", workflows=[cls])
    await worker._register()
    result = await worker._run_workflow_task({
        **claimed_task(observed=False), "workflow_type": "child-policy-worker", "arguments": serializer.envelope([]),
    })
    assert result is not None and result[0]["parent_close_policy"] == parent.value
    server.client.fail_workflow_task.assert_not_awaited()


async def test_capable_worker_transmits_both_typed_child_policies(monkeypatch: pytest.MonkeyPatch) -> None:
    cls = workflow_module.defn(name="child-policy-worker")(workflow("sequential", options()))
    server = ClaimServer(history=[])
    worker = await server.worker(monkeypatch, workflows=[cls])
    result = await worker._run_workflow_task({
        **claimed_task(observed=False), "workflow_type": "child-policy-worker", "arguments": serializer.envelope([]),
    })
    assert result is not None
    assert result[0]["parent_close_policy"] == "request_cancellation"
    assert result[0]["cancellation_policy"] == "wait_cancellation_completed"
    server.client.fail_workflow_task.assert_not_awaited()
