from __future__ import annotations

from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import serializer, workflow
from durable_workflow._cancellation_scope import CancellationScopeHistory, CancellationScopeOpenReceipt
from durable_workflow.client import Client
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.worker import Worker
from durable_workflow.workflow import LocalActivityExecutionAborted
from tests.test_cooperative_cancellation import request


def event(kind: str, payload: dict[str, Any], index: int) -> dict[str, Any]:
    return {"id": f"event-{index}", "sequence": index, "namespace": "tenant", "event_type": kind, "payload": payload}


def started() -> dict[str, Any]:
    return event("WorkflowStarted", {}, 1)


def opening(sequence: int = 1, scope_id: str = "scope-one", parent: str = "root",
            shield: bool = False, index: int = 2) -> dict[str, Any]:
    return event("CancellationScopeOpened", {
        "schema": "durable-workflow.cancellation-scope/v1", "workflow_run_id": "run-one", "sequence": sequence,
        "scope_id": scope_id, "parent_scope_id": parent, "shield_parent": shield,
    }, index)


def run(cls: type, history: list[dict[str, Any]]) -> workflow.ReplayOutcome:
    return workflow.replay(cls, history, [], run_id="run-one", payload_codec="avro",
                           allow_cancellation_scope_authoring=True)


def test_body_waits_for_original_opening_and_default_remains_disabled() -> None:
    calls: list[str] = []

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def body() -> str:
                calls.append("body")
                return "done"
            return (yield from ctx.cancellation_scope(body))

    with pytest.raises(LocalActivityExecutionAborted, match="candidate authoring is disabled"):
        workflow.replay(Probe, [started()], [], run_id="run-one")
    outcome = run(Probe, [started()])
    assert outcome.commands == []
    assert outcome.cancellation_scope_opening == workflow.CancellationScopeOpening(1, "root", False)
    assert calls == []
    for _ in range(2):
        outcome = run(Probe, [started(), opening()])
        assert outcome.cancellation_scope_opening is None
        assert outcome.commands[0].result == "done"
    assert calls == ["body", "body"]


@pytest.mark.parametrize("factory", [
    lambda ctx: workflow.ScheduleActivity("remote", []),
    lambda ctx: ctx.local_activity("local", []),
    lambda ctx: workflow.StartTimer(1),
    lambda ctx: ctx.start_child_workflow("child"),
    lambda ctx: workflow.WaitCondition(lambda: False, condition_key="waiting"),
])
def test_deferred_operations_retain_creation_membership_and_root_omits_wire_field(factory: Any) -> None:
    commands: list[Any] = []

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            scoped = yield from ctx.cancellation_scope(lambda: factory(ctx))
            root = factory(ctx)
            commands.extend([scoped, root])
            yield scoped

    run(Probe, [started(), opening()])
    scoped, root = commands
    assert scoped._cancellation_scope_id == "scope-one"
    assert root._cancellation_scope_id == "root"
    if isinstance(scoped, workflow.RecordLocalActivity):
        descriptor = workflow.PreparedLocalActivityCall(scoped, 2).descriptor("avro")
        assert descriptor["cancellation_scope_id"] == "scope-one"
    else:
        wire = workflow.commands_to_server_commands(commands, "queue", size_warning=None)
        assert wire[0]["cancellation_scope_id"] == "scope-one"
        assert "cancellation_scope_id" not in wire[1]


def test_nested_shielding_reuses_original_ids_and_restores_parent_after_body_failure() -> None:
    commands: list[workflow.StartTimer] = []

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def inner() -> None:
                commands.append(ctx.start_timer(1))
                raise ValueError("body failure")

            def outer():  # type: ignore[no-untyped-def]
                try:
                    yield from ctx.cancellation_scope(inner, shield_parent=True)
                except ValueError:
                    commands.append(ctx.start_timer(2))
                return "outer"

            result = yield from ctx.cancellation_scope(outer)
            commands.append(ctx.start_timer(3))
            return result

    history = [started(), opening()]
    outcome = run(Probe, history)
    assert outcome.cancellation_scope_opening == workflow.CancellationScopeOpening(2, "scope-one", True)
    assert commands == []
    history.append(opening(2, "scope-two", "scope-one", True, 3))
    outcome = run(Probe, history)
    assert outcome.commands[0].result == "outer"
    assert [command._cancellation_scope_id for command in commands] == ["scope-two", "scope-one", "root"]


def test_metadata_prefix_uses_original_authored_sequence_without_entering_scope_body() -> None:
    calls: list[str] = []

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.side_effect(lambda: "value")
            return (yield from ctx.cancellation_scope(lambda: calls.append("body")))

    outcome = run(Probe, [started()])
    assert len(outcome.commands) == 1
    assert outcome.cancellation_scope_opening == workflow.CancellationScopeOpening(2, "root", False)
    assert calls == []
    history = [started(), event("SideEffectRecorded", {
        "sequence": 1, "result": serializer.envelope("value", codec="avro"),
    }, 2), opening(2, index=3)]
    outcome = run(Probe, history)
    assert [type(command).__name__ for command in outcome.commands] == ["CompleteWorkflow"]
    assert calls == ["body"]


def test_parallel_copy_keeps_scope_membership_and_original_sequence_after_opening() -> None:
    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def body():  # type: ignore[no-untyped-def]
                return (yield [ctx.start_timer(1), ctx.schedule_activity("remote", [])])
            return (yield from ctx.cancellation_scope(body))

    outcome = run(Probe, [started(), opening()])
    wire = workflow.commands_to_server_commands(outcome.commands, "queue", size_warning=None)
    assert len(wire) == 2
    assert [command["cancellation_scope_id"] for command in wire] == ["scope-one", "scope-one"]
    assert [command["parallel_group_base_sequence"] for command in wire] == [2, 2]
    assert [command["parallel_group_index"] for command in wire] == [0, 1]


@pytest.mark.parametrize("change", [{"parent_scope_id": "scope-other"}, {"shield_parent": True}, {"sequence": 2}])
def test_changed_opening_cannot_enter_body(change: dict[str, Any]) -> None:
    calls: list[str] = []

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            return (yield from ctx.cancellation_scope(lambda: calls.append("body")))

    row = opening()
    row["payload"].update(change)
    with pytest.raises((LocalActivityExecutionAborted, NonDeterministicReplayError)):
        run(Probe, [started(), row])
    assert calls == []


@pytest.mark.parametrize("change", [
    {"scope_id": "root"}, {"scope_id": ""}, {"scope_id": True}, {"scope_id": "\ud800"},
    {"scope_id": "é" * 128}, {"parent_scope_id": "missing"}, {"parent_scope_id": "scope-one"},
    {"workflow_run_id": "foreign"}, {"schema": "foreign"}, {"sequence": True}, {"sequence": 0},
    {"sequence": 1 << 63}, {"shield_parent": 1},
])
def test_invalid_opening_tree_refuses_before_application_factory(change: dict[str, Any]) -> None:
    calls: list[str] = []

    class Probe:
        def __init__(self) -> None:
            calls.append("factory")
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.start_timer(1)

    row = opening()
    row["payload"].update(change)
    with pytest.raises(LocalActivityExecutionAborted, match="invalid_cancellation_scope_history"):
        run(Probe, [started(), row])
    assert calls == []


@pytest.mark.parametrize("mutation", [
    "namespace", "id", "order", "missing_start", "duplicate_scope", "duplicate_sequence",
])
def test_invalid_canonical_history_cannot_supply_a_scope(mutation: str) -> None:
    history = [started(), opening()]
    if mutation == "namespace":
        history[1]["namespace"] = "foreign"
    elif mutation == "id":
        history[1]["id"] = history[0]["id"]
    elif mutation == "order":
        history.reverse()
    elif mutation == "missing_start":
        history = history[1:]
    elif mutation == "duplicate_scope":
        history.append(opening(2, index=3))
    else:
        history.append(opening(1, "scope-two", index=3))
    with pytest.raises(ValueError, match="invalid_cancellation_scope_history"):
        CancellationScopeHistory.read(history, "run-one")


@pytest.mark.parametrize("payload", [
    {"sequence": 2, "cancellation_scope_id": "missing"},
    {"sequence": 1, "cancellation_scope_id": "scope-one"},
    {"sequence": True, "cancellation_scope_id": "scope-one"},
    {"sequence": 2, "cancellation_scope_id": None},
    {"sequence": 2, "cancellation_scope_id": "scope-one", "timer": {"cancellation_scope_id": "root"}},
])
def test_operation_requires_prior_scope_and_consistent_immediate_membership(payload: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        CancellationScopeHistory.read([started(), opening(), event("TimerScheduled", payload, 3)], "run-one")


@pytest.mark.parametrize("terminal_scope", ["root", "scope-other", None])
def test_operation_cannot_change_scope_between_admission_and_completion(terminal_scope: Any) -> None:
    payload = {"sequence": 2, "cancellation_scope_id": "scope-one"}
    history = [started(), opening(), event("TimerScheduled", payload, 3)]
    terminal = {**payload, "cancellation_scope_id": terminal_scope}
    with pytest.raises(ValueError):
        CancellationScopeHistory.read(history + [event("TimerFired", terminal, 4)], "run-one")


@pytest.mark.parametrize("root_command", [False, True])
def test_timer_replay_checks_original_membership_and_consumes_scope_as_authored_call(root_command: bool) -> None:
    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            timer = yield from ctx.cancellation_scope(lambda: ctx.start_timer(1))
            yield ctx.start_timer(1) if root_command else timer
            return "done"

    history = [started(), opening(), event("TimerScheduled", {
        "sequence": 2, "duration_seconds": 1, "cancellation_scope_id": "scope-one",
    }, 3), event("TimerFired", {"sequence": 2}, 4)]
    if root_command:
        with pytest.raises(NonDeterministicReplayError, match="cancellation_scope_membership_changed"):
            run(Probe, history)
    else:
        assert run(Probe, history).commands[0].result == "done"


@pytest.mark.parametrize("kind", [
    "CancellationScopeRequested", "CancellationScopeDeliveryPrepared", "CancellationScopeDelivered",
    "CancellationScopeRequestConflicted",
])
def test_candidate_authoring_still_refuses_unimplemented_scope_delivery_before_factory(kind: str) -> None:
    class Probe:
        def __init__(self) -> None:
            pytest.fail("unqualified cancellation cannot enter application code")

    with pytest.raises(LocalActivityExecutionAborted, match="cancellation_scope_execution_not_supported"):
        run(Probe, [started(), opening(), event(kind, {}, 3)])


async def test_worker_replays_proved_opening_on_same_claim_before_body() -> None:
    calls: list[str] = []

    @workflow.defn(name="candidate-scope-worker")
    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            return (yield from ctx.cancellation_scope(lambda: calls.append("body") or "done"))

    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="queue", worker_id="original", workflows=[Probe])
    worker._allow_cancellation_scope_authoring = True
    worker._cooperative_cancellation_supported = True
    canonical = [started(), opening()]
    client.open_cancellation_scope_on_claim.return_value = CancellationScopeOpenReceipt(
        "scope-one", "event-2", 1, "root", False, False, tuple(deepcopy(canonical)),
    )
    task = {"task_id": "task-one", "run_id": "run-one", "workflow_task_attempt": 4}
    outcome, history = await worker._replay_workflow_claim(
        Probe, task, [started()], [], payload_codec="avro", execute_local=lambda _: pytest.fail("no local callback"),
    )
    assert calls == ["body"]
    assert outcome.commands[0].result == "done"
    assert history == canonical
    client.open_cancellation_scope_on_claim.assert_awaited_once_with(
        task_id="task-one", run_id="run-one", lease_owner="original", workflow_task_attempt=4,
        sequence=1, parent_scope_id="root", shield_parent=False,
    )
    client.complete_workflow_task.assert_not_awaited()
    client.heartbeat_workflow_task.assert_not_awaited()


async def test_worker_commits_prefix_before_opening_and_does_not_fabricate_body_authority() -> None:
    calls: list[str] = []

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.side_effect(lambda: "prefix")
            return (yield from ctx.cancellation_scope(lambda: calls.append("body")))

    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="queue", worker_id="original")
    worker._allow_cancellation_scope_authoring = True
    worker._cooperative_cancellation_supported = True
    outcome, _ = await worker._replay_workflow_claim(
        Probe, {"task_id": "task-one", "run_id": "run-one", "workflow_task_attempt": 4}, [started()], [],
        payload_codec="avro", execute_local=lambda _: pytest.fail("no local callback"),
    )
    assert len(outcome.commands) == 1
    assert outcome.cancellation_scope_opening.sequence == 2
    assert calls == []
    client.open_cancellation_scope_on_claim.assert_not_awaited()


@pytest.mark.parametrize("kind", ["WorkflowCompleted", "WorkflowFailed", "WorkflowCancelled", "WorkflowTerminated"])
def test_closed_history_cannot_admit_an_unrecorded_scope(kind: str) -> None:
    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            return (yield from ctx.cancellation_scope(lambda: pytest.fail("no original opening")))

    with pytest.raises(NonDeterministicReplayError, match="cancellation_scope_opening_changed"):
        run(Probe, [started(), event(kind, {}, 2)])


def test_candidate_authoring_refuses_root_cancellation_until_scope_delivery_is_implemented() -> None:
    class Probe:
        def __init__(self) -> None:
            pytest.fail("unqualified cancellation cannot enter application code")

    row = request()
    row["payload"]["workflow_run_id"] = "run-one"
    row.update(id="request-event", sequence=3, namespace="tenant")
    with pytest.raises(LocalActivityExecutionAborted, match="candidate authoring lacks scope delivery"):
        run(Probe, [started(), opening(), row])


@pytest.mark.parametrize("root_command,sequence", [(False, 2), (True, 2), (False, 3)])
def test_condition_wait_requires_original_scope_and_authored_position(root_command: bool, sequence: int) -> None:
    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            wait = yield from ctx.cancellation_scope(
                lambda: workflow.WaitCondition(lambda: False, condition_key="wait"),
            )
            yield workflow.WaitCondition(lambda: False, condition_key="wait") if root_command else wait

    history = [started(), opening(), event("ConditionWaitOpened", {
        "sequence": sequence, "condition_wait_id": "wait-one", "condition_key": "wait",
        "cancellation_scope_id": "scope-one",
    }, 3)]
    if root_command or sequence != 2:
        with pytest.raises(NonDeterministicReplayError):
            run(Probe, history)
    else:
        outcome = run(Probe, history)
        assert len(outcome.commands) == 1
        assert outcome.commands[0]._cancellation_scope_id == "scope-one"


def test_changed_nested_parent_cannot_enter_nested_body() -> None:
    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def outer():  # type: ignore[no-untyped-def]
                return (yield from ctx.cancellation_scope(lambda: pytest.fail("nested parent changed")))
            return (yield from ctx.cancellation_scope(outer))

    with pytest.raises(NonDeterministicReplayError, match="cancellation_scope_opening_changed"):
        run(Probe, [started(), opening(), opening(2, "scope-two", "root", False, 3)])
