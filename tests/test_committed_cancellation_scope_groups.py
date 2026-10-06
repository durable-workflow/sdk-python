from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.cancellation import ScopedCancellationContext
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.workflow import LocalActivityExecutionAborted
from tests.test_committed_cancellation_scope_history import fixture
from tests.test_committed_cancellation_scope_replay import append_cleanup_event, run


@pytest.mark.parametrize("layout", ["flat", "nested"])
def test_timer_fence_does_not_move_the_prepared_group_boundary(layout: str) -> None:
    value = fixture("populated-scope-groups.json", layout)
    value["history"] = [row for row in value["history"] if row["event_type"] != "CancellationScopeDelivered"]
    original = next(row["payload"] for row in value["history"]
                    if row["event_type"] == "TimerScheduled" and row["payload"]["sequence"] == 5)
    append_cleanup_event(value, "TimerCancelled", deepcopy(original), "2026-10-04T00:00:09.123456Z")
    contexts: list[ScopedCancellationContext] = []
    result = run(probe(value, contexts), value)
    assert result.commands == [] and contexts == []
    assert result.cancellation_scope_delivery is not None
    assert result.cancellation_scope_delivery.boundary.sequence == 4
    assert result.cancellation_scope_delivery.boundary.sequence_span == 4


def probe(value: dict[str, Any], contexts: list[ScopedCancellationContext], *, change: str = "",
          satisfied: bool = False, cleanup: bool = False, expected_remaining: float = 21.0) -> type:
    original = next(row["payload"] for row in value["history"] if row["event_type"] == "ConditionWaitOpened")

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def inner():  # type: ignore[no-untyped-def]
                if change != "prefix":
                    assert (yield ctx.schedule_activity("prior-step", [])) == "prior-value"
                activity = ctx.schedule_activity(
                    "changed" if change == "activity-type" else "original-activity", [],
                    cancellation_policy="abandon" if change == "activity-policy" else "try_cancel",
                    schedule_to_close_timeout=60,
                )
                timer = ctx.start_timer(42 if change == "timer" else 3600)
                child = ctx.start_child_workflow(
                    "changed" if change == "child-type" else "original-child", [],
                    cancellation_policy="abandon" if change == "child-policy" else "wait_cancellation_completed",
                    parent_close_policy="abandon" if change == "parent-policy" else "request_cancel",
                )
                # The shared Native fixture has the original PHP predicate identity.
                # Supply that durable identity explicitly while probing portable replay.
                condition = workflow.WaitCondition(
                    predicate=lambda: satisfied,
                    condition_key="changed" if change == "condition-key" else "ready",
                    timeout_seconds=42 if change == "condition-timeout" else 30,
                    condition_definition_fingerprint=("changed" if change == "condition-predicate"
                                                      else original["condition_definition_fingerprint"]),
                )
                nested = (value["layout"] == "nested") != (change == "layout")
                members = [activity, [timer, child], condition] if nested else [activity, timer, child, condition]
                if change == "size":
                    members.pop()
                try:
                    if change == "shield":
                        with ctx.cancellation_shield():
                            yield members
                    else:
                        yield members
                    pytest.fail("committed cancellation must interrupt the original group")
                except WorkflowCancelled as error:
                    assert isinstance(error.context, ScopedCancellationContext)
                    assert error.context.remaining() == expected_remaining
                    contexts.append(error.context)
                    with ctx.cancellation_shield():
                        ctx.throw_if_cancellation_requested()
                        if cleanup:
                            yield ctx.start_timer(1)
                    return error.context.request_id

            def outer():  # type: ignore[no-untyped-def]
                return (yield from ctx.cancellation_scope(inner))

            request = yield from ctx.cancellation_scope(outer)
            assert not ctx.is_cancellation_requested and ctx.cancellation_context is None
            remaining = contexts[-1].remaining()
            result = yield ctx.schedule_activity("unaffected-root", [])
            return [result, request, remaining]

    return Probe


@pytest.mark.parametrize("layout", ["flat", "nested"])
@pytest.mark.parametrize("satisfied", [False, True])
def test_group_delivers_once_and_preserves_original_budget_and_parent_on_replacement(
    layout: str, satisfied: bool,
) -> None:
    value = fixture("populated-scope-groups.json", layout)
    contexts: list[ScopedCancellationContext] = []
    cls = probe(value, contexts, satisfied=satisfied)
    first = run(cls, value)
    wire = workflow.commands_to_server_commands(first.commands, "queue")
    assert len(wire) == 1 and wire[0]["activity_type"] == "unaffected-root"
    assert "cancellation_scope_id" not in wire[0]
    assert first.cancellation_scope_delivery is None
    original = next(row["payload"]["cancellation"] for row in value["history"]
                    if row["event_type"] == "CancellationScopeRequested")
    assert contexts[-1].to_dict() == original
    append_cleanup_event(value, "ActivityCompleted", {
        "sequence": 8, "activity_type": "unaffected-root",
        "result": serializer.envelope("survivor", codec="avro"),
    }, "2026-10-04T00:00:20.123456Z")
    value["task"].update({"workflow_task_attempt": 19, "lease_owner": "replacement"})
    expected = [workflow.CompleteWorkflow(["survivor", contexts[-1].request_id, 21.0])]
    assert run(cls, value).commands == expected
    assert run(cls, value).commands == expected
    assert len(contexts) == 3


@pytest.mark.parametrize("layout", ["flat", "nested"])
@pytest.mark.parametrize("prepared", [False, True])
def test_pending_group_preserves_original_range_without_executing_cleanup(layout: str, prepared: bool) -> None:
    value = fixture("populated-scope-groups.json", layout)
    value["history"] = [row for row in value["history"] if row["event_type"] != "CancellationScopeDelivered"
                        and (prepared or row["event_type"] != "CancellationScopeDeliveryPrepared")]
    contexts: list[ScopedCancellationContext] = []
    result = run(probe(value, contexts), value)
    assert result.commands == [] and contexts == []
    intent = result.cancellation_scope_delivery
    assert intent is not None
    assert intent.boundary.call_kind == "parallel"
    assert intent.boundary.sequence == 4 and intent.boundary.sequence_span == 4
    assert (intent.preparation is not None) is prepared


@pytest.mark.parametrize("change", [
    "activity-type", "activity-policy", "child-type", "child-policy", "parent-policy", "timer",
    "condition-key", "condition-timeout", "condition-predicate", "layout", "size", "shield", "prefix",
])
def test_changed_group_refuses_before_cleanup(change: str) -> None:
    value = fixture("populated-scope-groups.json", "nested")
    contexts: list[ScopedCancellationContext] = []
    with pytest.raises(NonDeterministicReplayError):
        run(probe(value, contexts, change=change), value)
    assert contexts == []


@pytest.mark.parametrize("layout", [
    "flat-local", "flat-selection", "flat-incomplete", "flat-signal", "flat-other-scope",
])
def test_unqualified_or_changed_group_refuses_before_factory(layout: str) -> None:
    value = fixture("populated-scope-groups.json", layout)
    entered: list[bool] = []

    class Probe:
        def __init__(self) -> None:
            entered.append(True)

        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.start_timer(1)

    with pytest.raises(LocalActivityExecutionAborted, match="scope"):
        run(Probe, deepcopy(value))
    assert entered == []


@pytest.mark.parametrize("layout", ["flat", "nested"])
@pytest.mark.parametrize("prepared", [False, True])
def test_completed_first_member_does_not_hide_pending_group_members(layout: str, prepared: bool) -> None:
    value = fixture("populated-scope-groups.json", layout)
    value["history"] = [row for row in value["history"] if row["event_type"] != "CancellationScopeDelivered"
                        and (prepared or row["event_type"] != "CancellationScopeDeliveryPrepared")]
    request_index = next(index for index, row in enumerate(value["history"])
                         if row["event_type"] == "CancellationScopeRequested")
    value["history"].insert(request_index, {
        "id": "first-group-member-completed", "namespace": value["history"][0]["namespace"],
        "sequence": 1, "event_type": "ActivityCompleted", "timestamp": "2026-10-04T00:00:02.623456Z",
        "payload": {"sequence": 4, "activity_type": "original-activity",
                    "result": serializer.envelope("first member done", codec="avro")},
    })
    for index, row in enumerate(value["history"], start=1):
        row["sequence"] = index
    contexts: list[ScopedCancellationContext] = []
    outcome = run(probe(value, contexts), value)
    assert outcome.commands == [] and contexts == []
    assert outcome.cancellation_scope_delivery is not None
    assert outcome.cancellation_scope_delivery.boundary.sequence_span == 4


@pytest.mark.parametrize("layout", ["flat", "nested"])
def test_group_cleanup_timer_replays_original_receipt_and_narrower_authority(layout: str) -> None:
    value = fixture("populated-scope-groups.json", layout)
    for event in value["history"]:
        if event["event_type"] in {"CancellationScopeDeliveryPrepared", "CancellationScopeDelivered"}:
            event["payload"]["authority_deadline_at"] = "2026-10-04T00:00:26.123456Z"
    delivered = next(row for row in value["history"] if row["event_type"] == "CancellationScopeDelivered")
    original = ScopedCancellationContext.from_dict(delivered["payload"]["cancellation"])
    snapshot = {
        "scope_id": original.scope_id, "operation_scope_id": original.scope_id,
        "request_id": original.request_id, "root_request_id": original.root_request_id,
        "delivery_history_event_id": delivered["id"],
        "preparation_history_event_id": delivered["payload"]["preparation_history_event_id"],
        "cleanup_deadline_at": "2026-10-04T00:00:30.123456Z",
        "authority_deadline_at": "2026-10-04T00:00:26.123456Z",
    }
    contexts: list[ScopedCancellationContext] = []
    cls = probe(value, contexts, cleanup=True, expected_remaining=17.0)
    first = workflow.commands_to_server_commands(run(cls, value).commands, "queue")
    assert first == [{"type": "start_timer", "delay_seconds": 1,
                      "cancellation_scope_id": original.scope_id,
                      "cancellation_cleanup": {field: snapshot[field] for field in (
                          "scope_id", "request_id", "delivery_history_event_id",
                      )}}]
    append_cleanup_event(value, "TimerScheduled", {
        "sequence": 8, "timer_id": "cleanup-timer", "delay_seconds": 1,
        "fire_at": "2026-10-04T00:00:10.123456Z", "cancellation_scope_id": original.scope_id,
        "cancellation_cleanup": snapshot,
    }, "2026-10-04T00:00:09.123456Z")
    value["task"].update({"workflow_task_attempt": 23, "lease_owner": "replacement"})
    assert workflow.commands_to_server_commands(run(cls, value).commands, "queue") == first
    append_cleanup_event(value, "TimerFired", {
        "sequence": 8, "timer_id": "cleanup-timer", "delay_seconds": 1,
        "cancellation_scope_id": original.scope_id,
    }, "2026-10-04T00:00:10.123456Z")
    root = workflow.commands_to_server_commands(run(cls, value).commands, "queue")
    assert len(root) == 1 and root[0]["activity_type"] == "unaffected-root"
    assert "cancellation_scope_id" not in root[0]
