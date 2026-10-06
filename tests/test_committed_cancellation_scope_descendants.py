from __future__ import annotations

from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow._cancellation_scope import CancellationScopeHistory
from durable_workflow._cancellation_scope_history import CommittedCancellationScopeHistory
from durable_workflow.cancellation import ScopedCancellationContext
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.workflow import LocalActivityExecutionAborted
from tests.test_committed_cancellation_scope_history import fixture
from tests.test_committed_cancellation_scope_replay import append_cleanup_event, run


def probe(
    value: dict[str, Any], seen: dict[str, ScopedCancellationContext], *, change: str = "", cleanup: str = ""
) -> type:
    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def capture(name: str, error: WorkflowCancelled):  # type: ignore[no-untyped-def]
                assert isinstance(error.context, ScopedCancellationContext)
                assert error.context is ctx.cancellation_context and ctx.is_cancellation_requested
                assert error.request_id == error.context.request_id
                assert error.context.to_dict() == value["contexts"][name]
                fired = any(
                    row["event_type"] == "TimerFired" and row["payload"]["timer_id"] == "descendant-cleanup"
                    for row in value["history"]
                )
                later = cleanup and ["grandchild", "child", "parent"].index(name) > [
                    "grandchild",
                    "child",
                    "parent",
                ].index(cleanup)
                assert error.context.remaining() == (15 if fired and later else 17)
                seen[name] = error.context
                if cleanup == name:
                    with ctx.cancellation_shield():
                        yield ctx.start_timer(1)

            def grandchild():  # type: ignore[no-untyped-def]
                if change != "prefix":
                    assert (yield ctx.schedule_activity("prior-step", [])) == "prior-value"
                try:
                    if value["layout"] == "timer":
                        yield ctx.start_timer(42 if change == "timer" else 3600)
                    else:
                        original = next(
                            row["payload"] for row in value["history"] if row["event_type"] == "ConditionWaitOpened"
                        )
                        activity = ctx.schedule_activity(
                            "changed" if change == "activity" else "original-activity",
                            [],
                            cancellation_policy="abandon" if change == "activity-policy" else "try_cancel",
                            schedule_to_close_timeout=60,
                        )
                        timer = ctx.start_timer(42 if change == "timer" else 3600)
                        child = ctx.start_child_workflow(
                            "changed" if change == "child" else "original-child",
                            [],
                            cancellation_policy="abandon"
                            if change == "child-policy"
                            else "wait_cancellation_completed",
                            parent_close_policy="request_cancel",
                        )
                        condition = workflow.WaitCondition(
                            predicate=lambda: False,
                            condition_key="ready",
                            timeout_seconds=30,
                            condition_definition_fingerprint=original["condition_definition_fingerprint"],
                        )
                        members = (
                            [activity, timer, child, condition]
                            if change == "layout"
                            else [activity, [timer, child], condition]
                        )
                        if change == "size":
                            members.pop()
                        yield members
                    pytest.fail("the original ancestor boundary must deliver cancellation")
                except WorkflowCancelled as error:
                    yield from capture("grandchild", error)

            def child_scope():  # type: ignore[no-untyped-def]
                try:
                    yield from ctx.cancellation_scope(grandchild, shield_parent=change == "shield")
                    ctx.throw_if_cancellation_requested()
                    pytest.fail("child must retain its accepted original request")
                except WorkflowCancelled as error:
                    yield from capture("child", error)

            def shielded_grandchild():  # type: ignore[no-untyped-def]
                assert not ctx.is_cancellation_requested and ctx.cancellation_context is None

            def shielded_child():  # type: ignore[no-untyped-def]
                yield from ctx.cancellation_scope(shielded_grandchild)

            def parent():  # type: ignore[no-untyped-def]
                yield from ctx.cancellation_scope(shielded_child, shield_parent=True)
                try:
                    yield from ctx.cancellation_scope(child_scope)
                    ctx.throw_if_cancellation_requested()
                    pytest.fail("parent must retain its accepted original request")
                except WorkflowCancelled as error:
                    yield from capture("parent", error)

            def outer():  # type: ignore[no-untyped-def]
                yield from ctx.cancellation_scope(parent)
                assert not ctx.is_cancellation_requested and ctx.cancellation_context is None

            yield from ctx.cancellation_scope(outer)
            assert not ctx.is_cancellation_requested and ctx.cancellation_context is None
            result = yield ctx.schedule_activity("unaffected-root", [])
            if cleanup:
                return {"result": result, "remaining": seen[cleanup].remaining()}
            return result

    return Probe


@pytest.mark.parametrize("layout", ["timer", "group"])
def test_ancestor_delivery_restores_each_original_context_and_preserves_outer_and_root(layout: str) -> None:
    value = fixture("committed-scope-descendants.json", layout)
    seen: dict[str, ScopedCancellationContext] = {}
    cls = probe(value, seen)
    result = run(cls, value)
    assert list(seen) == ["grandchild", "child", "parent"]
    assert result.commands == [workflow.ScheduleActivity("unaffected-root", [])]
    append_cleanup_event(
        value,
        "ActivityCompleted",
        {
            "sequence": 9 if layout == "timer" else 12,
            "activity_type": "unaffected-root",
            "result": serializer.envelope("survivor", codec="avro"),
        },
        "2026-10-04T00:00:20.123456Z",
    )
    assert run(cls, value).commands == [workflow.CompleteWorkflow("survivor")]
    value["task"].update({"lease_owner": "replacement", "workflow_task_attempt": 17})
    assert run(cls, value).commands == [workflow.CompleteWorkflow("survivor")]


@pytest.mark.parametrize("layout", ["timer", "group"])
@pytest.mark.parametrize("prepared, inherited", [(False, False), (False, True), (True, True)])
def test_pending_ancestor_retains_original_identity_range_and_preparation_without_cleanup(
    layout: str, prepared: bool, inherited: bool
) -> None:
    value = fixture("committed-scope-descendants.json", layout)
    value["history"] = [
        row
        for row in value["history"]
        if row["event_type"] != "CancellationScopeDelivered"
        and (prepared or row["event_type"] != "CancellationScopeDeliveryPrepared")
        and (inherited or row["event_type"] != "CancellationScopeRequested"
             or row["payload"]["scope_id"] == value["scopes"]["parent"])
    ]
    seen: dict[str, ScopedCancellationContext] = {}
    cls = probe(value, seen)
    result = run(cls, value)
    assert seen == {} and result.commands == []
    intent = result.cancellation_scope_delivery
    assert intent is not None and intent.context.to_dict() == value["contexts"]["parent"]
    assert intent.boundary.sequence == 8
    assert intent.boundary.sequence_span == (1 if layout == "timer" else 4)
    assert (intent.preparation is not None) is prepared
    value["task"].update({"lease_owner": "replacement", "workflow_task_attempt": 17})
    assert run(cls, value).cancellation_scope_delivery == intent
    assert seen == {}


def test_pending_ancestor_cannot_bypass_a_competing_intermediate_request() -> None:
    value = fixture("committed-scope-operation-projections.json", "competing")
    value["history"] = [
        row for row in value["history"]
        if row["event_type"] not in {"CancellationScopeDeliveryPrepared", "CancellationScopeDelivered"}
        and (row["event_type"] != "CancellationScopeRequested" or row["payload"]["scope_id"] != "desc-grandchild")
    ]
    scopes = CancellationScopeHistory.read(value["history"], value["task"]["run_id"])
    committed = CommittedCancellationScopeHistory.read(
        value["history"], value["task"]["run_id"], value["task"]["workflow_id"], scopes,
    )
    with pytest.raises(ValueError, match="original ancestor lineage"):
        committed.pending_request_for_scope("desc-grandchild", scopes)


@pytest.mark.parametrize("layout", ["timer", "group"])
@pytest.mark.parametrize("target", ["grandchild", "child", "parent"])
def test_cleanup_timer_retains_original_ancestor_receipt_and_each_scope_authority(layout: str, target: str) -> None:
    value = fixture("committed-scope-descendants.json", layout)
    seen: dict[str, ScopedCancellationContext] = {}
    cls = probe(value, seen, cleanup=target)
    fresh = workflow.commands_to_server_commands(run(cls, value).commands, "queue")
    expected = {
        "scope_id": value["scopes"][target],
        "request_id": value["contexts"][target]["lineage"][-1]["request_id"],
        "delivery_history_event_id": "ancestor-delivered",
    }
    assert fresh == [
        {
            "type": "start_timer",
            "delay_seconds": 1,
            "cancellation_scope_id": expected["scope_id"],
            "cancellation_cleanup": expected,
        }
    ]
    sequence = 9 if layout == "timer" else 12
    append_cleanup_event(
        value,
        "TimerScheduled",
        {
            "sequence": sequence,
            "timer_id": "descendant-cleanup",
            "delay_seconds": 1,
            "fire_at": "2026-10-04T00:00:11.123456Z",
            "cancellation_scope_id": expected["scope_id"],
            "cancellation_cleanup": {
                **expected,
                "operation_scope_id": expected["scope_id"],
                "root_request_id": value["contexts"]["parent"]["root_context"]["root_request_id"],
                "preparation_history_event_id": "ancestor-prepared",
                "cleanup_deadline_at": "2026-10-04T00:00:30.123456Z",
                "authority_deadline_at": "2026-10-04T00:00:26.123456Z",
            },
        },
        "2026-10-04T00:00:10.123456Z",
    )
    value["task"].update({"lease_owner": "replacement", "workflow_task_attempt": 17})
    assert workflow.commands_to_server_commands(run(cls, value).commands, "queue") == fresh
    append_cleanup_event(
        value,
        "TimerFired",
        {
            "sequence": sequence,
            "timer_id": "descendant-cleanup",
            "delay_seconds": 1,
            "cancellation_scope_id": expected["scope_id"],
        },
        "2026-10-04T00:00:11.123456Z",
    )
    assert run(cls, value).commands == [workflow.ScheduleActivity("unaffected-root", [])]
    append_cleanup_event(
        value,
        "ActivityCompleted",
        {
            "sequence": sequence + 1,
            "activity_type": "unaffected-root",
            "result": serializer.envelope("survivor", codec="avro"),
        },
        "2026-10-04T00:00:20.123456Z",
    )
    assert run(cls, value).commands == [workflow.CompleteWorkflow({"result": "survivor", "remaining": 6})]


@pytest.mark.parametrize(
    "change", ["shield", "prefix", "activity", "activity-policy", "timer", "child", "child-policy", "layout", "size"]
)
def test_changed_authored_subtree_refuses_before_cleanup(change: str) -> None:
    value = fixture("committed-scope-descendants.json", "group")
    seen: dict[str, ScopedCancellationContext] = {}
    with pytest.raises(NonDeterministicReplayError):
        run(probe(value, seen, change=change), value)
    assert seen == {}


@pytest.mark.parametrize(
    "field",
    [
        "scope_id",
        "operation_scope_id",
        "request_id",
        "root_request_id",
        "delivery_history_event_id",
        "preparation_history_event_id",
        "cleanup_deadline_at",
        "authority_deadline_at",
        "omitted",
        "null",
        "retrograde",
    ],
)
def test_descendant_cleanup_cannot_borrow_or_change_authority_before_workflow_construction(field: str) -> None:
    value = fixture("committed-scope-descendants.json", "timer")
    context = ScopedCancellationContext.from_dict(value["contexts"]["grandchild"])
    snapshot = {
        "scope_id": context.scope_id,
        "operation_scope_id": context.scope_id,
        "request_id": context.request_id,
        "root_request_id": context.root_request_id,
        "delivery_history_event_id": "ancestor-delivered",
        "preparation_history_event_id": "ancestor-prepared",
        "cleanup_deadline_at": "2026-10-04T00:00:30.123456Z",
        "authority_deadline_at": "2026-10-04T00:00:26.123456Z",
    }
    if field not in {"omitted", "null", "retrograde"}:
        snapshot[field] = "changed"
    payload: dict[str, Any] = {
        "sequence": 9,
        "timer_id": "descendant-cleanup",
        "delay_seconds": 1,
        "fire_at": "2026-10-04T00:00:09.123456Z" if field == "retrograde"
        else "2026-10-04T00:00:11.123456Z",
        "cancellation_scope_id": context.scope_id,
    }
    if field != "omitted":
        payload["cancellation_cleanup"] = None if field == "null" else snapshot
    append_cleanup_event(
        value,
        "TimerScheduled",
        payload,
        "2026-10-04T00:00:10.123456Z",
    )
    entered: list[bool] = []

    class Probe:
        def __init__(self) -> None:
            entered.append(True)

        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.start_timer(1)

    with pytest.raises(LocalActivityExecutionAborted, match="cleanup timer"):
        run(Probe, value)
    assert entered == []


def test_competing_descendant_root_is_refused_before_workflow_construction() -> None:
    value = fixture("committed-scope-operation-projections.json", "competing")
    entered: list[bool] = []

    class Probe:
        def __init__(self) -> None:
            entered.append(True)

        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.start_timer(1)

    with pytest.raises(LocalActivityExecutionAborted, match="descendant.*root"):
        run(Probe, value)
    assert entered == []
