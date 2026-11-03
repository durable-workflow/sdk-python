from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.cancellation import ScopedCancellationContext
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.workflow import LocalActivityExecutionAborted
from tests.test_committed_cancellation_scope_history import fixture


def run(cls: type, value: dict[str, Any]) -> workflow.ReplayOutcome:
    return workflow.replay(
        cls, value["history"], [], workflow_id=value["task"]["workflow_id"], run_id=value["task"]["run_id"],
        payload_codec="avro", allow_cancellation_scope_authoring=True, allow_cancellation_scope_delivery=True,
    )


def cleanup_timer_history() -> dict[str, Any]:
    value = fixture("committed-scope-delivery.json", "unshielded")
    delivered = next(event for event in value["history"] if event["event_type"] == "CancellationScopeDelivered")
    context = ScopedCancellationContext.from_dict(delivered["payload"]["cancellation"])
    snapshot = {
        "scope_id": context.scope_id, "operation_scope_id": context.scope_id,
        "request_id": context.request_id, "root_request_id": context.root_request_id,
        "delivery_history_event_id": delivered["id"],
        "preparation_history_event_id": delivered["payload"]["preparation_history_event_id"],
        "cleanup_deadline_at": context.deadline.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        "authority_deadline_at": delivered["payload"]["authority_deadline_at"],
    }
    append_cleanup_event(value, "TimerScheduled", {
        "sequence": 4, "timer_id": "cleanup-timer", "delay_seconds": 1,
        "fire_at": "2026-10-04T00:00:09.123456Z", "cancellation_scope_id": context.scope_id,
        "cancellation_cleanup": snapshot,
    }, "2026-10-04T00:00:08.123456Z")
    return value


def append_cleanup_event(value: dict[str, Any], kind: str, payload: dict[str, Any], timestamp: str) -> None:
    history = value["history"]
    history.append({"id": f"cleanup-event-{len(history) + 1}", "namespace": history[0]["namespace"],
                    "sequence": len(history) + 1, "event_type": kind, "timestamp": timestamp, "payload": payload})


def test_scoped_cleanup_timer_carries_original_delivery_and_replays_without_extending_the_budget() -> None:
    value = cleanup_timer_history()
    snapshot = value["history"][-1]["payload"]["cancellation_cleanup"]

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def inner():  # type: ignore[no-untyped-def]
                try:
                    yield ctx.start_timer(10)
                except WorkflowCancelled as error:
                    assert isinstance(error.context, ScopedCancellationContext)
                    with ctx.cancellation_shield():
                        yield ctx.start_timer(1)
                    return error.context

            def outer():  # type: ignore[no-untyped-def]
                return (yield from ctx.cancellation_scope(inner))

            cancellation = yield from ctx.cancellation_scope(outer)
            assert not ctx.is_cancellation_requested and ctx.cancellation_context is None
            yield ctx.start_timer(1)
            return {"request": cancellation.request_id, "remaining": cancellation.remaining()}

    fresh = deepcopy(value)
    fresh["history"].pop()
    decision = run(Probe, fresh)
    wire = workflow.commands_to_server_commands(decision.commands, "queue")
    assert wire[0]["cancellation_cleanup"] == {
        field: snapshot[field] for field in ("scope_id", "request_id", "delivery_history_event_id")
    }
    assert wire[0]["cancellation_scope_id"] == snapshot["scope_id"]
    # The original cleanup timer and its authority are already durable.
    # Cold replay waits for it rather than starting a second timer.
    assert run(Probe, value).commands == []
    assert run(Probe, value).commands == []
    append_cleanup_event(value, "TimerFired", {
        "sequence": 4, "timer_id": "cleanup-timer", "delay_seconds": 1,
        "cancellation_scope_id": snapshot["scope_id"],
    }, "2026-10-04T00:00:09.123456Z")
    parent = workflow.commands_to_server_commands(run(Probe, value).commands, "queue")
    assert parent == [{"type": "start_timer", "delay_seconds": 1}]
    append_cleanup_event(value, "TimerScheduled", {
        "sequence": 5, "timer_id": "parent-timer", "delay_seconds": 1,
        "fire_at": "2026-10-04T00:00:10.123456Z",
    }, "2026-10-04T00:00:09.123456Z")
    append_cleanup_event(value, "TimerFired", {
        "sequence": 5, "timer_id": "parent-timer", "delay_seconds": 1,
    }, "2026-10-04T00:00:10.123456Z")
    for _ in range(2):
        value["task"].update({"lease_owner": "replacement", "workflow_task_attempt": 17})
        assert run(Probe, value).commands == [workflow.CompleteWorkflow({
            "request": snapshot["request_id"], "remaining": 20,
        })]


@pytest.mark.parametrize("field", ["request_id", "delivery_history_event_id", "cleanup_deadline_at",
                                   "authority_deadline_at", "operation_scope_id"])
def test_cleanup_timer_refuses_changed_authority_before_workflow_construction(field: str) -> None:
    value = cleanup_timer_history()
    value["history"][-1]["payload"]["cancellation_cleanup"][field] = "changed"
    constructed: list[bool] = []

    class Probe:
        def __init__(self) -> None:
            constructed.append(True)

        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.start_timer(1)

    with pytest.raises(LocalActivityExecutionAborted, match="cleanup timer"):
        run(Probe, value)
    assert not constructed


@pytest.mark.parametrize("variant", ["shielded", "unshielded"])
def test_committed_scope_delivery_replays_original_identity_budget_and_unaffected_parent(variant: str) -> None:
    value = fixture("committed-scope-delivery.json", variant)
    observations: list[float] = []
    contexts: list[ScopedCancellationContext] = []

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def inner():  # type: ignore[no-untyped-def]
                assert not ctx.is_cancellation_requested
                assert ctx.cancellation_context is None
                try:
                    yield ctx.start_timer(10)
                    pytest.fail("original call must be cancelled")
                except WorkflowCancelled as error:
                    assert isinstance(error.context, ScopedCancellationContext)
                    assert error.context is ctx.cancellation_context
                    assert error.request_id == error.context.request_id
                    assert error.context.reason == "release one scope"
                    assert error.context.requester == {"type": "php", "label": "PHP API"}
                    assert error.context.source == "php"
                    contexts.append(error.context)
                    observations.append(error.context.remaining())
                    with ctx.cancellation_shield():
                        ctx.throw_if_cancellation_requested()
                    with pytest.raises(WorkflowCancelled) as repeated:
                        ctx.throw_if_cancellation_requested()
                    assert repeated.value.context is error.context
                    assert repeated.value.request_id == error.request_id
                    return error.context.request_id
                finally:
                    assert ctx.is_cancellation_requested

            def outer():  # type: ignore[no-untyped-def]
                result = yield from ctx.cancellation_scope(inner, shield_parent=variant == "shielded")
                assert not ctx.is_cancellation_requested
                assert ctx.cancellation_context is None
                return result

            request_id = yield from ctx.cancellation_scope(outer)
            assert not ctx.is_cancellation_requested
            assert ctx.cancellation_context is None
            result = yield ctx.schedule_activity("unaffected-root-operation", [])
            return {"request": request_id, "survivor": result, "remaining": contexts[-1].remaining()}

    first = run(Probe, value)
    assert [type(command).__name__ for command in first.commands] == ["ScheduleActivity"]
    assert first.commands[0]._cancellation_scope_id == "root"
    assert first.cancellation_scope_delivery is None
    assert observations == [22.623456]
    assert run(Probe, value).commands == first.commands
    history = value["history"]
    history.append({
        "id": "root-result", "namespace": history[0]["namespace"], "sequence": len(history) + 1,
        "event_type": "ActivityCompleted", "timestamp": "2026-10-04T00:00:20.000000Z", "payload": {
            "sequence": 4, "activity_type": "unaffected-root-operation",
            "result": serializer.envelope("survivor", codec="avro"),
        },
    })
    expected = workflow.CompleteWorkflow({
        "request": contexts[-1].request_id, "survivor": "survivor", "remaining": 10.123456,
    })
    for _ in range(2):
        value["task"].update({"lease_owner": "replacement", "workflow_task_attempt": 17})
        assert run(Probe, value).commands == [expected]
    assert observations == [22.623456] * 4
    for context in contexts:
        with pytest.raises(RuntimeError, match="active workflow replay"):
            context.remaining()


def test_pending_and_prepared_scope_request_stop_before_cleanup_and_keep_the_original_boundary() -> None:
    value = fixture("committed-scope-delivery.json", "unshielded")
    full_history = deepcopy(value["history"])
    cleanup: list[str] = []

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def inner():  # type: ignore[no-untyped-def]
                try:
                    yield ctx.start_timer(10)
                except WorkflowCancelled:
                    cleanup.append("cleanup")
                    return "done"

            return (yield from ctx.cancellation_scope(lambda: ctx.cancellation_scope(inner)))

    value["history"] = full_history[:-2]
    pending = run(Probe, value)
    assert pending.commands == []
    assert pending.cancellation_scope_delivery is not None
    assert pending.cancellation_scope_delivery.preparation is None
    assert cleanup == []
    value["history"] = full_history[:-1]
    prepared = run(Probe, value)
    assert prepared.commands == []
    assert prepared.cancellation_scope_delivery is not None
    assert prepared.cancellation_scope_delivery.context == pending.cancellation_scope_delivery.context
    assert prepared.cancellation_scope_delivery.boundary == pending.cancellation_scope_delivery.boundary
    assert prepared.cancellation_scope_delivery.preparation is not None
    assert cleanup == []
    value["history"] = full_history
    assert run(Probe, value).commands == [workflow.CompleteWorkflow("done")]
    assert cleanup == ["cleanup"]


@pytest.mark.parametrize("failure", ["default", "authoring_only", "missing_authoring", "competing_roots"])
def test_unqualified_scope_paths_refuse_before_constructing_application(failure: str) -> None:
    entered: list[str] = []
    value = fixture("committed-scope-delivery.json", "unshielded")
    options: dict[str, bool] = {}
    if failure == "authoring_only":
        options["allow_cancellation_scope_authoring"] = True
    elif failure == "missing_authoring":
        options["allow_cancellation_scope_delivery"] = True
    elif failure == "competing_roots":
        value = fixture("committed-scope-operation-projections.json", "competing")
        options = {"allow_cancellation_scope_authoring": True, "allow_cancellation_scope_delivery": True}

    class Probe:
        def __init__(self) -> None:
            entered.append("factory")

        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.start_timer(10)

    with pytest.raises(LocalActivityExecutionAborted, match="scope"):
        workflow.replay(Probe, value["history"], [], run_id=value["task"]["run_id"],
                        workflow_id=value["task"]["workflow_id"], **options)
    assert entered == []


@pytest.mark.parametrize("changed", ["position", "kind", "membership", "shield"])
def test_committed_scope_delivery_cannot_move_or_replace_the_authored_boundary(changed: str) -> None:
    value = fixture("committed-scope-delivery.json", "unshielded")

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def inner():  # type: ignore[no-untyped-def]
                if changed == "position":
                    yield ctx.side_effect(lambda: "new-prefix")
                if changed == "membership":
                    operation = workflow.StartTimer(10)
                    operation._cancellation_scope_id = "root"
                elif changed == "kind":
                    operation = ctx.schedule_activity("different", [])
                else:
                    operation = ctx.start_timer(10)
                if changed == "shield":
                    with ctx.cancellation_shield():
                        yield operation
                else:
                    yield operation

            return (yield from ctx.cancellation_scope(lambda: ctx.cancellation_scope(inner)))

    with pytest.raises((NonDeterministicReplayError, LocalActivityExecutionAborted)):
        run(Probe, value)


def _scoped_cleanup_probe() -> type:
    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def inner():  # type: ignore[no-untyped-def]
                try:
                    yield ctx.start_timer(10)
                except WorkflowCancelled as error:
                    with ctx.cancellation_shield():
                        yield ctx.start_timer(1)
                    return error.context

            def outer():  # type: ignore[no-untyped-def]
                return (yield from ctx.cancellation_scope(inner))

            cancellation = yield from ctx.cancellation_scope(outer)
            yield ctx.start_timer(1)
            return {"request": cancellation.request_id, "remaining": cancellation.remaining()}

    return Probe


def _with_delivery_span(value: dict[str, Any], span: Any) -> dict[str, Any]:
    for event in value["history"]:
        if event["event_type"] in {"CancellationScopeDeliveryPrepared", "CancellationScopeDelivered"}:
            event["payload"]["sequence_span"] = span
    return value


def _scoped_cleanup_outcomes(span: Any) -> list[Any]:
    """Replay the cleanup timer path (whose scope parser applies the cleanup
    lower bound, delivery sequence + span) through to completion."""
    probe = _scoped_cleanup_probe()
    value = _with_delivery_span(cleanup_timer_history(), span)
    outcomes: list[Any] = [run(probe, value).commands]
    append_cleanup_event(value, "TimerFired", {
        "sequence": 4, "timer_id": "cleanup-timer", "delay_seconds": 1,
        "cancellation_scope_id": value["history"][-1]["payload"]["cancellation_scope_id"],
    }, "2026-10-04T00:00:09.123456Z")
    outcomes.append(workflow.commands_to_server_commands(run(probe, value).commands, "queue"))
    append_cleanup_event(value, "TimerScheduled", {
        "sequence": 5, "timer_id": "parent-timer", "delay_seconds": 1, "fire_at": "2026-10-04T00:00:10.123456Z",
    }, "2026-10-04T00:00:09.123456Z")
    append_cleanup_event(value, "TimerFired", {
        "sequence": 5, "timer_id": "parent-timer", "delay_seconds": 1,
    }, "2026-10-04T00:00:10.123456Z")
    outcomes.append(run(probe, value).commands)
    return outcomes


def test_a_null_scope_delivery_span_replays_the_cleanup_path_exactly_like_one() -> None:
    one = _scoped_cleanup_outcomes(1)
    assert one[0] == [] and one[1] == [{"type": "start_timer", "delay_seconds": 1}]
    assert isinstance(one[2][0], workflow.CompleteWorkflow)
    assert _scoped_cleanup_outcomes(None) == one


def _cleanup_at(sequence: int, span: Any) -> Any:
    value = _with_delivery_span(cleanup_timer_history(), span)
    value["history"][-1]["payload"]["sequence"] = sequence
    try:
        return run(_scoped_cleanup_probe(), value).commands
    except (LocalActivityExecutionAborted, NonDeterministicReplayError) as error:
        return type(error).__name__, str(error).split(" Keep ")[0]


def test_a_null_scope_delivery_span_has_the_same_cleanup_lower_bound_as_one() -> None:
    delivered = next(event for event in cleanup_timer_history()["history"]
                     if event["event_type"] == "CancellationScopeDelivered")["payload"]["sequence"]
    # The cleanup timer may not reuse the delivered call's sequence.
    below = _cleanup_at(delivered, 1)
    assert isinstance(below, tuple) and "changes its original delivery or authority ceiling" in below[1]
    assert _cleanup_at(delivered, None) == below
    assert _cleanup_at(delivered + 1, None) == _cleanup_at(delivered + 1, 1) == []


@pytest.mark.parametrize("span", [0, False, -1, "1", 1.0, ""])
def test_a_malformed_scope_delivery_span_is_still_refused(span: Any) -> None:
    value = _with_delivery_span(cleanup_timer_history(), span)
    with pytest.raises(NonDeterministicReplayError, match="durable_command_sequence_invalid:"):
        run(_scoped_cleanup_probe(), value)


@pytest.mark.parametrize("span", [0, False, -1, "1", 1.0, ""])
def test_the_cleanup_lower_bound_refuses_a_malformed_span_on_its_own(span: Any) -> None:
    # Called directly, without the replay-level span check, the scope parser
    # still refuses rather than reading a falsy span as one call.
    from durable_workflow._cancellation_scope_history import _cleanup_operation

    value = _with_delivery_span(cleanup_timer_history(), span)
    history = value["history"]
    with pytest.raises(ValueError, match="without a canonical sequence span"):
        _cleanup_operation(history[-1], history[:-1])
    _with_delivery_span(value, None)
    assert _cleanup_operation(history[-1], history[:-1]) is True
