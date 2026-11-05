"""Source-only successor evidence controls; these are not producer/runtime proof.

The ledger deliberately covers remote/local activity, plain timer and untimed
signal calls without internal-timeout rows. Core supports additional shapes
which this fixture model refuses.
The new positive failure/cancellation histories also have Python replay witnesses.
"""

from __future__ import annotations

from contextlib import suppress
from copy import deepcopy
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.errors import ActivityFailed, WorkflowCancelled
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext, replay
from tests.test_redrive_successor_evidence import (
    CANCELLATION,
    _delivery,
    _renumber,
    _request,
    _step_three_activity,
)
from tests.test_replay_clock import check_redrive_successor, redrive_fixture


@workflow.defn(name="tests.replay.redrive-activity-recovery-witness")
class ActivityRecoveryWitness:
    def run(self, ctx: WorkflowContext, name: str, seconds: int):  # type: ignore[no-untyped-def]
        greeting = yield ctx.schedule_activity("tests.redrive.first", [name])
        yield ctx.start_timer(seconds)
        try:
            with suppress(ActivityFailed):
                yield ctx.schedule_activity("tests.redrive.second", [greeting])
            yield ctx.schedule_activity("tests.redrive.recovery", [])
        except WorkflowCancelled as error:
            assert error.context is not None
            return error.context.remaining()


@workflow.defn(name="tests.replay.redrive-timer-cancellation-witness")
class TimerCancellationWitness:
    def run(self, ctx: WorkflowContext, name: str, seconds: int):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("tests.redrive.first", [name])
        yield ctx.start_timer(seconds)
        try:
            yield ctx.start_timer(60)
        except WorkflowCancelled as error:
            assert error.context is not None
            return error.context.remaining()


def document() -> dict[str, Any]:
    return deepcopy(redrive_fixture(CANCELLATION))


def terminal(
    document: dict[str, Any], kind: str, *, before_request: bool = False, opened: dict[str, Any] | None = None,
) -> dict[str, Any]:
    history = document["history"]
    if opened is None:
        opened = _step_three_activity(history)
    boundary = _request(history) if before_request else _delivery(history)
    row = {"event_type": kind, "recorded_at": boundary["recorded_at"],
           "payload": {**opened["payload"], "message": "fixture terminal failure"}}
    history.insert(history.index(boundary), row)
    _renumber(history)
    return row


def bind_witness(document: dict[str, Any], name: str) -> None:
    document["workflow"]["type"] = name
    document["history"][0]["payload"]["workflow_type"] = name


def activity_recovery(kind: str, before_request: bool) -> dict[str, Any]:
    doc = document()
    bind_witness(doc, "tests.replay.redrive-activity-recovery-witness")
    terminal(doc, kind, before_request=before_request)
    _delivery(doc["history"])["payload"]["sequence"] = 4
    return doc


def cancelled_timer() -> dict[str, Any]:
    doc = document()
    bind_witness(doc, "tests.replay.redrive-timer-cancellation-witness")
    opened = _step_three_activity(doc["history"])
    opened.update(event_type="TimerScheduled", payload={"sequence": 3, "timer_id": "successor-timer",
                                                       "delay_seconds": 60})
    history = doc["history"]
    row = terminal(doc, "TimerCancelled", opened=opened)
    row["payload"] = {**opened["payload"], "cancelled_at": row["recorded_at"]}
    _delivery(history)["payload"]["call_kind"] = "timer"
    _renumber(history)
    return doc


def assert_replays(doc: dict[str, Any], witness: type) -> None:
    task = doc["workflow"]
    outcome = replay(witness, doc["history"], task["input"], run_id=task["run_id"],
                     workflow_id=task["workflow_id"], payload_codec="avro")
    assert list(outcome.commands) == [CompleteWorkflow(29.0)]


@pytest.mark.parametrize("kind", ["ActivityFailed", "ActivityTimedOut"])
@pytest.mark.parametrize("before_request", [False, True])
def test_failed_activity_positive_has_a_reachable_recovery_workflow(kind: str, before_request: bool) -> None:
    doc = activity_recovery(kind, before_request)
    assert_replays(doc, ActivityRecoveryWitness)
    check_redrive_successor(doc["history"], doc["workflow"])


def test_cancelled_timer_positive_has_a_reachable_cancellation_workflow() -> None:
    doc = cancelled_timer()
    assert_replays(doc, TimerCancellationWitness)
    check_redrive_successor(doc["history"], doc["workflow"])


@pytest.mark.parametrize("kind", ["condition", "child", "service_operation"])
def test_unmodeled_unscheduled_call_families_are_explicitly_refused(kind: str) -> None:
    doc = document()
    history = doc["history"]
    history.remove(_step_three_activity(history))
    _delivery(history)["payload"]["call_kind"] = kind
    _renumber(history)
    with pytest.raises(AssertionError, match="single call kind"):
        check_redrive_successor(history, doc["workflow"])


@pytest.mark.parametrize("kind", ["condition_timeout", "signal_timeout"])
@pytest.mark.parametrize("event_kind", ["TimerScheduled", "TimerFired", "TimerCancelled"])
def test_internal_timeout_rows_are_outside_the_plain_timer_model(kind: str, event_kind: str) -> None:
    doc = cancelled_timer()
    history = doc["history"]
    if event_kind == "TimerFired":
        next(row for row in history if row["event_type"] == "TimerCancelled")["event_type"] = "TimerFired"
    target = next(row for row in history if row["event_type"] == event_kind and row["payload"]["sequence"] == 3)
    target["payload"].update(timer_kind=kind, condition_wait_id="wait", signal_wait_id="wait")
    with pytest.raises(AssertionError):
        check_redrive_successor(history, doc["workflow"])


@pytest.mark.parametrize("kind", ["ActivityCompleted", "ActivityCancelled", "ActivityFailed", "ActivityTimedOut"])
@pytest.mark.parametrize("mutation", ["resource", "step", "mode", "before-opener"])
def test_activity_terminals_cannot_resolve_a_different_call(kind: str, mutation: str) -> None:
    doc = document()
    history = doc["history"]
    row = terminal(doc, kind)
    if mutation == "resource":
        row["payload"]["activity_execution_id"] = "foreign"
    elif mutation == "step":
        row["payload"]["sequence"] = 4
    elif mutation == "mode":
        row["payload"]["execution_mode"] = "local"
    else:
        history.remove(row)
        history.insert(history.index(_step_three_activity(history)), row)
        _renumber(history)
    with pytest.raises(AssertionError):
        check_redrive_successor(history, doc["workflow"])


@pytest.mark.parametrize("kind", ["ActivityCompleted", "ActivityCancelled", "ActivityFailed", "ActivityTimedOut"])
@pytest.mark.parametrize("local", [False, True])
@pytest.mark.parametrize("before_request", [False, True])
def test_activity_terminal_families_gate_the_original_call_at_the_request(
    kind: str, local: bool, before_request: bool,
) -> None:
    doc = document()
    opened = _step_three_activity(doc["history"])
    if local:
        opened["payload"]["local_activity"] = True
        _delivery(doc["history"])["payload"]["call_kind"] = "local_activity"
    terminal(doc, kind, before_request=before_request)
    if before_request:
        with pytest.raises(AssertionError, match="resolved before its request"):
            check_redrive_successor(doc["history"], doc["workflow"])
    else:
        check_redrive_successor(doc["history"], doc["workflow"])


@pytest.mark.parametrize("kind", ["TimerFired", "TimerCancelled"])
@pytest.mark.parametrize("mutation", ["resource", "step", "before-opener", "before-request"])
def test_timer_terminals_are_bound_to_the_original_timer(kind: str, mutation: str) -> None:
    doc = cancelled_timer()
    history = doc["history"]
    row = next(row for row in history if row["event_type"] == "TimerCancelled")
    row["event_type"] = kind
    if mutation == "resource":
        row["payload"]["timer_id"] = "foreign"
    elif mutation == "step":
        row["payload"]["sequence"] = 4
    else:
        history.remove(row)
        boundary = _request(history) if mutation == "before-request" else next(
            event for event in history if event["event_type"] == "TimerScheduled" and event["payload"]["sequence"] == 3)
        history.insert(history.index(boundary), row)
        _renumber(history)
    with pytest.raises(AssertionError):
        check_redrive_successor(history, doc["workflow"])


@pytest.mark.parametrize("local_field", ["local_activity", "execution_mode"])
def test_local_activity_shape_and_delivery_must_agree(local_field: str) -> None:
    doc = document()
    history = doc["history"]
    opened = _step_three_activity(history)
    opened["payload"][local_field] = True if local_field == "local_activity" else "local"
    with pytest.raises(AssertionError, match="recorded call's kind"):
        check_redrive_successor(history, doc["workflow"])
    _delivery(history)["payload"]["call_kind"] = "local_activity"
    check_redrive_successor(history, doc["workflow"])


@pytest.mark.parametrize("field", ["parallel_group_id", "selection_group_id", "cancellation_scope_id"])
def test_grouped_and_scoped_rows_are_outside_the_model(field: str) -> None:
    doc = document()
    _step_three_activity(doc["history"])["payload"][field] = "outside-scope"
    with pytest.raises(AssertionError):
        check_redrive_successor(doc["history"], doc["workflow"])


@pytest.mark.parametrize(("family", "first", "second"), [
    ("activity", "ActivityFailed", "ActivityCompleted"),
    ("activity", "ActivityCompleted", "ActivityFailed"),
    ("local_activity", "ActivityTimedOut", "ActivityCancelled"),
    ("local_activity", "ActivityCancelled", "ActivityTimedOut"),
    ("timer", "TimerCancelled", "TimerFired"),
    ("timer", "TimerFired", "TimerCancelled"),
    ("signal", "SignalApplied", "SignalApplied"),
])
@pytest.mark.parametrize("identical", [False, True], ids=["contradictory-or-duplicate", "identical-duplicate"])
def test_one_opener_cannot_have_two_terminal_rows(family: str, first: str, second: str, identical: bool) -> None:
    doc = document()
    history = doc["history"]
    opened = _step_three_activity(history)
    if family == "local_activity":
        opened["payload"]["local_activity"] = True
    elif family == "timer":
        opened.update(event_type="TimerScheduled", payload={"sequence": 3, "timer_id": "successor-timer"})
    elif family == "signal":
        opened.update(event_type="SignalWaitOpened", payload={
            "sequence": 3, "signal_wait_id": "successor-signal", "signal_name": "wake",
        })
    _delivery(history)["payload"]["call_kind"] = family
    row = terminal(doc, first, opened=opened)
    if family == "signal":
        row["payload"].update(signal_id="synthetic-signal", workflow_command_id="synthetic-signal-command")
    check_redrive_successor(history, doc["workflow"])
    duplicate = deepcopy(row)
    if not identical:
        duplicate["event_type"] = second
    history.insert(history.index(row) + 1, duplicate)
    _renumber(history)
    with pytest.raises(AssertionError, match="one terminal"):
        check_redrive_successor(history, doc["workflow"])
