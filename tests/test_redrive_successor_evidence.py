"""Valid redrive successor evidence (#237).

The 2.13.1 claim that the cancellation clock prefers a reused step's
reused_recorded_at rested on a history Workflow cannot write: the
cancellation was delivered before a "reused" step. Redrive writes the
successor's WorkflowStarted and its reused prefix in one write, so the
successor's own cancellation always follows the prefix, and remaining() is
computed from that delivery. Reused times are therefore not observable
through remaining(). These tests prove that non-leakage on successors built
to Workflow 28893f97 FailedRunRedrivePlan::successorEvents, and gate every
redrive fixture through the registry.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.errors import WorkflowCancelled
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, WorkflowContext, query_state, replay
from tests.redrive_fixture_registry import NON_QUALIFYING, SOURCE_RUNS, VALID_SUCCESSORS
from tests.test_replay_clock import REDRIVE_FIXTURES, check_redrive_successor, redrive_fixture

SIDE_EFFECT_CALLS: list[str] = []


@workflow.defn(name="tests.replay.redrive-successor-markers")
class RedriveSuccessorMarkersWorkflow:
    """Core TestRedriveMarkersWorkflow, plus the start clock it observes."""

    def __init__(self) -> None:
        self.markers: dict[str, Any] = {}

    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        started = ctx.now().isoformat()
        self.markers["token"] = yield ctx.side_effect(lambda: SIDE_EFFECT_CALLS.append("ran") or "fresh")
        self.markers["version"] = yield ctx.get_version("redrive-markers", -1, 1)
        greeting = yield ctx.schedule_activity("tests.redrive.first", [name])
        return (yield ctx.schedule_activity("tests.redrive.second", [greeting, started]))

    @workflow.query("markers")
    def query_markers(self) -> dict[str, Any]:
        return self.markers


@workflow.defn(name="tests.replay.redrive-successor-timer")
class RedriveSuccessorTimerWorkflow:
    """Core TestRedriveTimerWorkflow."""

    def run(self, ctx: WorkflowContext, name: str, seconds: int):  # type: ignore[no-untyped-def]
        greeting = yield ctx.schedule_activity("tests.redrive.first", [name])
        yield ctx.start_timer(seconds)
        return (yield ctx.schedule_activity("tests.redrive.second", [greeting]))


@workflow.defn(name="tests.replay.redrive-successor-cancellation-clock")
class RedriveSuccessorCancellationClockWorkflow:
    """The timer successor; its own cancellation reaches the second activity."""

    def run(self, ctx: WorkflowContext, name: str, seconds: int):  # type: ignore[no-untyped-def]
        greeting = yield ctx.schedule_activity("tests.redrive.first", [name])
        yield ctx.start_timer(seconds)
        try:
            return (yield ctx.schedule_activity("tests.redrive.second", [greeting]))
        except WorkflowCancelled as error:
            assert error.context is not None
            return error.context.remaining()


SUCCESSOR_WORKFLOWS = {
    "redrive-successor-reused-markers": RedriveSuccessorMarkersWorkflow,
    "redrive-successor-reused-timer": RedriveSuccessorTimerWorkflow,
    "redrive-successor-reused-cancellation-clock": RedriveSuccessorCancellationClockWorkflow,
}
CANCELLATION = "redrive-successor-reused-cancellation-clock"


def commands(name: str, history: list[dict[str, Any]] | None = None) -> list[Any]:
    fixture = redrive_fixture(name)
    task = fixture["workflow"]
    outcome = replay(SUCCESSOR_WORKFLOWS[name], fixture["history"] if history is None else history, task["input"],
                     run_id=task["run_id"], workflow_id=task["workflow_id"], payload_codec="avro")
    return list(outcome.commands)


def check(name: str, history: list[dict[str, Any]]) -> None:
    check_redrive_successor(history, redrive_fixture(name)["workflow"])


def event(history: list[dict[str, Any]], event_type: str) -> dict[str, Any]:
    matches = [item for item in history if item["event_type"] == event_type]
    assert len(matches) == 1, event_type
    return matches[0]


def changed(name: str, change: Any) -> list[dict[str, Any]]:
    history = deepcopy(redrive_fixture(name)["history"])
    change(history)
    return history


# Registry gate.
def _looks_like_redrive(path: Any) -> bool:
    text = path.read_text(encoding="utf-8")
    if "redrive" in path.stem or "reused_from_run_id" in text or "reused_recorded_at" in text:
        return True
    history = json.loads(text).get("history") or []
    return any(item.get("event_type") == "WorkflowStarted" and (item.get("payload") or {}).get("recovery_kind")
               == "redrive" for item in history if isinstance(item, dict))


def _verdict(name: str) -> str | None:
    fixture = redrive_fixture(name)
    try:
        check_redrive_successor(fixture["history"], fixture["workflow"])
    except AssertionError as error:
        return str(error)
    return None


def test_every_redrive_fixture_is_registered() -> None:
    selected = {path.stem for path in REDRIVE_FIXTURES.glob("*.json") if _looks_like_redrive(path)}
    assert selected == set(VALID_SUCCESSORS) | set(NON_QUALIFYING) | SOURCE_RUNS


def test_exactly_the_registered_successors_pass() -> None:
    passing = {path.stem for path in REDRIVE_FIXTURES.glob("*.json") if _looks_like_redrive(path)
               and _verdict(path.stem) is None}
    assert passing == set(VALID_SUCCESSORS)


@pytest.mark.parametrize("name", sorted(VALID_SUCCESSORS))
def test_registered_successor_resumes_after_its_reused_prefix(name: str) -> None:
    assert redrive_fixture(name)["history"][0]["payload"]["resume_step_sequence"] == VALID_SUCCESSORS[name]
    assert _verdict(name) is None


@pytest.mark.parametrize("name", sorted(NON_QUALIFYING))
def test_non_qualifying_fixture_is_pinned_and_replaced(name: str) -> None:
    entry = NON_QUALIFYING[name]
    raw = (REDRIVE_FIXTURES / f"{name}.json").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == entry.sha256
    assert _verdict(name) == entry.error
    assert entry.replacement in VALID_SUCCESSORS and _verdict(entry.replacement) is None


# The replacements replay as Core would: nothing re-executes.
def test_markers_successor_reuses_its_markers_and_the_start_clock() -> None:
    SIDE_EFFECT_CALLS.clear()
    assert commands("redrive-successor-reused-markers") == [
        ScheduleActivity("tests.redrive.second", ["Hello, Taylor!", "2026-11-03T00:00:00+00:00"]),
    ]
    fixture = redrive_fixture("redrive-successor-reused-markers")
    task = fixture["workflow"]
    markers = query_state(RedriveSuccessorMarkersWorkflow, fixture["history"], task["input"], "markers",
                          run_id=task["run_id"], workflow_id=task["workflow_id"], payload_codec="avro")
    assert markers == {"token": "0123456789abcdef", "version": 1}
    assert SIDE_EFFECT_CALLS == []


def test_timer_successor_does_not_start_its_timer_again() -> None:
    assert commands("redrive-successor-reused-timer") == [ScheduleActivity("tests.redrive.second", ["Hello, Taylor!"])]


def test_cancellation_successor_budget_is_computed_from_its_delivery() -> None:
    # Deadline 00:05:31 less the delivery at 00:05:02.
    assert commands(CANCELLATION) == [CompleteWorkflow(29.0)]


# Non-leakage.
REUSED_TIMES = ["2026-11-03T00:00:00+00:00", "2026-11-03T00:04:59.999999+00:00"]


@pytest.mark.parametrize("name", sorted(SUCCESSOR_WORKFLOWS))
@pytest.mark.parametrize("reused_at", REUSED_TIMES)
def test_valid_retiming_of_reused_events_changes_nothing(name: str, reused_at: str) -> None:
    def retime(history: list[dict[str, Any]]) -> None:
        for item in history:
            if "reused_recorded_at" in item["payload"]:
                item["payload"]["reused_recorded_at"] = reused_at

    history = changed(name, retime)
    check(name, history)
    assert commands(name, history) == commands(name)


def _time(item: dict[str, Any]) -> datetime:
    payload = item["payload"]
    return datetime.fromisoformat(payload.get("reused_recorded_at") or item["recorded_at"])


def leaking_clock_remaining(history: list[dict[str, Any]]) -> float:
    """A clock that read every event up to the delivery, preferring reuse times."""
    delivered = next(index for index, item in enumerate(history)
                     if item["event_type"] == "CooperativeCancellationDelivered")
    request = event(history, "CooperativeCancellationRequested")
    deadline = datetime.fromisoformat(request["payload"]["cleanup_deadline_at"])
    return (deadline - max(_time(item) for item in history[:delivered + 1])).total_seconds()


LATE = "2026-11-03T00:05:20+00:00"


def _late(event_type: str) -> Any:
    def change(history: list[dict[str, Any]]) -> None:
        item = event(history, event_type)
        item["recorded_at"] = LATE
        if "reused_recorded_at" in item["payload"]:
            item["payload"]["reused_recorded_at"] = LATE

    return change


@pytest.mark.parametrize("event_type", ["ActivityCompleted", "TimerFired"])
def test_a_reused_time_after_the_delivery_does_not_reach_the_budget(event_type: str) -> None:
    original = redrive_fixture(CANCELLATION)["history"]
    retimed = changed(CANCELLATION, _late(event_type))
    # The probe would see the reused event: 00:05:31 - 00:05:20.
    assert leaking_clock_remaining(original) == 29.0
    assert leaking_clock_remaining(retimed) == 11.0
    # The SDK does not: the budget is the delivery's.
    assert commands(CANCELLATION, retimed) == commands(CANCELLATION) == [CompleteWorkflow(29.0)]
    # Redrive cannot write this probe history.
    with pytest.raises(AssertionError):
        check(CANCELLATION, retimed)


def test_positive_control_the_delivery_time_reaches_the_budget() -> None:
    retimed = changed(CANCELLATION, _late("CooperativeCancellationDelivered"))
    assert commands(CANCELLATION, retimed) == [CompleteWorkflow(11.0)]


def test_the_2_13_1_order_is_not_a_successor() -> None:
    # Move the successor's cancellation ahead of the reused prefix.
    def early(history: list[dict[str, Any]]) -> None:
        request = event(history, "CooperativeCancellationRequested")
        delivery = event(history, "CooperativeCancellationDelivered")
        history.remove(request)
        history.remove(delivery)
        history[1:1] = [request, delivery]
        for number, item in enumerate(history, start=1):
            item["sequence"] = number

    with pytest.raises(AssertionError, match="reused events must directly follow WorkflowStarted"):
        check(CANCELLATION, changed(CANCELLATION, early))


@pytest.mark.parametrize("name", sorted(SUCCESSOR_WORKFLOWS))
@pytest.mark.parametrize("resume", [lambda value: value - 1, lambda value: value + 1, lambda value: None, str])
def test_successor_resume_must_follow_the_reused_prefix(name: str, resume: Any) -> None:
    def change(history: list[dict[str, Any]]) -> None:
        history[0]["payload"]["resume_step_sequence"] = resume(history[0]["payload"]["resume_step_sequence"])

    with pytest.raises(AssertionError, match="resume_step_sequence"):
        check(name, changed(name, change))


# Core's copied timer schedule (FailedRunRedrivePlan.php:437-455).
def _schedule(history: list[dict[str, Any]]) -> dict[str, Any]:
    return event(history, "TimerScheduled")


def _renumber(history: list[dict[str, Any]]) -> None:
    for number, item in enumerate(history, start=1):
        item["sequence"] = number


def _move_schedule_after_fired(history: list[dict[str, Any]]) -> None:
    schedule = _schedule(history)
    history.remove(schedule)
    history.insert(history.index(event(history, "TimerFired")) + 1, schedule)
    _renumber(history)


def _drop(event_type: str) -> Any:
    def change(history: list[dict[str, Any]]) -> None:
        history.remove(event(history, event_type))
        _renumber(history)

    return change


@pytest.mark.parametrize("name", ["redrive-successor-reused-timer", CANCELLATION])
@pytest.mark.parametrize("change", [
    lambda history: _schedule(history)["payload"].__setitem__("reused_from_run_id", "01JBREDRIVESOURCERUN000000"),
    lambda history: _schedule(history)["payload"].__setitem__("reused_recorded_at", "2026-11-03T00:00:05+00:00"),
    lambda history: _schedule(history)["payload"].__setitem__("timer_id", "another-timer"),
    lambda history: _schedule(history)["payload"].__setitem__("timer_id", " "),
    lambda history: _schedule(history)["payload"].__setitem__("sequence", 1),
    lambda history: _schedule(history).__setitem__("recorded_at", "2026-11-03T00:05:00.100000+00:00"),
    _move_schedule_after_fired,
    _drop("TimerScheduled"),
    _drop("TimerFired"),
], ids=["schedule-reused-from", "schedule-reused-at", "other-timer-id", "blank-timer-id", "other-step",
        "schedule-after-start", "schedule-after-fired", "no-schedule", "no-fired-timer"])
def test_only_core_shaped_copied_timer_schedules_are_accepted(name: str, change: Any) -> None:
    with pytest.raises(AssertionError):
        check(name, changed(name, change))


def _delivery(history: list[dict[str, Any]]) -> dict[str, Any]:
    return event(history, "CooperativeCancellationDelivered")


def _request(history: list[dict[str, Any]]) -> dict[str, Any]:
    return event(history, "CooperativeCancellationRequested")


SOURCE_RUN = "01JBREDRIVESOURCERUN000000"


@pytest.mark.parametrize(("change", "message"), [
    (lambda history: _delivery(history)["payload"].__setitem__("sequence", 2), "reuses a reused step"),
    (lambda history: _delivery(history)["payload"].__setitem__("sequence", 1), "reuses a reused step"),
    (lambda history: _request(history).__setitem__("recorded_at", "2026-11-03T00:04:59+00:00"), "predates"),
    (lambda history: _request(history)["payload"].__setitem__("workflow_run_id", SOURCE_RUN), "another run"),
    (lambda history: _delivery(history)["payload"].__setitem__("workflow_run_id", SOURCE_RUN), "another run"),
    (lambda history: _request(history)["payload"].__setitem__("workflow_instance_id", "another-instance"),
     "another workflow instance"),
    (lambda history: _request(history)["payload"]["cancellation"].__setitem__("root_workflow_run_id", SOURCE_RUN),
     "rooted at the successor run"),
    (lambda history: _delivery(history)["payload"]["cancellation"]["lineage"][-1].__setitem__(
        "workflow_run_id", SOURCE_RUN), "rooted at the successor run"),
    (lambda history: _request(history)["payload"].__setitem__("reused_from_run_id", SOURCE_RUN),
     "reused events must directly follow"),
    (lambda history: event(history, "ActivityScheduled")["payload"].__setitem__("sequence", 2),
     "reuses a reused step"),
])
def test_the_successor_cancellation_is_its_own(change: Any, message: str) -> None:
    with pytest.raises(AssertionError, match=message):
        check(CANCELLATION, changed(CANCELLATION, change))
