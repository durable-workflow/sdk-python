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
from tests.test_replay_clock import REDRIVE_FIXTURES, check_redrive_successor, redrive_fixture, redrive_time

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
    return redrive_time(payload.get("reused_recorded_at") or item["recorded_at"])


def leaking_clock_remaining(history: list[dict[str, Any]]) -> float:
    """A clock that read every event up to the delivery, preferring reuse times."""
    delivered = next(index for index, item in enumerate(history)
                     if item["event_type"] == "CooperativeCancellationDelivered")
    request = event(history, "CooperativeCancellationRequested")
    deadline = redrive_time(request["payload"]["cleanup_deadline_at"])
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
    lambda history: _schedule(history)["payload"].__setitem__("workflow_run_id", "bogus-run"),
    lambda history: _schedule(history)["payload"].__setitem__("workflow_run_id", "01JBREDRIVESUCCESSORRUN0000"),
    lambda history: _schedule(history)["payload"].__setitem__("timer_kind", "condition_timeout"),
    lambda history: _schedule(history)["payload"].__setitem__("cancellation_scope_id", "scope-one"),
    lambda history: _schedule(history)["payload"].__setitem__("cancellation_cleanup", {"scope_id": "scope-one"}),
    lambda history: _schedule(history)["payload"].__setitem__("parallel_group_id", "group-one"),
    lambda history: _schedule(history)["payload"].__setitem__("condition_wait_id", "wait-one"),
    lambda history: _schedule(history)["payload"].__setitem__("signal_wait_id", "wait-one"),
], ids=["schedule-reused-from", "schedule-reused-at", "other-timer-id", "blank-timer-id", "other-step",
        "schedule-after-start", "schedule-after-fired", "no-schedule", "no-fired-timer", "bogus-run",
        "successor-run", "timer-kind", "cancellation-scope", "cancellation-cleanup", "parallel-group",
        "condition-wait", "signal-wait"])
def test_only_core_shaped_copied_timer_schedules_are_accepted(name: str, change: Any) -> None:
    with pytest.raises(AssertionError):
        check(name, changed(name, change))


def _delivery(history: list[dict[str, Any]]) -> dict[str, Any]:
    return event(history, "CooperativeCancellationDelivered")


def _request(history: list[dict[str, Any]]) -> dict[str, Any]:
    return event(history, "CooperativeCancellationRequested")


SOURCE_RUN = "01JBREDRIVESOURCERUN000000"


def _both_contexts(history: list[dict[str, Any]], change: Any) -> None:
    change(_request(history)["payload"]["cancellation"])
    change(_delivery(history)["payload"]["cancellation"])


@pytest.mark.parametrize(("change", "message"), [
    (lambda history: _delivery(history)["payload"].__setitem__("sequence", 2), "reuses a reused step"),
    (lambda history: _delivery(history)["payload"].__setitem__("sequence", 1), "reuses a reused step"),
    (lambda history: _request(history).__setitem__("recorded_at", "2026-11-03T00:04:59+00:00"), "predates"),
    (lambda history: _request(history)["payload"].__setitem__("workflow_run_id", SOURCE_RUN), "another run"),
    (lambda history: _delivery(history)["payload"].__setitem__("workflow_run_id", SOURCE_RUN), "another run"),
    (lambda history: _request(history)["payload"].__setitem__("workflow_instance_id", "another-instance"),
     "another workflow instance"),
    (lambda history: _both_contexts(history, lambda context: context.__setitem__("root_workflow_run_id", SOURCE_RUN)),
     "rooted at the successor run"),
    (lambda history: _both_contexts(history, lambda context: context["lineage"][-1].__setitem__(
        "workflow_run_id", SOURCE_RUN)), "rooted at the successor run"),
    (lambda history: _request(history)["payload"].__setitem__("reused_from_run_id", SOURCE_RUN),
     "reused events must directly follow"),
    (lambda history: event(history, "ActivityScheduled")["payload"].__setitem__("sequence", 2),
     "reuses a reused step"),
])
def test_the_successor_cancellation_is_its_own(change: Any, message: str) -> None:
    with pytest.raises(AssertionError, match=message):
        check(CANCELLATION, changed(CANCELLATION, change))


# The successor cancellation is exactly one request, then exactly one delivery
# of it, carrying one context bound to the successor, delivered to a call the
# successor reached. Each mutation below was accepted by the bd41be60 check.
CANCELLED_SUCCESSORS = [CANCELLATION, "redrive-successor-reused-upserts-cancellation-clock"]


def _duplicate(event_type: str) -> Any:
    def change(history: list[dict[str, Any]]) -> None:
        original = event(history, event_type)
        history.insert(history.index(original) + 1, deepcopy(original))
        _renumber(history)

    return change


def _without(event_type: str) -> Any:
    def change(history: list[dict[str, Any]]) -> None:
        history.remove(event(history, event_type))
        _renumber(history)

    return change


def _context(target: str, change: Any) -> Any:
    def apply(history: list[dict[str, Any]]) -> None:
        for item in ([_request(history)] if target in {"request", "both"} else []) + (
            [_delivery(history)] if target in {"delivery", "both"} else []
        ):
            change(item["payload"]["cancellation"])

    return apply


def _drop_context(target: str) -> Any:
    def apply(history: list[dict[str, Any]]) -> None:
        for item in ([_request(history)] if target in {"request", "both"} else []) + (
            [_delivery(history)] if target in {"delivery", "both"} else []
        ):
            del item["payload"]["cancellation"]

    return apply


CANCELLATION_MUTATIONS = {
    "context-deleted-from-both": (_drop_context("both"), "carries its cancellation context"),
    "context-deleted-from-request": (_drop_context("request"), "carries its cancellation context"),
    "context-deleted-from-delivery": (_drop_context("delivery"), "carries its request's context"),
    "duplicate-request": (_duplicate("CooperativeCancellationRequested"), "exactly one request and one delivery"),
    "duplicate-delivery": (_duplicate("CooperativeCancellationDelivered"), "exactly one request and one delivery"),
    "request-deleted": (_without("CooperativeCancellationRequested"), "exactly one request and one delivery"),
    "delivery-deleted": (_without("CooperativeCancellationDelivered"), "exactly one request and one delivery"),
    "mismatched-context": (_context("delivery", lambda context: context["requester"].__setitem__("id", "other")),
                           "carries its request's context"),
    "delivery-before-request-time": (
        lambda history: _delivery(history).__setitem__("recorded_at", "2026-11-03T00:05:00.750000+00:00"),
        "not earlier than its request"),
    "context-request-id": (_context("both", lambda context: context.__setitem__("request_id", "other-request")),
                           "the context is this request"),
    "context-root-request": (_context("both", lambda context: context.__setitem__("root_request_id", "other")),
                             "its own root"),
    "context-root-instance": (_context("both", lambda context: context.__setitem__(
        "root_workflow_instance_id", "another-instance")), "rooted at the successor run"),
    "lineage-request": (_context("both", lambda context: context["lineage"][-1].__setitem__("request_id", "other")),
                        "rooted at the successor run"),
    "context-deadline": (_context("both", lambda context: context.__setitem__(
        "cleanup_deadline_at", "2026-11-03T00:05:40.000000Z")), "cleanup deadline"),
    "delivery-to-unrecorded-step-4": (lambda history: _delivery(history)["payload"].__setitem__("sequence", 4),
                                      "a successor step the successor reached"),
}


@pytest.mark.parametrize("name", CANCELLED_SUCCESSORS)
@pytest.mark.parametrize("mutation", sorted(CANCELLATION_MUTATIONS))
def test_the_successor_cancellation_is_bound_exactly(name: str, mutation: str) -> None:
    change, message = CANCELLATION_MUTATIONS[mutation]
    if mutation == "context-root-request":
        # A root request: its context names itself as its root.
        assert redrive_fixture(name)["history"][-1]["payload"]["cancellation"]["parent_request_id"] is None
    with pytest.raises(AssertionError, match=message):
        check(name, changed(name, change))


def test_a_delivery_to_a_later_step_the_successor_recorded_is_accepted() -> None:
    # The successor finishes step 3 and schedules step 4 before the cancel.
    def later(history: list[dict[str, Any]]) -> None:
        scheduled = event(history, "ActivityScheduled")
        completed = {"event_type": "ActivityCompleted", "recorded_at": "2026-11-03T00:05:00.600000+00:00",
                     "payload": {**scheduled["payload"], "result": "wwHioz3/VYAiNwoEb2s=", "payload_codec": "avro"}}
        fourth = deepcopy(scheduled)
        fourth["recorded_at"] = "2026-11-03T00:05:00.700000+00:00"
        fourth["payload"].update(sequence=4, activity_execution_id="01JBREDRIVESUCCESSORACT002")
        at = history.index(scheduled) + 1
        history[at:at] = [completed, fourth]
        _delivery(history)["payload"]["sequence"] = 4
        _renumber(history)

    check(CANCELLATION, changed(CANCELLATION, later))


@pytest.mark.parametrize("name", ["redrive-successor-reused-timer", CANCELLATION])
@pytest.mark.parametrize("change", [
    lambda history: _schedule(history)["payload"].__setitem__("workflow_run_id", "01JBREDRIVESOURCERUN000000"),
    lambda history: _schedule(history)["payload"].__setitem__("timer_kind", None),
    lambda history: _schedule(history)["payload"].__setitem__("cancellation_scope_id", None),
], ids=["source-run", "null-timer-kind", "null-scope"])
def test_a_plain_copied_schedule_may_name_the_source_run(name: str, change: Any) -> None:
    # Core copies the source run's schedule payload; a null key is plain.
    check(name, changed(name, change))


# #239: the successor cancellation as Core 28893f97 writes it. Each mutation
# below was accepted by the 262eb6ef check and is refused here.
def _step_three_activity(history: list[dict[str, Any]]) -> dict[str, Any]:
    """The successor's step-3 ActivityScheduled, inserted before the request if absent."""
    scheduled = [item for item in history if item["event_type"] == "ActivityScheduled"]
    if scheduled:
        return scheduled[0]
    item = {"event_type": "ActivityScheduled", "recorded_at": _request(history)["recorded_at"],
            "payload": {"activity_execution_id": "01JBREDRIVESUCCESSORACT001",
                        "activity_type": "tests.redrive.second", "sequence": 3}}
    history.insert(history.index(_request(history)), item)
    _renumber(history)
    return item


def _completed_before_request(history: list[dict[str, Any]]) -> None:
    scheduled = _step_three_activity(history)
    completed = {"event_type": "ActivityCompleted", "recorded_at": _request(history)["recorded_at"],
                 "payload": {**scheduled["payload"], "result": "wwHioz3/VYAiNwoEb2s=", "payload_codec": "avro"}}
    history.insert(history.index(scheduled) + 1, completed)
    _renumber(history)


def _scheduled_after_delivery(history: list[dict[str, Any]]) -> None:
    scheduled = _step_three_activity(history)
    history.remove(scheduled)
    scheduled["recorded_at"] = _delivery(history)["recorded_at"]
    history.insert(history.index(_delivery(history)) + 1, scheduled)
    _renumber(history)


def _source_run_prepended(context: dict[str, Any]) -> None:
    context["lineage"].insert(0, {"request_id": "source-request", "workflow_instance_id":
                                  context["root_workflow_instance_id"], "workflow_run_id": SOURCE_RUN})


CORE_CANCELLATION_MUTATIONS = {
    # 1. Root binding: a parent no longer skips the root check.
    "parent-with-another-root": (_context("both", lambda context: context.update(
        parent_request_id="parent-request", root_request_id="parent-request")), "its own root"),
    "parent-request-id-missing": (_context("both", lambda context: context.pop("parent_request_id")), "its own root"),
    # 2. Lineage: the whole lineage is the request's own address.
    "lineage-source-run-prepended": (_context("both", _source_run_prepended), "rooted at the successor run"),
    "lineage-entry-repeated": (_context("both", lambda context: context["lineage"].append(
        dict(context["lineage"][0]))), "rooted at the successor run"),
    # 3. Delivery step.
    "delivery-to-a-call-completed-before-the-request": (_completed_before_request, "resolved before its request"),
    "schedule-after-a-delivery-to-the-resume-step": (_scheduled_after_delivery, "the successor reached"),
    # 4. Fields Core always writes.
    "request-command-type-missing": (lambda history: _request(history)["payload"].pop("command_type"),
                                     "command_type"),
    "request-command-type-other": (lambda history: _request(history)["payload"].__setitem__(
        "command_type", "request_termination"), "command_type"),
    "delivery-call-kind-missing": (lambda history: _delivery(history)["payload"].pop("call_kind"), "call_kind"),
    "delivery-call-kind-unknown": (lambda history: _delivery(history)["payload"].__setitem__(
        "call_kind", "workflow"), "call_kind"),
    "delivery-call-kind-parallel": (lambda history: _delivery(history)["payload"].__setitem__(
        "call_kind", "parallel"), "call_kind"),
    "delivery-sequence-span": (lambda history: _delivery(history)["payload"].__setitem__("sequence_span", 2),
                               "one single call"),
    "context-schema-missing": (_context("both", lambda context: context.pop("schema")), "schema"),
    "context-schema-unknown": (_context("both", lambda context: context.__setitem__(
        "schema", "durable-workflow.cancellation-context/v3")), "schema"),
    "context-schema-scoped": (_context("both", lambda context: context.__setitem__(
        "schema", "durable-workflow.cancellation-context/v2")), "schema"),
    "context-scope-origin": (_context("both", lambda context: context.__setitem__("scope_origin", {})), "schema"),
    "context-requested-at-missing": (_context("both", lambda context: context.pop("requested_at")),
                                     "history time must be a string"),
    "context-requested-after-the-request": (_context("both", lambda context: context.__setitem__(
        "requested_at", "2026-11-03T00:05:05.000000Z")), "requested_at"),
    "context-requested-before-the-start": (_context("both", lambda context: context.__setitem__(
        "requested_at", "2026-11-03T00:04:59.000000Z")), "requested_at"),
    "context-deadline-at-the-request": (lambda history: [
        target.__setitem__("cleanup_deadline_at", _request(history)["payload"]["cancellation"]["requested_at"])
        for target in (_request(history)["payload"], _request(history)["payload"]["cancellation"],
                       _delivery(history)["payload"]["cancellation"])], "cleanup deadline"),
    "context-requester-empty": (_context("both", lambda context: context.__setitem__("requester", {})),
                                "requester"),
    "context-requester-unknown-key": (_context("both", lambda context: context["requester"].__setitem__(
        "email", "operator@example.test")), "requester"),
    "context-source-blank": (_context("both", lambda context: context.__setitem__("source", " ")), "source"),
    "request-reason-differs": (lambda history: _request(history)["payload"].__setitem__("reason", "other"),
                               "reason"),
}


@pytest.mark.parametrize("name", CANCELLED_SUCCESSORS)
@pytest.mark.parametrize("mutation", sorted(CORE_CANCELLATION_MUTATIONS))
def test_the_successor_cancellation_has_core_shape(name: str, mutation: str) -> None:
    change, message = CORE_CANCELLATION_MUTATIONS[mutation]
    with pytest.raises(AssertionError, match=message):
        check(name, changed(name, change))


def test_a_delivery_to_a_recorded_call_names_its_kind() -> None:
    with pytest.raises(AssertionError, match="the recorded call's kind"):
        check(CANCELLATION, changed(CANCELLATION, lambda history: _delivery(history)["payload"].__setitem__(
            "call_kind", "timer")))


def _local_activity(call_kind: str) -> Any:
    def change(history: list[dict[str, Any]]) -> None:
        event(history, "ActivityScheduled")["payload"]["local_activity"] = True
        _delivery(history)["payload"]["call_kind"] = call_kind

    return change


def test_a_delivery_to_a_recorded_local_activity_names_local_activity() -> None:
    # Core callKindAt: an ActivityScheduled with local_activity true is a
    # local_activity call. b9210162 refused this valid delivery.
    check(CANCELLATION, changed(CANCELLATION, _local_activity("local_activity")))


def test_an_activity_delivery_to_a_recorded_local_activity_is_refused() -> None:
    # b9210162 accepted this delivery, which Core refuses.
    with pytest.raises(AssertionError, match="the recorded call's kind"):
        check(CANCELLATION, changed(CANCELLATION, _local_activity("activity")))


def test_a_local_activity_delivery_to_a_recorded_activity_is_refused() -> None:
    with pytest.raises(AssertionError, match="the recorded call's kind"):
        check(CANCELLATION, changed(CANCELLATION, lambda history: _delivery(history)["payload"].__setitem__(
            "call_kind", "local_activity")))


@pytest.mark.parametrize("name", CANCELLED_SUCCESSORS)
def test_a_whitespace_requester_value_is_accepted_as_core_does(name: str) -> None:
    # Core CancellationContext::fromArray refuses only an empty string.
    check(name, changed(name, _context("both", lambda context: context["requester"].__setitem__("label", " "))))


@pytest.mark.parametrize("call_kind", ["activity", "timer", "child", "service_operation"])
def test_a_delivery_to_the_unscheduled_resume_step_may_name_any_single_call(call_kind: str) -> None:
    # Core lets a delivery consume a call not yet scheduled; its shape is open.
    name = "redrive-successor-reused-upserts-cancellation-clock"
    check(name, changed(name, lambda history: _delivery(history)["payload"].__setitem__("call_kind", call_kind)))


def test_a_call_completed_after_the_request_is_still_deliverable() -> None:
    # Core resolvedBeforeRequest: only a resolution before the request refuses it.
    def completed_between(history: list[dict[str, Any]]) -> None:
        scheduled = _step_three_activity(history)
        completed = {"event_type": "ActivityCompleted", "recorded_at": _delivery(history)["recorded_at"],
                     "payload": {**scheduled["payload"], "result": "wwHioz3/VYAiNwoEb2s=", "payload_codec": "avro"}}
        history.insert(history.index(_delivery(history)), completed)
        _renumber(history)

    check(CANCELLATION, changed(CANCELLATION, completed_between))


def _cancelled_activity(history: list[dict[str, Any]], before_request: bool = False) -> dict[str, Any]:
    scheduled = _step_three_activity(history)
    boundary = _request(history) if before_request else _delivery(history)
    cancelled = {
        "event_type": "ActivityCancelled", "recorded_at": boundary["recorded_at"],
        "payload": {**scheduled["payload"], "cancelled_at": boundary["recorded_at"]},
    }
    history.insert(history.index(boundary), cancelled)
    _renumber(history)
    return cancelled


def test_an_activity_cancelled_after_the_request_is_still_deliverable() -> None:
    check(CANCELLATION, changed(CANCELLATION, _cancelled_activity))


def test_an_activity_cancelled_before_the_request_is_not_deliverable() -> None:
    with pytest.raises(AssertionError, match="resolved before its request"):
        check(CANCELLATION, changed(CANCELLATION, lambda history: _cancelled_activity(history, True)))


@pytest.mark.parametrize("key", ["sequence", "activity_type", "activity_execution_id"])
@pytest.mark.parametrize("missing", [False, True], ids=["blank", "missing"])
def test_activity_cancellation_requires_its_core_identity(key: str, missing: bool) -> None:
    def mutate(history: list[dict[str, Any]]) -> None:
        payload = _cancelled_activity(history)["payload"]
        if missing:
            del payload[key]
        else:
            payload[key] = 0 if key == "sequence" else " "

    with pytest.raises(AssertionError, match=f"ActivityCancelled lacks {key}"):
        check(CANCELLATION, changed(CANCELLATION, mutate))


def _resolve_activity_before_next_call(
    history: list[dict[str, Any]], resolution: str, before_request: bool,
) -> None:
    scheduled = _step_three_activity(history)
    boundary = _request(history) if before_request else _delivery(history)
    terminal = {
        "event_type": resolution, "recorded_at": boundary["recorded_at"],
        "payload": {**scheduled["payload"], "result": "wwHioz3/VYAiNwoEb2s=", "payload_codec": "avro"},
    }
    if resolution == "ActivityCancelled":
        terminal["payload"].update(cancelled_at=boundary["recorded_at"])
        terminal["payload"].pop("result")
        terminal["payload"].pop("payload_codec")
    history.insert(history.index(boundary), terminal)
    _delivery(history)["payload"]["sequence"] = 4
    _renumber(history)


@pytest.mark.parametrize("resolution", ["ActivityCompleted", "ActivityCancelled"])
@pytest.mark.parametrize("before_request", [False, True], ids=["after-request", "before-request"])
def test_an_unscheduled_next_call_after_a_resolved_activity_is_deliverable(
    resolution: str, before_request: bool,
) -> None:
    check(CANCELLATION, changed(CANCELLATION, lambda history: _resolve_activity_before_next_call(
        history, resolution, before_request)))


@pytest.mark.parametrize("first_resolved", [False, True], ids=["earlier-open", "all-resolved"])
def test_the_next_call_follows_every_recorded_successor_call(first_resolved: bool) -> None:
    def mutate(history: list[dict[str, Any]]) -> None:
        _resolve_activity_before_next_call(history, "ActivityCompleted", True)
        first = next(item for item in history if item["event_type"] == "ActivityCompleted"
                     and item["payload"]["sequence"] == 3)
        fourth = deepcopy(_step_three_activity(history))
        fourth["recorded_at"] = _request(history)["recorded_at"]
        fourth["payload"].update(sequence=4, activity_execution_id="fourth-activity")
        terminal = {**deepcopy(first), "payload": {**first["payload"], **fourth["payload"]}}
        at = history.index(_request(history))
        history[at:at] = [fourth, terminal]
        if not first_resolved:
            history.remove(first)
        _delivery(history)["payload"]["sequence"] = 5
        _renumber(history)

    if first_resolved:
        check(CANCELLATION, changed(CANCELLATION, mutate))
    else:
        with pytest.raises(AssertionError, match="the successor reached"):
            check(CANCELLATION, changed(CANCELLATION, mutate))


def test_the_next_call_follows_an_atomic_memo_step() -> None:
    def mutate(history: list[dict[str, Any]]) -> None:
        history.remove(_step_three_activity(history))
        memo = deepcopy(event(redrive_fixture("redrive-reused-upserts-clock")["history"], "MemoUpserted"))
        for key in ("reused_from_run_id", "reused_recorded_at"):
            memo["payload"].pop(key)
        memo["payload"]["sequence"] = 3
        memo["recorded_at"] = _request(history)["recorded_at"]
        history.insert(history.index(_request(history)), memo)
        _delivery(history)["payload"]["sequence"] = 4
        _renumber(history)

    check(CANCELLATION, changed(CANCELLATION, mutate))


@pytest.mark.parametrize(("opened", "resolved", "identity"), [
    ("TimerScheduled", "TimerFired", "timer_id"),
    ("SignalWaitOpened", "SignalApplied", "signal_wait_id"),
])
@pytest.mark.parametrize("state", ["resolved", "open", "foreign-resolution"])
def test_the_next_call_requires_a_resolution_of_the_recorded_timer_or_signal(
    opened: str, resolved: str, identity: str, state: str,
) -> None:
    def mutate(history: list[dict[str, Any]]) -> None:
        scheduled = _step_three_activity(history)
        scheduled["event_type"] = opened
        scheduled["payload"] = {"sequence": 3, identity: "successor-wait"}
        if opened == "SignalWaitOpened":
            scheduled["payload"]["signal_name"] = "approve"
        terminal = {"event_type": resolved, "recorded_at": _request(history)["recorded_at"],
                    "payload": dict(scheduled["payload"])}
        if opened == "SignalWaitOpened":
            terminal["payload"].update(workflow_command_id="signal-command", signal_id="successor-signal")
            received = {"event_type": "SignalReceived", "recorded_at": terminal["recorded_at"],
                        "payload": {**terminal["payload"],
                                    "workflow_instance_id": _request(history)["payload"]["workflow_instance_id"],
                                    "workflow_run_id": _request(history)["payload"]["workflow_run_id"]}}
            received["payload"].pop("sequence")
            history.insert(history.index(_request(history)), received)
        if state != "open":
            if state == "foreign-resolution":
                terminal["payload"][identity] = "another-wait"
            history.insert(history.index(_request(history)), terminal)
        _delivery(history)["payload"]["sequence"] = 4
        _renumber(history)

    if state == "resolved":
        check(CANCELLATION, changed(CANCELLATION, mutate))
    else:
        with pytest.raises(AssertionError, match="the successor reached|no opened wait"):
            check(CANCELLATION, changed(CANCELLATION, mutate))


@pytest.mark.parametrize("mutation", ["open", "resolved-after-delivery", "foreign-resolution", "skipped-step"])
def test_an_unscheduled_delivery_cannot_skip_an_unreached_call(mutation: str) -> None:
    def mutate(history: list[dict[str, Any]]) -> None:
        _delivery(history)["payload"]["sequence"] = 4
        if mutation == "open":
            return
        _resolve_activity_before_next_call(history, "ActivityCompleted", True)
        terminal = next(
            item for item in history if item["event_type"] == "ActivityCompleted"
            and item["payload"]["sequence"] == 3)
        if mutation == "resolved-after-delivery":
            history.remove(terminal)
            terminal["recorded_at"] = _delivery(history)["recorded_at"]
            history.append(terminal)
        elif mutation == "foreign-resolution":
            terminal["payload"]["activity_execution_id"] = "other-activity"
        else:
            _delivery(history)["payload"]["sequence"] = 5
        _renumber(history)

    with pytest.raises(AssertionError, match="the successor reached"):
        check(CANCELLATION, changed(CANCELLATION, mutate))


def test_selection_handle_delivery_remains_outside_the_successor_model() -> None:
    def mutate(history: list[dict[str, Any]]) -> None:
        _delivery(history)["payload"].update(call_kind="selection_handle", operation_sequence=3)

    with pytest.raises(AssertionError, match="single call kind"):
        check(CANCELLATION, changed(CANCELLATION, mutate))


@pytest.mark.parametrize("field", ["sequence_span", "operation_sequence", "operation_sequence_span"])
def test_group_and_selection_operands_remain_outside_the_successor_model(field: str) -> None:
    with pytest.raises(AssertionError, match="one single call"):
        check(CANCELLATION, changed(CANCELLATION, lambda history: _delivery(history)["payload"].__setitem__(field, 1)))
