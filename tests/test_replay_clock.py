from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext, query_state, replay


@workflow.defn(name="tests.replay.persisted-clock")
class PersistedClockWorkflow:
    def run(self, ctx: WorkflowContext) -> str:
        return ctx.now().isoformat(timespec="microseconds")


@workflow.defn(name="tests.replay.persisted-uuid7")
class PersistedUUID7Workflow:
    def run(self, ctx: WorkflowContext) -> str:
        return str(ctx.uuid7())


@workflow.defn(name="tests.replay.marker-clock")
class MarkerClockWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        before = ctx.now().isoformat(timespec="microseconds")
        version = yield ctx.get_version("clock-case", 0, 7)
        after = ctx.now().isoformat(timespec="microseconds")
        return [before, version, after, str(ctx.uuid7())]


def result(workflow_cls: type, history: list[dict[str, Any]]) -> Any:
    outcome = replay(workflow_cls, history, [], run_id="original-clock-run", payload_codec="avro")
    assert len(outcome.commands) == 1
    command = outcome.commands[0]
    assert isinstance(command, CompleteWorkflow)
    return command.result


def test_recorded_start_time_survives_repeated_cold_reads_and_later_marker() -> None:
    history = [
        {"event_type": "WorkflowStarted", "recorded_at": "2026-06-01T12:00:00.123456Z", "payload": {}},
        {"event_type": "VersionMarkerRecorded", "recorded_at": "2026-06-01T12:00:03.987654Z",
         "payload": {"sequence": 1, "change_id": "clock-case", "version": 7,
                     "min_supported": 0, "max_supported": 7}},
    ]
    original = deepcopy(history)
    first = result(MarkerClockWorkflow, history)
    second = result(MarkerClockWorkflow, history)
    assert first == second
    assert first[:3] == ["2026-06-01T12:00:00.123456+00:00", 7, "2026-06-01T12:00:00.123456+00:00"]
    assert UUID(first[3]).int >> 80 == int(datetime.fromisoformat(first[0]).timestamp() * 1000)
    assert history == original


def test_explicit_payload_time_retains_precedence() -> None:
    history = [{"event_type": "WorkflowStarted", "recorded_at": "2026-06-01T12:00:00.123456Z",
                "payload": {"timestamp": "2026-06-01T14:01:02.654321+02:00"}}]
    assert result(PersistedClockWorkflow, history) == "2026-06-01T14:01:02.654321+02:00"


@pytest.mark.parametrize("invalid", [None, False, 123, "", "not-a-time", "2026-06-01T12:00:00"])
@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("workflow_cls", [PersistedClockWorkflow, PersistedUUID7Workflow])
def test_clock_helpers_refuse_invalid_time_without_fallback(
    invalid: Any, explicit: bool, workflow_cls: type,
) -> None:
    event: dict[str, Any] = {"event_type": "WorkflowStarted", "payload": {}}
    if explicit:
        event["recorded_at"] = "2026-06-01T12:00:00.123456Z"
        event["payload"]["timestamp"] = invalid
    else:
        event["recorded_at"] = invalid
    with pytest.raises(NonDeterministicReplayError, match="Workflow replay clock unavailable"):
        result(workflow_cls, [event])


@pytest.mark.parametrize("history", [
    [{"event_type": "WorkflowStarted", "payload": {}}],
    [{"event_type": "SignalReceived", "payload": {"signal_name": "unhandled"}}],
])
def test_missing_replay_time_is_explicit(history: list[dict[str, Any]]) -> None:
    with pytest.raises(NonDeterministicReplayError, match="Workflow replay clock unavailable"):
        result(PersistedClockWorkflow, history)


def test_empty_local_authoring_remains_available() -> None:
    assert datetime.fromisoformat(result(PersistedClockWorkflow, [])).tzinfo is not None
    assert UUID(result(PersistedUUID7Workflow, [])).version == 7


@workflow.defn(name="tests.replay.redrive-clock")
class RedriveClockWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        before = ctx.now().isoformat(timespec="microseconds")
        token = str(ctx.uuid7())
        recorded = yield ctx.schedule_activity("tests.first", [before])
        return [before, token, recorded]


def redriven_history(**start_payload: Any) -> list[dict[str, Any]]:
    return [
        {"event_type": "WorkflowStarted", "recorded_at": "2026-06-01T10:05:00.000000Z", "payload": start_payload},
        {"event_type": "ActivityCompleted", "recorded_at": "2026-06-01T10:05:00.500000Z",
         "payload": {"sequence": 1, "activity_type": "tests.first",
                     "result": serializer.encode("recorded", codec="avro"), "payload_codec": "avro",
                     "reused_from_run_id": "failed-run",
                     "reused_recorded_at": "2026-06-01T10:00:01.000000Z"}},
    ]


def test_redriven_run_replays_from_the_failed_run_start_time() -> None:
    history = redriven_history(recovery_kind="redrive", replayed_started_at="2026-06-01T10:00:00.000000Z")
    before, token, recorded = result(RedriveClockWorkflow, history)
    assert before == "2026-06-01T10:00:00.000000+00:00"
    assert UUID(token).int >> 80 == int(datetime.fromisoformat(before).timestamp() * 1000)
    assert recorded == "recorded"


def test_replayed_start_time_requires_a_redrive_start() -> None:
    history = redriven_history(replayed_started_at="2026-06-01T10:00:00.000000Z")
    assert result(RedriveClockWorkflow, history)[0] == "2026-06-01T10:05:00.000000+00:00"


@pytest.mark.parametrize("invalid", [None, 123, "", "not-a-time", "2026-06-01T10:00:00"])
def test_invalid_replayed_start_time_is_refused_without_fallback(invalid: Any) -> None:
    history = redriven_history(recovery_kind="redrive", replayed_started_at=invalid)
    with pytest.raises(NonDeterministicReplayError, match="replayed_started_at"):
        replay(RedriveClockWorkflow, history, [], run_id="redriven-run", payload_codec="avro")


# Core 2.19 lets redrive reuse memo and search-attribute upserts (workflow#1001)
# and an applied signal wait (workflow#1002). Core replays now() after them at
# payload.reused_recorded_at. This SDK's now() is the persisted start time, so
# none of these five events is a clock source: on a redriven run now() is the
# failed run's start (WorkflowStarted.replayed_started_at), whatever time the
# reused events carry.
REDRIVE_FIXTURES = Path(__file__).parent / "fixtures" / "replay_regressions"
REDRIVE_SOURCE_RUN = "01JBREDRIVESOURCERUN000000"
REDRIVE_REPLAYED_START = "2026-11-03T00:00:00+00:00"


@workflow.defn(name="tests.replay.redrive-visibility")
class RedriveVisibilityWorkflow:
    """Python counterpart of Core's TestRedriveVisibilityWorkflow."""

    stage = "greeting"

    def __init__(self) -> None:
        self.times: dict[str, str] = {}

    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        yield ctx.upsert_memo({"stage": "greeting", "customer": name})
        self.times["after_memo"] = ctx.now().isoformat()
        yield ctx.upsert_search_attributes({"Stage": self.stage})
        self.times["after_search_attributes"] = ctx.now().isoformat()
        greeting = yield ctx.schedule_activity("tests.redrive.first", [name])
        return (yield ctx.schedule_activity("tests.redrive.second", [greeting]))

    @workflow.query("replay_times")
    def replay_times(self) -> dict[str, str]:
        return self.times


@workflow.defn(name="tests.replay.redrive-signal")
class RedriveSignalWorkflow:
    """Python counterpart of Core's TestRedriveSignalWorkflow.

    The SDK has no signal-wait primitive; a handler plus a condition waits for
    the signal. SignalWaitOpened and SignalApplied are preserved, not replayed.
    """

    applied: list[Any] = []

    def __init__(self) -> None:
        self.approved_by: Any = None
        self.time_after_signal: str | None = None

    @workflow.signal("approve")
    def approve(self, approved_by: Any) -> None:
        type(self).applied.append(approved_by)
        self.approved_by = approved_by

    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.approved_by is not None, key="approve")
        self.time_after_signal = ctx.now().isoformat()
        greeting = yield ctx.schedule_activity("tests.redrive.first", [name])
        return (yield ctx.schedule_activity("tests.redrive.second", [greeting]))

    @workflow.query("replay_state")
    def replay_state(self) -> dict[str, Any]:
        return {"approved_by": self.approved_by, "time_after_signal": self.time_after_signal}


@workflow.defn(name="tests.replay.redrive-visibility-changed-stage")
class RedriveChangedStageWorkflow(RedriveVisibilityWorkflow):
    """The visibility workflow after a deploy changed its search attribute."""

    stage = "approval"


@workflow.defn(name="tests.replay.redrive-visibility-cleanup")
class RedriveVisibilityCleanupWorkflow(RedriveVisibilityWorkflow):
    """Observes the cancellation budget when a cancel reaches the first activity."""

    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        try:
            return (yield from super().run(ctx, name))
        except WorkflowCancelled as error:
            assert error.context is not None
            return {**self.times, "remaining": error.context.remaining()}


@workflow.defn(name="tests.replay.redrive-signal-cleanup")
class RedriveSignalCleanupWorkflow(RedriveSignalWorkflow):
    """Observes the cancellation budget if a cancel reaches the first activity."""

    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        try:
            return (yield from super().run(ctx, name))
        except WorkflowCancelled as error:
            assert error.context is not None
            return {"approved_by": self.approved_by, "remaining": error.context.remaining()}


REUSED_UPSERT_EVENTS = ("MemoUpserted", "SearchAttributesUpserted")
REUSED_SIGNAL_EVENTS = ("SignalWaitOpened", "SignalReceived", "SignalApplied")


def redrive_fixture(name: str) -> dict[str, Any]:
    fixture = json.loads((REDRIVE_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))
    assert isinstance(fixture, dict)
    return fixture


def redriven(name: str) -> list[dict[str, Any]]:
    history = redrive_fixture(name)["history"]
    assert isinstance(history, list)
    return history


def only_event(history: list[dict[str, Any]], event_type: str) -> dict[str, Any]:
    matches = [event for event in history if event["event_type"] == event_type]
    assert len(matches) == 1, event_type
    return matches[0]


def replay_redriven(workflow_cls: type, history: list[dict[str, Any]]) -> list[Any]:
    outcome = replay(workflow_cls, history, ["Taylor"], run_id="redriven-run", payload_codec="avro")
    return list(outcome.commands)


def visibility_times(history: list[dict[str, Any]]) -> dict[str, str]:
    times = query_state(RedriveVisibilityWorkflow, history, ["Taylor"], "replay_times",
                        run_id="redriven-run", payload_codec="avro")
    assert isinstance(times, dict)
    return times


def signal_state(history: list[dict[str, Any]]) -> dict[str, Any]:
    state = query_state(RedriveSignalWorkflow, history, ["Taylor"], "replay_state",
                        run_id="redriven-run", payload_codec="avro")
    assert isinstance(state, dict)
    return state


@pytest.mark.parametrize(("fixture", "event_types", "declared"), [
    ("redrive-reused-upserts-clock", REUSED_UPSERT_EVENTS,
     lambda fixture: [step["event_type"] for step in fixture["redrive_reused_upserts"]["reused_steps"]]),
    ("redrive-reused-signal-wait-clock", REUSED_SIGNAL_EVENTS,
     lambda fixture: fixture["redrive_reused_signal_wait"]["reused_events"]),
])
def test_redrive_fixture_carries_reuse_provenance_on_each_named_event(
    fixture: str, event_types: tuple[str, ...], declared: Any,
) -> None:
    document = redrive_fixture(fixture)
    history = document["history"]
    assert declared(document) == list(event_types)
    started = only_event(history, "WorkflowStarted")["payload"]
    assert started["recovery_kind"] == "redrive"
    assert started["replayed_started_at"] == REDRIVE_REPLAYED_START
    for event_type in event_types:
        event = only_event(history, event_type)
        payload = event["payload"]
        assert payload["reused_from_run_id"] == REDRIVE_SOURCE_RUN, event_type
        assert payload["reused_recorded_at"] == "2026-11-03T00:00:00+00:00", event_type
        # Redrive recorded the event again at 00:05; the source run recorded it at 00:00.
        assert event["recorded_at"] == "2026-11-03T00:05:00+00:00", event_type


def test_redriven_upserts_replay_at_the_failed_run_start_without_reupserting() -> None:
    history = redriven("redrive-reused-upserts-clock")
    assert visibility_times(history) == {
        "after_memo": REDRIVE_REPLAYED_START,
        "after_search_attributes": REDRIVE_REPLAYED_START,
    }
    # Only the step after the reused upserts is new; neither upsert is issued again.
    commands = replay_redriven(RedriveVisibilityWorkflow, history)
    assert [(type(command).__name__, getattr(command, "activity_type", None)) for command in commands] == [
        ("ScheduleActivity", "tests.redrive.first"),
    ]
    assert commands[0].arguments == ["Taylor"]
    memo = only_event(history, "MemoUpserted")["payload"]
    assert serializer.decode_envelope(memo["entries"]) == {"customer": "Taylor", "stage": "greeting"}
    assert serializer.decode_envelope(memo["merged"]) == {"customer": "Taylor", "stage": "greeting"}
    attributes = only_event(history, "SearchAttributesUpserted")["payload"]
    assert attributes["attributes"] == attributes["merged"] == {"Stage": "greeting"}


def test_redriven_memo_value_cannot_change_on_replay() -> None:
    history = deepcopy(redriven("redrive-reused-upserts-clock"))

    @workflow.defn(name="tests.replay.redrive-visibility-changed")
    class ChangedMemo(RedriveVisibilityWorkflow):
        def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
            return (yield from super().run(ctx, "Someone else"))

    with pytest.raises(NonDeterministicReplayError, match="memo entries differ"):
        replay_redriven(ChangedMemo, history)


def test_redriven_signal_wait_replays_at_the_failed_run_start_and_applies_once() -> None:
    history = redriven("redrive-reused-signal-wait-clock")
    RedriveSignalWorkflow.applied = []
    assert signal_state(history) == {"approved_by": "Grace", "time_after_signal": REDRIVE_REPLAYED_START}
    assert RedriveSignalWorkflow.applied == ["Grace"]
    RedriveSignalWorkflow.applied = []
    commands = replay_redriven(RedriveSignalWorkflow, history)
    assert RedriveSignalWorkflow.applied == ["Grace"]
    assert [(type(command).__name__, getattr(command, "activity_type", None)) for command in commands] == [
        ("ScheduleActivity", "tests.redrive.first"),
    ]
    assert commands[0].arguments == ["Taylor"]
    applied = only_event(history, "SignalApplied")["payload"]
    assert serializer.decode(applied["value"], codec="avro") == "Grace"
    received = only_event(history, "SignalReceived")["payload"]
    assert serializer.decode(received["arguments"], codec="avro") == ["Grace"]


def _retimed(history: list[dict[str, Any]], event_type: str, mode: str) -> list[dict[str, Any]]:
    changed = deepcopy(history)
    event = only_event(changed, event_type)
    if mode == "recorded_at":
        event["recorded_at"] = "2026-11-03T09:09:09+00:00"
    elif mode == "reused_recorded_at":
        event["payload"]["reused_recorded_at"] = "2026-11-02T22:22:22+00:00"
    else:
        del event["payload"]["reused_recorded_at"]
        del event["payload"]["reused_from_run_id"]
    return changed


@pytest.mark.parametrize("mode", ["recorded_at", "reused_recorded_at", "without_reuse_keys"])
@pytest.mark.parametrize("event_type", REUSED_UPSERT_EVENTS)
def test_reused_upsert_time_is_not_a_replay_clock_source(event_type: str, mode: str) -> None:
    history = _retimed(redriven("redrive-reused-upserts-clock"), event_type, mode)
    assert visibility_times(history) == {
        "after_memo": REDRIVE_REPLAYED_START,
        "after_search_attributes": REDRIVE_REPLAYED_START,
    }
    assert replay_redriven(RedriveVisibilityWorkflow, history) == replay_redriven(
        RedriveVisibilityWorkflow, redriven("redrive-reused-upserts-clock"),
    )


@pytest.mark.parametrize("mode", ["recorded_at", "reused_recorded_at", "without_reuse_keys"])
@pytest.mark.parametrize("event_type", REUSED_SIGNAL_EVENTS)
def test_reused_signal_wait_time_is_not_a_replay_clock_source(event_type: str, mode: str) -> None:
    history = _retimed(redriven("redrive-reused-signal-wait-clock"), event_type, mode)
    assert signal_state(history) == {"approved_by": "Grace", "time_after_signal": REDRIVE_REPLAYED_START}
    assert replay_redriven(RedriveSignalWorkflow, history) == replay_redriven(
        RedriveSignalWorkflow, redriven("redrive-reused-signal-wait-clock"),
    )


# Cancellation controls. Redrive writes WorkflowStarted with the reused prefix
# in one write, so a cancellation is requested and delivered only after the
# prefix, at later times. The cancellation clock starts at the delivery and
# only moves on results consumed after it, so a reused event's time cannot be
# observed through remaining() in such a history. These controls prove that
# retiming each reused event changes neither remaining() nor the commands.
def _retime_both(history: list[dict[str, Any]], event_type: str) -> list[dict[str, Any]]:
    changed = deepcopy(history)
    event = only_event(changed, event_type)
    event["recorded_at"] = "2026-11-03T00:05:01+00:00"
    event["payload"]["reused_recorded_at"] = "2026-11-02T23:59:30+00:00"
    return changed


def replay_cleanup(workflow_cls: type, history: list[dict[str, Any]], fixture: dict[str, Any]) -> list[Any]:
    outcome = replay(workflow_cls, history, ["Taylor"], run_id=fixture["workflow"]["run_id"],
                     workflow_id=fixture["workflow"]["workflow_id"], payload_codec="avro")
    return list(outcome.commands)


def test_reused_upserts_do_not_move_the_cancellation_budget() -> None:
    fixture = redrive_fixture("redrive-reused-upserts-cancellation-clock")
    history = fixture["history"]
    kinds = [event["event_type"] for event in history]
    assert kinds.index("CooperativeCancellationRequested") > max(kinds.index(kind) for kind in REUSED_UPSERT_EVENTS)
    expected = [CompleteWorkflow({"after_memo": REDRIVE_REPLAYED_START,
                                  "after_search_attributes": REDRIVE_REPLAYED_START, "remaining": 26.0})]
    assert replay_cleanup(RedriveVisibilityCleanupWorkflow, history, fixture) == expected
    for event_type in REUSED_UPSERT_EVENTS:
        retimed = _retime_both(history, event_type)
        assert replay_cleanup(RedriveVisibilityCleanupWorkflow, retimed, fixture) == expected, event_type


def _signal_successor_with_cancellation() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    upserts = redrive_fixture("redrive-reused-upserts-cancellation-clock")
    # The same post-prefix request and delivery, bound to the signal workflow.
    instance = upserts["workflow"]["workflow_id"]
    tail = json.loads(json.dumps(upserts["history"][-2:]).replace(instance, "regression-corpus-redrive-signal"))
    upserts["workflow"]["workflow_id"] = "regression-corpus-redrive-signal"
    upserts["workflow"]["type"] = RedriveSignalCleanupWorkflow.__workflow_name__  # type: ignore[attr-defined]
    for event in tail:
        event["sequence"] += 1
    # The reused signal wait is workflow sequence 1, so the next call is 2.
    tail[1]["payload"]["sequence"] = 2
    history = redriven("redrive-reused-signal-wait-clock") + tail
    history[0]["payload"]["workflow_type"] = upserts["workflow"]["type"]
    return history, upserts


def test_reused_signal_wait_events_do_not_move_the_cancellation_budget() -> None:
    history, fixture = _signal_successor_with_cancellation()
    kinds = [event["event_type"] for event in history]
    assert kinds.index("CooperativeCancellationRequested") > max(kinds.index(kind) for kind in REUSED_SIGNAL_EVENTS)
    RedriveSignalCleanupWorkflow.applied = []
    baseline = replay_cleanup(RedriveSignalCleanupWorkflow, history, fixture)
    assert RedriveSignalCleanupWorkflow.applied == ["Grace"]
    # Limitation: the SDK authors no signal waits, so its counterpart's wait is
    # not a numbered call and its first activity is call 1. The delivery
    # recorded at call 2 is therefore not reached and no remaining() is read.
    # Such a history comes only from a Core-authored workflow.
    assert [type(command).__name__ for command in baseline] == ["ScheduleActivity"]
    for event_type in REUSED_SIGNAL_EVENTS:
        retimed = _retime_both(history, event_type)
        assert replay_cleanup(RedriveSignalCleanupWorkflow, retimed, fixture) == baseline, event_type


# A redrive successor must be one Workflow 2.19 can write: a redrive
# WorkflowStarted bound to its source run and successor identity, then the
# reused prefix in the same write, then the successor's own events at or after
# its start. Redrive resumes at the step after the last one it reused.
REDRIVE_START_IDENTITY = (
    "workflow_type", "workflow_instance_id", "workflow_run_id", "workflow_command_id", "workflow_link_id",
    "continued_from_run_id", "replayed_started_at",
)


def check_redrive_successor(history: list[dict[str, Any]], workflow: dict[str, Any]) -> None:
    """Require a successor Workflow 2.19 can write for this fixture's workflow."""

    def require(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(message)

    require(bool(history) and history[0].get("event_type") == "WorkflowStarted",
            "history must open with WorkflowStarted")
    started = history[0]
    start = started["payload"]
    require(start.get("recovery_kind") == "redrive", "recovery_kind")
    for key in REDRIVE_START_IDENTITY:
        require(isinstance(start.get(key), str) and bool(start[key].strip()), key)
    # The start binds the fixture's workflow type, instance and successor run.
    require(start["workflow_type"] == workflow.get("type"), "workflow_type must be the fixture workflow")
    require(start["workflow_instance_id"] == workflow.get("workflow_id"), "workflow_instance_id must be the fixture's")
    require(start["workflow_run_id"] == workflow.get("run_id"), "workflow_run_id must be the fixture's")
    source = start["continued_from_run_id"]
    require(start["workflow_run_id"] != source, "the successor is a new run")
    started_at = datetime.fromisoformat(started["recorded_at"])
    require(datetime.fromisoformat(start["replayed_started_at"]) <= started_at, "replayed_started_at")
    prefix = True
    last_reused_step = 0
    for index, event in enumerate(history[1:], start=2):
        require(event.get("sequence") == index, "history order")
        payload = event["payload"]
        require(payload.get("workflow_instance_id", start["workflow_instance_id"]) == start["workflow_instance_id"],
                "event belongs to another workflow instance")
        if "reused_from_run_id" in payload or "reused_recorded_at" in payload:
            require(prefix, "reused events must directly follow WorkflowStarted")
            for key in ("reused_from_run_id", "reused_recorded_at"):
                require(isinstance(payload.get(key), str) and bool(payload[key]), f"reused event lacks {key}")
            require(payload["reused_from_run_id"] == source, "reused_from_run_id must match continued_from_run_id")
            # A reused event keeps the run it was first recorded in.
            require(payload.get("workflow_run_id", source) == source, "reused event belongs to another run")
            require(event["recorded_at"] == started["recorded_at"], "reused events are recorded with the start")
            require(datetime.fromisoformat(payload["reused_recorded_at"]) <= started_at, "reused_recorded_at")
            # SignalReceived has no step of its own; it belongs to its wait.
            step = payload.get("sequence")
            if event["event_type"] != "SignalReceived" and isinstance(step, int):
                last_reused_step = max(last_reused_step, step)
        else:
            prefix = False
            require(payload.get("workflow_run_id", start["workflow_run_id"]) == start["workflow_run_id"],
                    "successor event belongs to another run")
            require(datetime.fromisoformat(event["recorded_at"]) >= started_at,
                    "successor event predates the successor start")
    require(last_reused_step > 0, "no reused workflow step")
    resume = start.get("resume_step_sequence")
    require(type(resume) is int and resume == last_reused_step + 1,
            "resume_step_sequence must follow the last reused step")


REDRIVE_SUCCESSOR_FIXTURES = {
    "redrive-reused-upserts-clock": 3,
    "redrive-reused-signal-wait-clock": 2,
    "redrive-reused-upserts-cancellation-clock": 3,
    "redrive-reused-search-attribute-value-mismatch": 3,
    "redrive-reused-search-attribute-type-mismatch": 3,
}


def test_every_reused_fixture_is_a_checked_redrive_successor() -> None:
    reused = sorted(
        path.stem for path in REDRIVE_FIXTURES.glob("*.json")
        if "reused_from_run_id" in path.read_text(encoding="utf-8")
    )
    assert reused == sorted(REDRIVE_SUCCESSOR_FIXTURES)


@pytest.mark.parametrize(("fixture", "resume"), REDRIVE_SUCCESSOR_FIXTURES.items())
def test_redrive_fixture_is_a_valid_successor(fixture: str, resume: int) -> None:
    document = redrive_fixture(fixture)
    assert document["history"][0]["payload"]["resume_step_sequence"] == resume
    check_redrive_successor(document["history"], document["workflow"])


def test_signal_successor_with_cancellation_is_a_valid_successor() -> None:
    history, fixture = _signal_successor_with_cancellation()
    check_redrive_successor(history, fixture["workflow"])


def _broken(fixture: str, change: Any) -> list[dict[str, Any]]:
    history = deepcopy(redriven(fixture))
    change(history)
    return history


def _set_start(key: str, value: Any) -> Any:
    return lambda history: history[0]["payload"].__setitem__(key, value)


def _drop_start(key: str) -> Any:
    return lambda history: history[0]["payload"].pop(key)


@pytest.mark.parametrize("fixture", ["redrive-reused-upserts-clock", "redrive-reused-signal-wait-clock"])
@pytest.mark.parametrize(("change", "message"), [
    (lambda resume: resume - 1, "resume_step_sequence"),
    (lambda resume: resume + 1, "resume_step_sequence"),
    (lambda resume: None, "resume_step_sequence"),
    (lambda resume: str(resume), "resume_step_sequence"),
])
def test_successor_check_rejects_a_wrong_resume_step(fixture: str, change: Any, message: str) -> None:
    resume = REDRIVE_SUCCESSOR_FIXTURES[fixture]
    with pytest.raises(AssertionError, match=message):
        check_redrive_successor(_broken(fixture, _set_start("resume_step_sequence", change(resume))),
                                redrive_fixture(fixture)["workflow"])


def _last_reused(history: list[dict[str, Any]]) -> dict[str, Any]:
    return [event for event in history if "reused_from_run_id" in event["payload"]][-1]


@pytest.mark.parametrize(("change", "message"), [
    (_set_start("recovery_kind", "continue_as_new"), "recovery_kind"),
    *[(_drop_start(key), key) for key in REDRIVE_START_IDENTITY],
    *[(_set_start(key, ""), key) for key in REDRIVE_START_IDENTITY],
    (_set_start("continued_from_run_id", "01JBREDRIVESUCCESSORRUN0000"), "new run"),
    (lambda history: _last_reused(history)["payload"].__setitem__("reused_from_run_id", "other-run"),
     "continued_from_run_id"),
    (lambda history: _last_reused(history)["payload"].pop("reused_recorded_at"), "lacks reused_recorded_at"),
    (lambda history: _last_reused(history)["payload"].__setitem__("reused_recorded_at", ""),
     "lacks reused_recorded_at"),
    (lambda history: _last_reused(history)["payload"].__setitem__("reused_from_run_id", None),
     "lacks reused_from_run_id"),
    (_set_start("workflow_type", "tests.replay.other"), "fixture workflow"),
    (_set_start("workflow_instance_id", "other-instance"), "workflow_instance_id must be the fixture"),
    (_set_start("workflow_run_id", "other-successor-run"), "workflow_run_id must be the fixture"),
    (_set_start("workflow_type", "   "), "workflow_type"),
    (lambda history: _last_reused(history).__setitem__("recorded_at", "2026-11-03T00:05:01+00:00"),
     "recorded with the start"),
    (lambda history: [history[1]["payload"].pop(key) for key in ("reused_from_run_id", "reused_recorded_at")],
     "directly follow"),
    (lambda history: history.append({"sequence": len(history) + 1, "event_type": "ActivityScheduled",
                                     "recorded_at": "2026-11-03T00:04:59+00:00", "payload": {"sequence": 3}}),
     "predates"),
    (lambda history: history[1].__setitem__("sequence", 9), "history order"),
])
def test_successor_check_rejects_an_invalid_envelope_or_order(change: Any, message: str) -> None:
    with pytest.raises(AssertionError, match=message):
        check_redrive_successor(_broken("redrive-reused-upserts-clock", change),
                                redrive_fixture("redrive-reused-upserts-clock")["workflow"])


def _event(history: list[dict[str, Any]], event_type: str) -> dict[str, Any]:
    return next(event for event in history if event["event_type"] == event_type)


@pytest.mark.parametrize(("fixture", "change", "message"), [
    ("redrive-reused-signal-wait-clock",
     lambda history: _event(history, "SignalReceived")["payload"].__setitem__("workflow_run_id", "other-run"),
     "reused event belongs to another run"),
    ("redrive-reused-signal-wait-clock",
     lambda history: _event(history, "SignalReceived")["payload"].__setitem__("workflow_instance_id", "other"),
     "another workflow instance"),
    ("redrive-reused-signal-wait-clock",
     lambda history: _event(history, "SignalApplied")["payload"].pop("reused_from_run_id"),
     "lacks reused_from_run_id"),
    ("redrive-reused-signal-wait-clock",
     lambda history: _event(history, "SignalWaitOpened")["payload"].pop("reused_recorded_at"),
     "lacks reused_recorded_at"),
    ("redrive-reused-upserts-cancellation-clock",
     lambda history: _event(history, "CooperativeCancellationRequested")["payload"].__setitem__(
         "workflow_run_id", REDRIVE_SOURCE_RUN),
     "successor event belongs to another run"),
    ("redrive-reused-upserts-cancellation-clock",
     lambda history: _event(history, "CooperativeCancellationRequested")["payload"].__setitem__(
         "workflow_instance_id", "other"),
     "another workflow instance"),
])
def test_successor_check_binds_every_event_to_the_run_and_instance(fixture: str, change: Any, message: str) -> None:
    check_redrive_successor(redriven(fixture), redrive_fixture(fixture)["workflow"])
    with pytest.raises(AssertionError, match=message):
        check_redrive_successor(_broken(fixture, change), redrive_fixture(fixture)["workflow"])
