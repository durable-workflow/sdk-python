from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import NonDeterministicReplayError
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

    def __init__(self) -> None:
        self.times: dict[str, str] = {}

    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        yield ctx.upsert_memo({"stage": "greeting", "customer": name})
        self.times["after_memo"] = ctx.now().isoformat()
        yield ctx.upsert_search_attributes({"Stage": "greeting"})
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
