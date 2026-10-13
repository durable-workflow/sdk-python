from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any
from uuid import UUID

import pytest

from durable_workflow import workflow
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext, replay


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
