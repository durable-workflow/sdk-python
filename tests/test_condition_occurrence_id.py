from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.workflow import (
    CompleteWorkflow,
    WaitCondition,
    WorkflowContext,
    commands_to_server_commands,
    replay,
)
from tests.test_durable_selection import (
    DurableSelectionConditionTerminalWorkflow,
    _condition_terminal_selection_history,
)
from tests.test_wait_condition import OrphanedDuplicateDecision, _orphaned_duplicate_history

ROOT = Path(__file__).resolve().parent.parent


def _event(kind: str, seconds: int, **payload: Any) -> dict[str, Any]:
    at = datetime(2026, 11, 4, tzinfo=timezone.utc) + timedelta(seconds=seconds)
    return {"event_type": kind, "recorded_at": at.isoformat(), "payload": payload}


START = _event("WorkflowStarted", 0)


def _opened(wait_id: str, sequence: int, seconds: int, occurrence: str | None, timeout: int = 60) -> dict[str, Any]:
    event = _event("ConditionWaitOpened", seconds, condition_wait_id=wait_id, sequence=sequence,
                   condition_key="approval", timeout_seconds=timeout)
    if occurrence is not None:
        event["payload"]["condition_wait_occurrence_id"] = occurrence
    return event


def _signal(name: str, sequence: int, seconds: int) -> dict[str, Any]:
    return _event("SignalReceived", seconds, signal_name=name, workflow_sequence=sequence,
                  value=serializer.envelope([]), payload_codec="avro")


def _satisfied(wait_id: str, sequence: int, seconds: int) -> dict[str, Any]:
    return _event("ConditionWaitSatisfied", seconds, condition_wait_id=wait_id, sequence=sequence)


@workflow.defn(name="tests.condition-occurrence.approval")
class OccurrenceApproval:
    def __init__(self) -> None:
        self.ready = False

    @workflow.signal("note")
    def note(self) -> None:
        return None

    @workflow.signal("approve")
    def approve(self) -> None:
        self.ready = True

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.ready, key="approval", timeout=60)
        return "approved"


@workflow.defn(name="tests.condition-occurrence.two-calls")
class TwoOccurrenceCalls(OccurrenceApproval):
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.ready, key="approval", timeout=60)
        self.ready = False
        yield ctx.wait_condition(lambda: self.ready, key="second", timeout=30)
        return "approved"


@workflow.defn(name="tests.condition-occurrence.no-timeout")
class NoTimeoutOccurrence(OccurrenceApproval):
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.ready, key="approval")
        return "approved"


@workflow.defn(name="tests.condition-occurrence.grace")
class OccurrenceGrace(OccurrenceApproval):
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        ready = yield ctx.wait_condition(lambda: self.ready, key="approval", timeout=10)
        if not ready:
            yield ctx.wait_condition(lambda: self.ready, key="approval", timeout=30)
        return "approved"


@workflow.defn(name="tests.condition-occurrence.transient")
class TransientThenOccurrence:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: True)
        yield ctx.wait_condition(lambda: False)


@workflow.defn(name="tests.condition-occurrence.selection")
class SelectionOccurrences:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.select({
            "first": ctx.wait_condition(lambda: False, key="first"),
            "nested": [ctx.wait_condition(lambda: False, key="nested"), ctx.start_timer(5)],
            "last": ctx.wait_condition(lambda: False, key="last"),
        })


def test_occurrences_count_authored_calls_and_reset_for_each_run() -> None:
    first = WorkflowContext(run_id="one")
    second = WorkflowContext(run_id="two")
    commands = [first.wait_condition(lambda: False), first.wait_condition(lambda: False),
                second.wait_condition(lambda: False)]
    assert [command.to_server_command("q")["condition_wait_occurrence_id"] for command in commands] == [
        "python:condition-wait:0", "python:condition-wait:1", "python:condition-wait:0",
    ]


def test_a_transient_satisfied_call_still_owns_its_ordinal() -> None:
    (command,) = replay(TransientThenOccurrence, [], []).commands
    assert isinstance(command, WaitCondition)
    assert command.to_server_command("q")["condition_wait_occurrence_id"] == "python:condition-wait:1"


def test_explicit_occurrence_is_serialized() -> None:
    command = WaitCondition(lambda: False, condition_wait_occurrence_id="recorded-occurrence")
    assert command.to_server_command("q") == {
        "type": "open_condition_wait", "condition_wait_occurrence_id": "recorded-occurrence",
    }


def test_a_false_wake_reuses_the_original_occurrence_and_deadline() -> None:
    history = [START, _opened("w1", 1, 0, "python:condition-wait:0"), _signal("note", 1, 10)]
    (command,) = replay(OccurrenceApproval, history, []).commands
    assert isinstance(command, WaitCondition)
    wire = command.to_server_command("q")
    assert wire["condition_wait_occurrence_id"] == "python:condition-wait:0"
    assert wire["timeout_seconds"] == 50


@pytest.mark.parametrize("first", [None, "unbounded-first-id"])
def test_a_no_timeout_reopen_without_history_clocks_keeps_its_first_id(first: str | None) -> None:
    opened = _opened("w1", 1, 0, first)
    opened["payload"].pop("timeout_seconds")
    history = [dict(START), opened, _signal("note", 1, 10)]
    for event in history:
        event.pop("recorded_at", None)
    (command,) = replay(NoTimeoutOccurrence, history, []).commands
    assert isinstance(command, WaitCondition)
    wire = command.to_server_command("q")
    assert wire.get("condition_wait_occurrence_id") == first
    assert ("condition_wait_occurrence_id" in wire) == bool(first)
    assert "timeout_seconds" not in wire


@pytest.mark.parametrize(("first", "later"), [
    (None, None), (None, "new-worker-id"), ("", "new-worker-id"),
    ("first-open-id", None), ("first-open-id", "different-later-id"),
])
def test_upgrade_crossing_keeps_the_first_open_id_or_its_absence(first: str | None, later: str | None) -> None:
    history = [START, _opened("w1", 1, 0, first), _signal("note", 1, 10), _satisfied("w1", 1, 10),
               _opened("w2", 2, 11, later, timeout=50), _signal("note", 2, 20)]
    (command,) = replay(OccurrenceApproval, history, []).commands
    assert isinstance(command, WaitCondition)
    wire = command.to_server_command("q")
    assert wire.get("condition_wait_occurrence_id") == (first or None)
    assert ("condition_wait_occurrence_id" in wire) == bool(first)
    assert wire["timeout_seconds"] == 40


@pytest.mark.parametrize("first", [None, "original-orphan-id"])
def test_an_orphaned_duplicate_chain_keeps_its_first_open_id(first: str | None) -> None:
    history = _orphaned_duplicate_history(180, True)
    opens = [event for event in history if event["event_type"] == "ConditionWaitOpened"]
    if first is not None:
        opens[0]["payload"]["condition_wait_occurrence_id"] = first
    for event in opens[1:]:
        event["payload"]["condition_wait_occurrence_id"] = "later-duplicate-id"
    (command,) = replay(OrphanedDuplicateDecision, history, []).commands
    assert isinstance(command, WaitCondition)
    wire = command.to_server_command("q")
    assert wire.get("condition_wait_occurrence_id") == first
    assert ("condition_wait_occurrence_id" in wire) == bool(first)
    assert wire["timeout_seconds"] == 0


def test_the_next_authored_call_after_a_false_wake_gets_a_new_ordinal() -> None:
    history = [START, _opened("w1", 1, 0, "python:condition-wait:0"), _signal("note", 1, 10),
               _satisfied("w1", 1, 10), _opened("w2", 2, 11, "python:condition-wait:0", timeout=50),
               _signal("approve", 2, 20), _satisfied("w2", 2, 20)]
    (command,) = replay(TwoOccurrenceCalls, history, []).commands
    assert isinstance(command, WaitCondition)
    wire = command.to_server_command("q")
    assert wire["condition_wait_occurrence_id"] == "python:condition-wait:1"
    assert wire["condition_key"] == "second"
    assert wire["timeout_seconds"] == 30


def test_a_grace_wait_after_timeout_gets_a_new_ordinal() -> None:
    history = [START, _opened("w1", 1, 0, "python:condition-wait:0", timeout=10),
               _event("TimerFired", 10, timer_kind="condition_timeout", condition_wait_id="w1", sequence=1)]
    (command,) = replay(OccurrenceGrace, history, []).commands
    assert isinstance(command, WaitCondition)
    wire = command.to_server_command("q")
    assert wire["condition_wait_occurrence_id"] == "python:condition-wait:1"
    assert wire["timeout_seconds"] == 30


def test_selection_members_use_authoring_order_including_nested_members() -> None:
    wire = commands_to_server_commands(replay(SelectionOccurrences, [], []).commands, "q")
    assert [command["condition_wait_occurrence_id"] for command in wire
            if command["type"] == "open_condition_wait"] == [
        "python:condition-wait:0", "python:condition-wait:1", "python:condition-wait:2",
    ]


@pytest.mark.parametrize("recorded", [None, "foreign-selection-id"])
def test_a_recorded_selection_does_not_refuse_an_old_or_foreign_occurrence(recorded: str | None) -> None:
    history = _condition_terminal_selection_history("ConditionWaitSatisfied", "valid")
    if recorded is not None:
        history[0]["payload"]["condition_wait_occurrence_id"] = recorded
    (command,) = replay(DurableSelectionConditionTerminalWorkflow, history, []).commands
    assert command == CompleteWorkflow({"winner": "condition", "winner_value": True})


def test_occurrences_are_deterministic_in_fresh_replay_processes() -> None:
    script = (
        "import json; from durable_workflow.workflow import replay, commands_to_server_commands;"
        "from tests.test_condition_occurrence_id import SelectionOccurrences;"
        "print(json.dumps([c['condition_wait_occurrence_id'] for c in "
        "commands_to_server_commands(replay(SelectionOccurrences, [], []).commands, 'q') "
        "if c['type'] == 'open_condition_wait']))"
    )
    for _ in range(2):
        result = subprocess.run([sys.executable, "-c", script], check=True, capture_output=True, text=True,
                                cwd=ROOT, env={**os.environ, "PYTHONPATH": str(ROOT / "src")})
        assert json.loads(result.stdout) == [
            "python:condition-wait:0", "python:condition-wait:1", "python:condition-wait:2",
        ]
