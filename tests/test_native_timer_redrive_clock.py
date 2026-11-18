"""Source compatibility only; actual published producer/Worker proof is separate."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from durable_workflow import serializer
from durable_workflow.workflow import CompleteWorkflow, FailWorkflow, ScheduleActivity, replay
from tests.integration.redrive_timer_clock_workflow import RedriveTimerClockWorkflow

FIXTURE = Path(__file__).parent / "fixtures/replay_regressions/business-clock-core-redrive-acceptance-timer.json"
ORIGINAL_CLOCK = "2026-11-03T00:00:06.000000+00:00"


def successor() -> dict[str, Any]:
    # Reuse the immutable protocol sample without treating its Core provenance
    # as a native installed receipt. Bind this synthetic unit to the prepared
    # definition, whose source remains identical across all planned runs.
    document: dict[str, Any] = json.loads(FIXTURE.read_text())
    name = "tests.python-redrive-timer-clock"
    document["workflow"]["type"] = name
    for event in document["history"][:2]:
        event["payload"]["workflow_type"] = name
    document["history"][0]["payload"]["workflow_class"] = name
    return document


def commands(document: dict[str, Any]) -> list[Any]:
    task = document["workflow"]
    outcome = replay(
        RedriveTimerClockWorkflow, document["history"], task["input"],
        workflow_id=task["workflow_id"], run_id=task["run_id"], payload_codec="avro",
    )
    try:
        return list(outcome.commands)
    finally:
        outcome.close()


def assert_resumed_only_after_original_timer(document: dict[str, Any]) -> None:
    produced = commands(document)
    assert len(produced) == 1
    command = produced[0]
    assert isinstance(command, ScheduleActivity)
    assert command.activity_type == "tests.redrive.second"
    assert command.arguments == ["Hello, Taylor!", ORIGINAL_CLOCK]
    assert command.retry_policy.max_attempts == 1


def test_reused_activity_and_timer_resume_only_the_later_activity_on_fresh_reconstruction() -> None:
    document = successor()
    unchanged = deepcopy(document)
    assert_resumed_only_after_original_timer(document)
    assert_resumed_only_after_original_timer(document)
    assert document == unchanged


def test_repeated_redrive_preserves_the_first_timer_clock_and_immediate_predecessor() -> None:
    document = successor()
    previous = document["workflow"]["run_id"]
    document["workflow"]["run_id"] = "second-successor"
    for event in document["history"]:
        event["recorded_at"] = "2026-11-03T01:05:00+00:00"
        payload = event["payload"]
        if "workflow_run_id" in payload:
            payload["workflow_run_id"] = "second-successor"
        if "workflow_command_id" in payload:
            payload["workflow_command_id"] = "second-redrive-command"
        if "continued_from_run_id" in payload:
            payload["continued_from_run_id"] = previous
        if "workflow_link_id" in payload:
            payload["workflow_link_id"] = "second-redrive-link"
        if "reused_from_run_id" in payload:
            payload["reused_from_run_id"] = previous
        if "timer_id" in payload:
            payload["timer_id"] = "second-successor-timer"
        if "reused_activity_execution_id" in payload:
            payload["reused_activity_execution_id"] = payload["activity_execution_id"]
            payload["activity_execution_id"] = "second-successor-activity"
    assert_resumed_only_after_original_timer(document)


def test_missing_timer_reuse_time_cannot_borrow_successor_insertion_time() -> None:
    document = successor()
    document["history"][-1]["payload"].pop("reused_recorded_at")
    produced = commands(document)
    assert len(produced) == 1
    assert isinstance(produced[0], FailWorkflow)
    assert produced[0].exception_type == "ReplayClockUnavailable"


def test_timer_from_another_predecessor_cannot_supply_the_redrive_clock() -> None:
    document = successor()
    document["history"][-1]["payload"]["reused_from_run_id"] = "different-predecessor"
    produced = commands(document)
    assert len(produced) == 1
    assert isinstance(produced[0], FailWorkflow)
    assert produced[0].exception_type == "ReplayClockUnavailable"


def test_later_activity_completion_returns_the_original_timer_clock() -> None:
    document = successor()
    document["history"].append({
        "sequence": 6, "event_type": "ActivityCompleted", "recorded_at": "2026-11-03T00:05:03+00:00",
        "payload": {
            "sequence": 3, "activity_type": "tests.redrive.second", "activity_execution_id": "resumed-activity",
            "result": serializer.encode("resumed", codec="avro"), "payload_codec": "avro",
        },
    })
    produced = commands(document)
    assert len(produced) == 1
    assert isinstance(produced[0], CompleteWorkflow)
    assert produced[0].result == {
        "greeting": "Hello, Taylor!", "timer_clock": ORIGINAL_CLOCK, "resumed": "resumed",
    }


def test_fractional_source_timer_clock_requires_the_exact_reuse_timestamp() -> None:
    source_recorded_at = "2026-11-03T00:00:06.654321+00:00"
    source_time = datetime.fromisoformat(source_recorded_at)
    floored_recorded_at = source_time.replace(microsecond=0).isoformat()
    observed = []
    for reused_time in (floored_recorded_at, source_recorded_at):
        document = successor()
        fired = document["history"][-1]
        fired["payload"]["fired_at"] = source_recorded_at
        fired["payload"]["reused_recorded_at"] = reused_time
        unchanged = deepcopy(document)
        produced = commands(document)
        assert len(produced) == 1
        assert isinstance(produced[0], ScheduleActivity)
        assert produced[0].activity_type == "tests.redrive.second"
        observed.append(produced[0].arguments[1])
        assert document == unchanged
    # This is an offline producer-shaped prefix comparison. The reader follows
    # the authoritative reuse timestamp and cannot repair a producer's lost
    # fraction from the different fired_at field or successor insertion time.
    assert observed[0] == ORIGINAL_CLOCK
    assert observed[0] != source_time.isoformat(timespec="microseconds")
    assert observed[1] == source_time.isoformat(timespec="microseconds")
