from __future__ import annotations

from typing import Any

import pytest

from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.workflow import CompleteWorkflow, replay
from tests.business_clock_workflows import PrefixedCoreTimerBusinessClockWorkflow
from tests.test_replay_clock import redrive_fixture

ORIGINAL = "2026-11-03T00:00:06.000000+00:00"
INSERTED = "2026-11-03T00:05:00.000000+00:00"
REUSE_FIELDS = ("reused_from_run_id", "reused_recorded_at")


def fixture(prefixed: bool) -> dict[str, Any]:
    document = redrive_fixture("business-clock-core-redrive-acceptance-timer")
    if not prefixed:
        document["history"].pop(0)
        for index, event in enumerate(document["history"], start=1):
            event["sequence"] = index
    return document


def result(document: dict[str, Any]) -> dict[str, Any]:
    task = document["workflow"]
    outcome = replay(PrefixedCoreTimerBusinessClockWorkflow, document["history"], task["input"],
                     workflow_id=task["workflow_id"], run_id=task["run_id"], payload_codec="avro")
    try:
        assert len(outcome.commands) == 1
        command = outcome.commands[0]
        assert isinstance(command, CompleteWorkflow), command
        return dict(command.result)
    finally:
        outcome.close()


@pytest.mark.parametrize("prefixed", [False, True])
@pytest.mark.parametrize("missing", [REUSE_FIELDS, REUSE_FIELDS[:1], REUSE_FIELDS[1:]])
def test_prefix_before_resume_requires_both_reuse_fields_without_insertion_time_fallback(
    prefixed: bool, missing: tuple[str, ...],
) -> None:
    document = fixture(prefixed)
    expected = result(document)
    assert expected["clock"][-1] == ORIGINAL
    for key in missing:
        document["history"][-1]["payload"].pop(key)
    expected["clock"][-1] = None
    assert result(document) == expected
    assert result(document) == expected  # Cold reconstruction repeats the refusal.


@pytest.mark.parametrize("prefixed", [False, True])
@pytest.mark.parametrize("missing", ["workflow_command_id", "continued_from_run_id", "workflow_link_id",
                                   "workflow_run_id", "resume_step_sequence", "recovery_kind", "replayed_started_at"])
def test_unbound_redrive_header_cannot_turn_an_unmarked_timer_into_an_ordinary_result(
    prefixed: bool, missing: str,
) -> None:
    document = fixture(prefixed)
    started = next(event for event in document["history"] if event["event_type"] == "WorkflowStarted")
    started["payload"].pop(missing)
    for key in REUSE_FIELDS:
        document["history"][-1]["payload"].pop(key)
    assert result(document)["clock"] == [None] * 3


@pytest.mark.parametrize("prefixed", [False, True])
def test_bound_post_resume_timer_without_reuse_fields_uses_its_own_consumed_time(prefixed: bool) -> None:
    document = fixture(prefixed)
    started = next(event for event in document["history"] if event["event_type"] == "WorkflowStarted")
    started["payload"]["resume_step_sequence"] = 2
    for key in REUSE_FIELDS:
        document["history"][-1]["payload"].pop(key)
    assert result(document)["clock"][-1] == INSERTED


@pytest.mark.parametrize("prefix_fault", ["outcome", "identity"])
def test_unbound_acceptance_prefix_does_not_lend_timer_insertion_time(prefix_fault: str) -> None:
    document = fixture(True)
    for key in REUSE_FIELDS:
        document["history"][-1]["payload"].pop(key)
    if prefix_fault == "outcome":
        document["history"][0]["payload"]["outcome"] = "started"
    else:
        document["history"][0]["payload"]["workflow_run_id"] = "foreign"
    assert result(document)["clock"] == [None] * 3


def test_missing_start_retains_the_original_legacy_clock_error() -> None:
    document = fixture(True)
    document["history"].pop(1)
    with pytest.raises(NonDeterministicReplayError, match="missing WorkflowStarted persisted start time"):
        result(document)


@pytest.mark.parametrize("prefixed", [False, True])
@pytest.mark.parametrize("alias", ["WorkflowTaskCompleted", "StartAccepted"])
def test_conflicting_start_kind_alias_cannot_hide_an_unbound_redrive_header(prefixed: bool, alias: str) -> None:
    document = fixture(prefixed)
    started = next(event for event in document["history"] if event["event_type"] == "WorkflowStarted")
    started["type"] = alias
    for key in REUSE_FIELDS:
        document["history"][-1]["payload"].pop(key)
    assert result(document) == {"clock": [None] * 3, "legacy": "2026-11-03T00:00:00.000000+00:00"}


def test_pre_resume_activity_with_a_schedule_cannot_use_insertion_time_when_reuse_fields_are_missing() -> None:
    document = fixture(False)
    terminal = document["history"][1]
    for key in (*REUSE_FIELDS, "reused_activity_execution_id"):
        terminal["payload"].pop(key)
    document["history"].insert(1, {
        "event_type": "ActivityScheduled", "recorded_at": terminal["recorded_at"],
        "payload": {key: terminal["payload"][key] for key in ("sequence", "activity_type", "activity_execution_id")},
    })
    for index, event in enumerate(document["history"], start=1):
        event["sequence"] = index
    assert result(document)["clock"] == ["2026-11-03T00:00:00.000000+00:00", None, ORIGINAL]


@pytest.mark.parametrize("continued", [False, True])
def test_ordinary_and_continued_histories_without_redrive_metadata_keep_their_own_times(continued: bool) -> None:
    from tests.test_business_replay_clock import history
    from tests.test_business_replay_clock import result as scalar_result

    events = history()
    if continued:
        events[0]["payload"]["continued_from_run_id"] = "previous-run"
    assert scalar_result(events)["clock"] == [
        None, "2026-11-01T00:00:03.123456+00:00", "2026-11-01T00:00:05.654321+00:00",
    ]


@pytest.mark.parametrize("marker", ["reused_from_run_id", "reused_recorded_at", "reused_activity_execution_id"])
def test_ordinary_success_with_a_stray_reuse_marker_has_no_proved_source_time(marker: str) -> None:
    from tests.test_business_replay_clock import history
    from tests.test_business_replay_clock import result as scalar_result

    events = history()
    events[2]["payload"][marker] = "unbound"
    assert scalar_result(events)["clock"][1] is None
