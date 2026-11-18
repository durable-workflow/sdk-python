"""Schedule origin: the schedule fire that started a run, read from history (.github#243)."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from durable_workflow import ScheduleOrigin, serializer, workflow
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, WorkflowContext, replay


def origin_text(ctx: WorkflowContext) -> dict[str, Any] | None:
    origin = ctx.schedule_origin
    if origin is None:
        return None
    return {
        "schedule_id": origin.schedule_id,
        "occurrence_time": None if origin.occurrence_time is None else origin.occurrence_time.isoformat(),
        "trigger_number": origin.trigger_number,
    }


@workflow.defn(name="tests.schedule-origin.window")
class WindowWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        first = origin_text(ctx)
        origin = ctx.schedule_origin
        window = None
        if origin is not None and origin.occurrence_time is not None:
            end = origin.occurrence_time
            window = [(end - timedelta(days=1)).isoformat(), end.isoformat()]
        yield ctx.schedule_activity("reports.build", [window])
        return {"first": first, "after_activity": origin_text(ctx), "window": window}


def started(**payload: Any) -> dict[str, Any]:
    return {"event_type": "WorkflowStarted", "recorded_at": "2026-11-18T00:00:07.250000Z",
            "payload": {"workflow_type": "tests.schedule-origin.window", **payload}}


def triggered(**payload: Any) -> dict[str, Any]:
    return {"event_type": "ScheduleTriggered", "recorded_at": "2026-11-18T00:00:07.250000Z",
            "payload": {"schedule_id": "daily-report", "schedule_ulid": "01JSCHEDULE", "overlap_policy": "skip",
                        "cron_expression": "0 0 * * *", "timezone": "UTC", **payload}}


def completed() -> list[dict[str, Any]]:
    return [
        {"event_type": "ActivityScheduled", "payload": {
            "sequence": 1, "activity_execution_id": "report", "activity_type": "reports.build",
        }},
        {"event_type": "ActivityCompleted", "recorded_at": "2026-11-18T00:05:00Z", "payload": {
            "sequence": 1, "activity_execution_id": "report", "activity_type": "reports.build",
            "result": serializer.envelope("done"),
        }},
    ]


def first_task(events: list[dict[str, Any]]) -> ScheduleActivity:
    outcome = replay(WindowWorkflow, events, [], payload_codec="avro")
    try:
        assert len(outcome.commands) == 1
        command = outcome.commands[0]
        assert isinstance(command, ScheduleActivity), command
        return command
    finally:
        outcome.close()


def final(events: list[dict[str, Any]]) -> Any:
    outcome = replay(WindowWorkflow, events, [], payload_codec="avro")
    try:
        assert len(outcome.commands) == 1
        command = outcome.commands[0]
        assert isinstance(command, CompleteWorkflow), command
        return command.result
    finally:
        outcome.close()


# A jittered tick fired at 00:00:03.5 for the 00:00 cron slot; the run started
# later still (07.25), and neither is the recorded occurrence.
TICK = "2026-11-18T00:00:03.500000+00:00"
# A backfill enumerates a past occurrence; the run starts days later.
BACKFILL = "2026-11-01T00:00:00.000000+00:00"


@pytest.mark.parametrize("occurrence,trigger,window", [
    (TICK, 12, ["2026-11-17T00:00:03.500000+00:00", TICK]),
    (BACKFILL, 1, ["2026-10-31T00:00:00+00:00", "2026-11-01T00:00:00+00:00"]),
])
def test_scheduled_run_reads_its_occurrence_on_the_first_task_and_on_cold_replay(
    occurrence: str, trigger: int, window: list[str],
) -> None:
    head = [started(), triggered(trigger_number=trigger, occurrence_time=occurrence)]
    assert first_task(head).arguments == [window]

    events = head + completed()
    original = deepcopy(events)
    expected = {"schedule_id": "daily-report", "occurrence_time": datetime.fromisoformat(occurrence).isoformat(),
                "trigger_number": trigger}
    result = final(events)
    assert result == {"first": expected, "after_activity": expected, "window": window}
    # Cold replay of the same history reads the identical origin.
    assert final(events) == result
    assert events == original


def test_occurrence_is_timezone_aware_and_not_the_start_or_wall_time() -> None:
    events = [started(), triggered(trigger_number=3, occurrence_time="2026-11-18T00:00:03.500000Z")]
    seen: list[ScheduleOrigin | None] = []

    @workflow.defn(name="tests.schedule-origin.capture")
    class Capture:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            seen.append(ctx.schedule_origin)
            assert ctx.now() == datetime(2026, 11, 18, 0, 0, 7, 250000, tzinfo=timezone.utc)
            return None
            yield  # pragma: no cover

    replay(Capture, events, [], payload_codec="avro").close()
    assert seen == [ScheduleOrigin("daily-report", datetime(2026, 11, 18, 0, 0, 3, 500000, tzinfo=timezone.utc), 3)]


def test_non_scheduled_run_reads_none() -> None:
    assert first_task([started()]).arguments == [None]
    result = final([started(), *completed()])
    assert result == {"first": None, "after_activity": None, "window": None}


def test_manual_trigger_has_schedule_id_without_occurrence() -> None:
    result = final([started(), triggered(trigger_number=4), *completed()])
    expected = {"schedule_id": "daily-report", "occurrence_time": None, "trigger_number": 4}
    assert result == {"first": expected, "after_activity": expected, "window": None}


def test_continue_as_new_successor_reads_the_recorded_origin() -> None:
    # Server #1143 copies the source run's ScheduleTriggered into the successor.
    events = [started(continued_from_run_id="run-1"), triggered(trigger_number=1, occurrence_time=BACKFILL),
              *completed()]
    assert final(events)["first"] == {"schedule_id": "daily-report",
                                      "occurrence_time": "2026-11-01T00:00:00+00:00", "trigger_number": 1}


@pytest.mark.parametrize("payload", [
    {"schedule_id": ""},
    {"schedule_id": 7},
    {},
])
def test_unusable_marker_reads_none(payload: dict[str, Any]) -> None:
    event = {"event_type": "ScheduleTriggered", "payload": payload}
    assert final([started(), event, *completed()])["first"] is None


def test_unparseable_occurrence_and_trigger_are_absent_not_guessed() -> None:
    events = [started(), triggered(trigger_number=0, occurrence_time="2026-11-01T00:00:00"), *completed()]
    assert final(events)["first"] == {"schedule_id": "daily-report", "occurrence_time": None, "trigger_number": None}
    events[1]["payload"]["occurrence_time"] = "yesterday"
    events[1]["payload"]["trigger_number"] = True
    assert final(events)["first"] == {"schedule_id": "daily-report", "occurrence_time": None, "trigger_number": None}


def test_live_context_without_history_has_no_origin() -> None:
    assert WorkflowContext(workflow_id="wf", run_id="run").schedule_origin is None
