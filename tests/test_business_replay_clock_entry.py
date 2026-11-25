"""The WorkerReplayClock v1 entry carrier (server#1156, durable-workflow/.github#215)."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import Worker, serializer
from durable_workflow.client import Client
from durable_workflow.replay_clock import EntryInstant, entry_instant
from durable_workflow.workflow import CompleteWorkflow, replay
from tests.business_clock_workflows import NativeRedriveBusinessClockWorkflow, ScalarBusinessClockWorkflow
from tests.test_business_replay_clock import history

RUN = "business-clock-run"
START = "2026-11-01T00:00:00.000000+00:00"


def carrier(**changes: Any) -> dict[str, Any]:
    return {
        "schema": "durable-workflow.worker-replay-clock/v1", "workflow_run_id": RUN,
        "started_at": "2026-11-01T00:00:00.000000Z", "source": "run_started_at", **changes,
    }


def result(events: list[dict[str, Any]], entry: Any, workflow: type = ScalarBusinessClockWorkflow,
           run_id: str = RUN) -> Any:
    outcome = replay(workflow, events, [], payload_codec="avro", run_id=run_id, replay_clock_entry=entry)
    try:
        assert len(outcome.commands) == 1
        command = outcome.commands[0]
        assert isinstance(command, CompleteWorkflow), command
        return command.result
    finally:
        outcome.close()


def test_entry_read_returns_the_carried_start_and_replays_identically() -> None:
    events = history()
    original = deepcopy(events)
    first = result(events, carrier())
    assert first == result(events, carrier())
    assert first == {
        "clock": [START, "2026-11-01T00:00:03.123456+00:00", "2026-11-01T00:00:05.654321+00:00"],
        # History insertion time is not the entry instant, and now() stays fixed-start.
        "legacy": ["2026-11-01T00:00:00.123456+00:00"] * 3,
        "elapsed_seconds": 2.530865,
    }
    assert events == original


def test_absent_carrier_keeps_entry_unavailable() -> None:
    assert result(history(), None)["clock"][0] is None


def test_microseconds_are_kept_exactly_in_utc() -> None:
    value = entry_instant(carrier(started_at="2026-11-12T10:00:00.123456Z"), RUN)
    assert value == EntryInstant(datetime(2026, 11, 12, 10, 0, 0, 123456, tzinfo=timezone.utc), "run_started_at")
    assert result(history(), carrier(started_at="2026-10-31T23:59:59.999999Z"))["clock"][0] == (
        "2026-10-31T23:59:59.999999+00:00"
    )


@pytest.mark.parametrize("entry", [
    "not an object",
    [carrier()],
    {key: value for key, value in carrier().items() if key != "source"},
    {key: value for key, value in carrier().items() if key != "started_at"},
    carrier(schema="durable-workflow.worker-replay-clock/v2"),
    carrier(workflow_run_id="another-run"),
    carrier(workflow_run_id=""),
    carrier(workflow_run_id=None),
    carrier(source="history_inserted_at"),
    carrier(source=None),
    carrier(started_at="2026-11-01T00:00:00Z"),
    carrier(started_at="2026-11-01T00:00:00.000Z"),
    carrier(started_at="2026-11-01T00:00:00.000000+00:00"),
    carrier(started_at="2026-11-01T00:00:00.000000z"),
    carrier(started_at="2026-11-01 00:00:00.000000Z"),
    carrier(started_at="2026-02-30T00:00:00.000000Z"),
    carrier(started_at="2026-11-01T24:00:00.000000Z"),
    carrier(started_at=1761955200),
    carrier(started_at=None),
])
def test_unproved_carriers_are_absent(entry: Any) -> None:
    assert entry_instant(entry, RUN) is None
    assert result(history(), entry)["clock"][0] is None


def test_carrier_needs_the_task_run() -> None:
    assert entry_instant(carrier(workflow_run_id=""), "") is None
    assert result(history(), carrier(), run_id="")["clock"][0] is None


def test_redrive_anchor_on_ordinary_history_is_refused() -> None:
    assert result(history(), carrier(source="redrive_replayed_started_at"))["clock"][0] is None


def redrive_fixture() -> tuple[list[dict[str, Any]], str, str]:
    from tests.test_replay_clock import redrive_fixture as load

    fixture = load("business-clock-native-redrive-entry")
    events = fixture["history"]
    return events, fixture["workflow"]["run_id"], events[1]["payload"]["replayed_started_at"]


def test_history_proved_redrive_anchor_is_never_replaced() -> None:
    events, run, _ = redrive_fixture()
    expected = result(events, None, NativeRedriveBusinessClockWorkflow, run_id=run)
    assert expected["clock"][0] == "2026-11-03T00:00:00.000000+00:00"
    for entry in (
        carrier(workflow_run_id=run, source="redrive_replayed_started_at", started_at="2026-10-01T00:00:00.000000Z"),
        carrier(workflow_run_id=run),
    ):
        assert result(events, entry, NativeRedriveBusinessClockWorkflow, run_id=run) == expected


def test_unproved_redrive_history_accepts_only_a_redrive_anchor_for_entry() -> None:
    events, run, _ = redrive_fixture()
    del events[1]["payload"]["workflow_link_id"]  # The SDK can no longer prove the anchor itself.
    assert result(events, None, NativeRedriveBusinessClockWorkflow, run_id=run)["clock"] == [None, None]
    assert result(events, carrier(workflow_run_id=run), NativeRedriveBusinessClockWorkflow,
                  run_id=run)["clock"] == [None, None]
    anchored = result(events, carrier(workflow_run_id=run, source="redrive_replayed_started_at",
                                      started_at="2026-11-03T00:00:00.000000Z"),
                      NativeRedriveBusinessClockWorkflow, run_id=run)
    # Only entry gains the Server's anchor; the unproved reused boundary stays unavailable.
    assert anchored["clock"] == ["2026-11-03T00:00:00.000000+00:00", None]


def test_worker_reads_the_task_carrier_identically_on_every_lease() -> None:
    async def lease(attempt: int, entry: Any) -> Any:
        client = AsyncMock(spec=Client)
        worker = Worker(client, task_queue="clock", workflows=[ScalarBusinessClockWorkflow])
        task = {
            "task_id": f"task-{attempt}", "workflow_type": "tests.replay.business-clock",
            "workflow_task_attempt": attempt, "workflow_id": "business-clock-instance", "run_id": RUN,
            "history_events": history(), "arguments": serializer.envelope([], codec="avro"),
            "payload_codec": "avro",
        }
        if entry is not None:
            task["replay_clock"] = entry
        await worker._run_workflow_task(task)
        client.complete_workflow_task.assert_awaited_once()
        commands = client.complete_workflow_task.call_args.kwargs["commands"]
        assert [command["type"] for command in commands] == ["complete_workflow"]
        return serializer.decode_envelope(commands[0]["result"])

    first = asyncio.run(lease(1, carrier()))
    assert first["clock"][0] == START
    assert asyncio.run(lease(2, carrier())) == first
    assert asyncio.run(lease(3, None))["clock"][0] is None
