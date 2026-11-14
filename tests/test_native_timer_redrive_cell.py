"""Offline contract controls for the separate installed cold-Worker cell."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest

from tests.integration import test_native_timer_redrive as cell
from tests.test_native_timer_redrive_clock import successor


def prefix():
    previous, current = successor(), successor()
    source_time = cell.instant("2026-11-03T00:00:06.654321+00:00")
    source_start = cell.instant("2026-11-03T00:00:00+00:00")
    for event in previous["history"]:
        if event["event_type"] in {"TimerScheduled", "TimerFired"}:
            event["payload"]["timer_id"] = "old-timer"
    events = current["history"]
    cell.one(events, "WorkflowStarted")["payload"]["continued_from_run_id"] = "previous"
    fired = cell.one(events, "TimerFired")["payload"]
    fired["reused_from_run_id"] = "previous"
    fired["reused_recorded_at"] = source_time.isoformat()
    completed = cell.one(events, "ActivityCompleted")["payload"]
    completed["reused_from_run_id"] = "previous"
    completed["reused_activity_execution_id"] = cell.one(previous["history"], "ActivityCompleted")["payload"][
        "activity_execution_id"
    ]
    return events, previous["history"], {
        "predecessor_id": "previous", "source_clock": source_time, "source_start": source_start,
        "used_timer_ids": {"old-timer"},
    }


def test_exact_fraction_and_immediate_predecessor_preserve_unchanged_inputs():
    events, previous, arguments = prefix()
    original = deepcopy((events, previous, arguments))
    assert cell.verify_reuse(events, previous, **arguments)
    assert (events, previous, arguments) == original


@pytest.mark.parametrize("fault", [
    "floor", "root_lineage", "new_start", "pair_id", "old_id", "timer_task", "scheduled_activity", "changed_result",
])
def test_false_reuse_evidence_refuses(fault):
    events, previous, arguments = prefix()
    fired = cell.one(events, "TimerFired")["payload"]
    if fault == "floor":
        fired["reused_recorded_at"] = "2026-11-03T00:00:06+00:00"
    elif fault == "root_lineage":
        fired["reused_from_run_id"] = "root-instead-of-immediate-predecessor"
    elif fault == "new_start":
        cell.one(events, "WorkflowStarted")["payload"]["replayed_started_at"] = "2026-11-03T00:05:00Z"
    elif fault == "pair_id":
        fired["timer_id"] = "different-pair"
    elif fault == "old_id":
        arguments["used_timer_ids"].add(fired["timer_id"])
    elif fault == "timer_task":
        fired["task"] = {"id": "new-timer-work"}
    elif fault == "scheduled_activity":
        events.append({"event_type": "ActivityScheduled", "payload": {"sequence": 1}})
    else:
        cell.one(events, "ActivityCompleted")["payload"]["result"] = "changed"
    with pytest.raises(AssertionError):
        cell.verify_reuse(events, previous, **arguments)


@pytest.mark.parametrize("clock", ["2026-11-03T00:00:06.654321001Z", "2026-11-03T00:00:06", ""])
def test_unrepresentable_or_unzoned_clock_cannot_hide_precision_loss(clock):
    with pytest.raises(AssertionError):
        cell.instant(clock)


def test_changed_definition_or_case_source_refuses_before_child(monkeypatch):
    with pytest.raises(AssertionError):
        cell.definition("changed definition")
    monkeypatch.setenv("DURABLE_WORKFLOW_REDRIVE_CASE_SHA256", "0" * 64)
    monkeypatch.setattr(cell.subprocess, "run", lambda *a, **k: pytest.fail("must refuse before any child"))
    with pytest.raises(AssertionError):
        cell.run_cold_worker({})


@pytest.mark.parametrize("substitute", [False, True])
def test_literal_child_bootstrap_binds_captured_bytes_without_opening_a_dispatcher(substitute):
    source = "async def cold_worker(p): print('fixed inert result', p['marker'])\n"
    packet = {"source": source, "source_sha256": hashlib.sha256(source.encode()).hexdigest(), "marker": "owned"}
    if substitute:
        packet["source"] += "\nraise RuntimeError('substitute executed')\n"
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", cell.CHILD_ENTRY], input=json.dumps(packet),
        env={"PATH": os.defpath}, capture_output=True, text=True, timeout=5,
    )
    if substitute:
        assert result.returncode != 0 and "source differs" in result.stderr
        assert "substitute executed" not in result.stderr and not result.stdout
    else:
        assert result.returncode == 0 and result.stdout == "fixed inert result owned\n"


def test_accepted_workflow_definition_bytes_remain_frozen():
    path = Path(__file__).parent / "integration/redrive_timer_clock_workflow.py"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == cell.DEFINITION_SHA


@pytest.mark.parametrize("fault", [None, "timer_command", "first_callback", "no_task", "floor"])
def test_redrive_worker_evidence_requires_only_the_failed_activity_and_exact_clock(fault):
    clock = cell.instant("2026-11-03T00:00:06.654321Z")
    observed = {
        "definition_sha256": cell.DEFINITION_SHA, "fingerprints": {cell.WORKFLOW_TYPE: "sha256:" + "a" * 64},
        "worker_id": "fixed-worker", "pid": 123,
        "completions": [{"commands": [{"type": "schedule_activity", "activity_type": "tests.redrive.second"}]}],
        "callbacks": [{"activity": "second", "clock": clock.isoformat()}],
    }
    if fault == "timer_command":
        observed["completions"][0]["commands"].append({"type": "start_timer"})
    elif fault == "first_callback":
        observed["callbacks"].insert(0, {"activity": "first"})
    elif fault == "no_task":
        observed["completions"] = []
    elif fault == "floor":
        observed["callbacks"][0]["clock"] = "2026-11-03T00:00:06Z"
    if fault:
        with pytest.raises(AssertionError):
            cell.verify_worker(observed, redrive=True, clock=clock)
    else:
        cell.verify_worker(observed, redrive=True, clock=clock)
