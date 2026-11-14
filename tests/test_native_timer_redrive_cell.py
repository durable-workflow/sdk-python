"""Offline contract controls for the separate installed cold-Worker cell."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.integration import test_native_timer_redrive as cell
from tests.test_native_timer_redrive_clock import successor


def prefix():
    previous, current = successor(), successor()
    source_time = cell.instant("2026-11-03T00:00:06.654321+00:00")
    source_start = cell.instant("2026-11-03T00:00:00+00:00")
    for document in (previous, current):
        for event in document["history"]:
            event["timestamp"] = event.pop("recorded_at")
    for event in previous["history"]:
        if event["event_type"] in {"TimerScheduled", "TimerFired"}:
            event["payload"]["timer_id"] = "old-timer"
            event["payload"].pop("reused_recorded_at", None)
            event["payload"].pop("reused_from_run_id", None)
    cell.one(previous["history"], "WorkflowStarted")["payload"].pop("replayed_started_at", None)
    cell.one(previous["history"], "TimerFired")["timestamp"] = "2026-11-03T00:00:06.654321Z"
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
        "source_clock_text": "2026-11-03T00:00:06.654321Z", "source_start_text": "2026-11-03T00:00:00.000000Z",
    }


def test_exact_fraction_and_immediate_predecessor_preserve_unchanged_inputs():
    events, previous, arguments = prefix()
    original = deepcopy((events, previous, arguments))
    assert cell.verify_reuse(events, previous, **arguments)
    assert original == (events, previous, arguments)


@pytest.mark.parametrize("reformat", [None, "clock", "start"])
def test_repeated_reuse_preserves_inherited_strings_as_well_as_instants(reformat):
    events, previous, arguments = prefix()
    original_clock = arguments["source_clock"].isoformat()
    original_start = arguments["source_start"].isoformat()
    cell.one(previous, "TimerFired")["payload"]["reused_recorded_at"] = original_clock
    cell.one(previous, "WorkflowStarted")["payload"]["replayed_started_at"] = original_start
    if reformat == "clock":
        cell.one(events, "TimerFired")["payload"]["reused_recorded_at"] = original_clock.replace("+00:00", "Z")
    elif reformat == "start":
        cell.one(events, "WorkflowStarted")["payload"]["replayed_started_at"] = original_start.replace("+00:00", "Z")
    if reformat:
        assert cell.instant(cell.one(events, "TimerFired")["payload"]["reused_recorded_at"]) == arguments[
            "source_clock"
        ]
        assert cell.instant(cell.one(events, "WorkflowStarted")["payload"]["replayed_started_at"]) == arguments[
            "source_start"
        ]
        with pytest.raises(AssertionError):
            cell.verify_reuse(events, previous, **arguments)
    else:
        cell.verify_reuse(events, previous, **arguments)


@pytest.mark.parametrize("fraction,rendered", [
    ("000000", ""), ("654000", ".654"), ("654321", ".654321"),
])
def test_first_reuse_accepts_native_offset_and_precision_rendering(fraction, rendered):
    events, previous, arguments = prefix()
    arguments["source_clock_text"] = f"2026-11-03T00:00:06.{fraction}Z"
    arguments["source_clock"] = cell.instant(arguments["source_clock_text"])
    arguments["source_start_text"] = f"2026-11-03T00:00:00.{fraction}Z"
    arguments["source_start"] = cell.instant(arguments["source_start_text"])
    cell.one(events, "TimerFired")["payload"]["reused_recorded_at"] = f"2026-11-03T00:00:06{rendered}+00:00"
    cell.one(events, "WorkflowStarted")["payload"]["replayed_started_at"] = f"2026-11-03T00:00:00{rendered}+00:00"
    cell.verify_reuse(events, previous, **arguments)


@pytest.mark.parametrize("field", ["clock", "start"])
def test_first_reuse_refuses_changed_instant(field):
    events, previous, arguments = prefix()
    kind, name, source_name = (
        ("TimerFired", "reused_recorded_at", "source_clock") if field == "clock"
        else ("WorkflowStarted", "replayed_started_at", "source_start")
    )
    payload = cell.one(events, kind)["payload"]
    payload[name] = arguments[source_name].replace(microsecond=1).isoformat()
    with pytest.raises(AssertionError):
        cell.verify_reuse(events, previous, **arguments)


def test_source_boundary_uses_timestamp_only_public_history_shape():
    events, previous, arguments = prefix()
    assert all("timestamp" in event and "recorded_at" not in event for event in previous)
    text, clock = cell.source_timer_boundary(previous)
    assert (text, clock) == (arguments["source_clock_text"], arguments["source_clock"])
    arguments.update(source_clock_text=text, source_clock=clock)
    cell.verify_reuse(events, previous, **arguments)


def test_export_recorded_at_cannot_replace_public_timestamp():
    _, previous, _ = prefix()
    fired = cell.one(previous, "TimerFired")
    fired["recorded_at"] = fired.pop("timestamp")
    with pytest.raises(KeyError, match="timestamp"):
        cell.source_timer_boundary(previous)


@pytest.mark.parametrize("fault", [
    "floor", "root_lineage", "new_start", "pair_id", "old_id", "timer_task", "scheduled_activity", "changed_result",
    "extra_fired", "extra_scheduled", "changed_delay",
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
    elif fault == "extra_fired":
        fired["unreviewed"] = True
    elif fault == "extra_scheduled":
        cell.one(events, "TimerScheduled")["payload"]["unreviewed"] = True
    elif fault == "changed_delay":
        cell.one(events, "TimerScheduled")["payload"]["delay_seconds"] = 999
    else:
        cell.one(events, "ActivityCompleted")["payload"]["result"] = "changed"
    with pytest.raises(AssertionError):
        cell.verify_reuse(events, previous, **arguments)


@pytest.mark.parametrize("clock", ["2026-11-03T00:00:06.654321001Z", "2026-11-03T00:00:06", ""])
def test_unrepresentable_or_unzoned_clock_cannot_hide_precision_loss(clock):
    with pytest.raises(AssertionError):
        cell.instant(clock)


def test_changed_definition_or_absent_trusted_entry_refuses_before_child(monkeypatch):
    with pytest.raises(AssertionError):
        cell.definition("changed definition")
    monkeypatch.setattr(cell.subprocess, "run", lambda *a, **k: pytest.fail("must refuse before any child"))
    with pytest.raises(AssertionError):
        cell.run_cold_worker({})


def test_accepted_workflow_definition_bytes_remain_frozen():
    path = Path(__file__).parent / "integration/redrive_timer_clock_workflow.py"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == cell.DEFINITION_SHA


@pytest.mark.parametrize("fault", [
    None, "timer_command", "extra_command", "extra_completion", "missing_task", "foreign_task",
    "first_callback", "no_task", "floor", "empty_fingerprint",
])
def test_redrive_worker_evidence_requires_only_the_failed_activity_and_exact_clock(fault):
    clock = cell.instant("2026-11-03T00:00:06.654321Z")
    observed = {
        "definition_sha256": cell.DEFINITION_SHA, "fingerprints": {cell.WORKFLOW_TYPE: "sha256:" + "a" * 64},
        "worker_id": "fixed-worker", "pid": 123,
        "status": "failed",
        "completions": [
            {"task_id": "one", "commands": [cell.ScheduleActivity(
                "tests.redrive.second", ["Hello, Taylor!", clock.isoformat(timespec="microseconds")],
                retry_policy=cell.ActivityRetryPolicy(max_attempts=1),
            ).to_server_command("queue")]},
            {"task_id": "two", "commands": [{"type": "fail_workflow", "message": "fixed failure",
                                              "failed_step_sequence": 3, "failed_activity_execution_id": "execution"}]},
        ],
        "callbacks": [{"activity": "second", "clock": clock.isoformat(), "execution_id": "execution",
                       "task_id": "activity", "attempt_id": "attempt", "run_id": "run"}],
    }
    exported = {
        "workflow": {"run_id": "run"},
        "tasks": [{"id": "one", "type": "workflow"}, {"id": "two", "type": "workflow"},
                  {"id": "activity", "type": "activity"}],
    }
    if fault == "timer_command":
        observed["completions"][0]["commands"].append({"type": "start_timer"})
    elif fault == "first_callback":
        observed["callbacks"].insert(0, {"activity": "first"})
    elif fault == "no_task":
        observed["completions"] = []
    elif fault == "extra_command":
        observed["completions"][0]["commands"].append({"type": "upsert_search_attributes"})
    elif fault == "extra_completion":
        observed["completions"].append(deepcopy(observed["completions"][0]))
    elif fault == "missing_task":
        observed["completions"][0]["task_id"] = ""
    elif fault == "foreign_task":
        observed["completions"][0]["task_id"] = "foreign"
    elif fault == "empty_fingerprint":
        observed["fingerprints"][cell.WORKFLOW_TYPE] = ""
    elif fault == "floor":
        observed["callbacks"][0]["clock"] = "2026-11-03T00:00:06Z"
    if fault:
        with pytest.raises(AssertionError):
            cell.verify_worker(observed, redrive=True, clock=clock, queue="queue", exported=exported)
    else:
        cell.verify_worker(observed, redrive=True, clock=clock, queue="queue", exported=exported)


@pytest.mark.parametrize("fault", [None, "empty", "changed", "source"])
def test_recorded_worker_fingerprint_is_exact_across_original_and_successors(fault):
    fingerprint = "sha256:" + "a" * 64
    observations = [{"fingerprints": {cell.WORKFLOW_TYPE: fingerprint}} for _ in range(3)]
    histories = [[{"event_type": "WorkflowStarted", "payload": {
        "workflow_definition_fingerprint": fingerprint, "workflow_definition_fingerprint_source": "worker",
    }}] for _ in range(3)]
    if fault == "empty":
        observations[0]["fingerprints"][cell.WORKFLOW_TYPE] = ""
    elif fault == "changed":
        histories[1][0]["payload"]["workflow_definition_fingerprint"] = "sha256:" + "b" * 64
    elif fault == "source":
        histories[2][0]["payload"]["workflow_definition_fingerprint_source"] = "unrelated"
    if fault:
        with pytest.raises(AssertionError):
            cell.verify_recorded_fingerprints(observations, histories)
    else:
        cell.verify_recorded_fingerprints(observations, histories)


@pytest.mark.parametrize("fault", [None, "done_timer", "cancelled_wait", "pruned", "run", "redacted", "incomplete"])
def test_export_requires_bound_retained_all_status_inventory(fault):
    document = {
        "schema": "durable-workflow.v2.history-export", "schema_version": 2, "history_complete": True,
        "workflow": {"instance_id": "workflow", "run_id": "run", "workflow_type": cell.WORKFLOW_TYPE,
                     "details_state": "retained", "details_pruned_at": None},
        "selected_run": {"waits_projection_source": "workflow_run_waits"}, "redaction": {"applied": False},
        "tasks": [{"id": "workflow-task", "type": "workflow", "status": "completed"}], "waits": [],
        "timers": [{"id": "history-reconstructed-pair"}],
    }
    if fault == "done_timer":
        document["tasks"].append({"id": "timer", "type": "timer", "status": "completed"})
    elif fault == "cancelled_wait":
        document["waits"].append({"kind": "timer", "status": "cancelled"})
    elif fault == "pruned":
        document["workflow"]["details_state"] = "pruned"
    elif fault == "run":
        document["workflow"]["run_id"] = "foreign"
    elif fault == "redacted":
        document["redaction"]["applied"] = True
    elif fault == "incomplete":
        document["history_complete"] = False
    if fault:
        with pytest.raises(AssertionError):
            cell.verify_export(document, "workflow", "run", terminal=True, source=False)
    else:
        cell.verify_export(document, "workflow", "run", terminal=True, source=False)


@pytest.mark.parametrize("fault", [None, "absent_admitted", "wrong_refusal", "positive_failure"])
async def test_matching_nonconsuming_registration_is_required_and_fenced_cleanup_runs(fault):
    fingerprint = "sha256:" + "a" * 64
    events = [{"event_type": "WorkflowStarted", "payload": {
        "workflow_definition_fingerprint": fingerprint, "workflow_definition_fingerprint_source": "worker",
    }}]
    observed = {
        "fingerprints": {cell.WORKFLOW_TYPE: fingerprint}, "registration_namespace": "default",
        "registration": {"worker_id": "original", "task_queue": "queue",
                         "supported_workflow_types": [cell.WORKFLOW_TYPE],
                         "workflow_definition_fingerprints": {cell.WORKFLOW_TYPE: fingerprint},
                         "capability_manifest": {}},
    }

    class FakeClient:
        namespace = "default"

        def __init__(self):
            self.calls = []
            self.registered = False

        async def list_workflow_runs(self, queue):
            return SimpleNamespace(runs=[SimpleNamespace(run_id="source")])

        async def iter_history(self, queue, run_id):
            for event in events:
                yield event

        async def register_worker(self, **request):
            self.calls.append(("register", request))
            self.registered = True
            self.worker_id = request["worker_id"]
            return {"worker_id": self.worker_id, "registered": True, "registration_token": "b" * 32,
                    "namespace": "default", "task_queue": "queue"}

        async def redrive_workflow(self, queue, run_id):
            self.calls.append(("redrive", self.registered))
            if not self.registered and fault != "absent_admitted":
                raise cell.ServerError(409, {
                    "reason": "other" if fault == "wrong_refusal" else "workflow_definition_unavailable_or_changed",
                    "outcome": "rejected", "workflow_id": queue, "run_id": run_id,
                    "workflow_run_id": None, "accepted": False, "task_id": None,
                })
            if self.registered and fault == "positive_failure":
                raise cell.ServerError(503, {"reason": "fixed admission failure"})
            return SimpleNamespace(outcome="redriven", run_id="next")

        async def deregister_worker_registration(self, worker_id, **arguments):
            assert worker_id == self.worker_id
            assert arguments == {
                "registration_token": "b" * 32, "expected_namespace": "default", "timeout": 10,
            }
            self.calls.append(("deregister", worker_id))
            self.registered = False
            return {"worker_id": worker_id, "registration_token": "b" * 32,
                    "outcome": "deregistered", "recovered_workflow_task_count": 0}

    client = FakeClient()
    if fault:
        with pytest.raises(cell.ServerError if fault == "positive_failure" else AssertionError):
            await cell.admit_redrive(client, "queue", "source", observed, events)
        assert not client.registered
    else:
        receipt, evidence = await cell.admit_redrive(client, "queue", "source", observed, events)
        assert receipt.run_id == "next"
        assert not client.registered
        assert [name for name, _value in client.calls] == ["redrive", "register", "redrive", "deregister"]
        assert "b" * 32 not in json.dumps(evidence)
        assert evidence["matching_registration"]["workflow_definition_fingerprints"] == {
            cell.WORKFLOW_TYPE: fingerprint,
        }
