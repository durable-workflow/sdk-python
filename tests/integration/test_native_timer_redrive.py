"""Published-package cold Worker proof; run only through its reviewed manual cell."""

from __future__ import annotations

import asyncio
import hashlib
import json
import linecache
import os
import re
import subprocess
import sys
import types
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from durable_workflow import ActivityRetryPolicy, Client, Worker, activity
from durable_workflow.errors import ServerError
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, StartTimer

DEFINITION_SHA = "af24e69d5d4ed57e3cb56915a5e2febe535438e8bb2b33ecfd998098c1557b2b"
WORKFLOW_TYPE = "tests.python-redrive-timer-clock"
_COLD_BOOTSTRAP = ""
_COLD_INVENTORY: dict[str, Any] = {}
_COLD_PYTHON = ""


def instant(value: Any) -> datetime:
    assert isinstance(value, str) and value
    assert not re.search(r"\.\d{7}", value), "clock precision exceeds Python's exact microsecond representation"
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    assert result.tzinfo is not None
    return result.astimezone(timezone.utc)


def one(events: list[dict[str, Any]], kind: str, *, step: int | None = None) -> dict[str, Any]:
    selected = [
        event for event in events
        if event["event_type"] == kind and (step is None or event["payload"].get("sequence") == step)
    ]
    assert len(selected) == 1, (kind, step, len(selected))
    return selected[0]


def verify_reuse(
    events: list[dict[str, Any]], predecessor: list[dict[str, Any]], *,
    predecessor_id: str, source_clock: datetime, source_start: datetime, used_timer_ids: set[str],
) -> str:
    started = one(events, "WorkflowStarted")["payload"]
    assert started["continued_from_run_id"] == predecessor_id
    assert started["recovery_kind"] == "redrive"
    assert started["resume_step_sequence"] == 3
    assert instant(started["replayed_started_at"]) == source_start
    scheduled = one(events, "TimerScheduled", step=2)["payload"]
    fired = one(events, "TimerFired", step=2)["payload"]
    timer_id = scheduled["timer_id"]
    assert isinstance(timer_id, str) and timer_id and timer_id not in used_timer_ids
    assert fired["timer_id"] == timer_id
    assert fired["reused_from_run_id"] == predecessor_id
    assert instant(fired["reused_recorded_at"]) == source_clock
    expected_scheduled = deepcopy(one(predecessor, "TimerScheduled", step=2)["payload"])
    expected_fired = deepcopy(one(predecessor, "TimerFired", step=2)["payload"])
    for payload in (expected_scheduled, expected_fired):
        payload.pop("task", None)
        payload.pop("command", None)
        payload["timer_id"] = timer_id
    expected_fired.update(reused_from_run_id=predecessor_id, reused_recorded_at=fired["reused_recorded_at"])
    assert scheduled == expected_scheduled
    assert fired == expected_fired
    original = one(predecessor, "ActivityCompleted", step=1)["payload"]
    reused = one(events, "ActivityCompleted", step=1)["payload"]
    assert reused["result"] == original["result"]
    assert reused["reused_from_run_id"] == predecessor_id
    assert reused["reused_activity_execution_id"] == original["activity_execution_id"]
    assert not [event for event in events if event["event_type"] == "ActivityScheduled"]
    return timer_id


def definition(source: str) -> type:
    assert hashlib.sha256(source.encode()).hexdigest() == DEFINITION_SHA
    name, filename = "accepted_redrive_timer", "<accepted redrive timer definition>"
    module = types.ModuleType(name)
    module.__file__ = filename
    sys.modules[name] = module
    linecache.cache[filename] = (len(source), None, source.splitlines(keepends=True), filename)
    exec(compile(source, filename, "exec"), module.__dict__)
    return module.RedriveTimerClockWorkflow


async def cold_worker(packet: dict[str, Any]) -> None:
    callbacks: list[dict[str, Any]] = []
    completions: list[dict[str, Any]] = []
    registration: dict[str, Any] = {}

    class ObservedClient(Client):
        async def register_worker(self, **kwargs: Any) -> Any:
            acknowledgement = await super().register_worker(**kwargs)
            registration.update(deepcopy({key: value for key, value in kwargs.items() if key != "process_metrics"}))
            return acknowledgement

        async def complete_workflow_task(self, **kwargs: Any) -> Any:
            completions.append({"task_id": kwargs["task_id"], "commands": deepcopy(kwargs["commands"])})
            return await super().complete_workflow_task(**kwargs)

    @activity.defn(name="tests.redrive.first")
    async def first(name: str) -> str:
        info = activity.context().info
        callbacks.append({
            "activity": "first", "pid": os.getpid(), "run_id": info.run_id,
            "task_id": info.task_id, "attempt_id": info.activity_attempt_id, "execution_id": info.activity_execution_id,
        })
        return f"Hello, {name}!"

    @activity.defn(name="tests.redrive.second")
    async def second(greeting: str, clock: str) -> str:
        info = activity.context().info
        callbacks.append({
            "activity": "second", "clock": clock, "pid": os.getpid(), "run_id": info.run_id,
            "task_id": info.task_id, "attempt_id": info.activity_attempt_id,
            "execution_id": info.activity_execution_id,
        })
        if packet["fail_second"]:
            raise RuntimeError("fixed failed-activity redrive boundary")
        return "resumed"

    async with ObservedClient(packet["server_url"], token=packet["token"], namespace="default") as client:
        worker = Worker(
            client, task_queue=packet["queue"], worker_id="python-clock-" + uuid4().hex,
            workflows=[definition(packet["definition"])], activities=[first, second], sticky_cache_capacity=0,
        )
        fingerprints = dict(worker.workflow_definition_fingerprints)
        try:
            outcome = await worker.run_until(workflow_id=packet["workflow_id"], timeout=45)
        finally:
            await worker.stop()
        print("COLD_WORKER_RESULT=" + json.dumps({
            "pid": os.getpid(), "worker_id": worker.worker_id, "fingerprints": fingerprints,
            "definition_sha256": DEFINITION_SHA, "status": outcome.status,
            "callbacks": callbacks, "completions": completions, "registration": registration,
            "registration_namespace": client.namespace,
        }, sort_keys=True), flush=True)


def run_cold_worker(packet: dict[str, Any]) -> dict[str, Any]:
    assert _COLD_BOOTSTRAP and _COLD_INVENTORY and _COLD_PYTHON
    environment = {"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1"}
    result = subprocess.run(
        [_COLD_PYTHON, "-I", "-S", "-B", "-c", _COLD_BOOTSTRAP],
        input=json.dumps({"inventory": _COLD_INVENTORY, "worker": packet}),
        env=environment, capture_output=True, text=True, timeout=55, check=False,
    )
    assert result.returncode == 0, (result.returncode, result.stdout[-16384:], result.stderr[-16384:])
    lines = [line.removeprefix("COLD_WORKER_RESULT=") for line in result.stdout.splitlines()
             if line.startswith("COLD_WORKER_RESULT=")]
    assert len(lines) == 1
    return json.loads(lines[0])


def verify_export(document: dict[str, Any], workflow_id: str, run_id: str, *, terminal: bool, source: bool) -> None:
    assert document["schema"] == "durable-workflow.v2.history-export" and document["schema_version"] == 2
    selected = document["workflow"]
    assert selected["instance_id"] == workflow_id and selected["run_id"] == run_id
    assert selected["workflow_type"] == WORKFLOW_TYPE
    assert selected["details_state"] == "retained" and selected["details_pruned_at"] is None
    assert document["history_complete"] is terminal
    assert document["selected_run"]["waits_projection_source"] == "workflow_run_waits"
    assert document["redaction"]["applied"] is False
    for section, kind in (("tasks", "type"), ("waits", "kind")):
        rows = document[section]
        assert isinstance(rows, list)
        assert all(isinstance(row, dict) and isinstance(row[kind], str) and row[kind] for row in rows)
        assert all(isinstance(row["status"], str) and row["status"] for row in rows)
        assert sum(row[kind] == "timer" for row in rows) == (1 if source else 0)


def verify_recorded_fingerprints(observations: list[dict[str, Any]], histories: list[list[dict[str, Any]]]) -> None:
    assert len(observations) == len(histories) == 3
    expected = observations[0]["fingerprints"][WORKFLOW_TYPE]
    assert isinstance(expected, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", expected)
    for observed, events in zip(observations, histories, strict=True):
        assert observed["fingerprints"] == {WORKFLOW_TYPE: expected}
        started = one(events, "WorkflowStarted")["payload"]
        assert started["workflow_definition_fingerprint"] == expected
        assert started["workflow_definition_fingerprint_source"] == "worker"


def verify_worker(
    observed: dict[str, Any], *, redrive: bool, clock: datetime, queue: str, exported: dict[str, Any],
) -> None:
    assert observed["definition_sha256"] == DEFINITION_SHA
    assert set(observed["fingerprints"]) == {WORKFLOW_TYPE}
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", observed["fingerprints"][WORKFLOW_TYPE])
    assert observed["worker_id"] and isinstance(observed["pid"], int) and observed["pid"] > 0
    assert all(set(completion) == {"task_id", "commands"} for completion in observed["completions"])
    task_ids = [completion["task_id"] for completion in observed["completions"]]
    assert all(isinstance(identity, str) and identity for identity in task_ids)
    assert len(set(task_ids)) == len(task_ids) == (2 if redrive else 4)
    assert set(task_ids) == {task["id"] for task in exported["tasks"] if task["type"] == "workflow"}
    counts = {"workflow": 2, "activity": 1} if redrive else {"workflow": 4, "activity": 2, "timer": 1}
    assert Counter(task["type"] for task in exported["tasks"]) == counts
    assert [item["activity"] for item in observed["callbacks"]] == (["second"] if redrive else ["first", "second"])
    for callback in observed["callbacks"]:
        assert all(isinstance(callback[name], str) and callback[name] for name in (
            "task_id", "attempt_id", "execution_id", "run_id",
        ))
        assert callback["run_id"] == exported["workflow"]["run_id"]
    assert {callback["task_id"] for callback in observed["callbacks"]} == {
        task["id"] for task in exported["tasks"] if task["type"] == "activity"
    }
    assert all(len(completion["commands"]) == 1 for completion in observed["completions"])
    commands = [command for completion in observed["completions"] for command in completion["commands"]]
    clock_text = clock.isoformat(timespec="microseconds")
    scheduled = ScheduleActivity(
        "tests.redrive.second", ["Hello, Taylor!", clock_text], retry_policy=ActivityRetryPolicy(max_attempts=1),
    ).to_server_command(queue)
    prefix = [scheduled] if redrive else [
        ScheduleActivity("tests.redrive.first", ["Taylor"]).to_server_command(queue),
        StartTimer(1).to_server_command(queue), scheduled,
    ]
    assert commands[:-1] == prefix
    terminal = commands[-1]
    if observed["status"] == "completed":
        assert terminal == CompleteWorkflow({
            "greeting": "Hello, Taylor!", "timer_clock": clock_text, "resumed": "resumed",
        }).to_server_command(queue)
    else:
        required = {"type", "message", "failed_step_sequence", "failed_activity_execution_id"}
        allowed = required | {"exception_type", "exception_class", "exception", "non_retryable"}
        assert required <= set(terminal) <= allowed
        assert terminal["type"] == "fail_workflow" and terminal["failed_step_sequence"] == 3
        assert isinstance(terminal["message"], str) and terminal["message"]
        assert terminal["failed_activity_execution_id"] == observed["callbacks"][-1]["execution_id"]
    assert instant(observed["callbacks"][-1]["clock"]) == clock


async def admit_redrive(
    client: Client, queue: str, run_id: str, observed: dict[str, Any], events: list[dict[str, Any]],
):
    started = one(events, "WorkflowStarted")["payload"]
    fingerprint = observed["fingerprints"][WORKFLOW_TYPE]
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", fingerprint)
    assert started["workflow_definition_fingerprint"] == fingerprint
    assert started["workflow_definition_fingerprint_source"] == "worker"
    assert client.namespace == observed["registration_namespace"] == "default"
    before = [run.run_id for run in (await client.list_workflow_runs(queue)).runs]
    try:
        await client.redrive_workflow(queue, run_id)
    except ServerError as error:
        assert error.status == 409 and error.reason() == "workflow_definition_unavailable_or_changed"
        assert isinstance(error.body, dict) and error.body["outcome"] == "rejected"
        assert error.body["workflow_id"] == queue and error.body["run_id"] == run_id
        assert error.body["accepted"] is False and error.body["workflow_run_id"] is None
        assert error.body["task_id"] is None
        refusal = {"status": error.status, "body": error.body}
    else:
        raise AssertionError("redrive unexpectedly admitted without a matching registration")
    assert [run.run_id for run in (await client.list_workflow_runs(queue)).runs] == before
    assert [event async for event in client.iter_history(queue, run_id)] == events
    request = deepcopy(observed["registration"])
    assert request["task_queue"] == queue and request["supported_workflow_types"] == [WORKFLOW_TYPE]
    assert request["workflow_definition_fingerprints"] == {WORKFLOW_TYPE: fingerprint}
    holder_id = "python-clock-admission-" + uuid4().hex
    request["worker_id"] = holder_id
    acknowledgement = await client.register_worker(**request)
    token = acknowledgement.get("registration_token")
    assert isinstance(token, str) and re.fullmatch(r"[0-9a-f]{32}", token)
    try:
        assert acknowledgement["worker_id"] == holder_id and acknowledgement["registered"] is True
        assert acknowledgement["namespace"] == "default" and acknowledgement["task_queue"] == queue
        # The holder never creates a Worker or polls: only the later cold child
        # may claim and execute the successor's task.
        receipt = await client.redrive_workflow(queue, run_id)
    finally:
        removed = await client.deregister_worker_registration(
            holder_id, registration_token=token, expected_namespace="default", timeout=10,
        )
        assert removed["outcome"] == "deregistered" and removed["recovered_workflow_task_count"] == 0
    return receipt, {
        "absent_registration": refusal, "matching_registration": request,
        "registration_token_sha256": hashlib.sha256(token.encode()).hexdigest(),
        "cleanup": {key: value for key, value in removed.items() if key != "registration_token"},
    }


@pytest.mark.skipif(
    os.environ.get("DURABLE_WORKFLOW_REDRIVE_CELL") != "1", reason="reviewed manual published cell only",
)
async def test_cold_workers_preserve_fractional_timer_clock_across_repeated_redrive(
    server_url: str, server_token: str,
) -> None:
    source = Path(__file__).with_name("redrive_timer_clock_workflow.py").read_text()
    assert hashlib.sha256(source.encode()).hexdigest() == DEFINITION_SHA
    queue = "python-timer-clock-" + uuid4().hex
    common = {"server_url": server_url, "token": server_token, "queue": queue,
              "workflow_id": queue, "definition": source}
    observations = []
    histories = []
    exports = []
    admissions = []
    run_ids = []
    async with Client(server_url, token=server_token, namespace="default") as client:
        handle = await client.start_workflow(
            workflow_type=WORKFLOW_TYPE, workflow_id=queue, task_queue=queue, input=["Taylor", 1],
        )
        assert handle.run_id
        run_ids.append(handle.run_id)
        observations.append(await asyncio.to_thread(run_cold_worker, {**common, "fail_second": True}))
        assert observations[0]["status"] == "failed"
        histories.append([event async for event in client.iter_history(queue, handle.run_id)])
        frozen_source = deepcopy(histories[0])
        original = await client.describe_workflow_run(queue, handle.run_id)
        source_start = instant(original.started_at)
        source_clock = instant(one(histories[0], "TimerFired", step=2)["recorded_at"])
        assert source_clock.microsecond != 0, "real TimerFired must expose nonzero source precision"
        exports.append(await client.export_history(queue, handle.run_id))
        verify_export(exports[0], queue, handle.run_id, terminal=True, source=True)
        used_timer_ids = {one(histories[0], "TimerFired", step=2)["payload"]["timer_id"]}
        for repeat in (1, 2):
            receipt, admission = await admit_redrive(client, queue, run_ids[-1], observations[-1], histories[-1])
            admissions.append(admission)
            assert receipt.outcome == "redriven" and receipt.run_id and receipt.run_id not in run_ids
            run_id = receipt.run_id
            prefix = [event async for event in client.iter_history(queue, run_id)]
            timer_id = verify_reuse(
                prefix, histories[-1], predecessor_id=run_ids[-1], source_clock=source_clock,
                source_start=source_start, used_timer_ids=used_timer_ids,
            )
            used_timer_ids.add(timer_id)
            before = await client.describe_workflow_run(queue, run_id)
            assert before.wait_kind != "timer"
            admitted_export = await client.export_history(queue, run_id)
            verify_export(admitted_export, queue, run_id, terminal=False, source=False)
            observed = await asyncio.to_thread(run_cold_worker, {**common, "fail_second": repeat == 1})
            assert observed["status"] == ("failed" if repeat == 1 else "completed")
            completed_export = await client.export_history(queue, run_id)
            verify_export(completed_export, queue, run_id, terminal=True, source=False)
            verify_worker(observed, redrive=True, clock=source_clock, queue=queue, exported=completed_export)
            assert observed["callbacks"][0]["run_id"] == run_id
            observations.append(observed)
            run_ids.append(run_id)
            histories.append([event async for event in client.iter_history(queue, run_id)])
            exports.append({"before": admitted_export, "after": completed_export})
            assert histories[-1][:len(prefix)] == prefix
            assert [event async for event in client.iter_history(queue, handle.run_id)] == frozen_source
        result = await client.get_workflow_handle(queue, run_id=run_ids[-1]).result(timeout=10)
    verify_worker(observations[0], redrive=False, clock=source_clock, queue=queue, exported=exports[0])
    assert len({item["pid"] for item in observations}) == len({item["worker_id"] for item in observations}) == 3
    verify_recorded_fingerprints(observations, histories)
    assert result == {"greeting": "Hello, Taylor!", "timer_clock": source_clock.isoformat(timespec="microseconds"),
                      "resumed": "resumed"}
    print(json.dumps({
        "scope": "published cold-Worker timer redrive", "runs": run_ids, "source_started_at": original.started_at,
        "source_timer_clock": source_clock.isoformat(), "definition_sha256": DEFINITION_SHA,
        "workers": observations, "histories": histories, "exports": exports, "admissions": admissions,
    }, sort_keys=True))
