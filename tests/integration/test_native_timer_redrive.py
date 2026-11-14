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
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from durable_workflow import Client, Worker, activity

DEFINITION_SHA = "af24e69d5d4ed57e3cb56915a5e2febe535438e8bb2b33ecfd998098c1557b2b"
WORKFLOW_TYPE = "tests.python-redrive-timer-clock"
CHILD_ENTRY = """import asyncio, hashlib, json, sys
p = json.loads(sys.stdin.read())
source = p.pop('source')
if hashlib.sha256(source.encode()).hexdigest() != p.pop('source_sha256'):
    raise ValueError('cold Worker source differs')
scope = {'__name__': 'cold_redrive_case', '__file__': '<captured cold Worker>'}
exec(compile(source, '<captured cold Worker>', 'exec'), scope)
asyncio.run(scope['cold_worker'](p))
"""


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
    assert instant(fired["fired_at"]) == instant(one(predecessor, "TimerFired", step=2)["payload"]["fired_at"])
    for payload in (scheduled, fired):
        assert "task" not in payload and "command" not in payload
    assert scheduled["fire_at"] == one(predecessor, "TimerScheduled", step=2)["payload"]["fire_at"]
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

    class ObservedClient(Client):
        async def complete_workflow_task(self, **kwargs: Any) -> Any:
            completions.append({"task_id": kwargs["task_id"], "commands": deepcopy(kwargs["commands"])})
            return await super().complete_workflow_task(**kwargs)

    @activity.defn(name="tests.redrive.first")
    async def first(name: str) -> str:
        callbacks.append({"activity": "first", "pid": os.getpid()})
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
            "callbacks": callbacks, "completions": completions,
        }, sort_keys=True), flush=True)


def run_cold_worker(packet: dict[str, Any]) -> dict[str, Any]:
    source = Path(__file__).read_text()
    digest = hashlib.sha256(source.encode()).hexdigest()
    assert digest == os.environ["DURABLE_WORKFLOW_REDRIVE_CASE_SHA256"]
    packet = {**packet, "source": source, "source_sha256": digest}
    environment = {"PATH": os.defpath, "PYTHONDONTWRITEBYTECODE": "1"}
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", CHILD_ENTRY], input=json.dumps(packet),
        env=environment, capture_output=True, text=True, timeout=55, check=False,
    )
    assert result.returncode == 0, (result.returncode, result.stdout[-16384:], result.stderr[-16384:])
    lines = [line.removeprefix("COLD_WORKER_RESULT=") for line in result.stdout.splitlines()
             if line.startswith("COLD_WORKER_RESULT=")]
    assert len(lines) == 1
    return json.loads(lines[0])


def verify_worker(observed: dict[str, Any], *, redrive: bool, clock: datetime) -> None:
    assert observed["definition_sha256"] == DEFINITION_SHA
    assert set(observed["fingerprints"]) == {WORKFLOW_TYPE}
    assert observed["worker_id"] and isinstance(observed["pid"], int) and observed["pid"] > 0
    assert observed["completions"]
    commands = [command for completion in observed["completions"] for command in completion["commands"]]
    assert sum(command["type"] == "start_timer" for command in commands) == (0 if redrive else 1)
    expected = ["tests.redrive.second"] if redrive else ["tests.redrive.first", "tests.redrive.second"]
    assert [command["activity_type"] for command in commands if command["type"] == "schedule_activity"] == expected
    assert [item["activity"] for item in observed["callbacks"]] == (["second"] if redrive else ["first", "second"])
    assert instant(observed["callbacks"][-1]["clock"]) == clock


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
        used_timer_ids = {one(histories[0], "TimerFired", step=2)["payload"]["timer_id"]}
        for repeat in (1, 2):
            receipt = await client.redrive_workflow(queue, run_ids[-1])
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
            observed = await asyncio.to_thread(run_cold_worker, {**common, "fail_second": repeat == 1})
            assert observed["status"] == ("failed" if repeat == 1 else "completed")
            verify_worker(observed, redrive=True, clock=source_clock)
            assert observed["callbacks"][0]["run_id"] == run_id
            observations.append(observed)
            run_ids.append(run_id)
            histories.append([event async for event in client.iter_history(queue, run_id)])
            assert histories[-1][:len(prefix)] == prefix
            assert [event async for event in client.iter_history(queue, handle.run_id)] == frozen_source
        result = await client.get_workflow_handle(queue, run_id=run_ids[-1]).result(timeout=10)
    verify_worker(observations[0], redrive=False, clock=source_clock)
    assert len({item["pid"] for item in observations}) == len({item["worker_id"] for item in observations}) == 3
    assert len({json.dumps(item["fingerprints"], sort_keys=True) for item in observations}) == 1
    assert result == {"greeting": "Hello, Taylor!", "timer_clock": source_clock.isoformat(timespec="microseconds"),
                      "resumed": "resumed"}
    print(json.dumps({
        "scope": "published cold-Worker timer redrive", "runs": run_ids, "source_started_at": original.started_at,
        "source_timer_clock": source_clock.isoformat(), "definition_sha256": DEFINITION_SHA,
        "workers": observations, "histories": histories,
    }, sort_keys=True))
