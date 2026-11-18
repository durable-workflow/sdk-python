"""Actual scalar local retry timing; admitted only by the reviewed package cell."""

from __future__ import annotations

import json
import os
import time
from collections import Counter
from copy import deepcopy
from typing import Any
from uuid import uuid4

import pytest

from durable_workflow import ActivityRetryPolicy, Client, Worker, activity, workflow
from durable_workflow.workflow import CompleteWorkflow, replay

CASES = {
    "generated-default-cap": [60],
    "short-list": [1, 2, 2, 2],
    "empty-list": [0, 0],
}
ACTIVITY = "tests.python-retry-cap"
WORKFLOW = "tests.python-local-retry-cap"


def selected_policy(mode: str) -> dict[str, Any]:
    if mode == "generated-default-cap":
        return ActivityRetryPolicy.exponential(max_attempts=2, initial_interval_seconds=61).to_dict()
    if mode == "short-list":
        return {"max_attempts": 5, "backoff_seconds": [1, 2]}
    assert mode == "empty-list"
    return {"max_attempts": 3, "backoff_seconds": []}


@workflow.defn(name=WORKFLOW)
class LocalRetryCapWorkflow:
    def run(self, ctx: Any, mode: str) -> Any:
        result = yield ctx.local_activity(ACTIVITY, [mode], retry_policy=selected_policy(mode))
        return {"mode": mode, "result": result}


def verify_retry(
    mode: str, callbacks: list[dict[str, Any]], commands: list[dict[str, Any]], history: list[dict],
) -> None:
    backoff = CASES[mode]
    count = len(backoff) + 1
    assert all(type(item["attempt"]) is int for item in callbacks)
    assert [item["attempt"] for item in callbacks] == list(range(1, count + 1))
    assert all(type(item["at_ns"]) is int and item["at_ns"] > 0 for item in callbacks)
    gaps = [later["at_ns"] - earlier["at_ns"] for earlier, later in zip(callbacks[:-1], callbacks[1:], strict=True)]
    assert all(gap >= 0 for gap in gaps)
    # A monotonic clock observes actual callbacks, separately from reported
    # durations. Scheduling may add time; it must not erase a declared wait.
    assert all(gap >= delay * 1_000_000_000 - 10_000_000 for gap, delay in zip(gaps, backoff, strict=True))
    assert [command["type"] for command in commands] == ["record_local_activity", "complete_workflow"]
    recorded = commands[0]
    assert recorded["activity_type"] == ACTIVITY
    assert recorded["execution_mode"] == "local"
    assert recorded["retry_policy"] == selected_policy(mode)
    assert recorded["outcome"] == "completed"
    assert all(type(item["attempt_number"]) is int for item in recorded["attempts"])
    assert [item["attempt_number"] for item in recorded["attempts"]] == list(range(1, count + 1))
    assert [item["outcome"] for item in recorded["attempts"]] == [*(["failed"] * len(backoff)), "completed"]
    assert [item.get("backoff_seconds") for item in recorded["attempts"]] == [*backoff, None]
    events = [event for event in history if event["event_type"].startswith("Activity")]
    assert Counter(event["event_type"] for event in events) == {
        "ActivityScheduled": 1, "ActivityStarted": count,
        "ActivityRetryScheduled": len(backoff), "ActivityCompleted": 1,
    }
    assert all(event["payload"]["activity_type"] == ACTIVITY for event in events)
    assert all(event["payload"]["execution_mode"] == "local" for event in events)
    assert all(event["payload"]["activity"]["retry_policy"] == selected_policy(mode) for event in events)
    retries = [event["payload"] for event in events if event["event_type"] == "ActivityRetryScheduled"]
    assert [item["retry_after_attempt"] for item in retries] == list(range(1, count))
    assert [item["retry_backoff_seconds"] for item in retries] == backoff


async def observe_retry(client: Client, server_url: str, server_token: str, mode: str) -> dict[str, Any]:
    callbacks = []
    commands = []

    class ObservedClient(Client):
        async def complete_workflow_task(self, **kwargs: Any) -> Any:
            commands.extend(deepcopy(kwargs["commands"]))
            return await super().complete_workflow_task(**kwargs)

    @activity.defn(name=ACTIVITY)
    async def attempt(selected: str) -> str:
        assert selected == mode
        callbacks.append({"attempt": activity.context().info.attempt_number, "at_ns": time.monotonic_ns()})
        if len(callbacks) <= len(CASES[mode]):
            raise RuntimeError("fixed ordinary local retry")
        return "retained"

    queue = "python-retry-cap-" + uuid4().hex
    async with ObservedClient(server_url, token=server_token, namespace="default") as worker_client:
        worker = Worker(worker_client, task_queue=queue, workflows=[LocalRetryCapWorkflow], activities=[attempt])
        handle = await client.start_workflow(
            workflow_type=WORKFLOW, workflow_id=queue, task_queue=queue, input=[mode],
        )
        try:
            status = await worker.run_until(workflow_id=queue, timeout=100 if mode == "generated-default-cap" else 30)
            assert status.status == "completed"
        finally:
            await worker.stop()
    assert handle.run_id
    result = await handle.result(timeout=10)
    assert result == {"mode": mode, "result": "retained"}
    history = [event async for event in client.iter_history(queue, handle.run_id)]
    exported = await client.export_history(queue, handle.run_id)
    assert exported["workflow"]["instance_id"] == queue
    assert exported["workflow"]["run_id"] == handle.run_id
    assert exported["workflow"]["details_state"] == "retained"
    assert exported["history_complete"] is True
    assert exported["redaction"]["applied"] is False
    verify_retry(mode, callbacks, commands, history)
    retained = deepcopy(history)

    def refuse_callback(_command):
        pytest.fail("committed local retry must replay without callback execution")

    outcome = replay(
        LocalRetryCapWorkflow, history, [mode], workflow_id=queue, run_id=handle.run_id,
        local_activity_executor=refuse_callback,
    )
    try:
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == result
        assert history == retained
    finally:
        outcome.close()
    assert [event async for event in client.iter_history(queue, handle.run_id)] == retained
    return {"mode": mode, "workflow_id": queue, "run_id": handle.run_id,
            "callbacks": callbacks, "commands": commands, "history": history, "export": exported}


@pytest.mark.skipif(os.environ.get("DURABLE_WORKFLOW_RETRY_CAP_CELL") != "1", reason="reviewed package cell only")
async def test_installed_local_retries_preserve_cap_tail_and_committed_replay(
    server_url: str, server_token: str,
) -> None:
    observations = []
    async with Client(server_url, token=server_token, namespace="default") as client:
        for mode in CASES:
            observations.append(await observe_retry(client, server_url, server_token, mode))
    print(json.dumps({"scope": "ordinary scalar local retries", "observations": observations}, sort_keys=True))
