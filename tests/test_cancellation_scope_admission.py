from __future__ import annotations

from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import QueryFailed
from durable_workflow.worker import Worker
from durable_workflow.workflow import LocalActivityExecutionAborted


@workflow.defn(name="scope-admission-probe")
class ScopeProbe:
    calls: list[str] = []

    def __init__(self) -> None:
        self.calls.append("constructed")

    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        self.calls.append("run")
        yield ctx.start_timer(1)
        return "done"

    @workflow.query("state")
    def state(self) -> str:
        self.calls.append("query")
        return "state"


@pytest.fixture(autouse=True)
def reset_calls() -> None:
    ScopeProbe.calls = []


def scoped_histories() -> list[dict[str, Any]]:
    histories: list[dict[str, Any]] = [
        {"event_type": name, "payload": {"sequence": 1, "scope_id": "scope-one"}}
        for name in ("CancellationScopeOpened", "CancellationScopeRequested", "CancellationScopeDelivered", "CancellationScopeRequestConflicted")
    ]
    for location in (None, "activity", "timer", "child_workflow"):
        value = {"cancellation_scope_id": "scope-one"}
        histories.append({"event_type": "TimerScheduled", "payload": value if location is None else {location: value}})
    for malformed in (None, True, 1, "", [], {}):
        histories.append({"event_type": "TimerScheduled", "payload": {"cancellation_scope_id": malformed}})
    return histories


@pytest.mark.parametrize("event", scoped_histories())
def test_unqualified_scope_replay_stops_before_constructing_or_running_workflow(event: dict[str, Any]) -> None:
    with pytest.raises(LocalActivityExecutionAborted, match="cancellation_scope_execution_not_supported.*Python"):
        workflow.replay(ScopeProbe, [event], [], run_id="run-one", payload_codec="avro")
    assert ScopeProbe.calls == []


def test_query_replay_does_not_enter_scope_code_or_its_query_handler() -> None:
    with pytest.raises(QueryFailed, match="cancellation_scope_execution_not_supported.*Python"):
        workflow.query_state(ScopeProbe, [scoped_histories()[0]], [], "state", run_id="run-one", payload_codec="avro")
    assert ScopeProbe.calls == []


@pytest.mark.parametrize("explicit_root", [False, True])
def test_historical_omission_and_explicit_root_preserve_existing_replay(explicit_root: bool) -> None:
    payload: dict[str, Any] = {"sequence": 1, "duration_seconds": 1}
    if explicit_root:
        payload["cancellation_scope_id"] = "root"
    history = [{"event_type": name, "payload": deepcopy(payload)} for name in ("TimerScheduled", "TimerFired")]
    outcome = workflow.replay(ScopeProbe, history, [], run_id="run-one", payload_codec="avro")
    assert [type(command).__name__ for command in outcome.commands] == ["CompleteWorkflow"]
    assert ScopeProbe.calls == ["constructed", "run"]


def test_scope_named_application_values_are_preserved_as_data() -> None:
    @workflow.defn(name="scope-data-probe")
    class DataProbe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            return (yield ctx.side_effect(lambda: pytest.fail("Recorded application data must replay.")))

    value = {"cancellation_scope_id": "application-owned", "activity": {"cancellation_scope_id": None}}
    event = {"event_type": "SideEffectRecorded", "payload": {
        "sequence": 1, "result": serializer.envelope(value, codec="avro"),
    }}
    outcome = workflow.replay(DataProbe, [event], [], run_id="run-one", payload_codec="avro")
    assert outcome.commands[0].result == value


async def test_worker_abandons_unsupported_scope_without_publishing_a_terminal_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="scope", worker_id="scope-worker", workflows=[ScopeProbe])
    task = {"task_id": "scope-task", "workflow_id": "scope-instance", "run_id": "run-one",
        "workflow_type": "scope-admission-probe", "workflow_task_attempt": 2, "payload_codec": "avro",
        "arguments": serializer.envelope([], codec="avro"), "history_events": [scoped_histories()[0]]}
    assert await worker._run_workflow_task_core(task) is None
    assert ScopeProbe.calls == []
    client.complete_workflow_task.assert_not_awaited()
    client.fail_workflow_task.assert_not_awaited()
    assert "cancellation_scope_execution_not_supported" in caplog.text
