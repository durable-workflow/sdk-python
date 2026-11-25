"""Query and validation rebuilds never diverge from the workflow task's entry read (.github#215)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import Worker, serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import QueryFailed, UpdateValidationFailed
from durable_workflow.workflow import (
    CompleteWorkflow,
    WorkflowContext,
    _prepare_query_state,
    _prepare_update_validation,
    _UpdateValidationReplayOptions,
    replay,
)
from tests.business_clock_workflows import TolerantEntryWorkflow, clock_text
from tests.query_task_helpers import leased_query_task
from tests.test_business_replay_clock import history
from tests.test_replay_clock import redrive_fixture

RUN = "business-clock-run"
CARRIER = {
    "schema": "durable-workflow.worker-replay-clock/v1", "workflow_run_id": RUN,
    "started_at": "2026-11-01T00:00:00.000000Z", "source": "run_started_at",
}
ENTRY = "2026-11-01T00:00:00.000000+00:00"
ABSENT = object()


@workflow.defn(name="tests.replay.no-entry-clock")
class NoEntryReadWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("clock.first", [])
        self.after = clock_text(ctx)
        yield ctx.start_timer(2)
        return self.after

    @workflow.query("after")
    def after_query(self) -> str | None:
        return self.after


def workflow_task_entry(entry: Any) -> Any:
    outcome = replay(TolerantEntryWorkflow, history(), [], payload_codec="avro", run_id=RUN,
                     replay_clock_entry=entry)
    try:
        command = outcome.commands[0]
        assert isinstance(command, CompleteWorkflow)
        return command.result
    finally:
        outcome.close()


def query(entry: Any, *, attested: bool, workflow_cls: type = TolerantEntryWorkflow, name: str = "entry") -> Any:
    return _prepare_query_state(workflow_cls, history()[:3], [], name, payload_codec="avro", run_id=RUN,
                                replay_clock_entry=entry, replay_clock_attested=attested)()


def validate(entry: Any, *, attested: bool) -> Any:
    return _prepare_update_validation(
        TolerantEntryWorkflow, history()[:3], [], "touch", [],
        replay_options=_UpdateValidationReplayOptions(
            run_id=RUN, payload_codec="avro", replay_clock_entry=entry, replay_clock_attested=attested,
        ),
    )()


def test_a_carrier_attested_query_matches_the_workflow_task() -> None:
    assert workflow_task_entry(CARRIER) == ENTRY
    assert query(CARRIER, attested=True) == ENTRY
    assert validate(CARRIER, attested=True) is None


def test_an_explicit_null_attests_that_the_workflow_task_had_no_carrier() -> None:
    assert workflow_task_entry(None) is None
    assert query(None, attested=True) is None
    assert validate(None, attested=True) is None


@pytest.mark.parametrize("entry", [None, {**CARRIER, "workflow_run_id": "another-run"}])
def test_an_unattested_rebuild_fails_instead_of_answering_with_other_state(entry: Any) -> None:
    attested = entry is not None  # An invalid object is never attestation.
    with pytest.raises(QueryFailed, match="replay_clock"):
        query(entry, attested=attested)
    with pytest.raises(UpdateValidationFailed, match="replay_clock"):
        validate(entry, attested=attested)


def test_only_entry_reads_need_attestation() -> None:
    # A read after the first durable step has history authority on both sides.
    assert query(None, attested=False, workflow_cls=NoEntryReadWorkflow, name="after") == (
        "2026-11-01T00:00:03.123456+00:00"
    )


def test_a_history_proved_redrive_entry_needs_no_carrier() -> None:
    from tests.business_clock_workflows import NativeRedriveBusinessClockWorkflow

    fixture = redrive_fixture("business-clock-native-redrive-entry")
    # The rebuild itself succeeds; only the unknown query name refuses afterwards.
    with pytest.raises(QueryFailed, match="unknown query"):
        _prepare_query_state(
            NativeRedriveBusinessClockWorkflow, fixture["history"], [], "missing", payload_codec="avro",
            run_id=fixture["workflow"]["run_id"], replay_clock_attested=False,
        )


@pytest.mark.parametrize("carrier,expected", [(ABSENT, "failed"), (None, "completed"), (CARRIER, "completed")])
def test_worker_query_task_attestation(carrier: Any, expected: str) -> None:
    async def run() -> tuple[str, AsyncMock]:
        client = AsyncMock(spec=Client)
        worker = Worker(client, task_queue="clock", workflows=[TolerantEntryWorkflow])
        task = {
            "query_task_id": "query-1", "workflow_id": "business-clock-instance", "run_id": RUN,
            "workflow_type": "tests.replay.tolerant-entry-clock", "query_name": "entry",
            "history_events": history()[:3], "workflow_arguments": serializer.envelope([], codec="avro"),
            "query_arguments": serializer.envelope([], codec="avro"), "payload_codec": "avro",
        }
        if carrier is not ABSENT:
            task["replay_clock"] = carrier
        return await worker._run_query_task(leased_query_task(worker, task)), client

    outcome, client = asyncio.run(run())
    assert outcome == expected
    if expected == "failed":
        client.complete_query_task.assert_not_awaited()
        assert "replay_clock" in client.fail_query_task.await_args.kwargs["message"]
    else:
        assert client.complete_query_task.await_args.kwargs["result"] == (ENTRY if carrier else None)
