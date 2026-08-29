from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any

import pytest

from durable_workflow import Client, Worker, serializer, workflow
from durable_workflow.client import PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST
from durable_workflow.workflow import commands_to_server_commands, replay

MEMO_ENTRIES: dict[str, Any] = {
    "binary": b"same",
    "double": 7.0,
    "invalid_binary": b"\xff\x00",
    "long": 7,
    "nested": {
        "beta": 2,
        "alpha": 1,
    },
    "text": "same",
}

MEMO_BLOB = (
    "wwHioz3/VYAiNw4MDGJpbmFyeQgIc2FtZQxkb3VibGUGAAAAAAAAHEAcaW52YWxpZF9iaW5hcnkIBP8A"
    "CGxvbmcEDgxuZXN0ZWQOBAphbHBoYQQCCGJldGEEBAAIdGV4dAoIc2FtZQA="
)

CONTINUE_INITIAL_MEMO = {
    "existing": "retained",
    "overwritten": "before",
}

CONTINUE_MEMO_PATCH = {
    "added": "from-upsert",
    "overwritten": "after",
}

CONTINUE_MERGED_MEMO = {
    "added": "from-upsert",
    "existing": "retained",
    "overwritten": "after",
}


@workflow.defn(name="tests.memo-restart-python")
class MemoRestartWorkflow:
    def __init__(self) -> None:
        self.finished = False

    @workflow.signal("finish")
    def finish(self) -> None:
        self.finished = True

    def run(self, ctx):  # type: ignore[no-untyped-def]
        yield ctx.upsert_memo(MEMO_ENTRIES)
        yield ctx.wait_condition(
            lambda: self.finished,
            key="portable-memo-finished",
            timeout=300,
        )
        return "python-replayed-memo"


@workflow.defn(name="tests.memo-yielded-continue-python")
class MemoYieldedContinueWorkflow:
    def run(self, ctx, generation: int):  # type: ignore[no-untyped-def]
        if generation == 0:
            yield ctx.upsert_memo(CONTINUE_MEMO_PATCH)
            yield ctx.continue_as_new(1)
        return "python-continued-with-memo"


async def _wait_until_waiting_with_memo(handle: Any) -> Any:
    deadline = asyncio.get_running_loop().time() + 30
    last_description = None
    while asyncio.get_running_loop().time() < deadline:
        last_description = await handle.describe()
        if (last_description.status or "").lower() == "waiting" and last_description.memo:
            return last_description
        await asyncio.sleep(0.1)
    raise AssertionError(f"workflow did not expose waiting memo state: {last_description!r}")


def _memo_events(history: Any) -> list[dict[str, Any]]:
    assert isinstance(history, dict)
    events = history.get("events", history.get("history_events", []))
    assert isinstance(events, list)
    return [event for event in events if event.get("event_type") == "MemoUpserted"]


def _assert_typed_memo_event(event: dict[str, Any]) -> None:
    payload = event["payload"]
    for field in ("entries", "merged"):
        envelope = payload[field]
        assert envelope == {"codec": "avro", "blob": MEMO_BLOB}
        decoded = serializer.decode_envelope(envelope)
        assert type(decoded["long"]) is int
        assert decoded["long"] == 7
        assert type(decoded["double"]) is float
        assert decoded["double"] == 7.0
        assert type(decoded["binary"]) is bytes
        assert decoded["binary"] == b"same"
        assert type(decoded["invalid_binary"]) is bytes
        assert decoded["invalid_binary"] == b"\xff\x00"
        assert decoded["invalid_binary"].hex() == "ff00"
        assert type(decoded["text"]) is str
        assert decoded["text"] == "same"
        assert list(decoded["nested"]) == ["alpha", "beta"]
        assert decoded["nested"] == {"alpha": 1, "beta": 2}


async def _seed_worker_contract(client: Client, worker: Worker) -> None:
    """Make the workflow type startable before run_until begins polling."""
    await client.register_worker(
        worker_id=worker.worker_id,
        task_queue=worker.task_queue,
        supported_workflow_types=list(worker.workflows),
        workflow_definition_fingerprints=worker.workflow_definition_fingerprints,
        workflow_command_contracts=worker.workflow_command_contracts,
        supported_activity_types=[],
        capabilities=["memo_upserts"],
        capability_manifest=PORTABLE_WORKER_AFFINITY_CAPABILITY_MANIFEST,
    )


@pytest.mark.asyncio
async def test_fresh_python_worker_replays_persisted_server_memo_history(
    server_url: str,
    server_token: str,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    task_queue = f"memo-restart-python-{suffix}"
    workflow_id = f"memo-restart-python-{suffix}"

    async with Client(server_url, token=server_token, namespace="default") as client:
        first_worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[MemoRestartWorkflow],
            activities=[],
            worker_id=f"memo-python-before-{suffix}",
            poll_timeout=1.0,
            shutdown_timeout=5.0,
        )
        await _seed_worker_contract(client, first_worker)

        handle = await client.start_workflow(
            workflow_type="tests.memo-restart-python",
            task_queue=task_queue,
            workflow_id=workflow_id,
            input=[],
        )
        first_worker_task = asyncio.create_task(
            first_worker.run_until(workflow_id=workflow_id, timeout=60.0, poll_interval=0.1)
        )

        try:
            waiting = await _wait_until_waiting_with_memo(handle)
            assert waiting.memo == {
                "binary": {"$type": "bytes", "base64": "c2FtZQ=="},
                "double": 7.0,
                "invalid_binary": {"$type": "bytes", "base64": "/wA="},
                "long": 7,
                "nested": {"alpha": 1, "beta": 2},
                "text": "same",
            }

            first_history = await handle.get_history()
            first_memo_events = _memo_events(first_history)
            assert len(first_memo_events) == 1
            _assert_typed_memo_event(first_memo_events[0])
        finally:
            await first_worker.stop()
            with contextlib.suppress(asyncio.CancelledError):
                await first_worker_task

        replacement_worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[MemoRestartWorkflow],
            activities=[],
            worker_id=f"memo-python-after-{suffix}",
            poll_timeout=1.0,
            shutdown_timeout=5.0,
        )
        replacement_task = asyncio.create_task(
            replacement_worker.run_until(workflow_id=workflow_id, timeout=30.0, poll_interval=0.1)
        )

        await asyncio.sleep(0.2)
        await handle.signal("finish")
        completed = await replacement_task

        assert (completed.status or "").lower() == "completed"
        assert await handle.result(timeout=10.0) == "python-replayed-memo"

        final_description = await handle.describe()
        assert final_description.memo == waiting.memo

        final_history = await handle.get_history()
        final_memo_events = _memo_events(final_history)
        assert len(final_memo_events) == 1
        _assert_typed_memo_event(final_memo_events[0])


@pytest.mark.asyncio
async def test_yielded_continue_inherits_merged_memo_after_worker_restart(
    server_url: str,
    server_token: str,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    task_queue = f"memo-continue-python-{suffix}"
    workflow_id = f"memo-continue-python-{suffix}"

    async with Client(server_url, token=server_token, namespace="default") as client:
        first_worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[MemoYieldedContinueWorkflow],
            activities=[],
            worker_id=f"memo-continue-before-{suffix}",
        )
        await _seed_worker_contract(client, first_worker)

        handle = await client.start_workflow(
            workflow_type="tests.memo-yielded-continue-python",
            task_queue=task_queue,
            workflow_id=workflow_id,
            input=[0],
            memo=CONTINUE_INITIAL_MEMO,
        )
        assert handle.run_id is not None
        first_run_id = handle.run_id

        try:
            first_task = await client.poll_workflow_task(
                worker_id=first_worker.worker_id,
                task_queue=task_queue,
                timeout=10.0,
            )
            assert first_task is not None, "expected the initial workflow task"

            payload_codec = first_task.get("payload_codec") or serializer.AVRO_CODEC
            decoded = serializer.decode_envelope(
                first_task.get("arguments"),
                codec=payload_codec,
            )
            start_input = decoded if isinstance(decoded, list) else [decoded]
            outcome = replay(
                MemoYieldedContinueWorkflow,
                first_task.get("history_events", []),
                start_input,
                run_id=first_task.get("run_id", ""),
            )
            commands = commands_to_server_commands(
                outcome.commands,
                task_queue,
                payload_codec=payload_codec,
            )
            assert [command["type"] for command in commands] == [
                "upsert_memo",
                "continue_as_new",
            ]

            await client.complete_workflow_task(
                task_id=first_task["task_id"],
                lease_owner=first_worker.worker_id,
                workflow_task_attempt=first_task.get("workflow_task_attempt", 1),
                commands=commands,
            )
        finally:
            await client.deregister_worker_registration(first_worker.worker_id)

        replacement_worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[MemoYieldedContinueWorkflow],
            activities=[],
            worker_id=f"memo-continue-after-{suffix}",
            poll_timeout=1.0,
            shutdown_timeout=5.0,
        )
        completed = await replacement_worker.run_until(
            workflow_id=workflow_id,
            timeout=30.0,
            poll_interval=0.1,
        )

        assert (completed.status or "").lower() == "completed"
        assert completed.output == "python-continued-with-memo"
        assert completed.run_id != first_run_id
        assert completed.memo == CONTINUE_MERGED_MEMO

        runs = await handle.list_runs()
        assert runs.run_count == 2
        assert [run.run_id for run in runs.runs] == [first_run_id, completed.run_id]
        assert runs.runs[1].memo == CONTINUE_MERGED_MEMO

        first_history = await client.get_history(workflow_id, first_run_id)
        successor_history = await client.get_history(workflow_id, completed.run_id or "")
        first_memo_events = _memo_events(first_history)
        assert len(first_memo_events) == 1
        assert serializer.decode_envelope(first_memo_events[0]["payload"]["entries"]) == CONTINUE_MEMO_PATCH
        assert serializer.decode_envelope(first_memo_events[0]["payload"]["merged"]) == CONTINUE_MERGED_MEMO
        assert _memo_events(successor_history) == []
