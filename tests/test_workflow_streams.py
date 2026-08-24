from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.client import Client, WorkflowStreamAppendItem
from durable_workflow.external_storage import LocalFilesystemExternalStorage
from durable_workflow.workflow import (
    RecordSideEffect,
    WorkflowContext,
    commands_to_server_commands,
    replay,
)


@pytest.mark.asyncio
async def test_typed_client_lifecycle_resume_and_external_reference() -> None:
    client = Client("http://localhost:8080", token="token")
    envelope = serializer.envelope({"message": "one"})
    client._request = AsyncMock(
        side_effect=[
            {
                "workflow_id": "wf",
                "workflow_run_id": "run",
                "streams": [
                    {
                        "stream_name": "output",
                        "status": "open",
                        "last_offset": 0,
                        "total_items": 1,
                        "pending_items": 1,
                    }
                ],
            },
            {
                "workflow_id": "wf",
                "workflow_run_id": "run",
                "stream": {
                    "stream_name": "output",
                    "status": "open",
                    "last_offset": 0,
                    "total_items": 1,
                    "pending_items": 1,
                },
            },
            {
                "workflow_id": "wf",
                "workflow_run_id": "run",
                "stream": {
                    "stream_name": "output",
                    "status": "open",
                    "last_offset": 1,
                    "total_items": 2,
                    "pending_items": 2,
                },
                "items": [
                    {"offset": 0, "payload": envelope, "payload_codec": "avro"},
                    {"offset": 1, "payload_reference": "s3://bucket/item.avro", "payload_codec": "avro"},
                ],
                "next_offset": 2,
                "terminal": False,
            },
            {
                "stream": {
                    "stream_name": "output",
                    "status": "open",
                    "last_offset": 2,
                    "total_items": 3,
                    "pending_items": 3,
                },
                "accepted_offsets": [2],
                "accepted": 1,
                "deduped": 0,
            },
            {
                "stream": {
                    "stream_name": "output",
                    "status": "errored",
                    "last_offset": 2,
                    "total_items": 3,
                    "pending_items": 3,
                    "error_reason": "producer failed",
                }
            },
        ]
    )

    streams = await client.list_workflow_streams("wf", "run")
    described = await client.describe_workflow_stream("wf", "run", "output")
    page = await client.subscribe_workflow_stream("wf", "run", "output", from_offset=0, max_items=10, wait_seconds=3)
    appended = await client.append_workflow_stream(
        "wf",
        "run",
        "output",
        [WorkflowStreamAppendItem(payload_reference="s3://bucket/next.avro")],
    )
    closed = await client.close_workflow_stream("wf", "run", "output", error_reason="producer failed")

    assert streams[0].workflow_id == "wf"
    assert described.total_items == 1
    assert page.items[0].payload == {"message": "one"}
    assert page.items[1].payload is None
    assert page.items[1].payload_reference == "s3://bucket/item.avro"
    assert page.next_offset == 2
    assert appended.accepted_offsets == [2]
    assert closed.terminal is True
    assert closed.error_reason == "producer failed"
    await client.aclose()


@pytest.mark.asyncio
async def test_subscription_cancel_event_cancels_bounded_long_poll() -> None:
    client = Client("http://localhost:8080", token="token")
    release = asyncio.Event()

    async def blocked_request(*args: object, **kwargs: object) -> object:
        await release.wait()
        return {}

    client._request = AsyncMock(side_effect=blocked_request)
    cancelled = asyncio.Event()
    request = asyncio.create_task(
        client.subscribe_workflow_stream("wf", "run", "output", wait_seconds=60, cancel_event=cancelled)
    )
    await asyncio.sleep(0)
    cancelled.set()

    with pytest.raises(asyncio.CancelledError):
        await request
    await client.aclose()


@pytest.mark.asyncio
async def test_stream_items_use_configured_external_payload_storage(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    client = Client(
        "http://localhost:8080",
        token="token",
        external_storage=storage,
        external_storage_threshold_bytes=1,
    )
    external_envelope = serializer.external_storage_envelope(
        {"message": "stored outside the stream row"},
        external_storage=storage,
        threshold_bytes=1,
    )
    client._request = AsyncMock(
        side_effect=[
            {
                "stream": {"stream_name": "output", "status": "open"},
                "accepted_offsets": [0],
                "accepted": 1,
                "deduped": 0,
            },
            {
                "stream": {"stream_name": "output", "status": "open", "last_offset": 0},
                "items": [{"offset": 0, "payload": external_envelope, "payload_codec": "avro"}],
                "next_offset": 1,
                "terminal": False,
            },
        ]
    )

    await client.append_workflow_stream(
        "wf",
        "run",
        "output",
        [WorkflowStreamAppendItem(payload={"message": "stored outside the stream row"})],
    )
    page = await client.subscribe_workflow_stream("wf", "run", "output")

    append_body = client._request.await_args_list[0].kwargs["json"]
    assert append_body["items"][0]["payload"]["external_storage"]["uri"]
    assert append_body["items"][0]["payload_reference"]
    assert page.items[0].payload == {"message": "stored outside the stream row"}
    await client.aclose()


@workflow.defn(name="stream-author")
class StreamAuthor:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.append_workflow_stream(
            "output",
            [
                WorkflowStreamAppendItem(payload={"blob": "domain value"}),
                WorkflowStreamAppendItem(payload_reference="s3://bucket/item.avro"),
            ],
        )
        yield ctx.error_workflow_stream("output", "producer failed")
        return "done"


def test_workflow_authoring_uses_command_identity_and_replay_skips_duplicate_append() -> None:
    first = replay(
        StreamAuthor,
        [],
        [],
        workflow_id="wf",
        run_id="run",
        workflow_command_id="command-7",
    )
    assert isinstance(first.commands[0], RecordSideEffect)
    assert isinstance(first.commands[1], RecordSideEffect)
    wire = commands_to_server_commands(first.commands[:2], "queue")
    assert wire[0]["workflow_stream"]["command_identity"] == "command-7"
    assert wire[0]["workflow_stream"]["command_ordinal"] == 0
    assert wire[0]["workflow_stream"]["items"][0]["idempotency_key"] == "dw-stream:command-7:0:0"
    assert serializer.decode_envelope(wire[0]["workflow_stream"]["items"][0]["payload"]) == {"blob": "domain value"}
    assert wire[0]["workflow_stream"]["items"][1]["payload_reference"] == "s3://bucket/item.avro"
    assert wire[1]["workflow_stream"]["operation"] == "error"

    history = [
        {"event_type": "SideEffectRecorded", "payload": {"result": serializer.envelope(None)}},
        {"event_type": "SideEffectRecorded", "payload": {"result": serializer.envelope(None)}},
    ]
    replayed = replay(
        StreamAuthor,
        history,
        [],
        workflow_id="wf",
        run_id="run",
        workflow_command_id="command-7",
    )
    assert all(not isinstance(command, RecordSideEffect) for command in replayed.commands)
