from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow import serializer, workflow
from durable_workflow.client import Client
from durable_workflow.external_storage import LocalFilesystemExternalStorage
from durable_workflow.worker import MESSAGE_STREAMS_CAPABILITY, _workflow_command_contract
from durable_workflow.workflow import (
    MESSAGE_STREAM_CURSOR_SCHEMA,
    MESSAGE_STREAM_SCHEMA,
    MESSAGE_STREAM_SIGNAL,
    CompleteWorkflow,
    replay,
)


@workflow.defn(name="message-stream-workflow")
class MessageStreamWorkflow:
    def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
        messages = yield from ctx.message_stream("orders").receive(2)
        return [[message.message_id, message.position, message.arguments] for message in messages]


def _message(
    message_id: str,
    position: int,
    arguments: list[object],
    *,
    payload_envelope: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "event_type": "SignalReceived",
        "payload": {
            "signal_name": MESSAGE_STREAM_SIGNAL,
            "value": serializer.envelope(
                [
                    {
                        "schema": MESSAGE_STREAM_SCHEMA,
                        "stream_name": "orders",
                        "message_id": message_id,
                        "position": position,
                        "payload_envelope": payload_envelope or serializer.envelope(arguments),
                    }
                ]
            ),
            "payload_codec": serializer.AVRO_CODEC,
        },
    }


def _cursor(through_position: int) -> dict[str, object]:
    return {
        "event_type": "SignalReceived",
        "payload": {
            "signal_name": MESSAGE_STREAM_SIGNAL,
            "value": serializer.envelope(
                [
                    {
                        "schema": MESSAGE_STREAM_CURSOR_SCHEMA,
                        "stream_name": "orders",
                        "through_position": through_position,
                    }
                ]
            ),
            "payload_codec": serializer.AVRO_CODEC,
        },
    }


def test_replay_consumes_a_bounded_ordered_batch_and_reports_one_cursor() -> None:
    outcome = replay(
        MessageStreamWorkflow,
        [_message("message-1", 1, ["one"]), _message("message-2", 2, ["two"])],
        [],
    )

    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == [
        ["message-1", 1, ["one"]],
        ["message-2", 2, ["two"]],
    ]
    assert outcome.message_stream_cursors == [{"stream_name": "orders", "through_position": 2}]
    assert outcome.message_stream_waits == []


def test_replay_deduplicates_repeated_internal_delivery_by_position_and_identity() -> None:
    outcome = replay(
        MessageStreamWorkflow,
        [
            _message("message-1", 1, ["one"]),
            _message("message-1", 1, ["one"]),
            _message("message-2", 2, ["two"]),
        ],
        [],
    )

    assert outcome.message_stream_cursors == [{"stream_name": "orders", "through_position": 2}]
    assert len(outcome.commands[0].result) == 2  # type: ignore[union-attr]


def test_empty_stream_opens_a_durable_wait_and_reports_the_pending_position() -> None:
    outcome = replay(MessageStreamWorkflow, [], [])

    assert outcome.commands[0].condition_key == "message-stream:orders:0"  # type: ignore[union-attr]
    assert outcome.message_stream_cursors == []
    assert outcome.message_stream_waits == [{"stream_name": "orders", "after_position": 0}]


def test_continue_as_new_cursor_checkpoint_preserves_the_global_pending_position() -> None:
    outcome = replay(MessageStreamWorkflow, [_cursor(2)], [])

    assert outcome.commands[0].condition_key == "message-stream:orders:2"  # type: ignore[union-attr]
    assert outcome.message_stream_cursors == [{"stream_name": "orders", "through_position": 2}]
    assert outcome.message_stream_waits == [{"stream_name": "orders", "after_position": 2}]


def test_worker_contract_does_not_advertise_runtime_transport_as_a_user_signal() -> None:
    contract = _workflow_command_contract(MessageStreamWorkflow)

    assert MESSAGE_STREAM_SIGNAL not in contract["signals"]
    assert all(item["name"] != MESSAGE_STREAM_SIGNAL for item in contract["signal_contracts"])


def test_runtime_transport_signal_cannot_be_declared_by_user_workflow_code() -> None:
    with pytest.raises(ValueError, match="reserved by the workflow runtime"):
        workflow.signal(MESSAGE_STREAM_SIGNAL)


def test_replay_preserves_typed_avro_arguments_across_worker_replacement() -> None:
    values: list[object] = [b"bytes", 1, 1.0, [], {}, {"nested": [b"value"]}]
    history = [
        _message("message-1", 1, values),
        _message("message-1", 1, values),
        _message("message-2", 2, ["two"]),
    ]

    first_worker = replay(MessageStreamWorkflow, history, [])
    replacement_worker = replay(MessageStreamWorkflow, history, [])

    assert first_worker.commands[0].result == replacement_worker.commands[0].result  # type: ignore[union-attr]
    assert replacement_worker.commands[0].result[0][2] == values  # type: ignore[union-attr]
    assert isinstance(replacement_worker.commands[0].result[0][2][1], int)  # type: ignore[union-attr]
    assert isinstance(replacement_worker.commands[0].result[0][2][2], float)  # type: ignore[union-attr]
    assert MESSAGE_STREAMS_CAPABILITY == "message_streams"


def test_replay_resolves_reference_backed_message_payload_without_changing_identity(tmp_path: Path) -> None:
    storage = LocalFilesystemExternalStorage(tmp_path)
    payload_envelope = serializer.external_storage_envelope(
        [b"bytes", 7, 7.0],
        external_storage=storage,
        threshold_bytes=1,
        codec=serializer.AVRO_CODEC,
    )
    outcome = replay(
        MessageStreamWorkflow,
        [_message("message-1", 1, [], payload_envelope=payload_envelope)],
        [],
        external_storage=storage,
    )

    assert outcome.commands[0].result == [["message-1", 1, [b"bytes", 7, 7.0]]]  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_client_appends_message_identity_and_avro_input() -> None:
    client = Client("http://localhost:8080", token="token", namespace="default")
    response = httpx.Response(
        202,
        json={"accepted": True, "position": 3},
        request=httpx.Request("POST", "http://localhost"),
    )

    with patch.object(client._http, "request", new_callable=AsyncMock, return_value=response) as request:
        result = await client.append_message_stream("workflow-1", "orders", "message-3", args=[{"n": 3}])

    assert result["position"] == 3
    assert request.call_args.args[:2] == ("POST", "/api/workflows/workflow-1/message-streams/orders/messages")
    body = request.call_args.kwargs["json"]
    assert body["message_id"] == "message-3"
    assert serializer.decode_envelope(body["input"]) == [{"n": 3}]


@pytest.mark.asyncio
async def test_worker_completion_sends_cursor_and_wait_metadata() -> None:
    client = Client("http://localhost:8080", token="token", namespace="default")
    response = httpx.Response(200, json={"outcome": "completed"}, request=httpx.Request("POST", "http://localhost"))

    with patch.object(client._http, "request", new_callable=AsyncMock, return_value=response) as request:
        await client.complete_workflow_task(
            task_id="task-1",
            lease_owner="worker-1",
            workflow_task_attempt=1,
            commands=[{"type": "complete_workflow"}],
            message_stream_cursors=[{"stream_name": "orders", "through_position": 2}],
            message_stream_waits=[{"stream_name": "approval", "after_position": 0}],
        )

    body = request.call_args.kwargs["json"]
    assert body["message_stream_cursors"] == [{"stream_name": "orders", "through_position": 2}]
    assert body["message_stream_waits"] == [{"stream_name": "approval", "after_position": 0}]


@pytest.mark.asyncio
async def test_prefeature_worker_protocol_cannot_advertise_message_streams(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.14")
    client = Client("http://localhost:8080", token="token", namespace="default")

    with pytest.raises(ValueError, match="require worker protocol 1.15"):
        await client.register_worker(
            worker_id="worker-1",
            task_queue="queue",
            capabilities=[MESSAGE_STREAMS_CAPABILITY],
        )


@pytest.mark.asyncio
async def test_prefeature_worker_protocol_cannot_submit_message_stream_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", "1.14")
    client = Client("http://localhost:8080", token="token", namespace="default")

    with pytest.raises(ValueError, match="requires worker protocol 1.15"):
        await client.complete_workflow_task(
            task_id="task-1",
            lease_owner="worker-1",
            workflow_task_attempt=1,
            commands=[{"type": "complete_workflow"}],
            message_stream_cursors=[{"stream_name": "orders", "through_position": 2}],
        )
