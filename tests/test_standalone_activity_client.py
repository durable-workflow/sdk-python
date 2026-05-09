from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow import serializer
from durable_workflow.client import (
    Client,
    StandaloneActivityHandle,
)
from durable_workflow.errors import WorkflowFailed


def _mock_response(status: int = 200, json_data: dict | None = None) -> httpx.Response:
    content = json.dumps(json_data).encode() if json_data is not None else b""
    return httpx.Response(
        status_code=status,
        content=content,
        headers={"content-type": "application/json"} if json_data is not None else {},
        request=httpx.Request("GET", "http://test"),
    )


@pytest.fixture
def client() -> Client:
    return Client("http://localhost:8080", token="test-token", namespace="ns1")


class TestStartActivity:
    @pytest.mark.asyncio
    async def test_returns_handle_and_posts_envelope(self, client: Client) -> None:
        resp = _mock_response(201, {
            "activity_id": "job-1",
            "activity_execution_id": "exec-1",
            "workflow_run_id": "run-1",
            "workflow_id": "job-1",
            "workflow_type": "dw.standalone_activity",
            "activity_type": "shipping.send_invoice",
            "activity_class": None,
            "task_queue": "outbox",
            "namespace": "ns1",
            "status": "running",
            "payload_codec": "avro",
            "started_at": "2026-05-09T13:30:00Z",
            "schedule_to_start_deadline_at": None,
            "schedule_to_close_deadline_at": None,
            "command_status": "accepted",
            "command_source": "control_plane",
        })

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            handle = await client.start_activity(
                activity_type="shipping.send_invoice",
                task_queue="outbox",
                activity_id="job-1",
                input=[{"order_id": "o-42"}],
                retry_policy={"max_attempts": 3, "backoff_seconds": [1, 5, 15]},
                start_to_close_timeout_seconds=120,
            )

        assert isinstance(handle, StandaloneActivityHandle)
        assert handle.activity_id == "job-1"
        assert handle.workflow_run_id == "run-1"
        assert handle.activity_execution_id == "exec-1"
        assert handle.activity_type == "shipping.send_invoice"
        assert handle.workflow_type == "dw.standalone_activity"

        call = mock.call_args
        assert call.args[0] == "POST"
        assert call.args[1].endswith("/activities")
        body = call.kwargs.get("json") or call.args[2].get("json")
        assert body["activity_type"] == "shipping.send_invoice"
        assert body["task_queue"] == "outbox"
        assert body["activity_id"] == "job-1"
        assert body["retry_policy"] == {"max_attempts": 3, "backoff_seconds": [1, 5, 15]}
        assert body["start_to_close_timeout_seconds"] == 120

        envelope = body["input"]
        assert envelope["codec"] == "avro"
        assert serializer.decode(envelope["blob"], codec=envelope["codec"]) == [{"order_id": "o-42"}]

    @pytest.mark.asyncio
    async def test_omits_optional_fields_when_not_provided(self, client: Client) -> None:
        resp = _mock_response(201, {
            "activity_id": "auto-id",
            "activity_execution_id": "exec-2",
            "workflow_run_id": "run-2",
            "workflow_id": "auto-id",
            "workflow_type": "dw.standalone_activity",
            "activity_type": "noop",
            "task_queue": "default",
            "namespace": "ns1",
            "status": "running",
        })

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.start_activity(activity_type="noop", task_queue="default")

        body = mock.call_args.kwargs.get("json")
        assert "input" not in body
        assert "activity_id" not in body
        assert "retry_policy" not in body


class TestDescribeAndResultActivity:
    @pytest.mark.asyncio
    async def test_describe_decodes_result_envelope(self, client: Client) -> None:
        result_blob = serializer.envelope("Hello, Taylor!", codec="avro")
        resp = _mock_response(200, {
            "activity_id": "job-3",
            "workflow_id": "job-3",
            "workflow_run_id": "run-3",
            "workflow_type": "dw.standalone_activity",
            "activity_type": "tests.greeting-activity",
            "activity_status": "completed",
            "status": "completed",
            "closed_reason": "completed",
            "result": result_blob,
            "payload_codec": "avro",
        })

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            desc = await client.describe_activity("job-3")

        assert desc.status == "completed"
        assert desc.activity_status == "completed"
        assert desc.result == "Hello, Taylor!"

    @pytest.mark.asyncio
    async def test_get_activity_result_blocks_until_terminal(self, client: Client) -> None:
        terminal_envelope = serializer.envelope(42, codec="avro")
        running = _mock_response(200, {
            "activity_id": "job-4",
            "status": "running",
            "activity_status": "running",
        })
        completed = _mock_response(200, {
            "activity_id": "job-4",
            "status": "completed",
            "activity_status": "completed",
            "closed_reason": "completed",
            "result": terminal_envelope,
            "payload_codec": "avro",
        })

        responses = [running, completed]

        async def _request(*args, **kwargs):
            return responses.pop(0)

        with patch.object(client._http, "request", new=_request):
            handle = StandaloneActivityHandle(client, activity_id="job-4")
            result = await handle.result(poll_interval=0.0, timeout=2.0)

        assert result == 42

    @pytest.mark.asyncio
    async def test_get_activity_result_raises_on_failed(self, client: Client) -> None:
        resp = _mock_response(200, {
            "activity_id": "job-5",
            "status": "failed",
            "activity_status": "failed",
            "closed_reason": "failed",
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            handle = StandaloneActivityHandle(client, activity_id="job-5")
            with pytest.raises(WorkflowFailed):
                await handle.result(poll_interval=0.0, timeout=1.0)


class TestListActivities:
    @pytest.mark.asyncio
    async def test_returns_paged_executions(self, client: Client) -> None:
        resp = _mock_response(200, {
            "activities": [
                {
                    "activity_id": "job-1",
                    "workflow_run_id": "run-1",
                    "activity_type": "greeting",
                    "task_queue": "default",
                    "status": "running",
                    "status_bucket": "running",
                },
                {
                    "activity_id": "job-2",
                    "workflow_run_id": "run-2",
                    "activity_type": "greeting",
                    "task_queue": "default",
                    "status": "completed",
                    "status_bucket": "completed",
                },
            ],
            "activity_count": 2,
            "next_page_token": None,
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            page = await client.list_activities(status="completed", page_size=20)

        assert page.activity_count == 2
        assert [a.activity_id for a in page.activities] == ["job-1", "job-2"]

        call = mock.call_args
        assert "/activities?" in call.args[1]
        assert "status=completed" in call.args[1]
        assert "page_size=20" in call.args[1]
