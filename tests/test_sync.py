from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow.sync import Client, SyncWorkflowHandle


def _mock_response(status: int = 200, json_data: dict[str, Any] | None = None) -> httpx.Response:
    content = json.dumps(json_data).encode() if json_data is not None else b""
    return httpx.Response(
        status_code=status,
        content=content,
        headers={"content-type": "application/json"} if json_data is not None else {},
        request=httpx.Request("GET", "http://test"),
    )


class TestSyncClientHealth:
    def test_health(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(200, {"status": "ok"})
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            result = client.health()
            assert result["status"] == "ok"


class TestSyncClientStartWorkflow:
    def test_start_returns_sync_handle(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "workflow_id": "wf-1",
                "run_id": "run-1",
                "workflow_type": "greeter",
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            handle = client.start_workflow(
                workflow_type="greeter",
                task_queue="q1",
                workflow_id="wf-1",
            )
            assert isinstance(handle, SyncWorkflowHandle)
            assert handle.workflow_id == "wf-1"
            assert handle.run_id == "run-1"


class TestSyncClientDescribe:
    def test_describe(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "workflow_id": "wf-1",
                "run_id": "run-1",
                "workflow_type": "greeter",
                "status": "running",
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            desc = client.describe_workflow("wf-1")
            assert desc.status == "running"


class TestSyncClientSignal:
    def test_signal(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(200, {"ok": True})
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            client.signal_workflow("wf-1", "my-signal", args=["data"])


class TestSyncClientCancel:
    def test_cancel(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(200, {"ok": True})
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            client.cancel_workflow("wf-1", reason="test")


class TestSyncClientTerminate:
    def test_terminate(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(200, {"ok": True})
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            client.terminate_workflow("wf-1", reason="done")


class TestSyncClientQuery:
    def test_query(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(200, {"result": "active"})
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            result = client.query_workflow("wf-1", "status")
            assert result == {"result": "active"}


class TestSyncClientList:
    def test_list(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "workflows": [{"workflow_id": "wf-1", "run_id": "r1", "workflow_type": "g", "status": "running"}],
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            result = client.list_workflows(workflow_type="g")
            assert len(result.executions) == 1

    def test_list_task_queues(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "namespace": "ns1",
                "task_queues": [
                    {
                        "name": "orders",
                        "admission": {"workflow_tasks": {"status": "accepting"}},
                    }
                ],
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = client.list_task_queues()
            assert result.namespace == "ns1"
            assert result.task_queues[0].name == "orders"
            admission = result.task_queues[0].admission
            assert admission is not None
            assert admission.workflow_tasks is not None
            assert admission.workflow_tasks.status == "accepting"
            assert mock.call_args.args[:2] == ("GET", "/api/task-queues")

    def test_describe_task_queue(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "name": "orders/high priority",
                "namespace": "ns1",
                "pollers": [{"worker_id": "w1"}],
                "current_leases": [{"task_id": "t1"}],
                "admission": {"activity_tasks": {"status": "no_slots"}},
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = client.describe_task_queue("orders/high priority")
            assert result.name == "orders/high priority"
            assert result.pollers == [{"worker_id": "w1"}]
            assert result.current_leases == [{"task_id": "t1"}]
            assert result.admission is not None
            assert result.admission.activity_tasks is not None
            assert result.admission.activity_tasks.status == "no_slots"
            assert mock.call_args.args[:2] == ("GET", "/api/task-queues/orders%2Fhigh%20priority")


class TestSyncClientUpdate:
    def test_update(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(200, {"outcome": "completed", "result": "updated"})
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            result = client.update_workflow("wf-1", "my-update", args=["data"], wait_for="completed")
            assert result["outcome"] == "completed"


class TestSyncHandleUpdate:
    def test_handle_update(self) -> None:
        client = Client("http://localhost:8080")
        resp_start = _mock_response(200, {"workflow_id": "wf-1", "run_id": "r1", "workflow_type": "g"})
        resp_update = _mock_response(200, {"outcome": "completed"})
        mock = patch.object(
            client._async._http,
            "request",
            new_callable=AsyncMock,
            side_effect=[resp_start, resp_update],
        )
        with mock:
            handle = client.start_workflow(workflow_type="g", task_queue="q1", workflow_id="wf-1")
            result = handle.update("my-update", ["data"])
            assert result["outcome"] == "completed"


class TestSyncClientContextManager:
    def test_context_manager(self) -> None:
        with Client("http://localhost:8080") as client:
            assert client is not None


class TestSyncRunInsideLoop:
    @pytest.mark.asyncio
    async def test_raises_inside_loop(self) -> None:
        client = Client("http://localhost:8080")
        with pytest.raises(RuntimeError, match="already-running"):
            client.health()
