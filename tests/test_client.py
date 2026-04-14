from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow.client import CONTROL_PLANE_VERSION, PROTOCOL_VERSION, Client, WorkflowExecution, WorkflowHandle
from durable_workflow.errors import (
    InvalidArgument,
    QueryFailed,
    ServerError,
    Unauthorized,
    UpdateRejected,
    WorkflowAlreadyStarted,
    WorkflowNotFound,
)


def _mock_response(status: int = 200, json_data: dict | None = None, text: str = "") -> httpx.Response:
    content = json.dumps(json_data).encode() if json_data is not None else text.encode()
    return httpx.Response(
        status_code=status,
        content=content,
        headers={"content-type": "application/json"} if json_data is not None else {},
        request=httpx.Request("GET", "http://test"),
    )


@pytest.fixture
def client() -> Client:
    return Client("http://localhost:8080", token="test-token", namespace="ns1")


class TestHeaders:
    def test_control_plane_headers(self, client: Client) -> None:
        h = client._headers(worker=False)
        assert h["Authorization"] == "Bearer test-token"
        assert h["X-Namespace"] == "ns1"
        assert h["X-Durable-Workflow-Control-Plane-Version"] == CONTROL_PLANE_VERSION

    def test_worker_headers(self, client: Client) -> None:
        h = client._headers(worker=True)
        assert h["X-Durable-Workflow-Protocol-Version"] == PROTOCOL_VERSION
        assert "X-Durable-Workflow-Control-Plane-Version" not in h

    def test_no_token(self) -> None:
        c = Client("http://localhost:8080")
        h = c._headers()
        assert "Authorization" not in h


class TestStartWorkflow:
    @pytest.mark.asyncio
    async def test_success(self, client: Client) -> None:
        resp = _mock_response(201, {
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "workflow_type": "greeter",
            "namespace": "ns1",
            "status": "running",
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            handle = await client.start_workflow(
                workflow_type="greeter",
                task_queue="q1",
                workflow_id="wf-1",
                input=["hello"],
            )
            assert isinstance(handle, WorkflowHandle)
            assert handle.workflow_id == "wf-1"
            assert handle.run_id == "run-1"
            call_args = mock.call_args
            body = call_args.kwargs.get("json") or call_args[1].get("json")
            assert body["workflow_type"] == "greeter"
            assert body["input"]["codec"] == "json"
            import json as _json
            assert _json.loads(body["input"]["blob"]) == ["hello"]

    @pytest.mark.asyncio
    async def test_duplicate_raises(self, client: Client) -> None:
        resp = _mock_response(409, {"reason": "duplicate_not_allowed", "message": "dup"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(WorkflowAlreadyStarted),
        ):
            await client.start_workflow(
                workflow_type="greeter", task_queue="q1", workflow_id="wf-1"
            )


class TestDescribeWorkflow:
    @pytest.mark.asyncio
    async def test_success(self, client: Client) -> None:
        resp = _mock_response(200, {
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "workflow_type": "greeter",
            "status": "running",
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            desc = await client.describe_workflow("wf-1")
            assert isinstance(desc, WorkflowExecution)
            assert desc.status == "running"

    @pytest.mark.asyncio
    async def test_not_found(self, client: Client) -> None:
        resp = _mock_response(404, {"reason": "workflow_not_found", "message": "not found"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(WorkflowNotFound),
        ):
            await client.describe_workflow("wf-missing")


class TestSignalWorkflow:
    @pytest.mark.asyncio
    async def test_signal(self, client: Client) -> None:
        resp = _mock_response(200, {"ok": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.signal_workflow("wf-1", "my-signal", args=["data"])
            call_args = mock.call_args
            assert "/signal/my-signal" in call_args[0][1]
            body = call_args.kwargs.get("json") or call_args[1].get("json")
            assert body["input"] == ["data"]


class TestCancelWorkflow:
    @pytest.mark.asyncio
    async def test_cancel(self, client: Client) -> None:
        resp = _mock_response(200, {"ok": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.cancel_workflow("wf-1", reason="test")
            call_args = mock.call_args
            assert "/cancel" in call_args[0][1]
            body = call_args.kwargs.get("json") or call_args[1].get("json")
            assert body["reason"] == "test"


class TestTerminateWorkflow:
    @pytest.mark.asyncio
    async def test_terminate(self, client: Client) -> None:
        resp = _mock_response(200, {"ok": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.terminate_workflow("wf-1", reason="done")
            call_args = mock.call_args
            assert "/terminate" in call_args[0][1]


class TestQueryWorkflow:
    @pytest.mark.asyncio
    async def test_query(self, client: Client) -> None:
        resp = _mock_response(200, {"result": "active"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await client.query_workflow("wf-1", "status")
            assert result == {"result": "active"}

    @pytest.mark.asyncio
    async def test_query_not_found(self, client: Client) -> None:
        resp = _mock_response(404, {"reason": "query_not_found", "message": "query [status] not declared"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(QueryFailed),
        ):
            await client.query_workflow("wf-1", "status")

    @pytest.mark.asyncio
    async def test_query_rejected(self, client: Client) -> None:
        resp = _mock_response(409, {"reason": "query_rejected", "message": "workflow unavailable"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(QueryFailed),
        ):
            await client.query_workflow("wf-1", "status")


class TestListWorkflows:
    @pytest.mark.asyncio
    async def test_list(self, client: Client) -> None:
        resp = _mock_response(200, {
            "data": [
                {"workflow_id": "wf-1", "run_id": "r1", "workflow_type": "greeter", "status": "running"},
                {"workflow_id": "wf-2", "run_id": "r2", "workflow_type": "greeter", "status": "completed"},
            ],
            "next_page_token": "abc",
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await client.list_workflows(workflow_type="greeter", page_size=10)
            assert len(result.executions) == 2
            assert result.next_page_token == "abc"
            assert result.executions[0].workflow_id == "wf-1"


class TestErrorMapping:
    @pytest.mark.asyncio
    async def test_401_unauthorized(self, client: Client) -> None:
        resp = _mock_response(401, {"message": "invalid token"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(Unauthorized),
        ):
            await client.describe_workflow("wf-1")

    @pytest.mark.asyncio
    async def test_422_invalid_argument(self, client: Client) -> None:
        resp = _mock_response(422, {"message": "bad input", "errors": {"field": ["required"]}})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            with pytest.raises(InvalidArgument) as exc_info:
                await client.start_workflow(workflow_type="x", task_queue="q", workflow_id="w")
            assert exc_info.value.errors == {"field": ["required"]}

    @pytest.mark.asyncio
    async def test_500_server_error(self, client: Client) -> None:
        resp = _mock_response(500, {"message": "internal"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            with pytest.raises(ServerError) as exc_info:
                await client.describe_workflow("wf-1")
            assert exc_info.value.status == 500


class TestFailWorkflowTask:
    @pytest.mark.asyncio
    async def test_body_shape(self, client: Client) -> None:
        resp = _mock_response(200, {"task_id": "t1", "outcome": "failed"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.fail_workflow_task(
                task_id="t1",
                lease_owner="w1",
                workflow_task_attempt=1,
                message="replay error",
                failure_type="RuntimeError",
                stack_trace="traceback...",
            )
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert "failure" in body
            assert body["failure"]["message"] == "replay error"
            assert body["failure"]["type"] == "RuntimeError"
            assert body["failure"]["stack_trace"] == "traceback..."
            assert "message" not in body  # must be nested in failure, not top-level


class TestFailActivityTask:
    @pytest.mark.asyncio
    async def test_body_shape(self, client: Client) -> None:
        resp = _mock_response(200, {"task_id": "t1", "outcome": "failed"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.fail_activity_task(
                task_id="t1",
                activity_attempt_id="a1",
                lease_owner="w1",
                message="activity error",
                non_retryable=True,
            )
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert "failure" in body
            assert body["failure"]["message"] == "activity error"
            assert body["failure"]["non_retryable"] is True


class TestHeartbeatActivityTask:
    @pytest.mark.asyncio
    async def test_heartbeat(self, client: Client) -> None:
        resp = _mock_response(200, {"task_id": "t1", "cancel_requested": False})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await client.heartbeat_activity_task(
                task_id="t1",
                activity_attempt_id="a1",
                lease_owner="w1",
            )
            assert result["cancel_requested"] is False


class TestUpdateWorkflow:
    @pytest.mark.asyncio
    async def test_update(self, client: Client) -> None:
        resp = _mock_response(200, {"outcome": "completed", "result": "updated"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.update_workflow(
                "wf-1", "my-update", args=["data"], wait_for="completed", wait_timeout_seconds=10
            )
            assert result["outcome"] == "completed"
            call_args = mock.call_args
            assert "/update/my-update" in call_args[0][1]
            body = call_args.kwargs.get("json") or call_args[1].get("json")
            assert body["input"] == ["data"]
            assert body["wait_for"] == "completed"
            assert body["wait_timeout_seconds"] == 10

    @pytest.mark.asyncio
    async def test_update_rejected(self, client: Client) -> None:
        resp = _mock_response(409, {"reason": "update_rejected", "message": "rejected"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(UpdateRejected),
        ):
            await client.update_workflow("wf-1", "my-update")

    @pytest.mark.asyncio
    async def test_update_no_args(self, client: Client) -> None:
        resp = _mock_response(200, {"outcome": "accepted"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.update_workflow("wf-1", "my-update")
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert "input" not in body

    @pytest.mark.asyncio
    async def test_update_with_request_id(self, client: Client) -> None:
        resp = _mock_response(200, {"outcome": "accepted"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.update_workflow("wf-1", "my-update", request_id="req-123")
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["request_id"] == "req-123"


class TestRegisterWorker:
    @pytest.mark.asyncio
    async def test_register(self, client: Client) -> None:
        resp = _mock_response(201, {"worker_id": "w1", "registered": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.register_worker(
                worker_id="w1",
                task_queue="q1",
                supported_workflow_types=["greeter"],
                supported_activity_types=["greet"],
            )
            assert result["registered"] is True
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["runtime"] == "python"
