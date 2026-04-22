from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow import serializer
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

    def test_plane_scoped_tokens(self) -> None:
        c = Client(
            "http://localhost:8080",
            token="legacy-token",
            control_token="operator-token",
            worker_token="worker-token",
        )

        control_headers = c._headers(worker=False)
        worker_headers = c._headers(worker=True)

        assert control_headers["Authorization"] == "Bearer operator-token"
        assert worker_headers["Authorization"] == "Bearer worker-token"

    def test_single_plane_token_can_fetch_cluster_info(self) -> None:
        c = Client("http://localhost:8080", worker_token="worker-token")

        control_headers = c._headers(worker=False)
        worker_headers = c._headers(worker=True)

        assert control_headers["Authorization"] == "Bearer worker-token"
        assert worker_headers["Authorization"] == "Bearer worker-token"


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
            assert body["input"]["codec"] == "avro"
            assert serializer.decode(body["input"]["blob"], codec="avro") == ["hello"]

    @pytest.mark.asyncio
    async def test_start_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-start-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        expected = sdk["expected_body"]
        envelope_contract = sdk["payload_envelope"]

        resp = _mock_response(201, {
            "workflow_id": fixture["semantic_body"]["workflow_id"],
            "run_id": "run-polyglot-231",
            "workflow_type": fixture["semantic_body"]["workflow_type"],
            "namespace": "ns1",
            "status": "running",
        })

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            handle = await client.start_workflow(**sdk["kwargs"])

        assert handle.workflow_id == fixture["semantic_body"]["workflow_id"]

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        body = call_args.kwargs.get("json") or call_args[1].get("json")

        for field, value in expected.items():
            assert body[field] == value

        envelope = body[envelope_contract["field"]]
        assert envelope["codec"] == envelope_contract["codec"]
        assert serializer.decode(envelope["blob"], codec=envelope["codec"]) == envelope_contract["decoded"]

        semantic = fixture["semantic_body"]
        for field in ["workflow_type", "workflow_id", "task_queue", "memo", "search_attributes", "duplicate_policy"]:
            assert body[field] == semantic[field]

    @pytest.mark.asyncio
    async def test_warns_when_start_input_approaches_payload_limit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        client = Client(
            "http://localhost:8080",
            token="test-token",
            namespace="ns1",
            payload_size_limit_bytes=10,
            payload_size_warning_threshold_percent=50,
        )
        resp = _mock_response(201, {
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "workflow_type": "greeter",
        })

        with (
            caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"),
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
        ):
            await client.start_workflow(
                workflow_type="greeter",
                task_queue="q1",
                workflow_id="wf-1",
                input=["this payload is intentionally large"],
            )

        payload = caplog.records[0].durable_workflow_payload
        assert payload["kind"] == "workflow_input"
        assert payload["workflow_id"] == "wf-1"
        assert payload["task_queue"] == "q1"
        assert payload["namespace"] == "ns1"
        assert payload["threshold_bytes"] == 5
        await client.aclose()

    @pytest.mark.asyncio
    async def test_warns_when_search_attributes_approach_payload_limit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        client = Client(
            "http://localhost:8080",
            namespace="ns1",
            payload_size_limit_bytes=10,
            payload_size_warning_threshold_percent=50,
        )
        resp = _mock_response(201, {
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "workflow_type": "greeter",
        })

        with (
            caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"),
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
        ):
            await client.start_workflow(
                workflow_type="greeter",
                task_queue="q1",
                workflow_id="wf-1",
                search_attributes={"CustomerId": "customer-" + ("x" * 20)},
            )

        payloads = [record.durable_workflow_payload for record in caplog.records]
        search_payload = next(payload for payload in payloads if payload["kind"] == "search_attributes")
        assert search_payload["workflow_id"] == "wf-1"
        assert search_payload["task_queue"] == "q1"
        assert search_payload["codec"] == "json"
        await client.aclose()

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
    async def test_envelope_fields(self, client: Client) -> None:
        resp = _mock_response(200, {
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "workflow_type": "greeter",
            "status": "completed",
            "input": ["Ada"],
            "output": {"greeting": "Hello, Ada!"},
            "input_envelope": {"codec": "json", "blob": '["Ada"]'},
            "output_envelope": {"codec": "json", "blob": '{"greeting":"Hello, Ada!"}'},
            "payload_codec": "json",
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            desc = await client.describe_workflow("wf-1")
            assert desc.input == ["Ada"]
            assert desc.output == {"greeting": "Hello, Ada!"}
            assert desc.payload_codec == "json"

    @pytest.mark.asyncio
    async def test_envelope_fields_decode_in_one_batch(self, client: Client) -> None:
        resp = _mock_response(200, {
            "workflow_id": "wf-1",
            "run_id": "run-1",
            "workflow_type": "greeter",
            "status": "completed",
            "input_envelope": {"codec": "json", "blob": '["Ada"]'},
            "output_envelope": {"codec": "json", "blob": '{"greeting":"Hello, Ada!"}'},
        })

        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            patch.object(
                serializer,
                "decode_envelopes",
                return_value=[["Ada"], {"greeting": "Hello, Ada!"}],
            ) as decode_envelopes,
        ):
            desc = await client.describe_workflow("wf-1")

        decode_envelopes.assert_called_once_with([
            {"codec": "json", "blob": '["Ada"]'},
            {"codec": "json", "blob": '{"greeting":"Hello, Ada!"}'},
        ])
        assert desc.input == ["Ada"]
        assert desc.output == {"greeting": "Hello, Ada!"}

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
            assert body["input"]["codec"] == "avro"
            assert serializer.decode(body["input"]["blob"], codec="avro") == ["data"]

    @pytest.mark.asyncio
    async def test_signal_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-signal-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        expected = sdk["expected_body"]
        envelope_contract = sdk["payload_envelope"]

        resp = _mock_response(200, {"ok": True})

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.signal_workflow(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        body = call_args.kwargs.get("json") or call_args[1].get("json")

        for field, value in expected.items():
            assert body[field] == value

        envelope = body[envelope_contract["field"]]
        assert envelope["codec"] == envelope_contract["codec"]
        assert serializer.decode(envelope["blob"], codec=envelope["codec"]) == envelope_contract["decoded"]

        semantic = fixture["semantic_body"]
        assert sdk["args"]["workflow_id"] == semantic["workflow_id"]
        assert sdk["args"]["signal_name"] == semantic["signal_name"]


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
    async def test_worker_routed_unknown_query_raises_query_failed(self, client: Client) -> None:
        resp = _mock_response(404, {"reason": "rejected_unknown_query", "message": "unknown query 'status'"})
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

    @pytest.mark.asyncio
    async def test_worker_routed_query_unavailable_raises_query_failed(self, client: Client) -> None:
        resp = _mock_response(409, {"reason": "query_worker_unavailable", "message": "no worker"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(QueryFailed),
        ):
            await client.query_workflow("wf-1", "status")

    @pytest.mark.asyncio
    async def test_worker_routed_query_timeout_raises_query_failed(self, client: Client) -> None:
        resp = _mock_response(504, {"reason": "query_worker_timeout", "message": "timed out"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(QueryFailed),
        ):
            await client.query_workflow("wf-1", "status")


class TestListWorkflows:
    @pytest.mark.asyncio
    async def test_list(self, client: Client) -> None:
        resp = _mock_response(200, {
            "workflows": [
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


class TestTaskQueues:
    @pytest.mark.asyncio
    async def test_list_task_queues_parses_admission(self, client: Client) -> None:
        resp = _mock_response(200, {
            "namespace": "ns1",
            "task_queues": [
                {
                    "name": "orders",
                    "stats": {"approximate_backlog_count": 2},
                    "admission": {
                        "workflow_tasks": {
                            "status": "throttled",
                            "budget_source": "worker_registration.max_concurrent_workflow_tasks",
                            "active_worker_count": 2,
                            "configured_slot_count": 10,
                            "leased_count": 1,
                            "ready_count": 2,
                            "available_slot_count": 9,
                            "server_budget_source": "server.admission.workflow_tasks.max_active_leases_per_queue",
                            "server_max_active_leases_per_queue": 1,
                            "server_active_lease_count": 1,
                            "server_remaining_active_lease_capacity": 0,
                            "server_max_active_leases_per_namespace": 8,
                            "server_namespace_active_lease_count": 7,
                            "server_remaining_namespace_active_lease_capacity": 1,
                            "server_max_dispatches_per_minute": 60,
                            "server_dispatch_count_this_minute": 60,
                            "server_remaining_dispatch_capacity": 0,
                            "server_max_dispatches_per_minute_per_namespace": 240,
                            "server_namespace_dispatch_count_this_minute": 200,
                            "server_remaining_namespace_dispatch_capacity": 40,
                            "server_dispatch_budget_group": "downstream-openai",
                            "server_max_dispatches_per_minute_per_budget_group": 75,
                            "server_budget_group_dispatch_count_this_minute": 75,
                            "server_remaining_budget_group_dispatch_capacity": 0,
                            "server_lock_required": True,
                            "server_lock_supported": True,
                        },
                        "activity_tasks": {"status": "accepting", "configured_slot_count": 5},
                        "query_tasks": {
                            "status": "full",
                            "budget_source": "server.query_tasks.max_pending_per_queue",
                            "max_pending_per_queue": 10,
                            "approximate_pending_count": 10,
                            "remaining_pending_capacity": 0,
                            "lock_required": True,
                            "lock_supported": True,
                        },
                    },
                }
            ],
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.list_task_queues()

            assert result.namespace == "ns1"
            assert len(result.task_queues) == 1
            queue = result.task_queues[0]
            assert queue.name == "orders"
            assert queue.stats == {"approximate_backlog_count": 2}
            assert queue.admission is not None
            assert queue.admission.workflow_tasks is not None
            assert queue.admission.workflow_tasks.status == "throttled"
            assert queue.admission.workflow_tasks.server_remaining_active_lease_capacity == 0
            assert queue.admission.workflow_tasks.server_max_active_leases_per_namespace == 8
            assert queue.admission.workflow_tasks.server_namespace_active_lease_count == 7
            assert queue.admission.workflow_tasks.server_remaining_namespace_active_lease_capacity == 1
            assert queue.admission.workflow_tasks.server_max_dispatches_per_minute == 60
            assert queue.admission.workflow_tasks.server_dispatch_count_this_minute == 60
            assert queue.admission.workflow_tasks.server_remaining_dispatch_capacity == 0
            assert queue.admission.workflow_tasks.server_max_dispatches_per_minute_per_namespace == 240
            assert queue.admission.workflow_tasks.server_namespace_dispatch_count_this_minute == 200
            assert queue.admission.workflow_tasks.server_remaining_namespace_dispatch_capacity == 40
            assert queue.admission.workflow_tasks.server_dispatch_budget_group == "downstream-openai"
            assert queue.admission.workflow_tasks.server_max_dispatches_per_minute_per_budget_group == 75
            assert queue.admission.workflow_tasks.server_budget_group_dispatch_count_this_minute == 75
            assert queue.admission.workflow_tasks.server_remaining_budget_group_dispatch_capacity == 0
            assert queue.admission.activity_tasks is not None
            assert queue.admission.activity_tasks.configured_slot_count == 5
            assert queue.admission.query_tasks is not None
            assert queue.admission.query_tasks.status == "full"
            assert queue.admission.query_tasks.lock_supported is True
            assert queue.admission.raw is not None
            assert queue.admission.raw["query_tasks"]["max_pending_per_queue"] == 10
            assert mock.call_args.args[:2] == ("GET", "/api/task-queues")

    @pytest.mark.asyncio
    async def test_describe_task_queue_parses_details_and_escapes_name(self, client: Client) -> None:
        resp = _mock_response(200, {
            "name": "orders/high priority",
            "namespace": "ns1",
            "stats": {"pollers": {"active_count": 1}},
            "pollers": [{"worker_id": "w1", "status": "active"}],
            "current_leases": [{"task_id": "t1", "task_type": "workflow"}],
            "admission": {"workflow_tasks": {"status": "accepting"}},
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.describe_task_queue("orders/high priority")

            assert result.name == "orders/high priority"
            assert result.namespace == "ns1"
            assert result.pollers == [{"worker_id": "w1", "status": "active"}]
            assert result.current_leases == [{"task_id": "t1", "task_type": "workflow"}]
            assert result.admission is not None
            assert result.admission.workflow_tasks is not None
            assert result.admission.workflow_tasks.status == "accepting"
            assert mock.call_args.args[:2] == ("GET", "/api/task-queues/orders%2Fhigh%20priority")

    @pytest.mark.asyncio
    async def test_describe_task_queue_rejects_non_object_response(self, client: Client) -> None:
        resp = httpx.Response(
            status_code=200,
            content=b"[]",
            headers={"content-type": "application/json"},
            request=httpx.Request("GET", "http://test"),
        )
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(ServerError, match="invalid_task_queue_response"),
        ):
            await client.describe_task_queue("orders")


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

    @pytest.mark.asyncio
    async def test_details_are_sent_as_payload_envelope(self, client: Client) -> None:
        resp = _mock_response(200, {"task_id": "t1", "outcome": "failed"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.fail_activity_task(
                task_id="t1",
                activity_attempt_id="a1",
                lease_owner="w1",
                message="activity error",
                details={"retry_after": 30},
                codec=serializer.JSON_CODEC,
            )
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            envelope = body["failure"]["details"]

            assert envelope["codec"] == serializer.JSON_CODEC
            assert serializer.decode_envelope(envelope) == {"retry_after": 30}


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
            assert body["input"]["codec"] == "avro"
            assert serializer.decode(body["input"]["blob"], codec="avro") == ["data"]
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


class TestGetResult:
    @pytest.mark.asyncio
    async def test_completed_result_uses_event_payload_codec(self, client: Client) -> None:
        handle = WorkflowHandle(client, workflow_id="wf-1", run_id="run-1", workflow_type="greeter")
        client.describe_workflow = AsyncMock(
            return_value=WorkflowExecution(
                workflow_id="wf-1",
                run_id="run-1",
                workflow_type="greeter",
                status="completed",
                payload_codec="avro",
            )
        )
        client.get_history = AsyncMock(
            return_value={
                "events": [
                    {
                        "event_type": "WorkflowCompleted",
                        "payload": {
                            "output": serializer.encode({"greeting": "hello"}, codec="avro"),
                            "payload_codec": "avro",
                        },
                    }
                ]
            }
        )

        result = await client.get_result(handle)

        assert result == {"greeting": "hello"}

    @pytest.mark.asyncio
    async def test_completed_result_uses_describe_payload_codec_fallback(self, client: Client) -> None:
        handle = WorkflowHandle(client, workflow_id="wf-1", run_id="run-1", workflow_type="greeter")
        client.describe_workflow = AsyncMock(
            return_value=WorkflowExecution(
                workflow_id="wf-1",
                run_id="run-1",
                workflow_type="greeter",
                status="completed",
                payload_codec="avro",
            )
        )
        client.get_history = AsyncMock(
            return_value={
                "events": [
                    {
                        "event_type": "WorkflowCompleted",
                        "payload": {
                            "output": serializer.encode({"greeting": "hello"}, codec="avro"),
                        },
                    }
                ]
            }
        )

        result = await client.get_result(handle)

        assert result == {"greeting": "hello"}

    # Regression guards for #432: the server emits PascalCase event_type
    # strings and stores the workflow return value under `output`. The SDK
    # used to accept snake_case event names and fall back to `result` —
    # both forms of protocol drift are now refused.

    @pytest.mark.asyncio
    async def test_snake_case_workflow_completed_is_not_accepted(self, client: Client) -> None:
        handle = WorkflowHandle(client, workflow_id="wf-1", run_id="run-1", workflow_type="greeter")
        client.describe_workflow = AsyncMock(
            return_value=WorkflowExecution(
                workflow_id="wf-1",
                run_id="run-1",
                workflow_type="greeter",
                status="completed",
                payload_codec="avro",
            )
        )
        client.get_history = AsyncMock(
            return_value={
                "events": [
                    {
                        "event_type": "workflow_completed",
                        "payload": {
                            "output": serializer.encode({"greeting": "hello"}, codec="avro"),
                            "payload_codec": "avro",
                        },
                    }
                ]
            }
        )

        assert await client.get_result(handle) is None

    @pytest.mark.asyncio
    async def test_snake_case_workflow_failed_is_not_accepted(self, client: Client) -> None:
        handle = WorkflowHandle(client, workflow_id="wf-1", run_id="run-1", workflow_type="greeter")
        client.describe_workflow = AsyncMock(
            return_value=WorkflowExecution(
                workflow_id="wf-1",
                run_id="run-1",
                workflow_type="greeter",
                status="failed",
                payload_codec="avro",
            )
        )
        client.get_history = AsyncMock(
            return_value={
                "events": [
                    {
                        "event_type": "workflow_failed",
                        "payload": {"message": "boom", "exception_class": "RuntimeError"},
                    }
                ]
            }
        )

        assert await client.get_result(handle) is None

    @pytest.mark.asyncio
    async def test_result_field_is_not_read_as_output_fallback(self, client: Client) -> None:
        handle = WorkflowHandle(client, workflow_id="wf-1", run_id="run-1", workflow_type="greeter")
        client.describe_workflow = AsyncMock(
            return_value=WorkflowExecution(
                workflow_id="wf-1",
                run_id="run-1",
                workflow_type="greeter",
                status="completed",
                payload_codec="avro",
            )
        )
        client.get_history = AsyncMock(
            return_value={
                "events": [
                    {
                        "event_type": "WorkflowCompleted",
                        "payload": {
                            "result": serializer.encode({"greeting": "hello"}, codec="avro"),
                            "payload_codec": "avro",
                        },
                    }
                ]
            }
        )

        assert await client.get_result(handle) is None


class TestRegisterWorker:
    @pytest.mark.asyncio
    async def test_register(self, client: Client) -> None:
        resp = _mock_response(201, {"worker_id": "w1", "registered": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.register_worker(
                worker_id="w1",
                task_queue="q1",
                supported_workflow_types=["greeter"],
                workflow_definition_fingerprints={"greeter": "sha256:abc"},
                supported_activity_types=["greet"],
            )
            assert result["registered"] is True
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["runtime"] == "python"
            assert body["workflow_definition_fingerprints"] == {"greeter": "sha256:abc"}

    @pytest.mark.asyncio
    async def test_register_sends_worker_capacity_when_configured(self, client: Client) -> None:
        resp = _mock_response(201, {"worker_id": "w1", "registered": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.register_worker(
                worker_id="w1",
                task_queue="q1",
                max_concurrent_workflow_tasks=3,
                max_concurrent_activity_tasks=7,
            )
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["max_concurrent_workflow_tasks"] == 3
            assert body["max_concurrent_activity_tasks"] == 7

    @pytest.mark.asyncio
    async def test_register_rejects_non_positive_worker_capacity(self, client: Client) -> None:
        with pytest.raises(ValueError, match="max_concurrent_workflow_tasks"):
            await client.register_worker(
                worker_id="w1",
                task_queue="q1",
                max_concurrent_workflow_tasks=0,
            )

        with pytest.raises(ValueError, match="max_concurrent_activity_tasks"):
            await client.register_worker(
                worker_id="w1",
                task_queue="q1",
                max_concurrent_activity_tasks=0,
            )

    @pytest.mark.asyncio
    async def test_register_advertises_installed_package_version(self, client: Client) -> None:
        from importlib.metadata import version as _pkg_version

        import durable_workflow

        installed = _pkg_version("durable-workflow")
        assert durable_workflow.__version__ == installed

        resp = _mock_response(201, {"worker_id": "w1", "registered": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.register_worker(worker_id="w1", task_queue="q1")
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["sdk_version"] == f"durable-workflow-python/{installed}"

    @pytest.mark.asyncio
    async def test_register_honors_explicit_sdk_version_override(self, client: Client) -> None:
        resp = _mock_response(201, {"worker_id": "w1", "registered": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.register_worker(
                worker_id="w1", task_queue="q1", sdk_version="custom-runtime/9.9.9"
            )
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["sdk_version"] == "custom-runtime/9.9.9"


class TestQueryTasks:
    @pytest.mark.asyncio
    async def test_poll_query_task_returns_task_payload(self, client: Client) -> None:
        resp = _mock_response(200, {"task": {"query_task_id": "qt1"}})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            task = await client.poll_query_task(worker_id="w1", task_queue="q1", timeout=5.0)

            assert task == {"query_task_id": "qt1"}
            assert mock.call_args.args[:2] == ("POST", "/api/worker/query-tasks/poll")

    @pytest.mark.asyncio
    async def test_complete_query_task_sends_result_and_envelope(self, client: Client) -> None:
        resp = _mock_response(200, {"outcome": "completed"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.complete_query_task(
                query_task_id="qt1",
                lease_owner="w1",
                query_task_attempt=2,
                result={"ok": True},
                codec="json",
            )

            assert result["outcome"] == "completed"
            body = mock.call_args.kwargs["json"]
            assert body["lease_owner"] == "w1"
            assert body["query_task_attempt"] == 2
            assert body["result"] == {"ok": True}
            assert body["result_envelope"] == {"codec": "json", "blob": '{"ok":true}'}

    @pytest.mark.asyncio
    async def test_fail_query_task_sends_failure_reason(self, client: Client) -> None:
        resp = _mock_response(200, {"outcome": "failed"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.fail_query_task(
                query_task_id="qt1",
                lease_owner="w1",
                query_task_attempt=3,
                message="unknown query",
                reason="rejected_unknown_query",
                failure_type="QueryFailed",
            )

            body = mock.call_args.kwargs["json"]
            assert body["failure"] == {
                "message": "unknown query",
                "reason": "rejected_unknown_query",
                "type": "QueryFailed",
            }


class TestGetClusterInfo:
    @pytest.mark.asyncio
    async def test_returns_dict_payload(self, client: Client) -> None:
        payload = {"version": "2.0.0", "capabilities": {"workflow": True}}
        resp = _mock_response(200, payload)
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            info = await client.get_cluster_info()
            assert info == payload

    @pytest.mark.asyncio
    async def test_accepts_empty_dict(self, client: Client) -> None:
        resp = _mock_response(200, {})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            info = await client.get_cluster_info()
            assert info == {}

    @pytest.mark.asyncio
    async def test_rejects_list_payload(self, client: Client) -> None:
        resp = httpx.Response(200, content=b"[]", headers={"content-type": "application/json"},
                              request=httpx.Request("GET", "http://test"))
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            with pytest.raises(ServerError) as exc:
                await client.get_cluster_info()
            assert exc.value.reason() == "invalid_cluster_info"

    @pytest.mark.asyncio
    async def test_rejects_string_payload(self, client: Client) -> None:
        resp = httpx.Response(200, content=b'"oops"', headers={"content-type": "application/json"},
                              request=httpx.Request("GET", "http://test"))
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            with pytest.raises(ServerError) as exc:
                await client.get_cluster_info()
            assert exc.value.reason() == "invalid_cluster_info"


class TestHealth:
    @pytest.mark.asyncio
    async def test_returns_dict_payload(self, client: Client) -> None:
        resp = _mock_response(200, {"status": "ok"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            assert await client.health() == {"status": "ok"}

    @pytest.mark.asyncio
    async def test_rejects_non_dict(self, client: Client) -> None:
        resp = httpx.Response(200, content=b"true", headers={"content-type": "application/json"},
                              request=httpx.Request("GET", "http://test"))
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            with pytest.raises(ServerError) as exc:
                await client.health()
            assert exc.value.reason() == "invalid_health_response"
