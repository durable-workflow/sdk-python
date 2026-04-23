from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from durable_workflow import serializer
from durable_workflow.client import (
    CONTROL_PLANE_VERSION,
    PROTOCOL_VERSION,
    Client,
    ScheduleAction,
    ScheduleSpec,
    WorkflowExecution,
    WorkflowHandle,
)
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
    async def test_describe_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-describe-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            desc = await client.describe_workflow(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        assert call_args.kwargs.get("json") is None

        semantic = fixture["semantic_body"]
        response = fixture["response_body"]
        assert sdk["args"]["workflow_id"] == semantic["workflow_id"]
        assert desc.workflow_id == semantic["workflow_id"]
        assert desc.run_id == response["run_id"]
        assert desc.workflow_type == response["workflow_type"]
        assert desc.namespace == response["namespace"]
        assert desc.task_queue == response["task_queue"]
        assert desc.status == response["status"]
        assert desc.payload_codec == response["payload_codec"]
        assert desc.input == response["input"]

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


class TestGetHistory:
    @pytest.mark.asyncio
    async def test_history_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-history-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            history = await client.get_history(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        assert call_args.kwargs.get("json") is None

        semantic = fixture["semantic_body"]
        assert sdk["args"]["workflow_id"] == semantic["workflow_id"]
        assert sdk["args"]["run_id"] == semantic["run_id"]
        assert history == fixture["response_body"]

    @pytest.mark.asyncio
    async def test_history_export_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-history-export-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            bundle = await client.export_history(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        assert call_args.kwargs.get("json") is None

        semantic = fixture["semantic_body"]
        assert sdk["args"]["workflow_id"] == semantic["workflow_id"]
        assert sdk["args"]["run_id"] == semantic["run_id"]
        assert bundle["schema"] == semantic["schema"]
        assert bundle["workflow"]["instance_id"] == semantic["workflow_id"]
        assert bundle["workflow"]["run_id"] == semantic["run_id"]
        assert len(bundle["events"]) == semantic["event_count"]
        assert bundle == fixture["response_body"]


class TestWorkflowRunVisibility:
    @pytest.mark.asyncio
    async def test_list_runs_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-list-runs-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.list_workflow_runs(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        assert call_args.kwargs.get("json") is None

        semantic = fixture["semantic_body"]
        assert sdk["args"]["workflow_id"] == semantic["workflow_id"]
        assert result.workflow_id == semantic["workflow_id"]
        assert result.run_count == semantic["run_count"]
        assert [run.run_id for run in result.runs] == semantic["run_ids"]
        assert result.runs[0].workflow_type == fixture["response_body"]["runs"][0]["workflow_type"]
        assert result.runs[1].is_current_run is True

    @pytest.mark.asyncio
    async def test_describe_run_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-show-run-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            run = await client.describe_workflow_run(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        assert call_args.kwargs.get("json") is None

        semantic = fixture["semantic_body"]
        response = fixture["response_body"]
        assert sdk["args"]["workflow_id"] == semantic["workflow_id"]
        assert sdk["args"]["run_id"] == semantic["run_id"]
        assert run.workflow_id == semantic["workflow_id"]
        assert run.run_id == semantic["run_id"]
        assert run.workflow_type == response["workflow_type"]
        assert run.status == response["status"]
        assert run.status_bucket == response["status_bucket"]
        assert run.business_key == response["business_key"]
        assert run.memo == response["memo"]
        assert run.search_attributes == response["search_attributes"]
        assert run.actions == response["actions"]


class TestWorkflowHandleControlPlane:
    @pytest.mark.asyncio
    async def test_run_visibility_delegates_to_client(self, client: Client) -> None:
        handle = WorkflowHandle(client, workflow_id="wf-1", run_id="r1", workflow_type="greeter")
        client.get_history = AsyncMock(return_value={"events": []})
        client.export_history = AsyncMock(return_value={"schema": "durable.workflow.history.v2"})
        client.list_workflow_runs = AsyncMock(return_value="runs")
        client.describe_workflow_run = AsyncMock(return_value="run")

        assert await handle.get_history() == {"events": []}
        assert await handle.export_history() == {"schema": "durable.workflow.history.v2"}
        assert await handle.list_runs() == "runs"
        assert await handle.describe_run() == "run"

        client.get_history.assert_awaited_once_with("wf-1", "r1")
        client.export_history.assert_awaited_once_with("wf-1", "r1")
        client.list_workflow_runs.assert_awaited_once_with("wf-1")
        client.describe_workflow_run.assert_awaited_once_with("wf-1", "r1")

    @pytest.mark.asyncio
    async def test_run_specific_methods_require_run_id(self, client: Client) -> None:
        handle = WorkflowHandle(client, workflow_id="wf-1")

        with pytest.raises(ValueError, match="run_id is required"):
            await handle.get_history()
        with pytest.raises(ValueError, match="run_id is required"):
            await handle.export_history()
        with pytest.raises(ValueError, match="run_id is required"):
            await handle.describe_run()

    @pytest.mark.asyncio
    async def test_describe_run_accepts_explicit_run_id(self, client: Client) -> None:
        handle = WorkflowHandle(client, workflow_id="wf-1")
        client.describe_workflow_run = AsyncMock(return_value="run")

        assert await handle.describe_run("r2") == "run"
        client.describe_workflow_run.assert_awaited_once_with("wf-1", "r2")

    @pytest.mark.asyncio
    async def test_maintenance_delegates_to_client(self, client: Client) -> None:
        handle = WorkflowHandle(client, workflow_id="wf-1", run_id="r1", workflow_type="greeter")
        client.repair_workflow = AsyncMock(return_value="repair")
        client.archive_workflow = AsyncMock(return_value="archive")

        assert await handle.repair() == "repair"
        assert await handle.archive(reason="retention") == "archive"

        client.repair_workflow.assert_awaited_once_with("wf-1")
        client.archive_workflow.assert_awaited_once_with("wf-1", reason="retention")


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


class TestWebhookBridgeAdapters:
    @pytest.mark.asyncio
    async def test_start_workflow_bridge_event_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "bridge-webhook-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(202, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            outcome = await client.send_webhook_bridge_event(**sdk["args"], **sdk["kwargs"])

        assert outcome.accepted is True
        assert outcome.outcome == "accepted"
        assert outcome.workflow_id == fixture["semantic_body"]["workflow_id"]
        assert outcome.control_plane_outcome == "started_new"
        assert outcome.target is not None
        assert outcome.target["business_key"] == fixture["semantic_body"]["target"]["business_key"]

        call_args = mock.call_args
        assert call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        body = call_args.kwargs.get("json") or call_args[1].get("json")
        assert body == sdk["expected_body"]

    @pytest.mark.asyncio
    async def test_signal_bridge_event_returns_rejected_outcome_for_422(self, client: Client) -> None:
        resp = _mock_response(422, {
            "schema": "durable-workflow.v2.bridge-adapter-outcome.contract",
            "version": 1,
            "adapter": "pagerduty",
            "action": "signal_workflow",
            "accepted": False,
            "outcome": "rejected",
            "reason": "unknown_target",
            "idempotency_key": "pagerduty-event-3003",
            "target": {
                "workflow_id": "wf-remediation-42",
                "signal_name": "incident_escalated",
            },
            "correlation": {
                "provider": "pagerduty",
                "event_type": "incident.triggered",
            },
        })

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            outcome = await client.send_webhook_bridge_event(
                "pagerduty",
                action="signal_workflow",
                idempotency_key="pagerduty-event-3003",
                target={
                    "workflow_id": "wf-remediation-42",
                    "signal_name": "incident_escalated",
                },
                input={
                    "severity": "critical",
                    "service": "checkout",
                },
                correlation={
                    "provider": "pagerduty",
                    "event_type": "incident.triggered",
                },
            )

        assert outcome.accepted is False
        assert outcome.outcome == "rejected"
        assert outcome.reason == "unknown_target"
        assert outcome.idempotency_key == "pagerduty-event-3003"

    @pytest.mark.asyncio
    async def test_bridge_adapter_path_escapes_adapter_segment(self, client: Client) -> None:
        resp = _mock_response(202, {
            "schema": "durable-workflow.v2.bridge-adapter-outcome.contract",
            "version": 1,
            "adapter": "ops/foo",
            "action": "update_workflow",
            "accepted": True,
            "outcome": "accepted",
        })

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.send_webhook_bridge_event(
                "ops/foo",
                action="update_workflow",
                idempotency_key="evt-1",
                target={"workflow_id": "wf-1", "update_name": "acknowledge"},
            )

        assert mock.call_args.args[1] == "/api/bridge-adapters/webhook/ops%2Ffoo"


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

    @pytest.mark.asyncio
    async def test_cancel_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-cancel-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, {"ok": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.cancel_workflow(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        body = call_args.kwargs.get("json") or call_args[1].get("json")

        assert body == sdk["expected_body"]
        assert body["reason"] == fixture["semantic_body"]["reason"]
        assert sdk["args"]["workflow_id"] == fixture["semantic_body"]["workflow_id"]


class TestTerminateWorkflow:
    @pytest.mark.asyncio
    async def test_terminate(self, client: Client) -> None:
        resp = _mock_response(200, {"ok": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.terminate_workflow("wf-1", reason="done")
            call_args = mock.call_args
            assert "/terminate" in call_args[0][1]

    @pytest.mark.asyncio
    async def test_terminate_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-terminate-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, {"ok": True})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.terminate_workflow(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        body = call_args.kwargs.get("json") or call_args[1].get("json")

        assert body == sdk["expected_body"]
        assert body["reason"] == fixture["semantic_body"]["reason"]
        assert sdk["args"]["workflow_id"] == fixture["semantic_body"]["workflow_id"]


class TestWorkflowMaintenanceCommands:
    @pytest.mark.asyncio
    async def test_repair_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-repair-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.repair_workflow(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        assert (call_args.kwargs.get("json") or call_args[1].get("json")) == sdk["expected_body"]

        assert result.workflow_id == fixture["semantic_body"]["workflow_id"]
        assert result.outcome == fixture["response_body"]["outcome"]
        assert result.command_status == fixture["response_body"]["command_status"]
        assert result.command_id == fixture["response_body"]["command_id"]

    @pytest.mark.asyncio
    async def test_archive_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-archive-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.archive_workflow(**sdk["args"])

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]
        assert call_args.args[1] == f"/api{fixture['request']['path']}"
        body = call_args.kwargs.get("json") or call_args[1].get("json")

        assert body == sdk["expected_body"]
        assert body["reason"] == fixture["semantic_body"]["reason"]
        assert result.workflow_id == fixture["semantic_body"]["workflow_id"]
        assert result.outcome == fixture["response_body"]["outcome"]
        assert result.command_status == fixture["response_body"]["command_status"]
        assert result.command_id == fixture["response_body"]["command_id"]


class TestQueryWorkflow:
    @pytest.mark.asyncio
    async def test_query(self, client: Client) -> None:
        resp = _mock_response(200, {"result": "active"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await client.query_workflow("wf-1", "status")
            assert result == {"result": "active"}

    @pytest.mark.asyncio
    async def test_query_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-query-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        expected = sdk["expected_body"]
        envelope_contract = sdk["payload_envelope"]

        resp = _mock_response(200, {"result": {"status": "processing", "line_items": 3, "currency": "USD"}})

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.query_workflow(**sdk["args"])

        assert result["result"]["status"] == "processing"

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
        assert sdk["args"]["query_name"] == semantic["query_name"]

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

    @pytest.mark.asyncio
    async def test_list_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-list-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk_args = fixture["sdk_python"]["args"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.list_workflows(**sdk_args)

        call_args = mock.call_args
        assert call_args.args[0] == fixture["request"]["method"]

        url = urlsplit(call_args.args[1])
        assert url.path == f"/api{fixture['request']['path']}"
        assert parse_qs(url.query) == {
            "workflow_type": [fixture["semantic_body"]["workflow_type"]],
            "status": [fixture["semantic_body"]["status"]],
            "query": [fixture["semantic_body"]["query"]],
            "page_size": [str(fixture["semantic_body"]["page_size"])],
        }

        assert [execution.workflow_id for execution in result.executions] == fixture["semantic_body"]["workflow_ids"]
        assert result.next_page_token == fixture["semantic_body"]["next_page_token"]


class TestNamespaces:
    @pytest.mark.asyncio
    async def test_list_namespaces_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "namespace-list-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.list_namespaces(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert [namespace.name for namespace in result.namespaces] == fixture["semantic_body"]["namespace_names"]

        retention_days = {namespace.name: namespace.retention_days for namespace in result.namespaces}
        assert retention_days == fixture["semantic_body"]["retention_days"]

    @pytest.mark.asyncio
    async def test_describe_namespace_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "namespace-describe-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.describe_namespace(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert result.name == fixture["semantic_body"]["name"]
        assert result.description == fixture["semantic_body"]["description"]
        assert result.retention_days == fixture["semantic_body"]["retention_days"]
        assert result.status == fixture["semantic_body"]["status"]

    @pytest.mark.asyncio
    async def test_create_namespace_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "namespace-create-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.create_namespace(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]
        assert result.name == fixture["semantic_body"]["name"]
        assert result.retention_days == fixture["semantic_body"]["retention_days"]

    @pytest.mark.asyncio
    async def test_update_namespace_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "namespace-update-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.update_namespace(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]
        assert result.description == fixture["semantic_body"]["description"]
        assert result.retention_days == fixture["semantic_body"]["retention_days"]

    @pytest.mark.asyncio
    async def test_set_namespace_external_storage_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "namespace-set-storage-driver-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.set_namespace_external_storage(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]
        assert result.name == fixture["semantic_body"]["namespace"]
        assert result.external_payload_storage is not None
        assert result.external_payload_storage.driver == fixture["semantic_body"]["driver"]
        assert result.external_payload_storage.enabled is fixture["semantic_body"]["enabled"]
        assert result.external_payload_storage.threshold_bytes == fixture["semantic_body"]["threshold_bytes"]
        assert result.external_payload_storage.config["disk"] == fixture["semantic_body"]["disk"]
        assert result.external_payload_storage.config["bucket"] == fixture["semantic_body"]["bucket"]
        assert result.external_payload_storage.prefix == fixture["semantic_body"]["prefix"]

    @pytest.mark.asyncio
    async def test_namespace_name_is_url_encoded(self, client: Client) -> None:
        resp = _mock_response(200, {"name": "billing reports"})

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.describe_namespace("billing reports")

        assert mock.call_args.args[:2] == ("GET", "/api/namespaces/billing%20reports")


class TestStorage:
    @pytest.mark.asyncio
    async def test_external_storage_probe_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "storage-test-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.test_external_storage(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]
        assert result.status == fixture["semantic_body"]["status"]
        assert result.namespace == fixture["semantic_body"]["namespace"]
        assert result.driver == fixture["semantic_body"]["driver"]
        assert result.small_payload is not None
        assert result.small_payload.bytes == fixture["semantic_body"]["small_payload_bytes"]
        assert result.large_payload is not None
        assert result.large_payload.bytes == fixture["semantic_body"]["large_payload_bytes"]
        assert result.large_payload.reference_uri == fixture["semantic_body"]["large_payload_reference_uri"]


class TestSystemMaintenance:
    @pytest.mark.asyncio
    async def test_repair_status_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = (
            Path(__file__).parent / "fixtures" / "control-plane" / "system-repair-status-parity.json"
        )
        fixture = json.loads(fixture_path.read_text())
        assert fixture["operation"] == "system.repair.status"
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.repair_status(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs.get("json") is None

        assert result == fixture["response_body"]

        semantic = fixture["semantic_body"]
        candidates = result["candidates"]
        assert candidates["total_candidates"] == semantic["total_candidates"]
        assert candidates["existing_task_candidates"] == semantic["existing_task_candidates"]
        assert candidates["missing_task_candidates"] == semantic["missing_task_candidates"]
        assert candidates["scan_pressure"] is semantic["scan_pressure"]
        assert result["policy"]["scan_limit"] == semantic["scan_limit"]
        assert result["policy"]["scan_strategy"] == semantic["scan_strategy"]
        assert [scope["scope_key"] for scope in candidates["scopes"]] == semantic["scope_keys"]

    @pytest.mark.asyncio
    async def test_repair_pass_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = (
            Path(__file__).parent / "fixtures" / "control-plane" / "system-repair-pass-parity.json"
        )
        fixture = json.loads(fixture_path.read_text())
        assert fixture["operation"] == "system.repair.pass"
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.repair_pass(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs.get("json") == fixture["request"]["body"]

        assert result == fixture["response_body"]

        semantic = fixture["semantic_body"]
        assert result["repaired_existing_tasks"] == semantic["repaired_existing_tasks"]
        assert result["repaired_missing_tasks"] == semantic["repaired_missing_tasks"]
        assert result["dispatched_tasks"] == semantic["dispatched_tasks"]
        assert result["existing_task_failures"] == semantic["existing_task_failures"]
        assert result["missing_run_failures"] == semantic["missing_run_failures"]

    @pytest.mark.asyncio
    async def test_repair_pass_sends_empty_body_without_filters(self, client: Client) -> None:
        resp = _mock_response(200, {
            "throttled": False,
            "selected_existing_task_candidates": 0,
            "selected_missing_task_candidates": 0,
            "repaired_existing_tasks": 0,
            "repaired_missing_tasks": 0,
            "dispatched_tasks": 0,
            "selected_command_contract_candidates": 0,
            "backfilled_command_contracts": 0,
            "command_contract_backfill_unavailable": 0,
            "command_contract_failures": [],
            "existing_task_failures": [],
            "missing_run_failures": [],
        })

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            await client.repair_pass()

        assert mock.call_args.args[0] == "POST"
        assert mock.call_args.args[1] == "/api/system/repair/pass"
        assert mock.call_args.kwargs.get("json") == {}

    @pytest.mark.asyncio
    async def test_retention_status_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = (
            Path(__file__).parent / "fixtures" / "control-plane" / "system-retention-status-parity.json"
        )
        fixture = json.loads(fixture_path.read_text())
        assert fixture["operation"] == "system.retention.status"
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.retention_status(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs.get("json") is None

        assert result == fixture["response_body"]

        semantic = fixture["semantic_body"]
        assert result["namespace"] == semantic["namespace"]
        assert result["retention_days"] == semantic["retention_days"]
        assert result["cutoff"] == semantic["cutoff"]
        assert result["expired_run_count"] == semantic["expired_run_count"]
        assert result["expired_run_ids"] == semantic["expired_run_ids"]
        assert result["scan_limit"] == semantic["scan_limit"]
        assert result["scan_pressure"] is semantic["scan_pressure"]

    @pytest.mark.asyncio
    async def test_retention_pass_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = (
            Path(__file__).parent / "fixtures" / "control-plane" / "system-retention-pass-parity.json"
        )
        fixture = json.loads(fixture_path.read_text())
        assert fixture["operation"] == "system.retention.pass"
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.retention_pass(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs.get("json") == fixture["request"]["body"]

        assert result == fixture["response_body"]

        semantic = fixture["semantic_body"]
        assert result["processed"] == semantic["processed"]
        assert result["pruned"] == semantic["pruned"]
        assert result["skipped"] == semantic["skipped"]
        assert result["failed"] == semantic["failed"]

        pruned_ids = [r["run_id"] for r in result["results"] if r["outcome"] == "pruned"]
        skipped_ids = [r["run_id"] for r in result["results"] if r["outcome"] == "skipped"]
        assert pruned_ids == semantic["pruned_run_ids"]
        assert skipped_ids == semantic["skipped_run_ids"]

    @pytest.mark.asyncio
    async def test_retention_pass_sends_empty_body_without_filters(self, client: Client) -> None:
        resp = _mock_response(200, {
            "processed": 0,
            "pruned": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
        })

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            await client.retention_pass()

        assert mock.call_args.args[0] == "POST"
        assert mock.call_args.args[1] == "/api/system/retention/pass"
        assert mock.call_args.kwargs.get("json") == {}

    @pytest.mark.asyncio
    async def test_activity_timeout_status_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = (
            Path(__file__).parent / "fixtures" / "control-plane" / "system-activity-timeout-status-parity.json"
        )
        fixture = json.loads(fixture_path.read_text())
        assert fixture["operation"] == "system.activity_timeout.status"
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.activity_timeout_status(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs.get("json") is None

        assert result == fixture["response_body"]

        semantic = fixture["semantic_body"]
        assert result["expired_count"] == semantic["expired_count"]
        assert result["expired_execution_ids"] == semantic["expired_execution_ids"]
        assert result["scan_limit"] == semantic["scan_limit"]
        assert result["scan_pressure"] is semantic["scan_pressure"]

    @pytest.mark.asyncio
    async def test_activity_timeout_pass_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = (
            Path(__file__).parent / "fixtures" / "control-plane" / "system-activity-timeout-pass-parity.json"
        )
        fixture = json.loads(fixture_path.read_text())
        assert fixture["operation"] == "system.activity_timeout.pass"
        sdk = fixture["sdk_python"]

        resp = _mock_response(200, fixture["response_body"])

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.activity_timeout_pass(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs.get("json") == fixture["request"]["body"]

        assert result == fixture["response_body"]

        semantic = fixture["semantic_body"]
        assert result["processed"] == semantic["processed"]
        assert result["enforced"] == semantic["enforced"]
        assert result["skipped"] == semantic["skipped"]
        assert result["failed"] == semantic["failed"]

        enforced_ids = [r["execution_id"] for r in result["results"] if r["outcome"] == "enforced"]
        skipped_ids = [r["execution_id"] for r in result["results"] if r["outcome"] == "skipped"]
        assert enforced_ids == semantic["enforced_execution_ids"]
        assert skipped_ids == semantic["skipped_execution_ids"]

    @pytest.mark.asyncio
    async def test_activity_timeout_pass_sends_empty_body_without_filters(self, client: Client) -> None:
        resp = _mock_response(200, {
            "processed": 0,
            "enforced": 0,
            "skipped": 0,
            "failed": 0,
            "results": [],
        })

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            await client.activity_timeout_pass()

        assert mock.call_args.args[0] == "POST"
        assert mock.call_args.args[1] == "/api/system/activity-timeouts/pass"
        assert mock.call_args.kwargs.get("json") == {}


class TestTaskQueues:
    @pytest.mark.asyncio
    async def test_list_task_queues_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "task-queue-list-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.list_task_queues(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"

        semantic = fixture["semantic_body"]
        assert result.namespace == semantic["namespace"]
        assert [queue.name for queue in result.task_queues] == semantic["task_queue_names"]

        statuses = {
            queue.name: queue.admission.workflow_tasks.status
            for queue in result.task_queues
            if queue.admission is not None and queue.admission.workflow_tasks is not None
        }
        assert statuses == semantic["workflow_admission_statuses"]

    @pytest.mark.asyncio
    async def test_describe_task_queue_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "task-queue-describe-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.describe_task_queue(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"

        semantic = fixture["semantic_body"]
        assert result.namespace == semantic["namespace"]
        assert result.name == semantic["task_queue"]
        assert result.stats is not None
        assert result.stats["pollers"]["active_count"] == semantic["active_pollers"]
        assert result.admission is not None
        assert result.admission.workflow_tasks is not None
        assert result.admission.activity_tasks is not None
        assert result.admission.query_tasks is not None
        assert result.admission.workflow_tasks.status == semantic["workflow_admission_status"]
        assert result.admission.activity_tasks.status == semantic["activity_admission_status"]
        assert result.admission.query_tasks.status == semantic["query_admission_status"]
        assert [lease["task_id"] for lease in result.current_leases or []] == semantic["current_lease_ids"]

    @pytest.mark.asyncio
    async def test_list_task_queue_build_ids_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = (
            Path(__file__).parent
            / "fixtures"
            / "control-plane"
            / "task-queue-build-ids-parity.json"
        )
        fixture = json.loads(fixture_path.read_text())
        assert fixture["operation"] == "task_queue.build_ids"
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.list_task_queue_build_ids(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"

        semantic = fixture["semantic_body"]
        assert result.namespace == semantic["namespace"]
        assert result.task_queue == semantic["task_queue"]
        assert result.stale_after_seconds == fixture["response_body"]["stale_after_seconds"]
        assert [cohort.build_id for cohort in result.build_ids] == semantic["build_ids"]

        rollout_statuses = {
            (cohort.build_id if cohort.build_id is not None else "unversioned"): cohort.rollout_status
            for cohort in result.build_ids
        }
        assert rollout_statuses == semantic["rollout_statuses"]

    @pytest.mark.asyncio
    async def test_list_task_queue_build_ids_surfaces_cohort_worker_counts(
        self, client: Client
    ) -> None:
        resp = _mock_response(
            200,
            {
                "namespace": "default",
                "task_queue": "orders",
                "stale_after_seconds": 90,
                "build_ids": [
                    {
                        "build_id": "build-alpha",
                        "rollout_status": "active_with_draining",
                        "active_worker_count": 2,
                        "draining_worker_count": 1,
                        "stale_worker_count": 0,
                        "total_worker_count": 3,
                        "runtimes": ["worker-runtime"],
                        "sdk_versions": ["polyglot-sdk/2.0.0"],
                        "last_heartbeat_at": "2026-04-22T09:30:00Z",
                        "first_seen_at": "2026-04-22T08:00:00Z",
                    },
                    {
                        "build_id": None,
                        "rollout_status": "stale_only",
                        "active_worker_count": 0,
                        "draining_worker_count": 0,
                        "stale_worker_count": 1,
                        "total_worker_count": 1,
                        "runtimes": [],
                        "sdk_versions": [],
                        "last_heartbeat_at": None,
                        "first_seen_at": None,
                    },
                ],
            },
        )

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.list_task_queue_build_ids("orders")

        assert mock.call_args.args[0] == "GET"
        assert mock.call_args.args[1] == "/api/task-queues/orders/build-ids"
        assert result.task_queue == "orders"
        assert result.stale_after_seconds == 90
        assert len(result.build_ids) == 2

        alpha, unversioned = result.build_ids
        assert alpha.build_id == "build-alpha"
        assert alpha.rollout_status == "active_with_draining"
        assert alpha.active_worker_count == 2
        assert alpha.draining_worker_count == 1
        assert alpha.total_worker_count == 3
        assert alpha.runtimes == ["worker-runtime"]
        assert alpha.sdk_versions == ["polyglot-sdk/2.0.0"]
        assert unversioned.build_id is None
        assert unversioned.rollout_status == "stale_only"
        assert unversioned.stale_worker_count == 1
        assert unversioned.last_heartbeat_at is None
        assert unversioned.first_seen_at is None

    @pytest.mark.asyncio
    async def test_drain_task_queue_build_id_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = (
            Path(__file__).parent
            / "fixtures"
            / "control-plane"
            / "task-queue-build-id-drain-parity.json"
        )
        fixture = json.loads(fixture_path.read_text())
        assert fixture["operation"] == "task_queue.build_id.drain"
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.drain_task_queue_build_id(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        body = mock.call_args.kwargs.get("json")
        assert body == fixture["request"]["body"]

        semantic = fixture["semantic_body"]
        assert result.namespace == semantic["namespace"]
        assert result.task_queue == semantic["task_queue"]
        assert result.build_id == semantic["build_id"]
        assert result.drain_intent == semantic["drain_intent"]
        assert result.drained_at == semantic["drained_at"]

    @pytest.mark.asyncio
    async def test_resume_task_queue_build_id_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = (
            Path(__file__).parent
            / "fixtures"
            / "control-plane"
            / "task-queue-build-id-resume-parity.json"
        )
        fixture = json.loads(fixture_path.read_text())
        assert fixture["operation"] == "task_queue.build_id.resume"
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.resume_task_queue_build_id(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        body = mock.call_args.kwargs.get("json")
        assert body == fixture["request"]["body"]

        semantic = fixture["semantic_body"]
        assert result.namespace == semantic["namespace"]
        assert result.task_queue == semantic["task_queue"]
        assert result.build_id == semantic["build_id"]
        assert result.drain_intent == semantic["drain_intent"]
        assert result.drained_at is None

    @pytest.mark.asyncio
    async def test_drain_task_queue_build_id_targets_unversioned_cohort_with_null_body(
        self, client: Client
    ) -> None:
        resp = _mock_response(
            200,
            {
                "namespace": "default",
                "task_queue": "orders",
                "build_id": None,
                "drain_intent": "draining",
                "drained_at": "2026-04-22T09:50:00Z",
            },
        )

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.drain_task_queue_build_id("orders", None)

        assert mock.call_args.args[0] == "POST"
        assert mock.call_args.args[1] == "/api/task-queues/orders/build-ids/drain"
        assert mock.call_args.kwargs.get("json") == {"build_id": None}
        assert result.build_id is None
        assert result.drain_intent == "draining"
        assert result.drained_at == "2026-04-22T09:50:00Z"

    @pytest.mark.asyncio
    async def test_resume_task_queue_build_id_clears_drained_at(self, client: Client) -> None:
        resp = _mock_response(
            200,
            {
                "namespace": "default",
                "task_queue": "orders",
                "build_id": "build-alpha",
                "drain_intent": "active",
                "drained_at": None,
            },
        )

        with patch.object(
            client._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = await client.resume_task_queue_build_id("orders", "build-alpha")

        assert mock.call_args.args[1] == "/api/task-queues/orders/build-ids/resume"
        assert mock.call_args.kwargs.get("json") == {"build_id": "build-alpha"}
        assert result.drain_intent == "active"
        assert result.drained_at is None

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


class TestSearchAttributes:
    @pytest.mark.asyncio
    async def test_list_search_attributes_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "search-attribute-list-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.list_search_attributes(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert result.system_attributes == fixture["semantic_body"]["system_attributes"]
        assert result.custom_attributes == fixture["semantic_body"]["custom_attributes"]

    @pytest.mark.asyncio
    async def test_create_search_attribute_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "search-attribute-create-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.create_search_attribute(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]
        assert result == fixture["response_body"]

    @pytest.mark.asyncio
    async def test_delete_search_attribute_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "search-attribute-delete-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.delete_search_attribute(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert result == fixture["response_body"]

    @pytest.mark.asyncio
    async def test_search_attribute_name_is_url_encoded(self, client: Client) -> None:
        resp = _mock_response(200, {"name": "Customer Status", "outcome": "deleted"})

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.delete_search_attribute("Customer Status")

        assert mock.call_args.args[:2] == ("DELETE", "/api/search-attributes/Customer%20Status")


class TestSchedules:
    @pytest.mark.asyncio
    async def test_create_schedule_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "schedule-create-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(201, fixture["response_body"])

        kwargs = sdk["kwargs"].copy()
        kwargs["spec"] = self._schedule_spec(kwargs["spec"])
        kwargs["action"] = self._schedule_action(kwargs["action"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            handle = await client.create_schedule(**kwargs)

        assert handle.schedule_id == fixture["semantic_body"]["schedule_id"]
        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"

        body = mock.call_args.kwargs["json"]
        expected = fixture["request"]["body"]
        assert body["schedule_id"] == expected["schedule_id"]
        assert body["spec"] == expected["spec"]
        assert body["overlap_policy"] == expected["overlap_policy"]
        assert body["jitter_seconds"] == expected["jitter_seconds"]
        assert body["max_runs"] == expected["max_runs"]
        assert body["paused"] == expected["paused"]
        assert body["note"] == expected["note"]
        self._assert_schedule_action_matches_fixture(body["action"], expected["action"], sdk["payload_envelope"])

    @pytest.mark.asyncio
    async def test_list_schedules_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "schedule-list-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.list_schedules(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"

        semantic = fixture["semantic_body"]
        assert [schedule.schedule_id for schedule in result.schedules] == semantic["schedule_ids"]
        assert [schedule.action["workflow_type"] for schedule in result.schedules] == semantic["workflow_types"]
        assert result.next_page_token == semantic["next_page_token"]

        statuses = {schedule.schedule_id: schedule.status for schedule in result.schedules}
        assert statuses == semantic["statuses"]

    @pytest.mark.asyncio
    async def test_describe_schedule_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "schedule-describe-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.describe_schedule(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"

        semantic = fixture["semantic_body"]
        assert result.schedule_id == semantic["schedule_id"]
        assert result.status == semantic["status"]
        assert result.action is not None
        assert result.action["workflow_type"] == semantic["workflow_type"]
        assert result.action["task_queue"] == semantic["task_queue"]
        assert result.overlap_policy == semantic["overlap_policy"]
        assert result.fires_count == semantic["fires_count"]
        assert result.remaining_actions == semantic["remaining_actions"]

    @pytest.mark.asyncio
    async def test_update_schedule_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "schedule-update-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        kwargs = sdk["kwargs"].copy()
        kwargs["spec"] = self._schedule_spec(kwargs["spec"])
        kwargs["action"] = self._schedule_action(kwargs["action"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.update_schedule(**kwargs)

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"

        body = mock.call_args.kwargs["json"]
        expected = fixture["request"]["body"]
        assert body["spec"] == expected["spec"]
        assert body["overlap_policy"] == expected["overlap_policy"]
        assert body["jitter_seconds"] == expected["jitter_seconds"]
        assert body["max_runs"] == expected["max_runs"]
        assert body["note"] == expected["note"]
        self._assert_schedule_action_matches_fixture(body["action"], expected["action"], sdk["payload_envelope"])

    @pytest.mark.asyncio
    async def test_pause_schedule_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "schedule-pause-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.pause_schedule(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

    @pytest.mark.asyncio
    async def test_resume_schedule_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "schedule-resume-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.resume_schedule(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

    @pytest.mark.asyncio
    async def test_trigger_schedule_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "schedule-trigger-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.trigger_schedule(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

        semantic = fixture["semantic_body"]
        assert result.schedule_id == semantic["schedule_id"]
        assert result.outcome == semantic["outcome"]
        assert result.workflow_id == semantic["workflow_id"]
        assert result.run_id == semantic["run_id"]

    @pytest.mark.asyncio
    async def test_backfill_schedule_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "schedule-backfill-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.backfill_schedule(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

        semantic = fixture["semantic_body"]
        assert result.schedule_id == semantic["schedule_id"]
        assert result.outcome == semantic["outcome"]
        assert result.fires_attempted == semantic["fires_attempted"]

    @pytest.mark.asyncio
    async def test_delete_schedule_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "schedule-delete-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.delete_schedule(**sdk["args"])

        assert mock.call_args.args[0] == fixture["request"]["method"]
        assert mock.call_args.args[1] == f"/api{fixture['request']['path']}"
        assert mock.call_args.kwargs.get("json") is None

    @staticmethod
    def _schedule_spec(data: dict) -> ScheduleSpec:
        return ScheduleSpec(
            cron_expressions=data.get("cron_expressions"),
            intervals=data.get("intervals"),
            timezone=data.get("timezone"),
        )

    @staticmethod
    def _schedule_action(data: dict) -> ScheduleAction:
        return ScheduleAction(
            workflow_type=data["workflow_type"],
            task_queue=data.get("task_queue"),
            input=data.get("input"),
            execution_timeout_seconds=data.get("execution_timeout_seconds"),
            run_timeout_seconds=data.get("run_timeout_seconds"),
        )

    @staticmethod
    def _assert_schedule_action_matches_fixture(
        actual: dict,
        expected: dict,
        envelope_contract: dict,
    ) -> None:
        for field in ("workflow_type", "task_queue", "execution_timeout_seconds", "run_timeout_seconds"):
            assert actual[field] == expected[field]

        envelope = actual["input"]
        assert envelope["codec"] == envelope_contract["codec"]
        assert serializer.decode(envelope["blob"], codec=envelope["codec"]) == envelope_contract["decoded"]


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
    async def test_poll_workflow_task_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-task-poll-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            task = await client.poll_workflow_task(**fixture["sdk_python"]["kwargs"])

        assert task == fixture["response_body"]["task"]
        assert task["task_id"] == fixture["semantic_body"]["task_id"]
        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

    @pytest.mark.asyncio
    async def test_complete_workflow_task_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-task-complete-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.complete_workflow_task(**fixture["sdk_python"]["kwargs"])

        assert result == fixture["response_body"]
        assert result["outcome"] == fixture["semantic_body"]["outcome"]
        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

    @pytest.mark.asyncio
    async def test_fail_workflow_task_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-task-fail-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.fail_workflow_task(**fixture["sdk_python"]["kwargs"])

        assert result == fixture["response_body"]
        assert result["outcome"] == fixture["semantic_body"]["outcome"]
        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

    @pytest.mark.asyncio
    async def test_workflow_task_history_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-task-history-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            page = await client.workflow_task_history(**fixture["sdk_python"]["kwargs"])

        assert page == fixture["response_body"]
        assert "history_events" in page
        assert "total_history_events" in page
        assert "next_history_page_token" in page
        assert "events" not in page
        assert "next_page_token" not in page
        assert page["next_history_page_token"] == fixture["semantic_body"]["response_next_history_page_token"]
        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

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
    async def test_complete_activity_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "activity-complete-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        expected = sdk["expected_body"]

        resp = _mock_response(200, fixture["response_body"])
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.complete_activity_task(**sdk["kwargs"])

        assert result == fixture["response_body"]
        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")

        body = mock.call_args.kwargs["json"]
        assert body["activity_attempt_id"] == expected["activity_attempt_id"]
        assert body["lease_owner"] == expected["lease_owner"]

        envelope = body[sdk["payload_envelope"]["field"]]
        assert envelope["codec"] == sdk["payload_envelope"]["codec"]
        assert serializer.decode_envelope(envelope) == sdk["payload_envelope"]["decoded"]

    @pytest.mark.asyncio
    async def test_fail_activity_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "activity-fail-parity.json"
        fixture = json.loads(fixture_path.read_text())

        resp = _mock_response(200, fixture["response_body"])
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.fail_activity_task(**fixture["sdk_python"]["kwargs"])

        assert result == fixture["response_body"]
        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

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

    @pytest.mark.asyncio
    async def test_update_request_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "workflow-update-parity.json"
        fixture = json.loads(fixture_path.read_text())
        sdk = fixture["sdk_python"]
        expected = sdk["expected_body"]
        envelope_contract = sdk["payload_envelope"]

        resp = _mock_response(200, {"outcome": "update_completed", "result": {"quota": 50}})

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.update_workflow(**sdk["args"])

        assert result["outcome"] == "update_completed"

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
        assert sdk["args"]["update_name"] == semantic["update_name"]
        assert sdk["args"]["wait_for"] == semantic["wait_for"]


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
    async def test_register_worker_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "worker-register-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(201, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.register_worker(**fixture["sdk_python"]["kwargs"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]
        assert result == fixture["response_body"]
        assert result["worker_id"] == fixture["semantic_body"]["worker_id"]

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


class TestWorkers:
    @pytest.mark.asyncio
    async def test_list_workers_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "worker-list-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.list_workers(**fixture["sdk_python"]["kwargs"])

        method, path = mock.call_args.args[:2]
        split = urlsplit(path)
        assert method == fixture["request"]["method"]
        assert split.path == f"/api{fixture['request']['path']}"
        assert {key: values[0] for key, values in parse_qs(split.query).items()} == fixture["request"]["query"]
        assert result.namespace == fixture["semantic_body"]["namespace"]
        assert [worker.worker_id for worker in result.workers] == fixture["semantic_body"]["worker_ids"]
        assert result.workers[0].task_queue == fixture["semantic_body"]["task_queue"]

    @pytest.mark.asyncio
    async def test_describe_worker_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "worker-describe-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.describe_worker(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert result.worker_id == fixture["semantic_body"]["worker_id"]
        assert result.namespace == fixture["semantic_body"]["namespace"]
        assert result.runtime == fixture["semantic_body"]["runtime"]
        assert result.status == fixture["semantic_body"]["status"]
        assert result.supported_activity_types == fixture["response_body"]["supported_activity_types"]

    @pytest.mark.asyncio
    async def test_deregister_worker_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "worker-deregister-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.deregister_worker(**fixture["sdk_python"]["args"])

        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert result == fixture["response_body"]
        assert result["outcome"] == fixture["semantic_body"]["outcome"]

    @pytest.mark.asyncio
    async def test_worker_id_is_url_encoded(self, client: Client) -> None:
        resp = _mock_response(200, {"worker_id": "worker with spaces", "status": "active"})

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.describe_worker("worker with spaces")

        assert mock.call_args.args[:2] == ("GET", "/api/workers/worker%20with%20spaces")

    @pytest.mark.asyncio
    async def test_list_workers_rejects_non_object_response(self, client: Client) -> None:
        resp = _mock_response(200, ["not", "an", "object"])  # type: ignore[arg-type]

        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(ServerError, match="invalid_worker_response"),
        ):
            await client.list_workers()


class TestQueryTasks:
    @pytest.mark.asyncio
    async def test_poll_query_task_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "query-task-poll-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            task = await client.poll_query_task(**fixture["sdk_python"]["kwargs"])

        assert task == fixture["response_body"]["task"]
        assert task["query_task_id"] == fixture["semantic_body"]["query_task_id"]
        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

    @pytest.mark.asyncio
    async def test_complete_query_task_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "query-task-complete-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.complete_query_task(**fixture["sdk_python"]["kwargs"])

        assert result == fixture["response_body"]
        assert result["outcome"] == fixture["semantic_body"]["outcome"]
        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

    @pytest.mark.asyncio
    async def test_fail_query_task_matches_polyglot_fixture(self, client: Client) -> None:
        fixture_path = Path(__file__).parent / "fixtures" / "control-plane" / "query-task-fail-parity.json"
        fixture = json.loads(fixture_path.read_text())
        resp = _mock_response(200, fixture["response_body"])

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.fail_query_task(**fixture["sdk_python"]["kwargs"])

        assert result == fixture["response_body"]
        assert result["outcome"] == fixture["semantic_body"]["outcome"]
        assert mock.call_args.args[:2] == (fixture["request"]["method"], f"/api{fixture['request']['path']}")
        assert mock.call_args.kwargs["json"] == fixture["request"]["body"]

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
