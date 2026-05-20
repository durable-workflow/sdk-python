from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow import serializer
from durable_workflow.client import WorkflowHandle
from durable_workflow.external_storage import ExternalPayloadCache, LocalFilesystemExternalStorage
from durable_workflow.sync import Client, SyncStandaloneActivityHandle, SyncWorkflowHandle


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


class TestSyncClientConstructor:
    def test_external_storage_options_are_forwarded(self, tmp_path: Path) -> None:
        storage = LocalFilesystemExternalStorage(tmp_path)
        cache = ExternalPayloadCache(max_entries=2, max_bytes=1024)

        client = Client(
            "http://localhost:8080",
            external_storage=storage,
            external_storage_threshold_bytes=128,
            external_storage_cache=cache,
        )

        assert client._async.external_storage is storage
        assert client._async.external_storage_threshold_bytes == 128
        assert client._async.external_storage_cache is cache


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

    def test_priority_and_fairness_are_forwarded_on_start(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            201,
            {
                "workflow_id": "wf-prio",
                "run_id": "run-prio",
                "workflow_type": "greeter",
            },
        )
        with patch.object(
            client._async._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            client.start_workflow(
                workflow_type="greeter",
                task_queue="shared",
                workflow_id="wf-prio",
                priority=1,
                fairness_key="tenant-a",
                fairness_weight=3,
            )

        body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
        assert body["priority"] == 1
        assert body["fairness_key"] == "tenant-a"
        assert body["fairness_weight"] == 3

    def test_priority_and_fairness_are_omitted_when_unset(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            201,
            {
                "workflow_id": "wf-nopri",
                "run_id": "run-nopri",
                "workflow_type": "greeter",
            },
        )
        with patch.object(
            client._async._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            client.start_workflow(
                workflow_type="greeter",
                task_queue="shared",
                workflow_id="wf-nopri",
            )

        body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
        assert "priority" not in body
        assert "fairness_key" not in body
        assert "fairness_weight" not in body


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

    def test_list_task_queue_build_ids(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "namespace": "default",
                "task_queue": "orders",
                "stale_after_seconds": 60,
                "build_ids": [
                    {
                        "build_id": "build-alpha",
                        "rollout_status": "active",
                        "active_worker_count": 2,
                        "draining_worker_count": 0,
                        "stale_worker_count": 0,
                        "total_worker_count": 2,
                        "runtimes": ["worker-runtime"],
                        "sdk_versions": ["polyglot-sdk/2.0.0"],
                        "last_heartbeat_at": "2026-04-22T09:30:00Z",
                        "first_seen_at": "2026-04-22T08:00:00Z",
                    }
                ],
            },
        )
        with patch.object(
            client._async._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = client.list_task_queue_build_ids("orders")
            assert result.task_queue == "orders"
            assert result.stale_after_seconds == 60
            assert len(result.build_ids) == 1
            cohort = result.build_ids[0]
            assert cohort.build_id == "build-alpha"
            assert cohort.rollout_status == "active"
            assert cohort.total_worker_count == 2
            assert mock.call_args.args[:2] == ("GET", "/api/task-queues/orders/build-ids")

    def test_drain_task_queue_build_id(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "namespace": "default",
                "task_queue": "orders",
                "build_id": "build-alpha",
                "drain_intent": "draining",
                "drained_at": "2026-04-22T09:45:00Z",
            },
        )
        with patch.object(
            client._async._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = client.drain_task_queue_build_id("orders", "build-alpha")
            assert result.drain_intent == "draining"
            assert result.drained_at == "2026-04-22T09:45:00Z"
            assert result.build_id == "build-alpha"
            assert mock.call_args.args[:2] == (
                "POST",
                "/api/task-queues/orders/build-ids/drain",
            )
            assert mock.call_args.kwargs.get("json") == {"build_id": "build-alpha"}

    def test_resume_task_queue_build_id_for_unversioned_cohort(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "namespace": "default",
                "task_queue": "orders",
                "build_id": None,
                "drain_intent": "active",
                "drained_at": None,
            },
        )
        with patch.object(
            client._async._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = client.resume_task_queue_build_id("orders", None)
            assert result.drain_intent == "active"
            assert result.drained_at is None
            assert result.build_id is None
            assert mock.call_args.args[:2] == (
                "POST",
                "/api/task-queues/orders/build-ids/resume",
            )
            assert mock.call_args.kwargs.get("json") == {"build_id": None}


class TestSyncClientNamespaces:
    def test_delete_namespace(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "name": "billing reports",
                "status": "deleted",
                "deleted": {"workflow_runs": 2},
            },
        )
        with patch.object(
            client._async._http, "request", new_callable=AsyncMock, return_value=resp
        ) as mock:
            result = client.delete_namespace("billing reports")

        assert result.name == "billing reports"
        assert result.status == "deleted"
        assert result.deleted == {"workflow_runs": 2}
        assert mock.call_args.args[:2] == (
            "DELETE",
            "/api/namespaces/billing%20reports",
        )
        assert mock.call_args.kwargs.get("json") is None


class TestSyncClientRunVisibility:
    def test_list_workflow_runs(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "workflow_id": "wf-1",
                "run_count": 2,
                "runs": [
                    {"workflow_id": "wf-1", "run_id": "r1", "workflow_type": "greeter", "status": "completed"},
                    {
                        "workflow_id": "wf-1",
                        "run_id": "r2",
                        "workflow_type": "greeter",
                        "status": "running",
                        "is_current_run": True,
                    },
                ],
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = client.list_workflow_runs("wf-1")

        assert result.workflow_id == "wf-1"
        assert result.run_count == 2
        assert [run.run_id for run in result.runs] == ["r1", "r2"]
        assert result.runs[1].is_current_run is True
        assert mock.call_args.args[:2] == ("GET", "/api/workflows/wf-1/runs")

    def test_describe_workflow_run(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "workflow_id": "wf-1",
                "run_id": "r1",
                "workflow_type": "greeter",
                "status": "completed",
                "status_bucket": "terminal",
                "actions": {"archive": {"enabled": True}},
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            run = client.describe_workflow_run("wf-1", "r1")

        assert run.workflow_id == "wf-1"
        assert run.run_id == "r1"
        assert run.status_bucket == "terminal"
        assert run.actions == {"archive": {"enabled": True}}
        assert mock.call_args.args[:2] == ("GET", "/api/workflows/wf-1/runs/r1")


class TestSyncClientMaintenance:
    def test_repair_workflow(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "workflow_id": "wf-1",
                "outcome": "accepted",
                "command_status": "queued",
                "command_id": "cmd-1",
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = client.repair_workflow("wf-1")

        assert result.workflow_id == "wf-1"
        assert result.outcome == "accepted"
        assert result.command_status == "queued"
        assert result.command_id == "cmd-1"
        assert mock.call_args.args[:2] == ("POST", "/api/workflows/wf-1/repair")

    def test_archive_workflow(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "workflow_id": "wf-1",
                "outcome": "accepted",
                "command_status": "completed",
                "command_id": "cmd-2",
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = client.archive_workflow("wf-1", reason="retention policy")

        assert result.workflow_id == "wf-1"
        assert result.outcome == "accepted"
        assert result.command_status == "completed"
        assert result.command_id == "cmd-2"
        assert mock.call_args.args[:2] == ("POST", "/api/workflows/wf-1/archive")
        assert mock.call_args.kwargs["json"] == {"reason": "retention policy"}


class TestSyncClientUpdate:
    def test_update(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(200, {"outcome": "completed", "result": "updated"})
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            result = client.update_workflow("wf-1", "my-update", args=["data"], wait_for="completed")
            assert result["outcome"] == "completed"


class TestSyncStandaloneActivities:
    def test_start_activity_returns_sync_handle(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            201,
            {
                "activity_id": "job-1",
                "activity_execution_id": "exec-1",
                "workflow_run_id": "run-1",
                "workflow_type": "dw.standalone_activity",
                "activity_type": "shipping.send_invoice",
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            handle = client.start_activity(
                activity_type="shipping.send_invoice",
                task_queue="outbox",
                activity_id="job-1",
                input=[{"order_id": "o-42"}],
                retry_policy={"max_attempts": 3},
                start_to_close_timeout_seconds=120,
                schedule_to_start_timeout_seconds=10,
                schedule_to_close_timeout_seconds=180,
                heartbeat_timeout_seconds=30,
            )

        assert isinstance(handle, SyncStandaloneActivityHandle)
        assert handle.activity_id == "job-1"
        assert handle.workflow_run_id == "run-1"
        assert handle.activity_execution_id == "exec-1"
        assert handle.activity_type == "shipping.send_invoice"
        assert handle.workflow_type == "dw.standalone_activity"

        body = mock.call_args.kwargs.get("json")
        assert body["activity_type"] == "shipping.send_invoice"
        assert body["task_queue"] == "outbox"
        assert body["activity_id"] == "job-1"
        assert body["retry_policy"] == {"max_attempts": 3}
        assert body["start_to_close_timeout_seconds"] == 120
        assert body["schedule_to_start_timeout_seconds"] == 10
        assert body["schedule_to_close_timeout_seconds"] == 180
        assert body["heartbeat_timeout_seconds"] == 30
        assert serializer.decode(body["input"]["blob"], codec=body["input"]["codec"]) == [{"order_id": "o-42"}]

    def test_get_activity_handle_wraps_existing_activity(self) -> None:
        client = Client("http://localhost:8080")
        handle = client.get_activity_handle(
            "job-2",
            workflow_run_id="run-2",
            activity_execution_id="exec-2",
            activity_type="billing.charge",
        )

        assert isinstance(handle, SyncStandaloneActivityHandle)
        assert handle.activity_id == "job-2"
        assert handle.workflow_run_id == "run-2"
        assert handle.activity_execution_id == "exec-2"
        assert handle.activity_type == "billing.charge"

    def test_describe_activity_decodes_result(self) -> None:
        client = Client("http://localhost:8080")
        result_blob = serializer.envelope("sent", codec="avro")
        resp = _mock_response(
            200,
            {
                "activity_id": "job-3",
                "workflow_run_id": "run-3",
                "workflow_type": "dw.standalone_activity",
                "activity_type": "shipping.send_invoice",
                "status": "completed",
                "activity_status": "completed",
                "closed_reason": "completed",
                "result": result_blob,
                "payload_codec": "avro",
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp):
            desc = client.describe_activity("job-3")

        assert desc.activity_id == "job-3"
        assert desc.status == "completed"
        assert desc.result == "sent"

    def test_list_activities_returns_page(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(
            200,
            {
                "activities": [
                    {"activity_id": "job-4", "activity_type": "greeting", "status": "running"},
                    {"activity_id": "job-5", "activity_type": "greeting", "status": "completed"},
                ],
                "activity_count": 2,
                "next_page_token": "next",
            },
        )
        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            page = client.list_activities(status="completed", page_size=20, next_page_token="cursor")

        assert page.activity_count == 2
        assert [activity.activity_id for activity in page.activities] == ["job-4", "job-5"]
        assert page.next_page_token == "next"
        assert "status=completed" in mock.call_args.args[1]
        assert "page_size=20" in mock.call_args.args[1]
        assert "next_page_token=cursor" in mock.call_args.args[1]

    def test_get_activity_result_blocks_until_terminal(self) -> None:
        client = Client("http://localhost:8080")
        terminal_envelope = serializer.envelope(42, codec="avro")
        running = _mock_response(200, {"activity_id": "job-6", "status": "running"})
        completed = _mock_response(
            200,
            {
                "activity_id": "job-6",
                "status": "completed",
                "activity_status": "completed",
                "closed_reason": "completed",
                "result": terminal_envelope,
                "payload_codec": "avro",
            },
        )

        with patch.object(
            client._async._http,
            "request",
            new_callable=AsyncMock,
            side_effect=[running, completed, running, completed],
        ):
            handle = client.get_activity_handle("job-6")
            assert client.get_activity_result(handle, poll_interval=0.0, timeout=2.0) == 42
            assert handle.result(poll_interval=0.0, timeout=2.0) == 42

    def test_cancel_forwards_to_standalone_activity_host_run(self) -> None:
        client = Client("http://localhost:8080")
        resp = _mock_response(200, {"ok": True})

        with patch.object(client._async._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            handle = client.get_activity_handle("job-7")
            handle.cancel(reason="operator requested")

        assert mock.call_args.args[:2] == ("POST", "/api/workflows/job-7/cancel")
        assert mock.call_args.kwargs["json"] == {"reason": "operator requested"}

    def test_get_activity_result_forwards_timeout(self) -> None:
        client = Client("http://localhost:8080")
        handle = client.get_activity_handle("job-8")

        with patch.object(client._async, "get_activity_result", new_callable=AsyncMock, return_value="done") as mock:
            assert client.get_activity_result(handle, poll_interval=0.25, timeout=1.5) == "done"

        mock.assert_awaited_once_with(handle._handle, poll_interval=0.25, timeout=1.5)

    def test_handle_result_forwards_timeout(self) -> None:
        client = Client("http://localhost:8080")
        handle = client.get_activity_handle("job-9")

        with patch.object(handle._handle, "result", new_callable=AsyncMock, return_value="done") as mock:
            assert handle.result(poll_interval=0.25, timeout=1.5) == "done"

        mock.assert_awaited_once_with(poll_interval=0.25, timeout=1.5)


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


class TestSyncWorkflowHandleControlPlane:
    def test_run_visibility_and_maintenance_delegate_to_async_handle(self) -> None:
        async_handle = WorkflowHandle(AsyncMock(), workflow_id="wf-1", run_id="r1", workflow_type="greeter")
        async_handle.get_history = AsyncMock(return_value={"events": []})  # type: ignore[method-assign]
        async_handle.export_history = AsyncMock(return_value={"schema": "durable.workflow.history.v2"})  # type: ignore[method-assign]
        async_handle.list_runs = AsyncMock(return_value=[])  # type: ignore[method-assign]
        async_handle.describe_run = AsyncMock(return_value={"run_id": "r1"})  # type: ignore[method-assign]
        async_handle.repair = AsyncMock(return_value={"outcome": "accepted"})  # type: ignore[method-assign]
        async_handle.archive = AsyncMock(return_value={"outcome": "completed"})  # type: ignore[method-assign]
        handle = SyncWorkflowHandle(async_handle)

        assert handle.get_history() == {"events": []}
        assert handle.export_history() == {"schema": "durable.workflow.history.v2"}
        assert handle.list_runs() == []
        assert handle.describe_run() == {"run_id": "r1"}
        assert handle.repair() == {"outcome": "accepted"}
        assert handle.archive(reason="retention") == {"outcome": "completed"}
        async_handle.archive.assert_awaited_once_with(reason="retention")


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
