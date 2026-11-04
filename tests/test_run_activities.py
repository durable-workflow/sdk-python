from __future__ import annotations

from typing import Any

import httpx
import pytest

import durable_workflow
from durable_workflow.client import Client
from durable_workflow.errors import ServerError, WorkflowNotFound
from durable_workflow.retry_policy import TransportRetryPolicy


def response(activities: Any = None) -> dict[str, Any]:
    return {"workflow_id": "workflow/one", "run_id": "run?one", "activities": [] if activities is None else activities}


def client_for(payload: dict[str, Any], requests: list[httpx.Request], *, status: int = 200) -> Client:
    def reply(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, json=payload)

    client = Client("https://runtime.example", control_token="synthetic-control", worker_token="synthetic-worker",
                    namespace="tenant-a", retry_policy=TransportRetryPolicy(max_attempts=1))
    client._http = httpx.AsyncClient(base_url=client.base_url, transport=httpx.MockTransport(reply))
    return client


async def test_actual_run_route_preserves_typed_activity_attempt_and_authority() -> None:
    attempt = {"id": "attempt-2", "worker_attempt_id": "worker-attempt", "attempt_number": 2,
               "status": "running", "task_id": "task-2", "lease_owner": "worker-1",
               "lease_expires_at": "2026-11-04T12:00:30Z", "started_at": "2026-11-04T12:00:00Z",
               "last_heartbeat_at": "2026-11-04T12:00:05Z", "last_heartbeat_progress": {"details": {"part": 3}},
               "closed_at": None, "can_continue": True, "cancel_requested": False, "stop_reason": None,
               "failure_reason": None}
    activity = {"id": "execution-1", "idempotency_key": "business-key", "sequence": 4, "type": "invoice",
                "class": "InvoiceActivity", "execution_mode": "remote", "local_activity": False,
                "history_authority": "typed_history", "diagnostic_only": False,
                "history_event_types": ["ActivityScheduled", "ActivityStarted", "ActivityHeartbeatRecorded"],
                "history_unsupported_reason": None, "row_status": "running", "attempt_id": "attempt-2",
                "worker_attempt_id": "worker-attempt", "status": "running", "attempt_count": 2,
                "retry_policy": {"snapshot_version": 1, "backoff_seconds": [0]}, "connection": "default",
                "queue": "invoices", "last_heartbeat_at": attempt["last_heartbeat_at"],
                "last_heartbeat_progress": attempt["last_heartbeat_progress"], "created_at": "2026-11-04T11:59:00Z",
                "started_at": attempt["started_at"], "closed_at": None, "parallel_group_kind": "all",
                "parallel_group_id": "group-1", "parallel_group_base_sequence": 3, "parallel_group_size": 2,
                "parallel_group_index": 1, "parallel_group_path": [{"group_id": "group-1", "index": 1}],
                "reused_from_run_id": "old-run", "reused_activity_execution_id": "old-execution",
                "reused_recorded_at": "2026-11-03T12:00:00Z", "attempts": [attempt],
                "future_metadata": {"preserve": [True, None]}}
    requests: list[httpx.Request] = []
    async with client_for(response([activity]), requests) as client:
        view = await client.get_run_activities("workflow/one", "run?one")
    assert isinstance(view, durable_workflow.WorkflowActivityList)
    assert (view.workflow_id, view.run_id) == ("workflow/one", "run?one")
    item = view.activities[0]
    assert isinstance(item, durable_workflow.WorkflowActivity)
    assert item.activity_type == "invoice"
    assert item.activity_class == "InvoiceActivity"
    for key, value in activity.items():
        if key not in {"type", "class", "attempts", "future_metadata"}:
            assert getattr(item, key) == value
    assert item.raw == activity
    assert item.attempts is not None
    assert isinstance(item.attempts[0], durable_workflow.WorkflowActivityAttempt)
    for key, value in attempt.items():
        assert getattr(item.attempts[0], key) == value
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert request.url.raw_path == b"/api/workflows/workflow%2Fone/runs/run%3Fone/activities"
    assert request.headers["X-Namespace"] == "tenant-a"
    assert request.headers["Authorization"] == "Bearer synthetic-control"


async def test_empty_list_and_unavailable_optional_metadata_are_distinct() -> None:
    async with client_for(response(), []) as client:
        assert (await client.get_run_activities("workflow/one", "run?one")).activities == []
    async with client_for(response([{"id": "execution", "attempts": [{}]}, {"attempts": []}]), []) as client:
        values = (await client.get_run_activities("workflow/one", "run?one")).activities
    assert values[0].status is None
    assert values[0].attempt_count is None
    assert values[0].last_heartbeat_at is None
    assert values[0].diagnostic_only is None
    assert values[0].attempts is not None
    assert values[0].attempts[0].can_continue is None
    assert values[0].attempts[0].cancel_requested is None
    assert values[1].attempts == []
    async with client_for(response([{}]), []) as client:
        assert (await client.get_run_activities("workflow/one", "run?one")).activities[0].attempts is None


async def test_diagnostic_and_future_status_are_not_reinterpreted() -> None:
    items = [{"status": "unsupported", "history_authority": "unsupported_terminal_without_history",
              "diagnostic_only": True, "history_unsupported_reason": "terminal_activity_row_without_typed_history"},
             {"status": "future-paused", "attempt_count": 0, "local_activity": True}]
    async with client_for(response(items), []) as client:
        values = (await client.get_run_activities("workflow/one", "run?one")).activities
    assert values[0].status == "unsupported"
    assert values[0].diagnostic_only is True
    assert values[0].history_unsupported_reason == "terminal_activity_row_without_typed_history"
    assert values[1].status == "future-paused"
    assert values[1].attempt_count == 0


@pytest.mark.parametrize("change", [
    {"workflow_id": "other"}, {"run_id": "other"}, {"workflow_id": None}, {"run_id": None},
    {"activities": None}, {"activities": {}}, {"activities": ["private-marker"]},
])
async def test_changed_identity_or_invalid_collection_refuses(change: dict[str, Any]) -> None:
    async with client_for({**response(), **change}, []) as client:
        with pytest.raises(RuntimeError) as error:
            await client.get_run_activities("workflow/one", "run?one")
    assert "private-marker" not in str(error.value)


@pytest.mark.parametrize("missing", ["workflow_id", "run_id", "activities"])
async def test_required_response_fields_cannot_fabricate_an_empty_view(missing: str) -> None:
    payload = response()
    del payload[missing]
    async with client_for(payload, []) as client:
        with pytest.raises(RuntimeError):
            await client.get_run_activities("workflow/one", "run?one")


@pytest.mark.parametrize("item", [
    {"status": 1}, {"attempt_count": True}, {"local_activity": "false"}, {"diagnostic_only": 0},
    {"attempts": {}}, {"attempts": ["private-marker"]}, {"attempts": [{"can_continue": "true"}]},
    {"history_event_types": [1]}, {"parallel_group_path": [False]}, {"last_heartbeat_progress": "private-marker"},
])
async def test_malformed_known_metadata_is_not_coerced(item: dict[str, Any]) -> None:
    async with client_for(response([item]), []) as client:
        with pytest.raises(RuntimeError) as error:
            await client.get_run_activities("workflow/one", "run?one")
    assert "private-marker" not in str(error.value)


@pytest.mark.parametrize(("status", "error"), [(404, WorkflowNotFound), (503, ServerError)])
async def test_http_error_propagates(status: int, error: type[Exception]) -> None:
    requests: list[httpx.Request] = []
    async with client_for({"error": "not_found"}, requests, status=status) as client:
        with pytest.raises(error):
            await client.get_run_activities("workflow/one", "run?one")
    assert len(requests) == 1


@pytest.mark.parametrize("use_explicit_run", [False, True])
async def test_handle_binds_its_selected_run(use_explicit_run: bool) -> None:
    async with client_for(response(), []) as client:
        handle = client.get_workflow_handle("workflow/one", run_id=None if use_explicit_run else "run?one")
        view = await handle.get_activities("run?one") if use_explicit_run else await handle.get_activities()
    assert view.run_id == "run?one"


async def test_handle_without_run_refuses_before_request() -> None:
    requests: list[httpx.Request] = []
    async with client_for(response(), requests) as client:
        with pytest.raises(ValueError, match="run_id"):
            await client.get_workflow_handle("workflow/one").get_activities()
    assert not requests
