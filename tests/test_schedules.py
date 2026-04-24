from __future__ import annotations

import json
import logging
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from durable_workflow import serializer
from durable_workflow.client import (
    Client,
    ScheduleAction,
    ScheduleBackfillResult,
    ScheduleDescription,
    ScheduleHandle,
    ScheduleHistoryEvent,
    ScheduleHistoryPage,
    ScheduleList,
    ScheduleSpec,
    ScheduleTriggerResult,
)
from durable_workflow.errors import (
    InvalidArgument,
    ScheduleAlreadyExists,
    ScheduleNotFound,
)


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


class TestScheduleSpec:
    def test_cron_to_dict(self) -> None:
        spec = ScheduleSpec(cron_expressions=["0 * * * *"], timezone="UTC")
        d = spec.to_dict()
        assert d == {"cron_expressions": ["0 * * * *"], "timezone": "UTC"}

    def test_interval_to_dict(self) -> None:
        spec = ScheduleSpec(intervals=[{"every": "PT30M", "offset": "PT5M"}])
        d = spec.to_dict()
        assert d == {"intervals": [{"every": "PT30M", "offset": "PT5M"}]}

    def test_empty_to_dict(self) -> None:
        spec = ScheduleSpec()
        assert spec.to_dict() == {}


class TestScheduleAction:
    def test_minimal(self) -> None:
        action = ScheduleAction(workflow_type="greeter")
        d = action.to_dict()
        assert d == {"workflow_type": "greeter"}

    def test_full(self) -> None:
        action = ScheduleAction(
            workflow_type="greeter",
            task_queue="q1",
            input=["hello"],
            execution_timeout_seconds=3600,
            run_timeout_seconds=600,
        )
        d = action.to_dict()
        assert d["workflow_type"] == "greeter"
        assert d["task_queue"] == "q1"
        assert d["input"]["codec"] == "avro"
        assert d["execution_timeout_seconds"] == 3600
        assert d["run_timeout_seconds"] == 600


class TestCreateSchedule:
    @pytest.mark.asyncio
    async def test_success(self, client: Client) -> None:
        resp = _mock_response(201, {"schedule_id": "sched-1", "outcome": "created"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            handle = await client.create_schedule(
                schedule_id="sched-1",
                spec=ScheduleSpec(cron_expressions=["0 * * * *"]),
                action=ScheduleAction(workflow_type="greeter", task_queue="q1"),
                overlap_policy="skip",
                jitter_seconds=30,
            )
            assert isinstance(handle, ScheduleHandle)
            assert handle.schedule_id == "sched-1"
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["spec"]["cron_expressions"] == ["0 * * * *"]
            assert body["action"]["workflow_type"] == "greeter"
            assert body["overlap_policy"] == "skip"
            assert body["jitter_seconds"] == 30
            assert body["schedule_id"] == "sched-1"

    @pytest.mark.asyncio
    async def test_action_input_uses_codec_envelope(self, client: Client) -> None:
        resp = _mock_response(201, {"schedule_id": "sched-env", "outcome": "created"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.create_schedule(
                schedule_id="sched-env",
                spec=ScheduleSpec(cron_expressions=["0 * * * *"]),
                action=ScheduleAction(workflow_type="greeter", task_queue="q1", input=["Alice", 42]),
            )
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            action_input = body["action"]["input"]
            assert action_input["codec"] == "avro"
            assert serializer.decode(action_input["blob"], codec="avro") == ["Alice", 42]

    @pytest.mark.asyncio
    async def test_action_input_warning_uses_client_policy(
        self, client: Client, caplog: pytest.LogCaptureFixture
    ) -> None:
        client.payload_size_warning_config = serializer.PayloadSizeWarningConfig(
            limit_bytes=10,
            threshold_percent=50,
        )
        resp = _mock_response(201, {"schedule_id": "sched-large", "outcome": "created"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"),
        ):
            await client.create_schedule(
                schedule_id="sched-large",
                spec=ScheduleSpec(cron_expressions=["0 * * * *"]),
                action=ScheduleAction(
                    workflow_type="greeter",
                    task_queue="q1",
                    input=["this payload is intentionally large"],
                ),
            )

        payload = caplog.records[0].durable_workflow_payload
        assert payload["kind"] == "schedule_input"
        assert payload["workflow_type"] == "greeter"
        assert payload["schedule_id"] == "sched-large"
        assert payload["task_queue"] == "q1"
        assert payload["namespace"] == "ns1"
        assert payload["threshold_bytes"] == 5

    @pytest.mark.asyncio
    async def test_action_input_warning_can_be_disabled(
        self, client: Client, caplog: pytest.LogCaptureFixture
    ) -> None:
        client.payload_size_warning_config = None
        resp = _mock_response(201, {"schedule_id": "sched-quiet", "outcome": "created"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"),
        ):
            await client.create_schedule(
                schedule_id="sched-quiet",
                spec=ScheduleSpec(cron_expressions=["0 * * * *"]),
                action=ScheduleAction(
                    workflow_type="greeter",
                    task_queue="q1",
                    input=["this payload is intentionally large"],
                ),
            )

        assert caplog.records == []

    @pytest.mark.asyncio
    async def test_minimal(self, client: Client) -> None:
        resp = _mock_response(201, {"schedule_id": "auto-id", "outcome": "created"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            handle = await client.create_schedule(
                spec=ScheduleSpec(cron_expressions=["*/5 * * * *"]),
                action=ScheduleAction(workflow_type="ticker"),
            )
            assert handle.schedule_id == "auto-id"
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert "schedule_id" not in body
            assert "overlap_policy" not in body

    @pytest.mark.asyncio
    async def test_paused(self, client: Client) -> None:
        resp = _mock_response(201, {"schedule_id": "sched-1", "outcome": "created"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.create_schedule(
                schedule_id="sched-1",
                spec=ScheduleSpec(cron_expressions=["0 0 * * *"]),
                action=ScheduleAction(workflow_type="nightly"),
                paused=True,
                note="Paused until ready",
            )
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["paused"] is True
            assert body["note"] == "Paused until ready"

    @pytest.mark.asyncio
    async def test_already_exists(self, client: Client) -> None:
        resp = _mock_response(409, {
            "reason": "schedule_already_exists",
            "message": "already exists",
            "schedule_id": "sched-1",
        })
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(ScheduleAlreadyExists),
        ):
            await client.create_schedule(
                schedule_id="sched-1",
                spec=ScheduleSpec(cron_expressions=["0 * * * *"]),
                action=ScheduleAction(workflow_type="greeter"),
            )


class TestListSchedules:
    @pytest.mark.asyncio
    async def test_list(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedules": [
                {"schedule_id": "s1", "status": "active", "fires_count": 5},
                {"schedule_id": "s2", "status": "paused", "fires_count": 0},
            ],
            "next_page_token": None,
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await client.list_schedules()
            assert isinstance(result, ScheduleList)
            assert len(result.schedules) == 2
            assert result.schedules[0].schedule_id == "s1"
            assert result.schedules[0].status == "active"
            assert result.schedules[0].fires_count == 5
            assert result.schedules[1].status == "paused"

    @pytest.mark.asyncio
    async def test_empty_list(self, client: Client) -> None:
        resp = _mock_response(200, {"schedules": [], "next_page_token": None})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await client.list_schedules()
            assert result.schedules == []


class TestDescribeSchedule:
    @pytest.mark.asyncio
    async def test_success(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1",
            "status": "active",
            "spec": {"cron_expressions": ["0 * * * *"], "timezone": "UTC"},
            "action": {"workflow_type": "greeter", "task_queue": "q1"},
            "overlap_policy": "skip",
            "jitter_seconds": 30,
            "max_runs": 100,
            "remaining_actions": 95,
            "fires_count": 5,
            "failures_count": 0,
            "next_fire_at": "2026-04-15T01:00:00Z",
            "last_fired_at": "2026-04-15T00:00:00Z",
            "latest_workflow_instance_id": "wf-latest",
            "memo": {"key": "value"},
            "info": {"skipped_trigger_count": 2, "last_skip_reason": "overlap"},
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            desc = await client.describe_schedule("sched-1")
            assert isinstance(desc, ScheduleDescription)
            assert desc.schedule_id == "sched-1"
            assert desc.status == "active"
            assert desc.spec == {"cron_expressions": ["0 * * * *"], "timezone": "UTC"}
            assert desc.overlap_policy == "skip"
            assert desc.jitter_seconds == 30
            assert desc.max_runs == 100
            assert desc.remaining_actions == 95
            assert desc.fires_count == 5
            assert desc.latest_workflow_instance_id == "wf-latest"
            assert desc.memo == {"key": "value"}
            assert desc.info is not None
            assert desc.info["skipped_trigger_count"] == 2

    @pytest.mark.asyncio
    async def test_not_found(self, client: Client) -> None:
        resp = _mock_response(404, {"reason": "schedule_not_found", "message": "not found"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(ScheduleNotFound),
        ):
            await client.describe_schedule("sched-missing")


class TestUpdateSchedule:
    @pytest.mark.asyncio
    async def test_update_spec(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "updated"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.update_schedule(
                "sched-1",
                spec=ScheduleSpec(cron_expressions=["*/10 * * * *"]),
                jitter_seconds=60,
            )
            call_args = mock.call_args
            assert call_args[0][0] == "PUT"
            body = call_args.kwargs.get("json") or call_args[1].get("json")
            assert body["spec"]["cron_expressions"] == ["*/10 * * * *"]
            assert body["jitter_seconds"] == 60

    @pytest.mark.asyncio
    async def test_update_note(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "updated"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.update_schedule("sched-1", note="Updated note")
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["note"] == "Updated note"

    @pytest.mark.asyncio
    async def test_update_action_input_warning_uses_schedule_id(
        self, client: Client, caplog: pytest.LogCaptureFixture
    ) -> None:
        client.payload_size_warning_config = serializer.PayloadSizeWarningConfig(
            limit_bytes=10,
            threshold_percent=50,
        )
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "updated"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"),
        ):
            await client.update_schedule(
                "sched-1",
                action=ScheduleAction(
                    workflow_type="ticker",
                    task_queue="q2",
                    input=["this update payload is intentionally large"],
                ),
            )

        payload = caplog.records[0].durable_workflow_payload
        assert payload["kind"] == "schedule_input"
        assert payload["workflow_type"] == "ticker"
        assert payload["schedule_id"] == "sched-1"
        assert payload["task_queue"] == "q2"

    @pytest.mark.asyncio
    async def test_not_found(self, client: Client) -> None:
        resp = _mock_response(404, {"reason": "schedule_not_found", "message": "not found"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(ScheduleNotFound),
        ):
            await client.update_schedule("sched-missing", note="fail")


class TestPauseSchedule:
    @pytest.mark.asyncio
    async def test_pause(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "paused"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.pause_schedule("sched-1", note="Maintenance")
            call_args = mock.call_args
            assert "/pause" in call_args[0][1]
            body = call_args.kwargs.get("json") or call_args[1].get("json")
            assert body["note"] == "Maintenance"

    @pytest.mark.asyncio
    async def test_pause_no_note(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "paused"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.pause_schedule("sched-1")
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert "note" not in body


class TestResumeSchedule:
    @pytest.mark.asyncio
    async def test_resume(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "resumed"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.resume_schedule("sched-1", note="Back online")
            assert "/resume" in mock.call_args[0][1]


class TestTriggerSchedule:
    @pytest.mark.asyncio
    async def test_trigger_success(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1",
            "outcome": "triggered",
            "workflow_id": "wf-123",
            "run_id": "run-456",
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await client.trigger_schedule("sched-1")
            assert isinstance(result, ScheduleTriggerResult)
            assert result.outcome == "triggered"
            assert result.workflow_id == "wf-123"
            assert result.run_id == "run-456"

    @pytest.mark.asyncio
    async def test_trigger_buffered(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1",
            "outcome": "buffered",
            "buffer_depth": 1,
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await client.trigger_schedule("sched-1")
            assert result.outcome == "buffered"
            assert result.buffer_depth == 1

    @pytest.mark.asyncio
    async def test_trigger_with_overlap_policy(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "triggered", "workflow_id": "wf-1"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.trigger_schedule("sched-1", overlap_policy="allow_all")
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["overlap_policy"] == "allow_all"


class TestDeleteSchedule:
    @pytest.mark.asyncio
    async def test_delete(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "deleted"})
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            await client.delete_schedule("sched-1")
            assert mock.call_args[0][0] == "DELETE"

    @pytest.mark.asyncio
    async def test_not_found(self, client: Client) -> None:
        resp = _mock_response(404, {"reason": "schedule_not_found", "message": "not found"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(ScheduleNotFound),
        ):
            await client.delete_schedule("sched-missing")


class TestBackfillSchedule:
    @pytest.mark.asyncio
    async def test_backfill_success(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1",
            "outcome": "backfill_started",
            "fires_attempted": 3,
            "results": [
                {"fire_time": "2026-04-14T00:00:00Z", "workflow_id": "wf-1", "outcome": "started"},
                {"fire_time": "2026-04-14T01:00:00Z", "workflow_id": "wf-2", "outcome": "started"},
                {"fire_time": "2026-04-14T02:00:00Z", "outcome": "skipped", "reason": "overlap"},
            ],
        })
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock:
            result = await client.backfill_schedule(
                "sched-1",
                start_time="2026-04-14T00:00:00Z",
                end_time="2026-04-14T03:00:00Z",
            )
            assert isinstance(result, ScheduleBackfillResult)
            assert result.fires_attempted == 3
            assert len(result.results or []) == 3
            body = mock.call_args.kwargs.get("json") or mock.call_args[1].get("json")
            assert body["start_time"] == "2026-04-14T00:00:00Z"
            assert body["end_time"] == "2026-04-14T03:00:00Z"

    @pytest.mark.asyncio
    async def test_backfill_invalid_range(self, client: Client) -> None:
        resp = _mock_response(422, {
            "message": "end_time must be after start_time.",
            "reason": "invalid_time_range",
        })
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(InvalidArgument),
        ):
            await client.backfill_schedule(
                "sched-1",
                start_time="2026-04-15T00:00:00Z",
                end_time="2026-04-14T00:00:00Z",
            )


class TestScheduleHandle:
    @pytest.mark.asyncio
    async def test_handle_describe(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1", "status": "active",
            "fires_count": 10, "failures_count": 0,
        })
        handle = client.get_schedule_handle("sched-1")
        assert handle.schedule_id == "sched-1"
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            desc = await handle.describe()
            assert desc.schedule_id == "sched-1"

    @pytest.mark.asyncio
    async def test_handle_pause(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "paused"})
        handle = client.get_schedule_handle("sched-1")
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            await handle.pause(note="test")

    @pytest.mark.asyncio
    async def test_handle_resume(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "resumed"})
        handle = client.get_schedule_handle("sched-1")
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            await handle.resume()

    @pytest.mark.asyncio
    async def test_handle_trigger(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1", "outcome": "triggered", "workflow_id": "wf-1",
        })
        handle = client.get_schedule_handle("sched-1")
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await handle.trigger()
            assert result.outcome == "triggered"

    @pytest.mark.asyncio
    async def test_handle_delete(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "deleted"})
        handle = client.get_schedule_handle("sched-1")
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            await handle.delete()

    @pytest.mark.asyncio
    async def test_handle_update(self, client: Client) -> None:
        resp = _mock_response(200, {"schedule_id": "sched-1", "outcome": "updated"})
        handle = client.get_schedule_handle("sched-1")
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            await handle.update(jitter_seconds=120)

    @pytest.mark.asyncio
    async def test_handle_backfill(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1", "outcome": "backfill_started",
            "fires_attempted": 1, "results": [],
        })
        handle = client.get_schedule_handle("sched-1")
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            result = await handle.backfill(
                start_time="2026-04-14T00:00:00Z",
                end_time="2026-04-14T01:00:00Z",
            )
            assert result.fires_attempted == 1


class TestScheduleHistory:
    @pytest.mark.asyncio
    async def test_parses_events_and_pagination_cursor(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1",
            "namespace": "ns1",
            "events": [
                {
                    "id": "evt-1",
                    "sequence": 1,
                    "event_type": "ScheduleCreated",
                    "recorded_at": "2026-04-01T00:00:00+00:00",
                    "workflow_instance_id": None,
                    "workflow_run_id": None,
                    "payload": {},
                },
                {
                    "id": "evt-2",
                    "sequence": 2,
                    "event_type": "ScheduleTriggered",
                    "recorded_at": "2026-04-02T00:00:00+00:00",
                    "workflow_instance_id": "wf-abc",
                    "workflow_run_id": "run-abc",
                    "payload": {"outcome": "triggered", "trigger_number": 1},
                },
            ],
            "has_more": True,
            "next_cursor": 2,
        })

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock_req:
            page = await client.get_schedule_history("sched-1")

        assert isinstance(page, ScheduleHistoryPage)
        assert page.schedule_id == "sched-1"
        assert page.namespace == "ns1"
        assert page.has_more is True
        assert page.next_cursor == 2
        assert len(page.events) == 2
        assert isinstance(page.events[0], ScheduleHistoryEvent)
        assert page.events[0].sequence == 1
        assert page.events[0].event_type == "ScheduleCreated"
        assert page.events[1].workflow_instance_id == "wf-abc"
        assert page.events[1].payload == {"outcome": "triggered", "trigger_number": 1}

        call = mock_req.call_args
        assert call.args[0] == "GET"
        assert call.args[1] == "/api/schedules/sched-1/history"

    @pytest.mark.asyncio
    async def test_forwards_limit_and_after_sequence_in_query(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1",
            "namespace": "ns1",
            "events": [],
            "has_more": False,
            "next_cursor": None,
        })

        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp) as mock_req:
            await client.get_schedule_history("sched-1", limit=50, after_sequence=7)

        call = mock_req.call_args
        path = call.args[1]
        assert path.startswith("/api/schedules/sched-1/history?")
        assert "limit=50" in path
        assert "after_sequence=7" in path

    @pytest.mark.asyncio
    async def test_rejects_invalid_limit(self, client: Client) -> None:
        with pytest.raises(ValueError):
            await client.get_schedule_history("sched-1", limit=0)

    @pytest.mark.asyncio
    async def test_rejects_negative_after_sequence(self, client: Client) -> None:
        with pytest.raises(ValueError):
            await client.get_schedule_history("sched-1", after_sequence=-1)

    @pytest.mark.asyncio
    async def test_not_found_maps_to_schedule_not_found(self, client: Client) -> None:
        resp = _mock_response(404, {"reason": "schedule_not_found", "message": "not found"})
        with (
            patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp),
            pytest.raises(ScheduleNotFound),
        ):
            await client.get_schedule_history("missing")

    @pytest.mark.asyncio
    async def test_iter_pages_across_multiple_server_responses(self, client: Client) -> None:
        responses = [
            _mock_response(200, {
                "schedule_id": "sched-1",
                "namespace": "ns1",
                "events": [
                    {
                        "sequence": 1,
                        "event_type": "ScheduleCreated",
                        "recorded_at": "2026-04-01T00:00:00+00:00",
                        "payload": {},
                    },
                    {
                        "sequence": 2,
                        "event_type": "SchedulePaused",
                        "recorded_at": "2026-04-02T00:00:00+00:00",
                        "payload": {},
                    },
                ],
                "has_more": True,
                "next_cursor": 2,
            }),
            _mock_response(200, {
                "schedule_id": "sched-1",
                "namespace": "ns1",
                "events": [
                    {
                        "sequence": 3,
                        "event_type": "ScheduleResumed",
                        "recorded_at": "2026-04-03T00:00:00+00:00",
                        "payload": {},
                    },
                ],
                "has_more": False,
                "next_cursor": None,
            }),
        ]

        mock = AsyncMock(side_effect=responses)
        with patch.object(client._http, "request", new=mock):
            sequences = [event.sequence async for event in client.iter_schedule_history("sched-1")]

        assert sequences == [1, 2, 3]
        assert mock.call_count == 2
        second_call_path = mock.call_args_list[1].args[1]
        assert "after_sequence=2" in second_call_path

    @pytest.mark.asyncio
    async def test_handle_history_delegates_to_client(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1",
            "namespace": "ns1",
            "events": [
                {
                    "sequence": 1,
                    "event_type": "ScheduleCreated",
                    "recorded_at": "2026-04-01T00:00:00+00:00",
                    "payload": {},
                },
            ],
            "has_more": False,
            "next_cursor": None,
        })

        handle = client.get_schedule_handle("sched-1")
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            page = await handle.history(limit=10)

        assert isinstance(page, ScheduleHistoryPage)
        assert page.events[0].event_type == "ScheduleCreated"

    @pytest.mark.asyncio
    async def test_handle_iter_history_delegates_to_client(self, client: Client) -> None:
        resp = _mock_response(200, {
            "schedule_id": "sched-1",
            "namespace": "ns1",
            "events": [
                {
                    "sequence": 1,
                    "event_type": "ScheduleCreated",
                    "recorded_at": "2026-04-01T00:00:00+00:00",
                    "payload": {},
                },
            ],
            "has_more": False,
            "next_cursor": None,
        })

        handle = client.get_schedule_handle("sched-1")
        with patch.object(client._http, "request", new_callable=AsyncMock, return_value=resp):
            events = [event async for event in handle.iter_history()]

        assert len(events) == 1
        assert events[0].event_type == "ScheduleCreated"
