"""The worker rides out HTTP 500/503 from a failing-over Server (cloud#752).

During a database failover a Server can answer HTTP 500 without a protocol
reason, then 503 with ``Retry-After``. Every request the worker repeats must
survive that sequence: polls, activity outcomes and workflow-task
completions. The waits use the shared bounded backoff and honour a bounded
``Retry-After``. Sleeps are injected, so the tests spend no real backoff time.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from durable_workflow import Client, Worker, activity, serializer, workflow
from durable_workflow.errors import ServerError
from durable_workflow.retry_policy import TransportRetryPolicy, parse_retry_after
from durable_workflow.workflow import WorkflowContext

from .test_worker import compatible_cluster_info

SERVER_ERROR = (500, {"message": "Server Error"}, {})
COMMANDS = [{"type": "complete_workflow", "result": serializer.envelope("done", codec="avro")}]


def http_date(seconds_from_now: float) -> str:
    return format_datetime(datetime.now(timezone.utc) + timedelta(seconds=seconds_from_now), usegmt=True)


def lease(seconds: float) -> str:
    expires = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    return expires.isoformat(timespec="microseconds").replace("+00:00", "Z")


FAILOVER_FIXTURE = Path(__file__).parent / "fixtures/replay_regressions/failover-completion-resend-cold-history.json"


@workflow.defn(name="tests.failover.completion")
class FailoverCompletionWorkflow:
    def run(self, ctx: WorkflowContext, order: str) -> Generator[object, object, object]:
        settled = yield ctx.schedule_activity("settle", [order])
        return {"order": order, "settled": settled, "run": ctx.run_id}


@activity.defn(name="failover.echo")
def echo(value: str) -> str:
    return f"echo:{value}"


class FailoverServer:
    """A fake Server whose scripted routes fail before they answer normally."""

    def __init__(self, scripts: dict[str, list[tuple[int, Any, dict[str, str]]]]) -> None:
        self.scripts = scripts
        self.completions: list[dict[str, Any]] = []
        self.workflow_completion_sends: list[bytes] = []
        self.activity_polls = 0
        self.delivered = False
        info = compatible_cluster_info()
        info["worker_protocol"]["server_capabilities"].pop("worker_deregistration_fencing", None)  # type: ignore[index]
        self.info = info

    def scripted(self, route: str) -> httpx.Response | None:
        script = self.scripts.get(route)
        if not script:
            return None
        status, body, headers = script.pop(0)
        return httpx.Response(status, json=body, headers=headers)

    async def respond(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/cluster/info"):
            return httpx.Response(200, json=self.info)
        if path.endswith("/worker/register"):
            return httpx.Response(200, json={"worker_id": "worker-1", "registered": True})
        if path.endswith("/activity-tasks/poll"):
            self.activity_polls += 1
            failure = self.scripted("activity_poll")
            if failure is not None:
                return failure
            if not self.delivered:
                self.delivered = True
                return httpx.Response(200, json={"task": self.activity_task()})
            await asyncio.sleep(0.01)
            return httpx.Response(200, json={"task": None})
        if path.endswith("/activity-tasks/task-1/complete"):
            failure = self.scripted("activity_complete")
            if failure is not None:
                return failure
            self.completions.append(json.loads(request.content))
            return httpx.Response(200, json={"outcome": "completed", "recorded": True})
        if path.endswith("/workflow-tasks/wt-1/complete"):
            self.workflow_completion_sends.append(request.content)
            failure = self.scripted("workflow_complete")
            if failure is not None:
                return failure
            self.completions.append(json.loads(request.content))
            return httpx.Response(200, json={"outcome": "completed"})
        if path.endswith("/registrations/worker-1"):
            return httpx.Response(200, json={"worker_id": "worker-1", "outcome": "deregistered",
                                             "recovered_workflow_task_count": 0})
        return httpx.Response(200, json={"worker_id": "worker-1", "acknowledged": True})

    @staticmethod
    def activity_task() -> dict[str, Any]:
        return {
            "task_id": "task-1", "activity_attempt_id": "attempt-1", "attempt_number": 1,
            "activity_type": "failover.echo", "workflow_id": "wf-1", "run_id": "run-1",
            "lease_owner": "worker-1", "task_queue": "q", "lease_expires_at": lease(300),
            "arguments": serializer.envelope(["value"], codec="avro"), "payload_codec": "avro",
        }


def failover_worker(server: FailoverServer, **worker_options: Any) -> tuple[Client, Worker, list[float]]:
    # One transport attempt per request: the worker's own retry loops are under test.
    client = Client("http://runtime.invalid", token="token", retry_policy=TransportRetryPolicy(max_attempts=1))
    client._http = httpx.AsyncClient(base_url="http://runtime.invalid", transport=httpx.MockTransport(server.respond))
    worker = Worker(client, task_queue="q", worker_id="worker-1", **worker_options)
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        await asyncio.sleep(0)

    worker._retry_sleep = sleep
    return client, worker, sleeps


@pytest.fixture
def ceiling_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    # Full jitter draws uniform(0, ceiling); take the ceiling so waits are exact.
    monkeypatch.setattr("durable_workflow.worker.random.uniform", lambda low, high: high)


@pytest.mark.usefixtures("ceiling_jitter")
async def test_worker_run_survives_500_then_503_on_poll_and_on_activity_completion(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = FailoverServer({
        "activity_poll": [SERVER_ERROR, (503, {"message": "Service Unavailable"}, {"Retry-After": "2"})],
        "activity_complete": [SERVER_ERROR, (503, {}, {"Retry-After": http_date(3)})],
    })
    client, worker, sleeps = failover_worker(server, workflows=[], activities=[echo])
    running = asyncio.create_task(worker.run())
    try:
        for _ in range(500):
            if server.completions and server.activity_polls >= 5:
                break
            await asyncio.sleep(0.01)
        assert not running.done(), running.exception() if running.done() else None
        assert worker._registered is True
        # The activity ran once and its completion was delivered once, unchanged.
        assert len(server.completions) == 1
        completion = server.completions[0]
        assert completion["activity_attempt_id"] == "attempt-1"
        assert completion["lease_owner"] == "worker-1"
        assert serializer.decode_envelope(completion["result"], codec="avro") == "echo:value"
        assert server.activity_polls >= 5, "the poller keeps polling after the failover"
        assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
    finally:
        await worker.stop()
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await client.aclose()

    poll_waits = [sleeps[0], sleeps[1]]
    completion_waits = sleeps[2:]
    # 500: the first shared backoff step; 503: its Retry-After of 2 s outweighs the 0.5 s step.
    assert poll_waits == [0.25, 2.0]
    # The HTTP-date form is honoured too (whole seconds, so up to 3 s from now).
    assert len(completion_waits) == 2
    assert completion_waits[0] == 0.25
    assert 1.0 < completion_waits[1] <= 3.0


@pytest.mark.usefixtures("ceiling_jitter")
async def test_workflow_task_completion_survives_500_then_503_with_retry_after() -> None:
    server = FailoverServer({
        "workflow_complete": [SERVER_ERROR, (503, {}, {"Retry-After": "4"})],
    })
    client, worker, sleeps = failover_worker(server, workflows=[], activities=[])
    try:
        await worker._complete_workflow_task_with_retry(
            task_id="wt-1", attempt=2, commands=COMMANDS, lease_expires_at=lease(60),
        )
    finally:
        await client.aclose()
    assert sleeps == [0.25, 4.0]
    assert len(server.completions) == 1
    assert server.completions[0]["lease_owner"] == "worker-1"
    assert server.completions[0]["workflow_task_attempt"] == 2
    assert server.completions[0]["commands"] == COMMANDS


@pytest.mark.usefixtures("ceiling_jitter")
async def test_replayed_workflow_task_completion_is_resent_unchanged_through_a_failover() -> None:
    fixture = json.loads(FAILOVER_FIXTURE.read_text(encoding="utf-8"))
    server = FailoverServer({
        "workflow_complete": [SERVER_ERROR, (503, {"reason": "storage_unavailable"}, {"Retry-After": "3"})],
    })
    client, worker, sleeps = failover_worker(server, workflows=[FailoverCompletionWorkflow], activities=[])
    task = {
        "task_id": "wt-1", "workflow_task_attempt": 4, "lease_expires_at": lease(60),
        "workflow_type": fixture["workflow"]["type"], "workflow_id": "order-088",
        "run_id": fixture["workflow"]["run_id"], "payload_codec": "avro",
        "arguments": serializer.encode(fixture["workflow"]["input"]), "history_events": fixture["history"],
    }
    try:
        commands = await worker._run_workflow_task(task)
    finally:
        await client.aclose()
    assert commands is not None and [command["type"] for command in commands] == ["complete_workflow"]
    assert sleeps == [0.25, 3.0]
    # Three sends of one request: the same task, lease owner, attempt and commands.
    assert len(server.workflow_completion_sends) == 3
    assert len(set(server.workflow_completion_sends)) == 1
    delivered = server.completions[0]
    assert (delivered["lease_owner"], delivered["workflow_task_attempt"]) == ("worker-1", 4)
    result = serializer.decode_envelope(delivered["commands"][0]["result"], codec="avro")
    assert result == fixture["expected"]["result"]


@pytest.mark.usefixtures("ceiling_jitter")
async def test_retry_waits_are_bounded_even_when_retry_after_is_huge() -> None:
    outage = [(503, {}, {"Retry-After": "86400"})]
    server = FailoverServer({
        "activity_poll": [SERVER_ERROR] * 12 + outage,
        "workflow_complete": [SERVER_ERROR] * 8 + outage,
    })
    client, worker, sleeps = failover_worker(server, workflows=[], activities=[echo])
    running = asyncio.create_task(worker._poll_activity_tasks())
    try:
        for _ in range(500):
            if server.delivered:
                break
            await asyncio.sleep(0.01)
        assert server.delivered, "the poller recovered after 13 failures"
        # Shared full jitter, capped at 10 s for polls; Retry-After capped at 60 s.
        assert sleeps == [0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 60.0]
        worker._stop.set()
        await asyncio.wait_for(running, timeout=2)
        await asyncio.gather(*worker._in_flight, return_exceptions=True)

        sleeps.clear()
        await worker._complete_workflow_task_with_retry(
            task_id="wt-1", attempt=1, commands=COMMANDS, lease_expires_at=lease(300),
        )
        # Workflow-task completions use the 4 s cap of the shared schedule.
        assert sleeps == [0.25, 0.5, 1.0, 2.0, 4.0, 4.0, 4.0, 4.0, 60.0]
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await client.aclose()


@pytest.mark.usefixtures("ceiling_jitter")
async def test_workflow_task_completion_stops_resending_when_the_lease_runs_out(
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = FailoverServer({"workflow_complete": [SERVER_ERROR] * 100})
    client, worker, sleeps = failover_worker(server, workflows=[], activities=[])
    try:
        with caplog.at_level(logging.ERROR, logger="durable_workflow.worker"), \
                pytest.raises(ServerError) as raised:
            await worker._complete_workflow_task_with_retry(
                task_id="wt-1", attempt=1, commands=COMMANDS, lease_expires_at=lease(0.9),
            )
    finally:
        await client.aclose()
    assert raised.value.status == 500
    assert sleeps == [0.25, 0.5], "resent only while the lease covered the next wait"
    assert "workflow task wt-1 completion not delivered after 3 transient failures" in caplog.text


async def test_workflow_task_completion_keeps_its_short_budget_for_typed_5xx() -> None:
    server = FailoverServer({"workflow_complete": [(500, {"reason": "invalid_history_sequence"}, {})] * 5})
    client, worker, sleeps = failover_worker(server, workflows=[], activities=[])
    try:
        with pytest.raises(ServerError):
            await worker._complete_workflow_task_with_retry(
                task_id="wt-1", attempt=1, commands=COMMANDS, lease_expires_at=lease(300),
            )
    finally:
        await client.aclose()
    assert sleeps == [0.05, 0.2]
    assert len(server.scripts["workflow_complete"]) == 2, "three attempts, as before"


async def test_definitive_4xx_completion_refusal_is_not_resent() -> None:
    server = FailoverServer({"workflow_complete": [(409, {"reason": "task_not_leased"}, {"Retry-After": "1"})]})
    client, worker, sleeps = failover_worker(server, workflows=[], activities=[])
    try:
        with pytest.raises(ServerError) as raised:
            await worker._complete_workflow_task_with_retry(
                task_id="wt-1", attempt=1, commands=COMMANDS, lease_expires_at=lease(300),
            )
    finally:
        await client.aclose()
    assert raised.value.status == 409
    assert raised.value.retry_after == 1.0
    assert sleeps == []


async def test_client_errors_carry_the_parsed_retry_after() -> None:
    server = FailoverServer({"workflow_complete": [(503, {}, {"Retry-After": http_date(30)})]})
    client, worker, _ = failover_worker(server, workflows=[], activities=[])
    try:
        with pytest.raises(ServerError) as raised:
            await client.complete_workflow_task(task_id="wt-1", lease_owner="worker-1", workflow_task_attempt=1,
                                                commands=COMMANDS)
    finally:
        await client.aclose()
    assert raised.value.retry_after is not None
    assert 28.0 <= raised.value.retry_after <= 30.0


@pytest.mark.parametrize(("value", "expected"), [
    ("0", 0.0),
    ("7", 7.0),
    (" 120 ", 120.0),
    ("Wed, 25 Nov 2026 22:40:51 GMT", 10.0),
    ("Wed, 25 Nov 2026 22:40:31 GMT", 0.0),  # a date in the past waits nothing
    ("-5", None),
    ("1.5", None),
    ("soon", None),
    ("", None),
    (None, None),
])
def test_parse_retry_after_accepts_delta_seconds_and_http_dates(value: str | None, expected: float | None) -> None:
    now = datetime(2026, 11, 25, 22, 40, 41, tzinfo=timezone.utc)
    assert parse_retry_after(value, now=now) == expected


async def test_transport_retry_honours_retry_after_up_to_its_ceiling(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("durable_workflow.retry_policy.asyncio.sleep", sleep)
    monkeypatch.setattr("durable_workflow.retry_policy.random.uniform", lambda low, high: 1.0)
    request = httpx.Request("POST", "http://runtime.invalid/api/worker/heartbeat")
    responses = iter([
        httpx.Response(503, headers={"Retry-After": "2"}, request=request),
        httpx.Response(503, headers={"Retry-After": "600"}, request=request),
        httpx.Response(200, request=request),
    ])

    async def send() -> httpx.Response:
        response = next(responses)
        response.raise_for_status()
        return response

    policy = TransportRetryPolicy(max_attempts=3, initial_backoff_seconds=0.1, max_backoff_seconds=5.0)
    assert (await policy.execute(send)).status_code == 200
    assert sleeps == [2.0, 5.0]
