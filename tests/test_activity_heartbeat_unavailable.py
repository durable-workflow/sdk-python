"""A user-called activity heartbeat raises a typed, catchable outage error.

During a Server failover (durable-workflow/cloud#752) the Server can answer
HTTP 500 ``server_error`` and then 503 for longer than the client's transport
retries. ``ActivityContext.heartbeat()`` does not retry further; it raises
``ActivityHeartbeatUnavailable`` so activity code decides whether to keep
going. Definitive refusals keep their own errors. Every test uses a scripted
fake Server and injected waits, so nothing depends on timing.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
import pytest

import durable_workflow
from durable_workflow import Client, activity, serializer
from durable_workflow.activity import ActivityContext, ActivityInfo
from durable_workflow.errors import (
    ActivityCancelled,
    ActivityHeartbeatUnavailable,
    ActivityWorkerLost,
    DurableWorkflowError,
    ServerError,
    Unauthorized,
)
from durable_workflow.retry_policy import TransportRetryPolicy

from .test_worker_transient_5xx_retry import SERVER_ERROR, FailoverServer, failover_worker, lease

HEARTBEAT_PATH = "/api/worker/activity-tasks/t1/heartbeat"
ACCEPTED = (200, {"can_continue": True, "cancel_requested": False}, {})


class HeartbeatServer:
    """Answers activity heartbeats from a script, then accepts them."""

    def __init__(self, script: list[tuple[int, Any, dict[str, str]] | Exception]) -> None:
        self.script = script
        self.sends: list[httpx.Request] = []

    def respond(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path == HEARTBEAT_PATH
        self.sends.append(request)
        step = self.script.pop(0) if self.script else ACCEPTED
        if isinstance(step, Exception):
            raise step
        status, body, headers = step
        return httpx.Response(status, json=body, headers=headers)


def heartbeat_context(server: HeartbeatServer, *, transport_attempts: int = 2) -> tuple[Client, ActivityContext]:
    # A real Client, so the error is built by the real request path. The
    # transport retries are kept but never wait (a Retry-After is capped at 0).
    policy = TransportRetryPolicy(
        max_attempts=transport_attempts, initial_backoff_seconds=0.0, max_backoff_seconds=0.0, jitter=False,
    )
    client = Client("http://runtime.invalid", token="token", retry_policy=policy)
    client._http = httpx.AsyncClient(base_url="http://runtime.invalid", transport=httpx.MockTransport(server.respond))
    info = ActivityInfo(
        task_id="t1", activity_type="export", activity_attempt_id="aa1", attempt_number=1,
        task_queue="q", worker_id="w1",
    )
    return client, ActivityContext(info=info, client=client)


def test_is_exported_and_keeps_existing_server_error_handlers_working() -> None:
    assert durable_workflow.ActivityHeartbeatUnavailable is ActivityHeartbeatUnavailable
    assert "ActivityHeartbeatUnavailable" in durable_workflow.__all__
    assert issubclass(ActivityHeartbeatUnavailable, ServerError)
    assert issubclass(ActivityHeartbeatUnavailable, DurableWorkflowError)


async def test_server_error_500_raises_the_typed_error_with_retry_after_and_cause() -> None:
    status, body, _ = SERVER_ERROR
    server = HeartbeatServer([(status, body, {"Retry-After": "7"}), (status, body, {"Retry-After": "7"})])
    client, ctx = heartbeat_context(server)
    try:
        with pytest.raises(ActivityHeartbeatUnavailable) as raised:
            await ctx.heartbeat({"row": 3})
    finally:
        await client.aclose()

    error = raised.value
    assert error.status == 500
    assert error.body == body
    assert error.reason() == "server_error"
    assert error.retry_after == 7.0
    assert error.task_id == "t1"
    assert type(error.cause) is ServerError
    assert error.cause.status == 500
    assert error.__cause__ is error.cause
    assert "heartbeat not delivered" in str(error)
    # Both transport attempts were used; heartbeat() added no hidden retry.
    assert len(server.sends) == 2
    # The outage is not lost authority: the next heartbeat may go through.
    assert not ctx.is_cancelled
    ctx._raise_if_cannot_continue()


async def test_server_error_500_without_retry_after_has_none() -> None:
    status, body, _ = SERVER_ERROR
    server = HeartbeatServer([(status, body, {})])
    client, ctx = heartbeat_context(server, transport_attempts=1)
    try:
        with pytest.raises(ActivityHeartbeatUnavailable) as raised:
            await ctx.heartbeat()
    finally:
        await client.aclose()
    assert raised.value.retry_after is None
    assert len(server.sends) == 1


async def test_catching_server_error_still_catches_it() -> None:
    status, body, _ = SERVER_ERROR
    server = HeartbeatServer([(status, body, {})])
    client, ctx = heartbeat_context(server, transport_attempts=1)
    try:
        with pytest.raises(ServerError) as raised:
            await ctx.heartbeat()
    finally:
        await client.aclose()
    assert isinstance(raised.value, ActivityHeartbeatUnavailable)


@pytest.mark.parametrize(
    "step",
    [
        (500, {"message": "Server Error"}, {}),
        (502, "Bad Gateway", {}),
        (504, {}, {}),
        (503, {"reason": "storage_unavailable", "message": "storage is unavailable"}, {}),
        (503, {"reason": "backend_unavailable", "message": "backend is unavailable"}, {}),
        (500, {"reason": "server_error", "retryable": True}, {}),
    ],
    ids=["500-untyped", "502-text", "504-empty", "503-storage", "503-backend", "500-retryable"],
)
async def test_transient_http_failures_raise_the_typed_error(step: tuple[int, Any, dict[str, str]]) -> None:
    server = HeartbeatServer([step])
    client, ctx = heartbeat_context(server, transport_attempts=1)
    try:
        with pytest.raises(ActivityHeartbeatUnavailable) as raised:
            await ctx.heartbeat()
    finally:
        await client.aclose()
    assert raised.value.status == step[0]
    assert isinstance(raised.value.cause, ServerError)


@pytest.mark.parametrize(
    "failure",
    [httpx.ConnectError("connection refused"), httpx.ReadTimeout("read timed out")],
    ids=["connect", "timeout"],
)
async def test_connection_and_timeout_failures_raise_the_typed_error(failure: httpx.TransportError) -> None:
    server = HeartbeatServer([failure, failure])
    client, ctx = heartbeat_context(server)
    try:
        with pytest.raises(ActivityHeartbeatUnavailable) as raised:
            await ctx.heartbeat()
    finally:
        await client.aclose()
    error = raised.value
    assert error.status == 0
    assert error.body is None
    assert error.retry_after is None
    assert isinstance(error.cause, type(failure))
    assert error.__cause__ is error.cause
    assert type(failure).__name__ in str(error)
    assert len(server.sends) == 2


@pytest.mark.parametrize(
    ("step", "expected"),
    [
        ((500, {"reason": "invariant_violation"}, {}), ServerError),
        ((503, {"reason": "server_error", "retryable": False}, {}), ServerError),
        ((409, {"reason": "stale_attempt", "recorded": False}, {}), ServerError),
        ((401, {"reason": "unauthorized", "message": "bad token"}, {}), Unauthorized),
    ],
    ids=["500-typed-reason", "503-retryable-false", "409-stale-attempt", "401"],
)
async def test_definitive_refusals_keep_their_existing_errors(
    step: tuple[int, Any, dict[str, str]], expected: type[Exception],
) -> None:
    server = HeartbeatServer([step, step])
    client, ctx = heartbeat_context(server)
    try:
        with pytest.raises(DurableWorkflowError) as raised:
            await ctx.heartbeat()
    finally:
        await client.aclose()
    assert type(raised.value) is expected
    assert not isinstance(raised.value, ActivityHeartbeatUnavailable)


async def test_lost_lease_still_raises_activity_worker_lost() -> None:
    server = HeartbeatServer([(200, {"can_continue": False, "reason": "lease_expired", "cancel_requested": False}, {})])
    client, ctx = heartbeat_context(server)
    try:
        with pytest.raises(ActivityWorkerLost) as raised:
            await ctx.heartbeat()
    finally:
        await client.aclose()
    assert raised.value.reason == "lease_expired"


async def test_cancellation_still_raises_activity_cancelled() -> None:
    server = HeartbeatServer([(200, {"can_continue": True, "cancel_requested": True}, {})])
    client, ctx = heartbeat_context(server)
    try:
        with pytest.raises(ActivityCancelled):
            await ctx.heartbeat()
    finally:
        await client.aclose()
    assert ctx.is_cancelled


class FailoverHeartbeatServer(FailoverServer):
    """The failover fake Server with a scripted activity heartbeat route."""

    def __init__(self, heartbeats: list[tuple[int, Any, dict[str, str]]]) -> None:
        super().__init__({"heartbeat": heartbeats})
        self.heartbeats: list[dict[str, Any]] = []

    async def respond(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/activity-tasks/task-1/heartbeat"):
            self.heartbeats.append(httpx.Response(200, content=request.content).json())
            failure = self.scripted("heartbeat")
            if failure is not None:
                return failure
            return httpx.Response(200, json={"can_continue": True, "cancel_requested": False})
        return await super().respond(request)

    @staticmethod
    def activity_task() -> dict[str, Any]:
        return {
            "task_id": "task-1", "activity_attempt_id": "attempt-1", "attempt_number": 1,
            "activity_type": "heartbeat.failover.export", "workflow_id": "wf-1", "run_id": "run-1",
            "lease_owner": "worker-1", "task_queue": "q", "lease_expires_at": lease(300),
            "arguments": serializer.envelope([3], codec="avro"), "payload_codec": "avro",
        }


async def test_activity_catches_the_outage_retries_and_completes(caplog: pytest.LogCaptureFixture) -> None:
    status, body, _ = SERVER_ERROR
    server = FailoverHeartbeatServer([
        (status, body, {"Retry-After": "3"}),
        (503, {"reason": "storage_unavailable"}, {}),
    ])
    waits: list[float] = []

    @activity.defn(name="heartbeat.failover.export")
    async def export(rows: int) -> dict[str, int]:
        ctx = activity.context()
        outages = 0
        for row in range(rows):
            delay = 1.0
            while True:
                try:
                    await ctx.heartbeat({"row": row})
                    break
                except ActivityHeartbeatUnavailable as outage:
                    outages += 1
                    waits.append(max(delay, outage.retry_after or 0.0))
                    await asyncio.sleep(0)  # stands in for asyncio.sleep(waits[-1])
                    delay = min(delay * 2, 8.0)
        return {"rows": rows, "outages": outages}

    client, worker, _ = failover_worker(server, workflows=[], activities=[export])
    running = asyncio.create_task(worker.run())
    try:
        for _ in range(500):
            if server.completions:
                break
            await asyncio.sleep(0.01)
        assert not running.done(), running.exception() if running.done() else None
    finally:
        await worker.stop()
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await client.aclose()

    # The activity waited the Server's Retry-After, then its own backoff step.
    assert waits == [3.0, 2.0]
    # Row 0 was sent three times (500, 503, accepted), then rows 1 and 2 once.
    assert [beat.get("details") for beat in server.heartbeats] == [
        {"row": 0}, {"row": 0}, {"row": 0}, {"row": 1}, {"row": 2},
    ]
    assert len(server.completions) == 1
    completion = server.completions[0]
    assert completion["activity_attempt_id"] == "attempt-1"
    assert serializer.decode_envelope(completion["result"], codec="avro") == {"rows": 3, "outages": 2}
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
