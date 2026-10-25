from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

import httpx
import pytest

from durable_workflow._poll_transport import MEDIA_TYPE, unwrap_response
from durable_workflow.client import Client
from durable_workflow.errors import InvalidArgument, ServerError
from durable_workflow.retry_policy import TransportRetryPolicy


def envelope(status: int, body: Any, **changes: Any) -> dict[str, Any]:
    return {"schema": "durable-workflow.poll-transport.v1", "status": status, "headers": {},
            "body": body, **changes}


def wire_response(body: Any, *, status: int = 200) -> httpx.Response:
    return httpx.Response(status, headers={"content-type": MEDIA_TYPE.upper() + "; charset=utf-8"},
                          json=body, request=httpx.Request("POST", "http://fixture/api/worker/workflow-tasks/poll"))


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["workflow", "activity", "query", "update-validation"])
@pytest.mark.parametrize("negotiated", [False, True])
async def test_poll_negotiation_accepts_legacy_and_progress(kind: str, negotiated: bool) -> None:
    async def serve(request: httpx.Request) -> httpx.Response:
        assert request.headers["accept"] == MEDIA_TYPE + ", application/json;q=0.9"
        data = {"task": None, "poll_status": "empty"}
        if negotiated:
            return httpx.Response(200, headers={"content-type": MEDIA_TYPE},
                                  content=b"\n\n" + json.dumps(envelope(200, data)).encode())
        return httpx.Response(200, json=data)

    async with Client("http://fixture") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://fixture", transport=httpx.MockTransport(serve))
        poll = getattr(client, f"poll_{kind.replace('-', '_')}_task")
        assert await poll(worker_id="fixture-worker", task_queue="fixture-queue", timeout=0) is None


@pytest.mark.asyncio
async def test_non_poll_request_keeps_ordinary_response() -> None:
    async def serve(request: httpx.Request) -> httpx.Response:
        assert MEDIA_TYPE not in request.headers.get("accept", "")
        return httpx.Response(200, json={"healthy": True})

    async with Client("http://fixture") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://fixture", transport=httpx.MockTransport(serve))
        assert await client.health() == {"healthy": True}


def test_unwrap_preserves_task_values_and_original_headers() -> None:
    body = {"task": {"counter": 9007199254740993, "value": 1.0, "payload": {"codec": "avro", "data": "AA=="}}}
    response = unwrap_response(wire_response(envelope(200, body, headers={
        "x-durable-workflow-protocol-version": "1.20", "retry-after": "2",
    })))
    assert response.json() == body
    assert response.headers["x-durable-workflow-protocol-version"] == "1.20"
    assert response.headers["retry-after"] == "2"
    assert len(response.content) == int(response.headers["content-length"])


@pytest.mark.parametrize("change", [
    {"schema": "unknown"}, {"status": True}, {"status": 199}, {"status": 600},
    {"body": []}, {"headers": []}, {"headers": {"retry-after": 2}},
    {"headers": {"authorization": "not-allowed"}}, {"headers": {"retry-after": "2\r\nX-Test: bad"}},
])
def test_invalid_transport_is_not_silently_treated_as_an_empty_poll(change: dict[str, Any]) -> None:
    with pytest.raises(httpx.RemoteProtocolError, match="Invalid worker poll transport"):
        unwrap_response(wire_response(envelope(200, {"task": None}) | change))


def test_outer_http_error_cannot_be_rewritten_as_success() -> None:
    with pytest.raises(httpx.RemoteProtocolError):
        unwrap_response(wire_response(envelope(200, {"task": None}), status=503))


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [409, 422, 429, 503])
async def test_late_errors_retain_typed_error_and_retry_semantics(status: int) -> None:
    attempts = []
    retry_policy = TransportRetryPolicy(max_attempts=2, initial_backoff_seconds=0, jitter=False)

    async def serve(request: httpx.Request) -> httpx.Response:
        attempts.append(json.loads(request.content)["poll_request_id"])
        reason = "long_poll_capacity_exhausted" if status == 429 else "fixture_late_failure"
        body = {"error": "fixture failure", "reason": reason, "retryable": True}
        if status == 429:
            body.update({"task": None, "poll_status": reason, "task_kind": "workflow_task",
                         "task_queue": "fixture-queue", "retry_after_seconds": 2})
        return httpx.Response(200, headers={"content-type": MEDIA_TYPE}, json=envelope(
            status, body,
            headers={"retry-after": "2"},
        ))

    async with Client("http://fixture", retry_policy=retry_policy) as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://fixture", transport=httpx.MockTransport(serve))
        with pytest.raises(InvalidArgument if status == 422 else ServerError) as raised:
            await client.poll_workflow_task(worker_id="fixture-worker", task_queue="fixture-queue", timeout=0)
    if isinstance(raised.value, ServerError):
        assert raised.value.status == status
        assert raised.value.reason() == ("long_poll_capacity_exhausted" if status == 429 else "fixture_late_failure")
    assert len(attempts) == (2 if status == 503 else 1)
    assert len(set(attempts)) == 1


@asynccontextmanager
async def progressing_peer(
    *, complete_after: float, requests: list[dict[str, Any]] | None = None,
) -> AsyncIterator[tuple[str, list[asyncio.Event]]]:
    closed = asyncio.Event()
    started = asyncio.Event()
    handlers: set[asyncio.Task[None]] = set()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            for header in headers.split(b"\r\n"):
                if header.lower().startswith(b"content-length:"):
                    body = await reader.readexactly(int(header.split(b":", 1)[1]))
                    if requests is not None:
                        requests.append(json.loads(body))
            writer.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n"
                         + f"Content-Type: {MEDIA_TYPE}\r\n\r\n".encode())
            started.set()
            finish = time.monotonic() + complete_after
            while time.monotonic() < finish:
                writer.write(b"1\r\n\n\r\n")
                await writer.drain()
                if reader.at_eof():
                    return
                await asyncio.sleep(0.01)
            if not reader.at_eof():
                data = json.dumps(envelope(200, {"task": None, "poll_status": "empty"})).encode()
                writer.write(f"{len(data):x}\r\n".encode() + data + b"\r\n0\r\n\r\n")
                await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            with suppress(ConnectionError):
                await writer.wait_closed()
            handlers.discard(task)
            closed.set()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    try:
        assert server.sockets
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}", [started, closed]
    finally:
        server.close()
        await server.wait_closed()
        for task in list(handlers):
            task.cancel()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["workflow", "activity", "query", "update-validation"])
async def test_progress_cannot_renew_the_absolute_attempt_deadline(
    kind: str, caplog: pytest.LogCaptureFixture,
) -> None:
    async with (
        progressing_peer(complete_after=0.6) as (endpoint, events),
        Client(endpoint, retry_policy=TransportRetryPolicy(max_attempts=1)) as client,
    ):
        started = time.monotonic()
        with caplog.at_level(logging.WARNING), pytest.raises(httpx.ReadTimeout, match="absolute HTTP attempt"):
            await asyncio.wait_for(client._request(
                "POST", f"/worker/{kind}-tasks/poll", worker=True,
                json={"timeout_seconds": 30}, timeout=0.12,
            ), timeout=2)
        assert time.monotonic() - started < 0.5
        await asyncio.wait_for(events[1].wait(), timeout=1)
        assert "configured wait of 30.000s" in caplog.text
        assert "ended after" in caplog.text


@pytest.mark.asyncio
async def test_public_poll_keeps_its_retry_budget_and_logical_identity() -> None:
    requests: list[dict[str, Any]] = []
    async with (
        progressing_peer(complete_after=1.2, requests=requests) as (endpoint, _events),
        Client(endpoint, retry_policy=TransportRetryPolicy(max_attempts=1)) as client,
    ):
        started = time.monotonic()
        result = await client.poll_workflow_task_response(
            worker_id="fixture-worker", task_queue="fixture-queue", timeout=0,
        )
        assert result == {"task": None, "poll_status": "timeout"}
        assert 1.8 < time.monotonic() - started < 3
        assert len(requests) == 2
        assert len({request["poll_request_id"] for request in requests}) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [502, 504])
async def test_gateway_cutoff_warns_with_observed_and_configured_wait(
    status: int, caplog: pytest.LogCaptureFixture,
) -> None:
    async def serve(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="fixture gateway error")

    async with Client("http://fixture", retry_policy=TransportRetryPolicy(max_attempts=1)) as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(base_url="http://fixture", transport=httpx.MockTransport(serve))
        with caplog.at_level(logging.WARNING), pytest.raises(ServerError) as raised:
            await client.poll_workflow_task(worker_id="fixture-worker", task_queue="fixture-queue", timeout=30)
    assert raised.value.status == status
    assert "configured wait of 30.000s" in caplog.text
    assert "ended after" in caplog.text


@pytest.mark.asyncio
async def test_progress_completes_inside_the_attempt_deadline() -> None:
    async with (
        progressing_peer(complete_after=0.12) as (endpoint, _events),
        Client(endpoint, retry_policy=TransportRetryPolicy(max_attempts=1)) as client,
    ):
        result = await client._request("POST", "/worker/workflow-tasks/poll", worker=True,
                                     json={"timeout_seconds": 1}, timeout=0.5)
        assert result == {"task": None, "poll_status": "empty"}


@pytest.mark.asyncio
async def test_cancellation_closes_a_progressing_request_without_a_cutoff_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async with (
        progressing_peer(complete_after=1) as (endpoint, events),
        Client(endpoint, retry_policy=TransportRetryPolicy(max_attempts=1)) as client,
    ):
        request = asyncio.create_task(client._request("POST", "/worker/workflow-tasks/poll", worker=True,
                                                     json={"timeout_seconds": 30}, timeout=5))
        await asyncio.wait_for(events[0].wait(), timeout=1)
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        await asyncio.wait_for(events[1].wait(), timeout=1)
        assert "ended after" not in caplog.text
