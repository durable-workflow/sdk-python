from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest

from durable_workflow.client import Client
from durable_workflow.retry_policy import TransportRetryPolicy


@asynccontextmanager
async def response_server(*, delay: float | None) -> AsyncIterator[str]:
    """A real HTTP peer, with response progress controlled by the test."""
    writers: list[asyncio.StreamWriter] = []
    handlers: set[asyncio.Task[None]] = set()

    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        handlers.add(task)
        writers.append(writer)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            for header in headers.split(b"\r\n"):
                if header.lower().startswith(b"content-length:"):
                    await reader.readexactly(int(header.split(b":", 1)[1]))
            if delay is None:
                await reader.read()
                return
            await asyncio.sleep(delay)
            body = json.dumps({"task": None, "poll_status": "empty"}).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handlers.discard(task)

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    try:
        assert server.sockets
        yield f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    finally:
        server.close()
        await server.wait_closed()
        for writer in writers:
            writer.close()
        if handlers:
            await asyncio.gather(*handlers)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["control", "discovery", "worker"])
async def test_configured_timeout_bounds_a_stalled_response(operation: str) -> None:
    async with (
        response_server(delay=None) as endpoint,
        Client(endpoint, timeout=0.05, retry_policy=TransportRetryPolicy(max_attempts=1)) as client,
    ):
        if operation == "control":
            request = client.health()
        elif operation == "discovery":
            request = client.get_cluster_info()
        else:
            request = client.complete_workflow_task(
                task_id="synthetic-timeout-task",
                lease_owner="synthetic-timeout-worker",
                workflow_task_attempt=1,
                commands=[],
            )
        with pytest.raises(httpx.ReadTimeout):
            await asyncio.wait_for(request, timeout=2)


@pytest.mark.asyncio
async def test_explicit_poll_timeout_overrides_a_shorter_client_default() -> None:
    async with (
        response_server(delay=0.1) as endpoint,
        Client(endpoint, timeout=0.01, retry_policy=TransportRetryPolicy(max_attempts=1)) as client,
    ):
        task = await client.poll_workflow_task(
            worker_id="synthetic-timeout-worker",
            task_queue="synthetic-timeout-queue",
            timeout=0,
        )
        assert task is None
