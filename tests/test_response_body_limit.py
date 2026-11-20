"""Decoded response bodies are bounded, so a small gzip answer cannot expand without limit."""
from __future__ import annotations

import gzip
import json
from collections.abc import AsyncIterator

import httpx
import pytest

from durable_workflow import Client
from durable_workflow.errors import ResponseBodyTooLarge
from durable_workflow.retry_policy import TransportRetryPolicy

LIMIT = 1024 * 1024


class Chunks(httpx.AsyncByteStream):
    def __init__(self, data: bytes, size: int = 16 * 1024) -> None:
        self.data, self.size, self.sent, self.closed = data, size, 0, False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for start in range(0, len(self.data), self.size):
            self.sent += 1
            yield self.data[start:start + self.size]

    async def aclose(self) -> None:
        self.closed = True


def client_answering(body: bytes, *, encoding: str | None, limit: int = LIMIT) -> tuple[Client, list[Chunks]]:
    streams: list[Chunks] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        assert request.headers["accept-encoding"] == "gzip"
        stream = Chunks(body)
        streams.append(stream)
        headers = {"content-type": "application/json", **({"content-encoding": encoding} if encoding else {})}
        return httpx.Response(200, headers=headers, stream=stream)

    client = Client("http://limit.invalid", token="t", max_response_body_bytes=limit,
                    retry_policy=TransportRetryPolicy(max_attempts=3))
    # Keep the client's own headers and response hook; replace only the network.
    default = client._http
    client._http = httpx.AsyncClient(base_url="http://limit.invalid", transport=httpx.MockTransport(respond),
                                     headers=default.headers, event_hooks=default.event_hooks)
    return client, streams


def json_bytes(size: int) -> bytes:
    return json.dumps({"pad": "0" * size}).encode()


async def test_a_gzip_bomb_is_refused_before_it_is_inflated() -> None:
    bomb = gzip.compress(json_bytes(64 * LIMIT))  # 64 MiB of JSON in about 64 KiB of gzip
    assert len(bomb) < 128 * 1024
    client, streams = client_answering(bomb, encoding="gzip")
    async with client:
        with pytest.raises(ResponseBodyTooLarge) as refusal:
            await client._request("GET", "/workflows/w")
    assert refusal.value.limit == LIMIT
    assert len(streams) == 1, "a body over the limit is not retried"
    assert streams[0].sent < 4, "refused within the first chunks, not after inflating everything"
    assert streams[0].closed


async def test_an_identity_body_over_the_limit_is_refused() -> None:
    client, _ = client_answering(json_bytes(LIMIT + 1), encoding=None)
    async with client:
        with pytest.raises(ResponseBodyTooLarge):
            await client._request("GET", "/workflows/w")


@pytest.mark.parametrize("encoding", ["gzip", None])
async def test_bodies_within_the_limit_decode_normally(encoding: str | None) -> None:
    body = json_bytes(LIMIT // 2)
    client, _ = client_answering(gzip.compress(body) if encoding else body, encoding=encoding)
    async with client:
        assert await client._request("GET", "/workflows/w") == json.loads(body)


async def test_an_unrequested_encoding_is_refused() -> None:
    client, _ = client_answering(b"{}", encoding="br")
    async with client:
        with pytest.raises(httpx.DecodingError, match="unrequested"):
            await client._request("GET", "/workflows/w")


def test_the_default_bound_is_128_mib_and_validated() -> None:
    assert Client("http://limit.invalid").max_response_body_bytes == 128 * 1024 * 1024
    for invalid in (0, -1, 1.5, True):
        with pytest.raises(ValueError, match="max_response_body_bytes"):
            Client("http://limit.invalid", max_response_body_bytes=invalid)  # type: ignore[arg-type]
