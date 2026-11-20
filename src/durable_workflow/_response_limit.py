"""Bound the decoded size of every HTTP response body.

httpx decodes ``Content-Encoding: gzip`` transparently, so a small compressed
answer could expand without limit in memory (a 1.5 MB gzip body can inflate to
gigabytes). Each response stream is wrapped before httpx reads it: the wrapper
counts the bytes the body decodes to, inflating at most 1 MiB at a time, and
raises :class:`~durable_workflow.errors.ResponseBodyTooLarge` before httpx
decodes a chunk that would cross the limit.
"""

from __future__ import annotations

import zlib
from collections.abc import AsyncIterator, Awaitable, Callable

import httpx

from .errors import ResponseBodyTooLarge

#: Default bound on a decoded response body: 128 MiB.
DEFAULT_MAX_RESPONSE_BODY_BYTES = 128 * 1024 * 1024
#: Encodings this client asks for; httpx is not offered others.
ACCEPT_ENCODING = "gzip"
_INFLATE_STEP = 1 << 20


class _BoundedBody(httpx.AsyncByteStream):
    def __init__(self, inner: httpx.AsyncByteStream, *, gzip: bool, limit: int, url: str) -> None:
        self._inner = inner
        self._gzip = gzip
        self._limit = limit
        self._url = url

    async def __aiter__(self) -> AsyncIterator[bytes]:
        inflater = zlib.decompressobj(16 + zlib.MAX_WBITS) if self._gzip else None
        decoded = 0
        async for chunk in self._inner:
            if inflater is None:
                decoded += len(chunk)
            else:
                pending = chunk
                while pending and not inflater.eof:
                    try:
                        decoded += len(inflater.decompress(pending, _INFLATE_STEP))
                    except zlib.error:
                        # Not ours to judge: httpx reports the invalid encoding.
                        inflater = None
                        break
                    if decoded > self._limit:
                        break
                    pending = inflater.unconsumed_tail
            if decoded > self._limit:
                raise ResponseBodyTooLarge(self._limit, self._url)
            yield chunk

    async def aclose(self) -> None:
        await self._inner.aclose()


def response_body_limit(limit: int) -> Callable[[httpx.Response], Awaitable[None]]:
    """An httpx response hook that bounds the decoded body at ``limit`` bytes."""

    async def bound(response: httpx.Response) -> None:
        encoding = response.headers.get("content-encoding", "").strip().lower()
        if encoding not in ("", "identity", "gzip"):
            raise httpx.DecodingError(
                f"unrequested response content-encoding {encoding!r}", request=response.request,
            )
        if isinstance(response.stream, httpx.AsyncByteStream):
            response.stream = _BoundedBody(
                response.stream, gzip=encoding == "gzip", limit=limit, url=str(response.request.url.path),
            )

    return bound
