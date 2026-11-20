"""Worker answers without repeated ``server_capabilities`` (server#1262).

A worker request sends the digest of the capabilities this client holds, or
``none``. A Server that supports the digest answers with each copy of its
current capabilities either tagged with a ``digest`` member, which is kept for
later requests, or replaced by the digest string, which is restored here before
any caller sees the answer. Older Servers ignore the header and answer in full,
so callers always observe complete capabilities.

The Server only replaces copies with the digest that request sent, so an answer
is resolved against the pair captured when its request was sent, not the pair
held when it arrives. Overlapping requests across a change of capabilities (a
rolling deploy) each restore their own object. Contract:
``docs/contracts/server-capabilities-digest.md`` in the Server repository.
"""

from __future__ import annotations

import copy
from typing import Any

from .errors import ServerCapabilitiesDigestError

HEADER = "X-Durable-Workflow-Server-Capabilities-Digest"
_KEY = "server_capabilities"
_PREFIX = "sha256:"
# The Server elides copies at the top level and up to two objects deep.
_MAX_DEPTH = 3

Captured = tuple[str | None, dict[str, Any] | None]


class ServerCapabilitiesDigest:
    """The newest (digest, capabilities) pair this client has learned."""

    def __init__(self) -> None:
        self._digest: str | None = None
        self._capabilities: dict[str, Any] | None = None

    def capture(self) -> Captured:
        """The pair a request is sent with; pass it to :meth:`restore` for its answer."""
        return self._digest, self._capabilities

    @staticmethod
    def header(captured: Captured) -> str:
        return captured[0] if captured[0] is not None and captured[1] is not None else "none"

    def forget(self) -> None:
        """Drop the held pair, so the next request sends ``none`` and gets full capabilities."""
        self._digest = None
        self._capabilities = None

    def restore(self, answer: Any, captured: Captured, *, drop_unknown: bool = False) -> bool:
        """Strip and learn tagged copies, then restore elided ones, in place.

        An elided copy naming a digest neither captured for the request nor
        tagged in this answer raises :class:`ServerCapabilitiesDigestError`;
        with ``drop_unknown`` it is removed instead and ``True`` is returned.
        """
        if not isinstance(answer, dict):
            return False
        learned: dict[str, dict[str, Any]] = {}
        self._learn(answer, learned, 1)
        return self._resolve(answer, captured, learned, 1, drop_unknown)

    def _learn(self, answer: dict[str, Any], learned: dict[str, dict[str, Any]], depth: int) -> None:
        for key, value in answer.items():
            if key == _KEY:
                if isinstance(value, dict) and isinstance(value.get("digest"), str) \
                        and value["digest"].startswith(_PREFIX):
                    digest = value.pop("digest")
                    learned[digest] = value
                    # Later requests send the newest digest this client has seen.
                    # Callers own the answer, so the held pair is a private copy.
                    self._digest, self._capabilities = digest, copy.deepcopy(value)
            elif isinstance(value, dict) and depth < _MAX_DEPTH:
                self._learn(value, learned, depth + 1)

    def _resolve(self, answer: dict[str, Any], captured: Captured, learned: dict[str, dict[str, Any]],
                 depth: int, drop_unknown: bool) -> bool:
        dropped = False
        for key, value in list(answer.items()):
            if key == _KEY:
                if isinstance(value, str) and value.startswith(_PREFIX):
                    if value == captured[0] and captured[1] is not None:
                        answer[key] = copy.deepcopy(captured[1])
                    elif value in learned:
                        answer[key] = copy.deepcopy(learned[value])
                    elif drop_unknown:
                        del answer[key]
                        dropped = True
                    else:
                        raise ServerCapabilitiesDigestError(
                            "the Server elided server_capabilities this worker does not hold "
                            f"({value}; sent {self.header(captured)})"
                        )
            elif isinstance(value, dict) and depth < _MAX_DEPTH:
                dropped = self._resolve(value, captured, learned, depth + 1, drop_unknown) or dropped
        return dropped
