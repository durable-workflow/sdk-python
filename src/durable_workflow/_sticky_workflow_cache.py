"""Bounded snapshots of durable wire history, never live workflow instances."""

from __future__ import annotations

import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

CacheKey = tuple[str, str, str]


def complete_history(history: list[dict[str, Any]]) -> bool:
    if not history:
        return False
    types = [event.get("event_type") for event in history[:2]]
    if types[0] != "WorkflowStarted" and types != ["StartAccepted", "WorkflowStarted"]:
        return False
    return all(type(event.get("sequence")) is int and event["sequence"] == index
               for index, event in enumerate(history, 1))


@dataclass(frozen=True)
class _Entry:
    encoded: bytes
    expires_at: float
    resume_token: str | None
    resume_offset: int


class StickyWorkflowCache:
    def __init__(self, capacity: int, max_bytes: int, ttl_seconds: int) -> None:
        for name, value, minimum in (("capacity", capacity, 0), ("max_bytes", max_bytes, 1),
                                     ("ttl_seconds", ttl_seconds, 1)):
            if type(value) is not int or value < minimum:
                raise ValueError(f"sticky_cache_{name} must be an integer of at least {minimum}")
        if ttl_seconds > 3600:
            raise ValueError("sticky_cache_ttl_seconds must be at most 3600")
        self.capacity = capacity
        self.max_bytes = max_bytes
        self.ttl_seconds = ttl_seconds
        self._entries: OrderedDict[CacheKey, _Entry] = OrderedDict()
        self._bytes = 0
        self._metrics = {"hit": 0, "miss": 0, "eviction": 0, "forced_cold_replay": 0}

    @property
    def enabled(self) -> bool:
        return self.capacity > 0

    def _expire(self) -> None:
        now = time.monotonic()
        for key, entry in list(self._entries.items()):
            if entry.expires_at <= now:
                self.discard(key)

    def discard(self, key: CacheKey) -> None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._bytes -= len(entry.encoded)

    def lookup(self, key: CacheKey) -> tuple[list[dict[str, Any]], str | None, int] | None:
        self._expire()
        entry = self._entries.get(key)
        if entry is None:
            return None
        self._entries.move_to_end(key)
        # Decoding isolates the retained snapshot from replay and handler mutation.
        history: list[dict[str, Any]] = json.loads(entry.encoded)
        return history, entry.resume_token, entry.resume_offset

    def remember(self, key: CacheKey, history: list[dict[str, Any]], *,
                 resume_token: str | None = None, resume_offset: int = 0) -> bool:
        self._expire()
        self.discard(key)
        if not self.enabled or not complete_history(history):
            return False
        encoded = json.dumps(history, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(encoded) > self.max_bytes:
            return False
        while self._entries and (len(self._entries) >= self.capacity or self._bytes + len(encoded) > self.max_bytes):
            oldest = next(iter(self._entries))
            self.discard(oldest)
            self._metrics["eviction"] += 1
        self._entries[key] = _Entry(encoded, time.monotonic() + self.ttl_seconds, resume_token, resume_offset)
        self._bytes += len(encoded)
        return True

    def record_replay(self, *, hit: bool, forced: bool = False) -> None:
        self._metrics["hit" if hit else "miss"] += 1
        if forced:
            self._metrics["forced_cold_replay"] += 1

    def metrics(self) -> dict[str, int]:
        self._expire()
        return {**self._metrics, "entries": len(self._entries), "history_bytes": self._bytes}

    def clear(self) -> None:
        self._entries.clear()
        self._bytes = 0
