"""Proposed protocol-reader primitive using stock buffered I/O and inert inputs.

The raw input must be supplied before any protocol read. Actual pinned-driver
initial/post-TLS binding, TLS/auth inputs and process cleanup remain unqualified.
"""

from __future__ import annotations

import io
from typing import Any

from tests.worker_startup_read_budget import ReceiveBudget


class _ReservedRaw(io.RawIOBase):
    def __init__(self, raw: io.RawIOBase, budget: ReceiveBudget) -> None:
        super().__init__()
        self._raw = raw
        self._budget = budget

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        view = memoryview(buffer).cast("B")
        if view.readonly:
            raise ValueError("Protocol read buffer must be writable")
        value = self._budget.read_some(len(view), self._raw.read)
        view[:len(value)] = value
        return len(value)

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._raw.close()
        finally:
            super().close()


class ProtocolReader:
    """Charge all raw protocol read-ahead once, with an outer allocation bound."""

    def __init__(self, raw: io.RawIOBase, budget: ReceiveBudget) -> None:
        if not isinstance(raw, io.RawIOBase):
            raise TypeError("Protocol source must be an unbuffered raw stream")
        self._budget = budget
        self._buffer = io.BufferedReader(_ReservedRaw(raw, budget), buffer_size=8192)

    def read(self, count: int) -> bytes:
        self._budget.check_request(count)
        value = self._buffer.read(count)
        self._budget.check_deadline()
        if len(value) != count:
            self._budget._refuse("RAW_READ_INCOMPLETE", "truncated")
        return value

    def close(self) -> None:
        self._buffer.close()
