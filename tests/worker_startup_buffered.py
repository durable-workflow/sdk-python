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
        self.received_bytes = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        view = memoryview(buffer).cast("B")
        if view.readonly:
            raise ValueError("Protocol read buffer must be writable")
        value = self._budget.read_some(len(view), self._raw.read)
        view[:len(value)] = value
        self.received_bytes += len(value)
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
        self._reserved_raw = _ReservedRaw(raw, budget)
        self._buffer = io.BufferedReader(self._reserved_raw, buffer_size=8192)
        self._returned_bytes = 0

    @property
    def unread_bytes(self) -> int:
        return self._reserved_raw.received_bytes - self._returned_bytes

    def read(self, count: int) -> bytes:
        self._budget.check_request(count)
        value = self._buffer.read(count)
        self._budget.check_deadline()
        if len(value) != count:
            self._budget._refuse("RAW_READ_INCOMPLETE", "truncated")
        self._returned_bytes += len(value)
        return value

    def close(self) -> None:
        self._buffer.close()


class ProtocolFileBinding:
    """Proposed mixin before the pinned Connection in the future owner's MRO.

    Initialize this binding before the deferred driver constructor. Its property
    intercepts the driver's two fresh makefile assignments before any read.
    No driver is imported or constructed by this source module.
    """

    def __init__(self, budget: ReceiveBudget) -> None:
        self._protocol_budget = budget
        self._protocol_reader: ProtocolReader | None = None

    @property
    def _rfile(self) -> ProtocolReader | None:
        return getattr(self, "_protocol_reader", None)

    @_rfile.setter
    def _rfile(self, value: io.BufferedReader | None) -> None:
        previous = self._rfile
        if value is None:
            if previous is not None:
                previous.close()
            self._protocol_reader = None
            return
        if type(value) is not io.BufferedReader:
            self._protocol_budget._refuse("READ_STREAM_INVALID", "invalid")
        raw = None
        reader = None
        try:
            self._protocol_budget.check_deadline()
            if previous is not None and previous.unread_bytes:
                self._protocol_budget._refuse("STREAM_REPLACEMENT_UNREAD", "invalid")
            raw = value.detach()
            reader = ProtocolReader(raw, self._protocol_budget)
            if previous is not None:
                previous.close()
            self._protocol_reader = reader
        except BaseException:
            if reader is not None:
                reader.close()
            elif raw is not None:
                raw.close()
            else:
                value.close()
            raise
