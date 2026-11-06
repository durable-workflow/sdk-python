from __future__ import annotations

import io

import pytest

from tests.worker_startup_buffered import ProtocolReader
from tests.worker_startup_read_budget import ReadRefusal, ReceiveBudget


class RecordingRaw(io.RawIOBase):
    def __init__(self, data: bytes) -> None:
        super().__init__()
        self.data = data
        self.offset = 0
        self.requests: list[int] = []
        self.close_count = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        self.requests.append(len(buffer))
        data = self.data[self.offset:self.offset + len(buffer)]
        buffer[:len(data)] = data
        self.offset += len(data)
        return len(data)

    def close(self) -> None:
        if not self.closed:
            self.close_count += 1
        super().close()


def test_logical_callback_predecessor_does_not_account_for_stock_read_ahead() -> None:
    raw = RecordingRaw(b"x" * 8192)
    buffered = io.BufferedReader(raw, buffer_size=8192)
    budget = ReceiveBudget(buffered.read, 10, clock=lambda: 1)
    try:
        assert budget.read(1) == b"x"
        assert budget.reserved_bytes == 1
        assert raw.requests == [8192]
        assert raw.offset == 8192
    finally:
        buffered.close()


def test_successor_charges_read_ahead_before_raw_read_and_does_not_double_count_cache() -> None:
    raw = RecordingRaw(b"x" * 8192)
    budget = ReceiveBudget(lambda _: b"", 10, clock=lambda: 1)
    reader = ProtocolReader(raw, budget)
    try:
        assert raw.requests == []
        assert reader.read(1) == b"x"
        assert budget.reserved_bytes == 8192
        assert raw.requests == [8192]
        assert reader.read(100) == b"x" * 100
        assert budget.reserved_bytes == 8192
        assert raw.requests == [8192]
    finally:
        reader.close()


def test_logical_oversize_is_rejected_before_buffered_allocation_or_raw_read() -> None:
    raw = RecordingRaw(b"x" * 65536)
    budget = ReceiveBudget(lambda _: b"", 10, clock=lambda: 1)
    reader = ProtocolReader(raw, budget)
    try:
        with pytest.raises(ReadRefusal, match="RECEIVE_LIMIT_EXCEEDED"):
            reader.read(32769)
        assert raw.requests == []
        assert budget.reserved_bytes == 0
    finally:
        reader.close()


def test_fresh_protocol_streams_share_the_same_phase_reservation() -> None:
    budget = ReceiveBudget(lambda _: b"", 10, clock=lambda: 1)
    first_raw = RecordingRaw(b"a" * 32768)
    second_raw = RecordingRaw(b"b" * 32768)
    extra_raw = RecordingRaw(b"c" * 8192)
    readers = [ProtocolReader(raw, budget) for raw in (first_raw, second_raw, extra_raw)]
    try:
        assert readers[0].read(32768) == b"a" * 32768
        assert readers[1].read(32768) == b"b" * 32768
        assert budget.reserved_bytes == 65536
        with pytest.raises(ReadRefusal, match="RECEIVE_LIMIT_EXCEEDED"):
            readers[2].read(1)
        assert extra_raw.requests == []
    finally:
        for reader in readers:
            reader.close()


def test_expiry_prevents_returning_cached_protocol_bytes() -> None:
    now = [1]
    raw = RecordingRaw(b"x" * 8192)
    budget = ReceiveBudget(lambda _: b"", 10, clock=lambda: now[0])
    reader = ProtocolReader(raw, budget)
    try:
        assert reader.read(1) == b"x"
        now[0] = 10
        with pytest.raises(ReadRefusal, match="TIME_BUDGET_EXPIRED"):
            reader.read(1)
        assert raw.requests == [8192]
        assert budget.reserved_bytes == 8192
    finally:
        reader.close()


def test_short_eof_refuses_and_cannot_be_retried_or_refunded() -> None:
    raw = RecordingRaw(b"short")
    budget = ReceiveBudget(lambda _: b"", 10, clock=lambda: 1)
    reader = ProtocolReader(raw, budget)
    try:
        with pytest.raises(ReadRefusal, match="RAW_READ_INCOMPLETE"):
            reader.read(6)
        before = list(raw.requests)
        reserved = budget.reserved_bytes
        with pytest.raises(ReadRefusal, match="READ_ALREADY_REFUSED"):
            reader.read(1)
        assert raw.requests == before
        assert budget.reserved_bytes == reserved
    finally:
        reader.close()


def test_close_is_idempotent_and_does_not_drain_protocol_data() -> None:
    raw = RecordingRaw(b"unconsumed")
    reader = ProtocolReader(raw, ReceiveBudget(lambda _: b"", 10, clock=lambda: 1))
    reader.close()
    reader.close()
    assert raw.close_count == 1
    assert raw.requests == []


def test_an_existing_buffered_reader_is_not_admitted_as_a_raw_source() -> None:
    raw = RecordingRaw(b"x")
    buffered = io.BufferedReader(raw)
    try:
        with pytest.raises(TypeError, match="unbuffered raw stream"):
            ProtocolReader(buffered, ReceiveBudget(lambda _: b"", 10, clock=lambda: 1))
        assert raw.requests == []
    finally:
        buffered.close()
