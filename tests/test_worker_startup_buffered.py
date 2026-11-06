from __future__ import annotations

import io

import pytest

from tests.worker_startup_buffered import ProtocolFileBinding, ProtocolReader
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


def test_assignment_binding_detaches_the_fresh_buffer_before_its_first_read() -> None:
    raw = RecordingRaw(b"x" * 8192)
    original_buffer = io.BufferedReader(raw)
    budget = ReceiveBudget(lambda _: b"", 10, clock=lambda: 1)
    binding = ProtocolFileBinding(budget)
    try:
        binding._rfile = original_buffer
        assert raw.requests == []
        assert binding._rfile is not None
        assert binding._rfile.read(1) == b"x"
        assert raw.requests == [8192]
        assert budget.reserved_bytes == 8192
        assert binding._rfile.unread_bytes == 8191
        with pytest.raises(ValueError):
            original_buffer.read(1)
    finally:
        binding._rfile = None


def test_fresh_replacement_cannot_reset_the_phase_budget() -> None:
    budget = ReceiveBudget(lambda _: b"", 10, clock=lambda: 1)
    binding = ProtocolFileBinding(budget)
    first = RecordingRaw(b"a" * 32768)
    second = RecordingRaw(b"b" * 32768)
    extra = RecordingRaw(b"c" * 8192)
    try:
        binding._rfile = io.BufferedReader(first)
        assert binding._rfile is not None
        assert binding._rfile.read(32768) == b"a" * 32768
        binding._rfile = io.BufferedReader(second)
        assert first.closed
        assert binding._rfile is not None
        assert binding._rfile.read(32768) == b"b" * 32768
        binding._rfile = io.BufferedReader(extra)
        assert second.closed
        assert binding._rfile is not None
        with pytest.raises(ReadRefusal, match="RECEIVE_LIMIT_EXCEEDED"):
            binding._rfile.read(1)
        assert budget.reserved_bytes == 65536
        assert extra.requests == []
    finally:
        binding._rfile = None


def test_replacement_cannot_discard_prefetched_protocol_data() -> None:
    budget = ReceiveBudget(lambda _: b"", 10, clock=lambda: 1)
    binding = ProtocolFileBinding(budget)
    first = RecordingRaw(b"a" * 8192)
    extra = RecordingRaw(b"b" * 8192)
    try:
        binding._rfile = io.BufferedReader(first)
        reader = binding._rfile
        assert reader is not None
        assert reader.read(1) == b"a"
        with pytest.raises(ReadRefusal, match="STREAM_REPLACEMENT_UNREAD"):
            binding._rfile = io.BufferedReader(extra)
        assert binding._rfile is reader
        assert extra.closed
        assert extra.requests == []
        assert budget.reserved_bytes == 8192
        with pytest.raises(ReadRefusal, match="READ_ALREADY_REFUSED"):
            reader.read(1)
    finally:
        binding._rfile = None
    assert first.closed


def test_expired_assignment_closes_its_new_stream_without_reading() -> None:
    raw = RecordingRaw(b"unused")
    binding = ProtocolFileBinding(ReceiveBudget(lambda _: b"", 10, clock=lambda: 10))
    with pytest.raises(ReadRefusal, match="TIME_BUDGET_EXPIRED"):
        binding._rfile = io.BufferedReader(raw)
    assert raw.closed
    assert raw.requests == []
    assert binding._rfile is None


def test_none_assignment_closes_without_a_drain_or_remaining_read_budget() -> None:
    now = [1]
    raw = RecordingRaw(b"unused")
    binding = ProtocolFileBinding(ReceiveBudget(lambda _: b"", 10, clock=lambda: now[0]))
    binding._rfile = io.BufferedReader(raw)
    now[0] = 10
    binding._rfile = None
    binding._rfile = None
    assert raw.close_count == 1
    assert raw.requests == []
