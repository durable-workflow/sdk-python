from __future__ import annotations

from typing import Any

import pytest

from tests.worker_startup_read_budget import ReadRefusal, ReceiveBudget


def test_sequential_requests_share_one_reservation_budget() -> None:
    calls = []

    def read(count: int) -> bytes:
        calls.append(count)
        return b"x" * count

    budget = ReceiveBudget(read, 10, clock=lambda: 1)
    assert len(budget.read(32768)) == 32768
    assert len(budget.read(32768)) == 32768
    with pytest.raises(ReadRefusal, match="RECEIVE_LIMIT_EXCEEDED") as error:
        budget.read(1)
    assert error.value.state == "truncated"
    assert budget.reserved_bytes == 65536
    assert calls == [32768, 32768]


def test_oversized_request_is_refused_before_the_callback_or_allocation() -> None:
    calls = []
    budget = ReceiveBudget(lambda count: calls.append(count) or b"", 10, clock=lambda: 1)
    with pytest.raises(ReadRefusal, match="RECEIVE_LIMIT_EXCEEDED"):
        budget.read(32769)
    assert calls == []
    assert budget.reserved_bytes == 0


def test_failed_read_keeps_its_reservation_and_cannot_be_retried() -> None:
    calls = []

    def fail(count: int) -> bytes:
        calls.append(count)
        raise RuntimeError("unretained transport diagnostic")

    budget = ReceiveBudget(fail, 10, clock=lambda: 1)
    with pytest.raises(ReadRefusal, match="RAW_READ_FAILED") as error:
        budget.read(16)
    assert str(error.value) == "RAW_READ_FAILED"
    assert error.value.__cause__ is None
    with pytest.raises(ReadRefusal, match="READ_ALREADY_REFUSED"):
        budget.read(16)
    assert budget.refusal == ("RAW_READ_FAILED", "truncated")
    assert budget.reserved_bytes == 16
    assert calls == [16]


def test_expired_before_first_read_is_missing_and_issues_no_callback() -> None:
    calls = []
    budget = ReceiveBudget(lambda count: calls.append(count) or b"", 10, clock=lambda: 10)
    with pytest.raises(ReadRefusal, match="TIME_BUDGET_EXPIRED") as error:
        budget.read(4)
    assert error.value.state == "missing"
    assert budget.started is False
    assert calls == []


def test_fast_reads_cannot_reset_the_absolute_deadline() -> None:
    now = [0]

    def read(count: int) -> bytes:
        now[0] += 6
        return b"x" * count

    budget = ReceiveBudget(read, 10, clock=lambda: now[0])
    assert budget.read(4) == b"xxxx"
    with pytest.raises(ReadRefusal, match="TIME_BUDGET_EXPIRED") as error:
        budget.read(4)
    assert error.value.state == "truncated"
    assert budget.reserved_bytes == 8
    assert now == [12]


@pytest.mark.parametrize("count", [True, False, -1, 1.0, "1", None])
def test_invalid_read_count_does_not_reach_the_reader(count: Any) -> None:
    calls = []
    budget = ReceiveBudget(lambda value: calls.append(value) or b"", 10, clock=lambda: 1)
    with pytest.raises(ReadRefusal, match="READ_COUNT_INVALID"):
        budget.read(count)
    assert calls == []


@pytest.mark.parametrize("deadline", [True, -1, 1.0, None, 2**63])
def test_deadline_requires_a_bounded_monotonic_integer(deadline: Any) -> None:
    with pytest.raises(ReadRefusal, match="DEADLINE_INVALID"):
        ReceiveBudget(lambda count: b"x" * count, deadline)


@pytest.mark.parametrize("value", [b"", b"xx", bytearray(b"x"), memoryview(b"x"), "x", None])
def test_partial_or_changed_native_result_cannot_be_promoted(value: Any) -> None:
    budget = ReceiveBudget(lambda _: value, 10, clock=lambda: 1)
    with pytest.raises(ReadRefusal, match="RAW_READ_INCOMPLETE") as error:
        budget.read(1)
    assert error.value.state == "truncated"
    assert budget.reserved_bytes == 1


@pytest.mark.parametrize("sample", [True, -1, 1.0, None, 2**63])
def test_invalid_clock_sample_prevents_a_read(sample: Any) -> None:
    calls = []
    budget = ReceiveBudget(lambda count: calls.append(count) or b"", 10, clock=lambda: sample)
    with pytest.raises(ReadRefusal, match="MONOTONIC_SAMPLE_INVALID"):
        budget.read(1)
    assert calls == []


def test_clock_reversal_prevents_the_next_read() -> None:
    now = [5]
    calls = []
    budget = ReceiveBudget(lambda count: calls.append(count) or b"x" * count, 10, clock=lambda: now[0])
    assert budget.read(1) == b"x"
    now[0] = 4
    with pytest.raises(ReadRefusal, match="MONOTONIC_SAMPLE_INVALID"):
        budget.read(1)
    assert calls == [1]
