"""Pure receive-budget wrapper; it cannot interrupt a blocked driver callback.

The proposed driver adapter must route every header/body/setup read through one
instance before connecting. A process owner and confirmed cleanup remain absent.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Literal, NoReturn

State = Literal["missing", "truncated", "invalid"]


class ReadRefusal(ValueError):
    def __init__(self, code: str, state: State) -> None:
        super().__init__(code)
        self.code = code
        self.state = state


class ReceiveBudget:
    REQUEST_LIMIT = 32_768
    PHASE_LIMIT = 65_536

    def __init__(
        self, read_bytes: Callable[[int], bytes], deadline_ns: int,
        *, clock: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if type(deadline_ns) is not int or not 0 <= deadline_ns <= 2**63 - 1:
            raise ReadRefusal("DEADLINE_INVALID", "invalid")
        self._read_bytes = read_bytes
        self._clock = clock
        self._deadline_ns = deadline_ns
        self.reserved_bytes = 0
        self.started = False
        self.refusal: tuple[str, State] | None = None
        self._last_sample = 0

    def _refuse(self, code: str, state: State) -> NoReturn:
        self.refusal = (code, state)
        raise ReadRefusal(code, state) from None

    def _check_time(self) -> None:
        try:
            sample = self._clock()
        except Exception:
            self._refuse("MONOTONIC_SAMPLE_INVALID", "invalid")
        if type(sample) is not int or not self._last_sample <= sample <= 2**63 - 1:
            self._refuse("MONOTONIC_SAMPLE_INVALID", "invalid")
        self._last_sample = sample
        if sample >= self._deadline_ns:
            self._refuse("TIME_BUDGET_EXPIRED", "truncated" if self.started else "missing")

    def _validate_request(self, count: int) -> None:
        if self.refusal is not None:
            raise ReadRefusal("READ_ALREADY_REFUSED", self.refusal[1])
        if type(count) is not int or count < 0:
            self._refuse("READ_COUNT_INVALID", "invalid")
        if count > self.REQUEST_LIMIT:
            self._refuse("RECEIVE_LIMIT_EXCEEDED", "truncated")

    def check_request(self, count: int) -> None:
        """Bound a logical request without charging already buffered bytes again."""
        self._validate_request(count)
        self._check_time()

    def check_deadline(self) -> None:
        self._validate_request(0)
        self._check_time()

    def read(self, count: int) -> bytes:
        return self._read(count, self._read_bytes, exact=True)

    def read_some(self, count: int, read_bytes: Callable[[int], bytes]) -> bytes:
        """Charge a raw-stream reservation; short returns never refund it."""
        return self._read(count, read_bytes, exact=False)

    def _read(self, count: int, read_bytes: Callable[[int], bytes], *, exact: bool) -> bytes:
        self._validate_request(count)
        if self.reserved_bytes + count > self.PHASE_LIMIT:
            self._refuse("RECEIVE_LIMIT_EXCEEDED", "truncated")
        self._check_time()
        self.reserved_bytes += count
        self.started = True
        try:
            value = read_bytes(count)
        except Exception:
            self._refuse("RAW_READ_FAILED", "truncated")
        self._check_time()
        if type(value) is not bytes or len(value) > count or (exact and len(value) != count):
            self._refuse("RAW_READ_INCOMPLETE", "truncated")
        return value
