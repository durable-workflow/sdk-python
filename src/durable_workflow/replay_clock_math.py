"""Carbon 3.15.1's measured millisecond clock domain, without wall time."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def carbon_milliseconds(time: datetime) -> int | None:
    if time.utcoffset() is None:
        return None
    delta = time - _EPOCH
    micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    # Timestamp.php forms a float microsecond value. Outside its exact integer
    # range we have no proved quantization, so cannot supply a group clock.
    if abs(micros) > 1 << 53:
        return None
    value = micros / 1000.0
    integral = math.trunc(value)
    # PHP 8.4 round(..., 0, PHP_ROUND_HALF_UP): compare with the half edge
    # rather than Python's ties-to-even round or a floor/truncation substitute.
    if abs(value) >= abs(integral) + 0.5:
        integral += -1 if value < 0 else 1
    return integral


def carbon_from_milliseconds(value: int) -> datetime:
    return _EPOCH + timedelta(milliseconds=value)
