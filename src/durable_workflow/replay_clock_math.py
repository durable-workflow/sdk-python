"""Exact canonical-v1 milliseconds for the unreleased business replay clock."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def canonical_milliseconds(time: datetime) -> int | None:
    if time.utcoffset() is None:
        return None
    delta = time - _EPOCH
    micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    magnitude = (abs(micros) + 500) // 1000
    return -magnitude if micros < 0 else magnitude


def from_canonical_milliseconds(value: int) -> datetime | None:
    try:
        return _EPOCH + timedelta(milliseconds=value)
    except OverflowError:
        return None
