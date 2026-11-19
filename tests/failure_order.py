"""Helpers for the run's parallel failure-order marker (sdk-python#307)."""

from __future__ import annotations

from typing import Any

from durable_workflow.workflow import PARALLEL_FAILURE_ORDER_MARKER, RecordVersionMarker

FAILURE_ORDER_MARKER = RecordVersionMarker(
    change_id=PARALLEL_FAILURE_ORDER_MARKER, version=1, min_supported=1, max_supported=1,
)


def failure_order_marker_event(sequence: int = 1, **extra: Any) -> dict[str, Any]:
    return {"event_type": "VersionMarkerRecorded", **extra, "payload": {
        "sequence": sequence, "change_id": PARALLEL_FAILURE_ORDER_MARKER, "version": 1,
        "min_supported": 1, "max_supported": 1,
    }}
