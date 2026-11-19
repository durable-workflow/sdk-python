"""Helpers for the run's parallel failure-order marker (sdk-python#307).

A run records ``PARALLEL_FAILURE_ORDER_MARKER`` directly before the first
all-group that can fail, when that group is first scheduled. Tests about other
behavior that build a history from a fresh run's commands drop it, which
replays as a run an earlier worker started.
"""

from __future__ import annotations

from typing import Any

from durable_workflow.workflow import PARALLEL_FAILURE_ORDER_MARKER, RecordVersionMarker

FAILURE_ORDER_MARKER = RecordVersionMarker(
    change_id=PARALLEL_FAILURE_ORDER_MARKER, version=1, min_supported=1, max_supported=1,
)


def is_failure_order_marker(command: Any) -> bool:
    if isinstance(command, RecordVersionMarker):
        return command.change_id == PARALLEL_FAILURE_ORDER_MARKER
    return (isinstance(command, dict) and command.get("type") == "record_version_marker"
            and command.get("change_id") == PARALLEL_FAILURE_ORDER_MARKER)


def without_failure_order_marker(commands: list[Any]) -> list[Any]:
    return [command for command in commands if not is_failure_order_marker(command)]


def failure_order_marker_event(sequence: int = 1, **extra: Any) -> dict[str, Any]:
    return {"event_type": "VersionMarkerRecorded", **extra, "payload": {
        "sequence": sequence, "change_id": PARALLEL_FAILURE_ORDER_MARKER, "version": 1,
        "min_supported": 1, "max_supported": 1,
    }}
