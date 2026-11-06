"""Closed activity-prefix evidence for Server 3d4d4afa's native redrive writer.

The producer is rust/src/runtime/redrive.rs, blob
004836fcd1b9e39255d3cad6a4b0eec01d14aff7. This profile validates the
retained StartAccepted row explicitly. It does not qualify Core timer reuse.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any


def _require(value: Any, reason: str) -> None:
    if not value:
        raise AssertionError(reason)


def _time(value: Any) -> datetime:
    _require(isinstance(value, str), "recorded instant")
    _require(re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
        r"(?:\.[0-9]{1,6})?(?:Z|\+00:00)", value,
    ), "native UTC timestamp shape")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _require(parsed.tzinfo is not None and parsed.utcoffset() is not None, "aware instant")
    return parsed


def check_native_redrive_successor(history: list[dict[str, Any]], workflow: Mapping[str, Any]) -> None:
    _require(len(history) >= 2, "native redrive prefix")
    for index, event in enumerate(history, start=1):
        _require(type(event.get("sequence")) is int and event["sequence"] == index, "native history order")
        _time(event.get("recorded_at"))
    accepted, started = history[:2]
    _require(accepted.get("event_type") == "StartAccepted", "native acceptance prefix")
    _require(started.get("event_type") == "WorkflowStarted", "native start boundary")
    start, acceptance = started["payload"], accepted["payload"]
    _require(isinstance(start, Mapping) and isinstance(acceptance, Mapping), "native start payloads")
    _require(start.get("recovery_kind") == "redrive", "native recovery kind")
    _require(acceptance.get("outcome") == "redriven", "native accepted outcome")
    for key in ("workflow_instance_id", "workflow_run_id", "workflow_command_id", "continued_from_run_id",
                "workflow_link_id"):
        _require(isinstance(start.get(key), str) and bool(start[key].strip()), key)
    for key in ("workflow_instance_id", "workflow_run_id", "workflow_command_id"):
        _require(acceptance.get(key) == start[key], "native accepted identity")
    _require(start["workflow_instance_id"] == workflow.get("workflow_id"), "native fixture instance")
    _require(start["workflow_run_id"] == workflow.get("run_id"), "native fixture run")
    _require(start.get("workflow_type") == workflow.get("type"), "native fixture type")
    _require(acceptance.get("workflow_type") == workflow.get("type"), "native accepted type")
    _require(acceptance.get("workflow_class") == workflow.get("type"), "native accepted class")
    _require(start["workflow_run_id"] != start["continued_from_run_id"], "native successor is a new run")
    inserted = _time(started["recorded_at"])
    _require(_time(start.get("replayed_started_at")) <= inserted, "native original start")
    _require(type(start.get("resume_step_sequence")) is int
             and start["resume_step_sequence"] == len(history) - 1, "native resume after prefix")
    identities: set[str] = set()
    for sequence, event in enumerate(history[2:], start=1):
        _require(event.get("event_type") == "ActivityCompleted", "native activity-only reused prefix")
        payload = event["payload"]
        _require(type(payload.get("sequence")) is int and payload["sequence"] == sequence,
                 "native logical prefix order")
        for key in ("activity_execution_id", "reused_activity_execution_id", "activity_type"):
            _require(isinstance(payload.get(key), str) and bool(payload[key].strip()), key)
        identity = payload["activity_execution_id"]
        _require(identity not in identities and identity != payload["reused_activity_execution_id"],
                 "native fresh activity identity")
        identities.add(identity)
        _require(payload.get("reused_from_run_id") == start["continued_from_run_id"], "native source run")
        _require(payload.get("payload_codec") == "avro" and "result" in payload, "native Avro result")
        result = payload["result"]
        _require(not isinstance(result, Mapping) or "external_payload" not in result, "native inline result")
        _require(_time(payload.get("reused_recorded_at")) <= inserted, "native original result time")
