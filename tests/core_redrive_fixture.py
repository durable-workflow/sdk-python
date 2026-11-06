"""Core's retained acceptance row plus the unchanged successor checker.

DefaultWorkflowControlPlane at 98bd9868 (blob af6ac905847db3f5b8fd43d0e7f825475a7b6318)
emits this prefix. Removing only that validated row gives the existing Core
successor representation; timestamps and payloads are never rewritten.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any


def check_prefixed_core_successor(history: list[dict[str, Any]], workflow: Mapping[str, Any]) -> None:
    from tests.test_replay_clock import check_redrive_successor, redrive_time

    assert len(history) >= 3, "Core accepted successor"
    acceptance, started = history[:2]
    assert acceptance.get("event_type") == "StartAccepted", "Core acceptance prefix"
    assert started.get("event_type") == "WorkflowStarted", "Core start boundary"
    accepted, start = acceptance["payload"], started["payload"]
    assert isinstance(accepted, Mapping) and isinstance(start, Mapping), "Core start payloads"
    assert accepted.get("outcome") == "redriven", "Core accepted outcome"
    for key in ("workflow_instance_id", "workflow_run_id", "workflow_command_id"):
        assert isinstance(start.get(key), str) and start[key].strip(), key
        assert accepted.get(key) == start[key], "Core accepted identity"
    assert accepted.get("workflow_type") == workflow.get("type"), "Core accepted workflow type"
    assert isinstance(accepted.get("workflow_class"), str) and accepted["workflow_class"].strip(), "Core class"
    redrive_time(acceptance["recorded_at"])
    for index, event in enumerate(history, start=1):
        assert type(event.get("sequence")) is int and event["sequence"] == index, "Core complete history order"
    successor = deepcopy(history[1:])
    for index, event in enumerate(successor, start=1):
        event["sequence"] = index
    check_redrive_successor(successor, workflow)
