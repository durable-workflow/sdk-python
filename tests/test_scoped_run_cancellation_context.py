from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import CancellationContext, ScopedCancellationContext, serializer
from durable_workflow._cooperative_cancellation import read_cancellation_history
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, WorkflowContext, replay
from tests.test_cooperative_cancellation import completed_activity


def fixtures() -> dict[str, Any]:
    return json.loads((Path(__file__).parent / "fixtures/scoped-run-cancellation-context.json").read_text())


def history() -> list[dict[str, Any]]:
    snapshot = fixtures()["child"]
    return [
        {"event_type": "CooperativeCancellationRequested", "timestamp": "2026-10-04T00:00:05Z", "payload": {
            "workflow_run_id": "child-run", "workflow_instance_id": "child-instance",
            "workflow_command_id": "child-request", "reason": "maintenance",
            "cleanup_deadline_at": snapshot["cleanup_deadline_at"], "cancellation": deepcopy(snapshot),
        }},
        {"event_type": "CooperativeCancellationDelivered", "timestamp": "2026-10-04T00:00:08Z", "payload": {
            "workflow_run_id": "child-run", "workflow_command_id": "child-request",
            "sequence": 1, "call_kind": "timer", "cancellation": deepcopy(snapshot),
        }},
    ]


@pytest.mark.parametrize("name", ["child", "grandchild"])
def test_native_context_preserves_the_complete_tree_and_original_metadata(name: str) -> None:
    snapshot = fixtures()[name]
    context = CancellationContext.from_dict(snapshot)
    assert context.to_dict() == snapshot
    assert CancellationContext.from_dict(context.to_dict()) == context
    assert context.root_request_id == "root-request"
    assert context.reason == "maintenance"
    assert context.source == "api"
    assert context.requester == {"type": "operator", "id": "operator-1"}
    assert context.requested_at == datetime(2026, 10, 4, 0, 0, 0, 123456, timezone.utc)
    assert context.scope_origin is not None
    assert context.scope_origin.root_deadline == datetime(2026, 10, 4, 0, 0, 30, 123456, timezone.utc)
    if name == "child":
        assert context.parent_request_id == "inner-request"
        assert [entry.scope_id for entry in context.scope_origin.lineage] == ["outer", "inner"]
        assert context.scope_origin.deadline == datetime(2026, 10, 4, 0, 0, 20, 123456, timezone.utc)
        assert context.deadline == datetime(2026, 10, 4, 0, 0, 15, 123456, timezone.utc)
    else:
        assert context.parent_request_id == "child-scope-request"
        assert [entry.scope_id for entry in context.scope_origin.lineage] == ["outer", "inner", "root", "child-scope"]
        assert [entry.request_id for entry in context.lineage] == [
            "root-request", "child-scope-request", "grandchild-request",
        ]
        assert context.deadline == datetime(2026, 10, 4, 0, 0, 12, 123456, timezone.utc)


def test_avro_timezone_and_object_order_do_not_change_original_scope_metadata() -> None:
    original = fixtures()["grandchild"]
    snapshot = deepcopy(original)
    snapshot["requested_at"] = "2026-10-03T20:00:00.123456-04:00"
    snapshot["cleanup_deadline_at"] = snapshot["scope_authority_deadline_at"] = "2026-10-03T20:00:12.123456-04:00"
    snapshot["requester"] = dict(reversed(list(snapshot["requester"].items())))
    snapshot["scope_origin"]["lineage"] = [
        dict(reversed(list(entry.items()))) for entry in snapshot["scope_origin"]["lineage"]
    ]
    decoded = serializer.decode(serializer.encode(snapshot), codec="avro")
    assert CancellationContext.from_dict(decoded).to_dict() == original


def test_origin_and_nested_scope_addresses_are_immutable() -> None:
    snapshot = fixtures()["grandchild"]
    context = CancellationContext.from_dict(snapshot)
    snapshot["scope_origin"]["lineage"][3]["scope_id"] = "changed"
    origin = context.scope_origin
    assert isinstance(origin, ScopedCancellationContext)
    assert origin.scope_id == "child-scope" and origin.root_scope_id == "outer"
    assert origin.request_id == "child-scope-request" and origin.parent_request_id == "child-request"
    assert origin.workflow_instance_id == "child-instance" and origin.workflow_run_id == "child-run"
    assert origin.requested_at == context.requested_at
    with pytest.raises(FrozenInstanceError):
        origin.lineage[3].scope_id = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        origin.root_context.requester["id"] = "changed"  # type: ignore[index]
    detached = origin.to_dict()
    detached["lineage"][3]["scope_id"] = "changed"
    assert origin.to_dict() == fixtures()["grandchild"]["scope_origin"]


def test_cold_replay_keeps_original_origin_and_the_same_narrowed_cleanup_clock() -> None:
    contexts: list[CancellationContext] = []
    observations: list[list[float]] = []

    class Cleanup:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            assert ctx.cancellation_context is None
            try:
                yield ctx.start_timer(10)
            except WorkflowCancelled as error:
                assert error.context is not None and error.context is ctx.cancellation_context
                assert error.context.to_dict() == fixtures()["child"]
                contexts.append(error.context)
                seen = [error.context.remaining()]
                observations.append(seen)
                with ctx.cancellation_shield():
                    yield ctx.schedule_activity("cleanup", [])
                    seen.append(error.context.remaining())
                return {"remaining": seen, "cancellation": error.context.to_dict()}
            return None

    events = history()
    pending = replay(Cleanup, events, [], run_id="child-run")
    assert len(pending.commands) == 1 and isinstance(pending.commands[0], ScheduleActivity)
    assert observations == [[7.123456]]
    events.extend([
        {**completed_activity(2, "cleanup", "cleaned"), "timestamp": "2026-10-04T00:00:12Z"},
        {"event_type": "SignalReceived", "timestamp": "2026-10-04T00:00:29Z", "payload": {
            "signal_name": "future", "arguments": serializer.envelope([]),
        }},
    ])
    for _replacement in range(2):
        assert replay(Cleanup, events, [], run_id="child-run").commands == [CompleteWorkflow({
            "remaining": [7.123456, 3.123456], "cancellation": fixtures()["child"],
        })]
    for context in contexts:
        with pytest.raises(RuntimeError, match="active workflow replay"):
            context.remaining()


def test_canonical_delivery_cannot_replace_a_committed_scope_origin() -> None:
    events = history()
    events[1]["payload"]["cancellation"]["scope_origin"]["lineage"][1]["scope_id"] = "different"
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history(events, run_id="child-run")


def invalid_contexts() -> list[Any]:
    original = fixtures()["grandchild"]
    rows = []
    for key, value in {
        "schema": "durable-workflow.cancellation-context/v1", "root_request_id": "other",
        "root_workflow_instance_id": "other", "root_workflow_run_id": "other",
        "parent_request_id": "root-request", "reason": "other", "source": "other",
        "requested_at": "2026-10-04T00:00:01.123456Z", "scope_origin": [],
        "cleanup_deadline_at": "2026-10-04T00:00:15.123456Z",
        "scope_authority_deadline_at": "2026-10-04T00:00:15.123456Z",
    }.items():
        rows.append(pytest.param({**original, key: value}, id=key))
    for key in ["scope_origin", "scope_authority_deadline_at", "parent_request_id"]:
        snapshot = deepcopy(original)
        del snapshot[key]
        rows.append(pytest.param(snapshot, id="missing "+key))
    for name in ["requester", "discarded hop", "widened global budget", "reused request", "reused run",
                 "discarded root", "widened scope budget", "run reassigned", "run reentry", "repeated scope request",
                 "repeated address", "recursive root", "unsupported scope field", "invalid schema type"]:
        snapshot = deepcopy(original)
        if name == "requester":
            snapshot["requester"]["id"] = "other"
        elif name == "discarded hop":
            snapshot["lineage"][1]["request_id"] = "child-request"
        elif name == "widened global budget":
            snapshot["cleanup_deadline_at"] = snapshot["scope_authority_deadline_at"] = "2026-10-04T00:00:30.123456Z"
        elif name == "reused request":
            snapshot["request_id"] = snapshot["lineage"][2]["request_id"] = "inner-request"
        elif name == "reused run":
            snapshot["lineage"][2]["workflow_run_id"] = "child-run"
        elif name == "discarded root":
            snapshot["scope_origin"]["lineage"].pop(0)
        elif name == "widened scope budget":
            snapshot["scope_origin"]["lineage"][3]["cleanup_deadline_at"] = "2026-10-04T00:00:16.123456Z"
        elif name == "run reassigned":
            snapshot["scope_origin"]["lineage"][3]["workflow_instance_id"] = "other"
        elif name == "run reentry":
            snapshot["scope_origin"]["lineage"][3]["workflow_run_id"] = "root-run"
            snapshot["scope_origin"]["lineage"][3]["workflow_instance_id"] = "root-instance"
        elif name == "repeated scope request":
            snapshot["scope_origin"]["lineage"][3]["request_id"] = "inner-request"
        elif name == "repeated address":
            snapshot["scope_origin"]["lineage"][3]["scope_id"] = "root"
        elif name == "recursive root":
            snapshot["scope_origin"]["root_context"]["schema"] = "durable-workflow.cancellation-context/v2"
            snapshot["scope_origin"]["root_context"]["scope_origin"] = original["scope_origin"]
        elif name == "unsupported scope field":
            snapshot["scope_origin"]["lineage"][3]["authority"] = "unrecorded"
        else:
            snapshot["schema"] = []
        rows.append(pytest.param(snapshot, id=name))
    return rows


@pytest.mark.parametrize("snapshot", invalid_contexts())
def test_invalid_origin_identity_or_budget_is_refused(snapshot: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        CancellationContext.from_dict(snapshot)
