from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from durable_workflow._cancellation_scope import CancellationScopeHistory
from durable_workflow._cancellation_scope_history import CommittedCancellationScopeHistory, normalize_scope_members

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name: str, variant: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())[variant]  # type: ignore[no-any-return]


def read(value: dict[str, Any]) -> CommittedCancellationScopeHistory:
    return CommittedCancellationScopeHistory.read(
        value["history"], value["task"]["run_id"], value["task"]["workflow_id"],
    )


@pytest.mark.parametrize("variant", ["operations", "groups", "descendants", "competing"])
def test_native_v5_projections_match_original_operations_and_descendant_authority(variant: str) -> None:
    value = fixture("committed-scope-operation-projections.json", variant)
    committed = read(value)
    scope_id = value["scope_id"]
    preparation = committed.preparations[scope_id]
    delivery = committed.deliveries[preparation.boundary.sequence]
    assert preparation.context == delivery.context
    assert preparation.boundary == delivery.boundary
    assert preparation.authority_deadline == delivery.authority_deadline
    assert committed.pending_requests == {}
    projection = preparation.event["payload"]
    assert [member["timer_id"] for member in projection["timer_members"]] == [
        "timer-plain", "signal-timer-id", "condition-timer-id",
    ]
    assert [member["kind"] for member in projection["wait_members"]] == ["signal", "condition", "condition"]
    assert projection["wait_members"][2]["timer_id"] is None
    assert [member["cancellation_policy"] for member in projection["child_members"]] == [
        "try_cancel", "wait_cancellation_completed", "abandon",
    ]
    assert projection["child_members"][1]["child_workflow_run_id"] == "child-continued-run"
    if variant in {"descendants", "competing"}:
        child, grandchild = projection["descendant_members"]
        assert child["scope_id"] == "desc-child"
        assert grandchild["scope_id"] == "desc-grandchild"
        assert child["propagation_history_event_id"] == (
            "original-child-conflict" if variant == "competing" else "inherited-child-accepted"
        )
        assert child["authority_deadline_at"] == (
            "2026-10-04T00:00:20.123456Z" if variant == "competing" else "2026-10-04T00:00:30.123456Z"
        )
    else:
        assert projection["descendant_members"] == []


@pytest.mark.parametrize("variant", ["shielded", "unshielded"])
def test_empty_scope_delivery_proves_original_request_and_unscheduled_call(variant: str) -> None:
    value = fixture("committed-scope-delivery.json", variant)
    committed = read(value)
    assert len(committed.deliveries) == len(committed.preparations) == 1
    delivery = next(iter(committed.deliveries.values()))
    assert delivery.boundary.call_kind == "timer"
    assert delivery.context.root_context.request_id == delivery.context.request_id
    assert delivery.context.deadline == delivery.authority_deadline


@pytest.mark.parametrize("variant", [
    "activity-try_cancel", "activity-wait_cancellation_completed", "activity-abandon",
    "child-try_cancel", "child-wait_cancellation_completed", "child-abandon", "timer", "condition", "condition-untimed",
])
def test_native_populated_single_calls_keep_prior_results_and_original_operation_policies(variant: str) -> None:
    value = fixture("populated-scope-single-calls.json", variant)
    committed = read(value)
    preparation = next(iter(committed.preparations.values()))
    delivery = committed.deliveries[preparation.boundary.sequence]
    assert delivery.boundary.sequence == 4
    assert delivery.boundary.call_kind == variant.split("-")[0]
    assert delivery.context == preparation.context
    assert preparation.event["payload"]["activity_members"]
    assert committed.pending_requests == {}


@pytest.mark.parametrize("variant", ["operations", "groups", "descendants", "competing"])
@pytest.mark.parametrize("field", ["activity_members", "timer_members", "wait_members", "child_members"])
def test_frozen_projection_cannot_omit_or_retarget_original_members(variant: str, field: str) -> None:
    value = fixture("committed-scope-operation-projections.json", variant)
    preparation = next(row for row in value["history"] if row["event_type"] == "CancellationScopeDeliveryPrepared")
    if preparation["payload"][field]:
        preparation["payload"][field][0]["descriptor_hash"] = "0" * 64
    else:
        preparation["payload"][field] = [{}]
    with pytest.raises(ValueError, match="scope"):
        read(value)


@pytest.mark.parametrize("failure", [
    "request_id", "run", "instance", "deadline", "boundary", "preparation_id", "before_preparation",
    "missing_request", "duplicate_request", "duplicate_delivery", "future_member", "changed_child_continuation",
    "descendant_deadline", "descendant_propagation", "cross_shield", "extended_inherited_deadline",
])
def test_changed_canonical_request_boundary_and_subtree_are_refused(failure: str) -> None:
    value = fixture("committed-scope-operation-projections.json", "descendants")
    history = value["history"]
    preparation = next(row for row in history if row["event_type"] == "CancellationScopeDeliveryPrepared")
    delivery = next(row for row in history if row["event_type"] == "CancellationScopeDelivered")
    request = next(row for row in history if row["event_type"] == "CancellationScopeRequested")
    if failure == "request_id":
        delivery["payload"]["request_id"] = "new-request"
    elif failure == "run":
        preparation["payload"]["workflow_run_id"] = "new-run"
    elif failure == "instance":
        delivery["payload"]["cancellation"]["lineage"][-1]["workflow_instance_id"] = "new-instance"
    elif failure == "deadline":
        delivery["payload"]["authority_deadline_at"] = "2026-10-04T00:00:31.123456Z"
    elif failure == "boundary":
        delivery["payload"]["sequence"] += 1
    elif failure == "preparation_id":
        delivery["payload"]["preparation_history_event_id"] = "new-preparation"
    elif failure == "before_preparation":
        history.remove(delivery)
        history.insert(history.index(preparation), delivery)
    elif failure == "missing_request":
        history.remove(request)
    elif failure in {"duplicate_request", "duplicate_delivery"}:
        duplicate = deepcopy(request if failure == "duplicate_request" else delivery)
        duplicate["id"] += "-duplicate"
        history.insert(history.index(request if failure == "duplicate_request" else delivery) + 1, duplicate)
    elif failure == "future_member":
        timer = next(row for row in history if row["id"] == "plain-timer")
        history.remove(timer)
        history.append(timer)
    elif failure == "changed_child_continuation":
        continuation = next(row for row in history if row["id"] == "child-continued-before-preparation")
        continuation["payload"]["child_workflow_run_id"] = "new-child-run"
    elif failure == "descendant_deadline":
        preparation["payload"]["descendant_members"][0]["authority_deadline_at"] = "2026-10-04T00:00:31.123456Z"
    elif failure == "descendant_propagation":
        preparation["payload"]["descendant_members"][0]["propagation_history_event_id"] = "new-propagation"
    elif failure == "cross_shield":
        next(row for row in history if row["id"] == "desc-child-opened")["payload"]["shield_parent"] = True
    else:
        next(row for row in history if row["id"] == "inherited-child-accepted")["payload"]["cancellation"][
            "lineage"
        ][-1]["cleanup_deadline_at"] = "2026-10-04T00:00:29.123456Z"
    for index, row in enumerate(history, 1):
        row["sequence"] = index
    with pytest.raises(ValueError):
        read(value)


def test_pending_request_uses_original_unshielded_ancestor_and_preparation_survives_replacement() -> None:
    value = fixture("committed-scope-operation-projections.json", "descendants")
    history = value["history"]
    history.pop()  # Delivery not yet committed.
    committed = read(value)
    scopes = CancellationScopeHistory.read(history, value["task"]["run_id"])
    request = committed.pending_request_for_scope("desc-grandchild", scopes)
    assert request is not None
    assert request.context.scope_id == value["scope_id"]
    preparation = committed.preparations[value["scope_id"]]
    value["task"].update({"lease_owner": "replacement", "workflow_task_attempt": 17})
    assert read(value).preparations[value["scope_id"]] == preparation


def test_original_run_request_is_the_canonical_parent_of_its_unshielded_scope() -> None:
    value = json.loads((FIXTURES / "run-inherited-scope-delivery.json").read_text())
    value["history"] = value["history"][:5]
    committed = read(value)
    scopes = CancellationScopeHistory.read(value["history"], value["task"]["run_id"])
    scope_id = value["history"][2]["payload"]["scope_id"]
    request = committed.pending_request_for_scope(scope_id, scopes)
    assert request is not None and request.context.request_id == value["history"][4]["payload"]["request_id"]
    assert request.context.parent_request_id == value["history"][3]["payload"]["workflow_command_id"]
    assert [entry.scope_id for entry in request.context.lineage] == ["root", scope_id]


@pytest.mark.parametrize("failure", ["missing", "later", "shield", "metadata", "deadline"])
def test_run_inheritance_requires_an_earlier_exact_parent_and_preserves_shielding(failure: str) -> None:
    value = json.loads((FIXTURES / "run-inherited-scope-delivery.json").read_text())
    history = value["history"] = value["history"][:5]
    if failure == "missing":
        history.pop(3)
    elif failure == "later":
        history[3], history[4] = history[4], history[3]
    elif failure == "shield":
        history[2]["payload"]["shield_parent"] = True
    elif failure == "metadata":
        history[4]["payload"]["cancellation"]["root_context"]["reason"] = "changed"
    else:
        history[4]["payload"]["cancellation"]["lineage"][1]["cleanup_deadline_at"] = "2026-10-05T00:00:25.000000Z"
    for index, event in enumerate(history, 1):
        event["sequence"] = index
    with pytest.raises(ValueError):
        read(value)


@pytest.mark.parametrize("field", ["activity_members", "timer_members", "wait_members", "child_members"])
def test_projection_rejects_boolean_sequences_and_unknown_fields(field: str) -> None:
    value = fixture("committed-scope-operation-projections.json", "operations")
    preparation = next(row for row in value["history"] if row["event_type"] == "CancellationScopeDeliveryPrepared")
    member = deepcopy(preparation["payload"][field][0]) if preparation["payload"][field] else {
        "sequence": 1, "activity_execution_id": "activity-one", "descriptor_hash": "0" * 64,
    }
    member["sequence"] = True
    with pytest.raises(ValueError):
        normalize_scope_members(field, [member])
    member["sequence"] = 1
    member["unexpected"] = "value"
    with pytest.raises(ValueError):
        normalize_scope_members(field, [member])
