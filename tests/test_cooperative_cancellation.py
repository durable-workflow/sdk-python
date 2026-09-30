from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from durable_workflow._cooperative_cancellation import CancellationDelivery, read_cancellation_history
from durable_workflow.errors import NonDeterministicReplayError


def observation() -> dict[str, Any]:
    return {
        "request_id": "request-1",
        "requested_at": "2026-09-30T12:00:00Z",
        "cleanup_deadline_at": "2026-09-30T12:10:00Z",
        "history_refresh_page_token": "opaque-first-page",
    }


def request() -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationRequested",
        "workflow_command_id": "request-1",
        "recorded_at": "2026-09-30T12:00:00.000120Z",
        "payload": {
            "workflow_command_id": "request-1",
            "workflow_run_id": "run-1",
            "cleanup_deadline_at": "2026-09-30T12:10:00Z",
        },
    }


def marker(sequence: int = 2, call_kind: str = "activity", **fields: Any) -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationDelivered",
        "workflow_command_id": "request-1",
        "payload": {
            "workflow_command_id": "request-1",
            "workflow_run_id": "run-1",
            "sequence": sequence,
            "call_kind": call_kind,
            **fields,
        },
    }


def test_observation_retains_original_identity_without_becoming_delivery() -> None:
    state = read_cancellation_history([request()], run_id="run-1", observation=observation())
    assert state.request is not None
    assert state.request.requested_at == observation()["requested_at"]
    assert state.request.history_refresh_page_token == "opaque-first-page"
    assert state.delivery is None
    assert state.eligible(1)


def test_prior_result_remains_resolved_but_later_result_is_eligible() -> None:
    result = {"event_type": "ActivityCompleted", "payload": {"sequence": 1}}
    later = {"event_type": "TimerFired", "payload": {"sequence": 2}}
    state = read_cancellation_history([result, request(), later])
    assert not state.eligible(1)
    assert state.eligible(2)
    assert state.eligible(1, 2)


def test_prior_parallel_failure_wins_over_a_later_request() -> None:
    state = read_cancellation_history(
        [
            {"event_type": "ActivityFailed", "payload": {"sequence": 1}},
            request(),
        ]
    )
    assert not state.eligible(1, 2)


def test_cold_history_preserves_one_canonical_delivery_and_its_range() -> None:
    state = read_cancellation_history([request(), marker(2, "parallel", sequence_span=3)], run_id="run-1")
    assert state.delivery == CancellationDelivery("request-1", 2, "parallel", 3)
    assert [state.delivery.interrupts(sequence) for sequence in range(1, 6)] == [False, True, True, True, False]
    assert not state.eligible(5)


def test_selection_handle_targets_its_earlier_operation_range() -> None:
    state = read_cancellation_history(
        [
            request(),
            marker(5, "selection_handle", operation_sequence=2, operation_sequence_span=2),
        ]
    )
    assert state.delivery is not None
    assert state.delivery.interrupts(2)
    assert state.delivery.interrupts(3)
    assert not state.delivery.interrupts(5)


@pytest.mark.parametrize(
    "fields",
    [
        {"sequence": True},
        {"sequence": 0},
        {"sequence": -1},
        {"sequence": 2**63 - 1},
        {"call_kind": "side_effect"},
        {"sequence_span": 2},
        {"sequence_span": 0},
        {"call_kind": "parallel", "sequence_span": 1001},
        {"operation_sequence": 1},
        {"operation_sequence_span": 2},
        {"call_kind": "selection_handle"},
        {"call_kind": "selection_handle", "operation_sequence": 2},
        {"call_kind": "selection_handle", "operation_sequence": 1, "operation_sequence_span": 2},
    ],
)
def test_invalid_marker_ranges_fail_closed(fields: dict[str, Any]) -> None:
    event = marker()
    event["payload"].update(fields)
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([request(), event])


@pytest.mark.parametrize(
    "history",
    [
        [marker()],
        [marker(), request()],
        [request(), request()],
        [request(), marker(), marker()],
    ],
)
def test_missing_reordered_or_duplicate_canonical_markers_fail_closed(history: list[dict[str, Any]]) -> None:
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history(history)


@pytest.mark.parametrize("field", ["request_id", "cleanup_deadline_at"])
def test_observation_cannot_replace_original_request(field: str) -> None:
    observed = observation()
    observed[field] = "different" if field == "request_id" else "2026-09-30T12:20:00Z"
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([request()], observation=observed)


@pytest.mark.parametrize(
    "field,value",
    [
        ("request_id", ""),
        ("requested_at", "2026-09-30T12:00:00"),
        ("cleanup_deadline_at", "2026-09-30T11:00:00Z"),
        ("history_refresh_page_token", ""),
    ],
)
def test_invalid_observation_fails_closed(field: str, value: Any) -> None:
    observed = observation()
    observed[field] = value
    with pytest.raises(NonDeterministicReplayError):
        read_cancellation_history([], observation=observed)


def test_delivery_run_and_request_identity_are_checked() -> None:
    for field, value in [("workflow_run_id", "other-run"), ("workflow_command_id", "other-request")]:
        event = deepcopy(marker())
        event["payload"][field] = value
        with pytest.raises(NonDeterministicReplayError):
            read_cancellation_history([request(), event], run_id="run-1")
