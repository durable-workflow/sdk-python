from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import pytest

from tests.worker_startup_snapshot import OriginalIdentity, decode_snapshot

IDENTITY = OriginalIdentity(
    "default", "task-family-original", "01BX5ZZKBKACTAV9WEVGEMMVRZ", "01ARZ3NDEKTSV4RRFFQ69G5FAV"
)
NOW = "2026-11-05 20:00:00.123456"
BITS = ("lease_owner_present", "dispatch_error_present", "claim_error_present", "task_error_present")
COUNTERS = ("attempt_count", "repair_count")


def row(kind: str) -> dict[str, Any]:
    value: dict[str, Any] = {
        "database_time_utc": NOW,
        "session_time_zone": "SYSTEM",
        "system_time_zone": "UTC",
        "original_run_id": IDENTITY.original_run_id,
        "namespace": IDENTITY.namespace,
        "status": "ready" if kind == "task" else "running",
    }
    if kind == "task":
        value.update(
            {
                "original_task_id": IDENTITY.original_task_id,
                "task_type": "workflow",
                "queue": "task-family-original",
                "compatibility": None,
                "available_at": None,
                "leased_at": None,
                "lease_expires_at": None,
                "last_dispatch_attempt_at": None,
                "last_dispatched_at": None,
                "last_claim_failed_at": None,
                "repair_available_at": None,
                "attempt_count": 0,
                "repair_count": 0,
                "lease_owner_present": "0",
                "dispatch_error_present": "0",
                "claim_error_present": "0",
                "task_error_present": "0",
            }
        )
    else:
        value["workflow_instance_id"] = IDENTITY.workflow_instance_id
        value["execution_deadline_at"] = None
        value["run_deadline_at"] = None
    return value


@pytest.mark.parametrize("kind", ["task", "run"])
def test_exact_original_projection_is_not_a_phase_verdict(kind: Any) -> None:
    supplied = row(kind)
    result = decode_snapshot(kind, IDENTITY, [supplied])
    assert result.projection_state == "complete" and result.phase_verdict == "unknown"
    assert result.row is not supplied and result.row is not None
    assert set(result.row) == set(supplied)
    assert result.row["database_time_utc"] == NOW and result.row["session_time_zone"] == "SYSTEM"
    supplied["status"] = "SECRET changed later"
    assert result.row["status"] != supplied["status"]


@pytest.mark.parametrize("value", [0, "0", 1, "1"])
def test_presence_bits_have_exact_numeric_semantics(value: Any) -> None:
    supplied = row("task")
    supplied.update(dict.fromkeys(BITS, value))
    result = decode_snapshot("task", IDENTITY, [supplied])
    assert result.projection_state == "complete" and result.row is not None
    for field in BITS:
        assert result.row[field] is (value in (1, "1"))
    assert result.row["attempt_count"] == 0


@pytest.mark.parametrize("value", [None, False, True, "false", "true", "", "NULL", 0.0, 2, "01", [], {}])
def test_truthiness_does_not_admit_unproved_presence_bits(value: Any) -> None:
    supplied = row("task")
    supplied["lease_owner_present"] = value
    result = decode_snapshot("task", IDENTITY, [supplied])
    assert result.projection_state == "invalid" and result.refusal_code == "PRESENCE_BIT_INVALID"
    assert result.row is None and result.phase_verdict == "unknown"


@pytest.mark.parametrize("value", [0, "0", 4_294_967_295, "4294967295"])
def test_unsigned_counters_preserve_zero_and_the_historical_schema_limit(value: Any) -> None:
    supplied = row("task")
    supplied.update(dict.fromkeys(COUNTERS, value))
    result = decode_snapshot("task", IDENTITY, [supplied])
    assert result.projection_state == "complete" and result.row is not None
    assert result.row["attempt_count"] == int(value) and type(result.row["attempt_count"]) is int


@pytest.mark.parametrize("value", [None, False, True, -1, 4_294_967_296, "4294967296", "00", " 1", "1.0", 1.0, "-1"])
def test_counter_coercion_cannot_create_an_attempt_or_repair_count(value: Any) -> None:
    supplied = row("task")
    supplied["attempt_count"] = value
    result = decode_snapshot("task", IDENTITY, [supplied])
    assert result.projection_state == "invalid" and result.refusal_code == "COUNTER_INVALID"


@pytest.mark.parametrize(
    "kind,field",
    [
        ("task", "namespace"),
        ("task", "original_run_id"),
        ("task", "original_task_id"),
        ("run", "namespace"),
        ("run", "original_run_id"),
        ("run", "workflow_instance_id"),
    ],
)
def test_latest_replacement_or_case_changed_identity_is_refused(kind: Any, field: str) -> None:
    supplied = row(kind)
    supplied[field] = str(supplied[field]).swapcase()
    result = decode_snapshot(kind, IDENTITY, [supplied])
    assert result.projection_state == "invalid" and result.refusal_code == "IDENTITY_MISMATCH"
    assert result.row is None


@pytest.mark.parametrize("kind", ["task", "run"])
def test_missing_duplicate_and_extra_rows_cannot_become_one_original_row(kind: Any) -> None:
    assert decode_snapshot(kind, IDENTITY, []).projection_state == "missing"
    assert decode_snapshot(kind, IDENTITY, []).row is None  # No borrowed DB clock sample.
    for rows in ([row(kind), row(kind)], (row(kind),), {"row": row(kind)}, "SECRET", [None]):
        result = decode_snapshot(kind, IDENTITY, rows)
        assert result.projection_state == "invalid" and result.row is None


@pytest.mark.parametrize("state", ["missing", "truncated", "invalid"])
def test_incomplete_transport_state_never_promotes_a_valid_row(state: Any) -> None:
    result = decode_snapshot("task", IDENTITY, [row("task")], source_state=state)
    assert result.projection_state == state and result.row is None and result.phase_verdict == "unknown"


@pytest.mark.parametrize("kind,field", [(kind, field) for kind in ("task", "run") for field in sorted(row(kind))])
def test_every_proposed_projection_field_is_required(kind: Any, field: str) -> None:
    supplied = row(kind)
    del supplied[field]
    assert decode_snapshot(kind, IDENTITY, [supplied]).refusal_code == "FIELD_SET_INVALID"


@pytest.mark.parametrize("kind", ["task", "run"])
def test_independent_fixtures_match_the_sql_column_inventory(kind: str) -> None:
    source = (
        Path(__file__).parent / "fixtures/worker_startup_reconstruction/sql-proposal" / f"original-{kind}.sql"
    ).read_text()
    selected = source.split("SELECT", 1)[1].split("FROM", 1)[0]
    names = []
    for expression in selected.split(","):
        match = re.search(r"(?:\bAS\s+([a-z_]+)|\b[tr]\.([a-z_]+))\s*$", expression)
        assert match is not None, "lexical field inventory changed; this is not SQL syntax validation"
        names.append(next(value for value in match.groups() if value is not None))
    assert len(names) == len(set(names)) and set(names) == set(row(kind))


@pytest.mark.parametrize("kind", ["task", "run"])
def test_unknown_payload_or_error_columns_are_not_copied_or_echoed(kind: Any) -> None:
    supplied = row(kind)
    supplied["SECRET unknown payload"] = "SECRET"
    result = decode_snapshot(kind, IDENTITY, [supplied])
    assert result.projection_state == "invalid" and result.field is None and result.row is None
    assert "SECRET" not in json.dumps(asdict(result))


@pytest.mark.parametrize("value", [None, "", "NULL"])
def test_nullable_text_keeps_null_empty_and_literal_null_distinct(value: Any) -> None:
    supplied = row("task")
    supplied["compatibility"] = value
    result = decode_snapshot("task", IDENTITY, [supplied])
    assert result.projection_state == "complete" and result.row is not None
    assert result.row["compatibility"] == value and type(result.row["compatibility"]) is type(value)


@pytest.mark.parametrize(
    "value", ["", "NULL", "2026-02-30 20:00:00.123456", "2026-11-05T20:00:00Z", dt.datetime(2026, 11, 5)]
)
def test_timestamp_normalization_must_be_proven_by_a_future_driver(value: Any) -> None:
    supplied = row("task")
    supplied["leased_at"] = value
    assert decode_snapshot("task", IDENTITY, [supplied]).refusal_code == "TIMESTAMP_INVALID"


@pytest.mark.parametrize("field", ["database_time_utc", "session_time_zone", "system_time_zone"])
def test_clock_and_timezone_evidence_cannot_be_omitted(field: str) -> None:
    supplied = row("run")
    supplied[field] = None
    result = decode_snapshot("run", IDENTITY, [supplied])
    assert result.projection_state == "invalid" and result.field == field


@pytest.mark.parametrize(
    "kind,statuses",
    [
        ("task", {"ready", "leased", "cancelled", "completed", "failed"}),
        ("run", {"pending", "running", "waiting", "cancelled", "terminated", "completed", "failed"}),
    ],
)
def test_historical_status_vocabulary_is_retained_without_causal_interpretation(kind: Any, statuses: set[str]) -> None:
    for status in statuses:
        supplied = row(kind)
        supplied["status"] = status
        result = decode_snapshot(kind, IDENTITY, [supplied])
        assert result.projection_state == "complete" and result.phase_verdict == "unknown"
    supplied = row(kind)
    supplied["status"] = "future_unknown_status"
    assert decode_snapshot(kind, IDENTITY, [supplied]).refusal_code == "STATUS_INVALID"


@pytest.mark.parametrize("family", ["activity", "timer", ""])
def test_other_task_families_do_not_substitute_for_the_original_workflow_task(family: str) -> None:
    supplied = row("task")
    supplied["task_type"] = family
    assert decode_snapshot("task", IDENTITY, [supplied]).refusal_code == "WRONG_TASK_FAMILY"


@pytest.mark.parametrize(
    "identity", [replace(IDENTITY, namespace=""), replace(IDENTITY, original_task_id="latest"), None]
)
def test_invalid_expected_identity_is_not_a_missing_original_row(identity: Any) -> None:
    assert decode_snapshot("task", identity, []).refusal_code == "IDENTITY_INVALID"


def test_missing_input_and_unknown_source_flags_are_explicit() -> None:
    assert decode_snapshot("task", IDENTITY, None).projection_state == "missing"
    assert (
        decode_snapshot("task", IDENTITY, [row("task")], source_state="unknown").refusal_code == "SOURCE_STATE_INVALID"
    )
