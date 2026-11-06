from __future__ import annotations

import datetime as dt
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from tests import worker_startup_raw as raw
from tests.test_worker_startup_snapshot import IDENTITY, NOW, row
from tests.worker_startup_snapshot import RUN_FIELDS, TASK_FIELDS, ProjectionRefusal


def supplied(kind: Any) -> tuple[tuple[str, ...], tuple[Any, ...]]:
    columns = raw.TASK_COLUMNS if kind == "task" else raw.RUN_COLUMNS
    values = row(kind)
    return columns, tuple(None if values[field] is None else str(values[field]).encode("ascii") for field in columns)


@pytest.mark.parametrize("kind,fields", [("task", TASK_FIELDS), ("run", RUN_FIELDS)])
def test_positional_original_rows_decode_without_a_capture_or_phase_verdict(kind: Any, fields: set[str]) -> None:
    aliases, values = supplied(kind)
    result = raw.decode_raw_snapshot(kind, IDENTITY, aliases, [values])
    assert set(aliases) == fields
    assert result.projection_state == "complete"
    assert result.phase_verdict == "unknown"
    assert result.row is not None
    assert result.row["database_time_utc"] == NOW
    assert result.row["session_time_zone"] == "SYSTEM"


@pytest.mark.parametrize("kind", ["task", "run"])
def test_empty_and_duplicate_results_do_not_select_or_borrow_a_row(kind: Any) -> None:
    aliases, values = supplied(kind)
    missing = raw.decode_raw_snapshot(kind, IDENTITY, aliases, [])
    assert missing.projection_state == "missing"
    assert missing.row is None
    duplicate = raw.decode_raw_snapshot(kind, IDENTITY, aliases, [values, values])
    assert duplicate.projection_state == "invalid"
    assert duplicate.row is None


@pytest.mark.parametrize("change", ["duplicate", "order", "case", "missing", "bytes", "list"])
def test_column_collision_or_order_change_is_refused_before_mapping(change: str) -> None:
    columns, values = supplied("task")
    aliases: Any = columns
    if change == "duplicate":
        aliases = (columns[1], *columns[1:])
    elif change == "order":
        aliases = (columns[1], columns[0], *columns[2:])
    elif change == "case":
        aliases = (columns[0].upper(), *columns[1:])
    elif change == "missing":
        aliases = columns[:-1]
    elif change == "bytes":
        aliases = tuple(field.encode() for field in columns)
    else:
        aliases = list(columns)
    result = raw.decode_raw_snapshot("task", IDENTITY, aliases, [values])
    assert result.refusal_code == "RAW_COLUMN_SET_INVALID"
    assert result.row is None


@pytest.mark.parametrize("value", [dt.datetime(2026, 11, 5), 1, True, 1.0, [], b"x" * 256, b"\xff"])
def test_native_or_oversized_values_are_never_coerced(value: Any) -> None:
    aliases, supplied_values = supplied("task")
    values = list(supplied_values)
    values[aliases.index("compatibility")] = value
    result = raw.decode_raw_snapshot("task", IDENTITY, aliases, [tuple(values)])
    assert result.projection_state == "invalid"
    assert result.field == "compatibility"
    assert result.row is None


def test_rejected_value_is_not_formatted_or_retained() -> None:
    class DoNotFormat:
        def __str__(self) -> str:
            raise AssertionError("implicit string conversion")

        def __repr__(self) -> str:
            raise AssertionError("implicit representation")

    aliases, supplied_values = supplied("task")
    values = list(supplied_values)
    values[aliases.index("queue")] = DoNotFormat()
    result = raw.decode_raw_snapshot("task", IDENTITY, aliases, [tuple(values)])
    assert result.refusal_code == "RAW_SCALAR_TYPE_OR_SIZE"
    assert result.row is None


@pytest.mark.parametrize("value,expected", [(None, None), (b"", ""), (b"NULL", "NULL")])
def test_null_empty_and_literal_null_keep_their_meaning(value: Any, expected: str | None) -> None:
    aliases, supplied_values = supplied("task")
    values = list(supplied_values)
    values[aliases.index("compatibility")] = value
    result = raw.decode_raw_snapshot("task", IDENTITY, aliases, [tuple(values)])
    assert result.projection_state == "complete"
    assert result.row is not None
    assert result.row["compatibility"] == expected


@pytest.mark.parametrize("value", [b"2026-11-05 20:00:00", NOW.encode()])
def test_whole_seconds_are_padded_without_clock_conversion(value: bytes) -> None:
    aliases, supplied_values = supplied("run")
    values = list(supplied_values)
    values[0] = value
    result = raw.decode_raw_snapshot("run", IDENTITY, aliases, [tuple(values)])
    assert result.projection_state == "complete"
    assert result.row is not None
    assert result.row["database_time_utc"] == (NOW if len(value) == 26 else "2026-11-05 20:00:00.000000")
    assert result.phase_verdict == "unknown"


@pytest.mark.parametrize("value", [
    b"2026-02-30 20:00:00", b"0000-00-00 00:00:00", b"2026-11-05", b"2026-11-05 20:00:00.1",
    b"2026-11-05 20:00:00.123456Z", b"2026-11-05T20:00:00",
])
def test_invalid_or_partial_timestamp_is_not_normalized_into_evidence(value: bytes) -> None:
    aliases, supplied_values = supplied("run")
    values = (value, *supplied_values[1:])
    result = raw.decode_raw_snapshot("run", IDENTITY, aliases, [values])
    assert result.projection_state == "invalid"
    assert result.row is None


@pytest.mark.parametrize("kind", ["task", "run"])
def test_query_parameters_remain_separate_from_the_closed_sql_text(kind: Any) -> None:
    identity = replace(IDENTITY, namespace="quote' :namespace %(original_run_id)s")
    query, parameters = raw.bind_original_query(kind, identity)
    assert identity.namespace not in query
    assert parameters["namespace"] == identity.namespace
    assert query.count("%(namespace)s") == 1
    assert query.count("%(original_run_id)s") == 1
    assert set(parameters) == {
        "namespace", "original_run_id", "original_task_id" if kind == "task" else "workflow_instance_id",
    }


def test_changed_sql_source_cannot_be_used_as_an_arbitrary_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = raw.SQL_DIRECTORY / "original-task.sql"
    (tmp_path / source.name).write_bytes(source.read_bytes().replace(b"SELECT", b"DELETE", 1))
    monkeypatch.setattr(raw, "SQL_DIRECTORY", tmp_path)
    with pytest.raises(ProjectionRefusal, match="SQL_SOURCE_CHANGED"):
        raw.bind_original_query("task", IDENTITY)


@pytest.mark.parametrize("kind", ["task", "run"])
def test_ordered_aliases_match_the_immutable_projection_text(kind: Any) -> None:
    query = (raw.SQL_DIRECTORY / f"original-{kind}.sql").read_text()
    selected = query.split("SELECT\n", 1)[1].split("\nFROM ", 1)[0]
    aliases = tuple(line.strip().rstrip(",").rsplit(" AS ", 1)[-1].rsplit(".", 1)[-1] for line in selected.splitlines())
    assert aliases == (raw.TASK_COLUMNS if kind == "task" else raw.RUN_COLUMNS)


def test_missing_projection_text_returns_a_fixed_refusal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(raw, "SQL_DIRECTORY", tmp_path)
    with pytest.raises(ProjectionRefusal, match="SQL_SOURCE_UNAVAILABLE"):
        raw.bind_original_query("run", IDENTITY)


def test_raw_identity_comparison_is_case_sensitive() -> None:
    aliases, supplied_values = supplied("task")
    values = list(supplied_values)
    values[aliases.index("original_run_id")] = IDENTITY.original_run_id.lower().encode()
    result = raw.decode_raw_snapshot("task", IDENTITY, aliases, [tuple(values)])
    assert result.refusal_code == "IDENTITY_MISMATCH"
    assert result.row is None


@pytest.mark.parametrize("value", [b"0", b"4294967295"])
def test_canonical_raw_counter_boundaries_pass_the_existing_decoder(value: bytes) -> None:
    aliases, supplied_values = supplied("task")
    values = list(supplied_values)
    values[aliases.index("attempt_count")] = value
    values[aliases.index("lease_owner_present")] = b"1"
    result = raw.decode_raw_snapshot("task", IDENTITY, aliases, [tuple(values)])
    assert result.projection_state == "complete"
    assert result.row is not None
    assert result.row["attempt_count"] == int(value)
    assert result.row["lease_owner_present"] is True


@pytest.mark.parametrize("change", ["short", "list", "noncanonical-bit", "noncanonical-counter"])
def test_incomplete_row_or_noncanonical_number_cannot_be_coerced(change: str) -> None:
    aliases, supplied_values = supplied("task")
    values: Any = supplied_values
    if change == "short":
        values = values[:-1]
    elif change == "list":
        values = list(values)
    else:
        changed = list(values)
        changed[aliases.index("lease_owner_present" if change == "noncanonical-bit" else "attempt_count")] = b"01"
        values = tuple(changed)
    result = raw.decode_raw_snapshot("task", IDENTITY, aliases, [values])
    assert result.projection_state == "invalid"
    assert result.row is None
