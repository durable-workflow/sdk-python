"""Pure binding and normalization of supplied historical fixture projections.

No driver, query, capture-completeness proof, transport or process is provided.
The future collector must establish complete bounded capture before using this
transform; a complete projection retains the decoder's unknown phase verdict.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Literal, cast

from tests.worker_startup_snapshot import (
    RUN_TIMES,
    TASK_TIMES,
    OriginalIdentity,
    ProjectionRefusal,
    SnapshotProjection,
    decode_snapshot,
    identity_fields,
    require,
    timestamp,
)

Kind = Literal["task", "run"]
SQL_DIRECTORY = Path(__file__).parent / "fixtures/worker_startup_reconstruction/sql-proposal"
SQL_IDENTITIES = {
    "task": (1140, "4385b4d0f08e22be9f053f282283111282f7215d12e9247d03ca0642e3e2b825"),
    "run": (675, "025db37ab5311650d694f139941e206c2ae085d9b862426efa409a53ddd2ae13"),
}
TASK_COLUMNS = (
    "database_time_utc", "session_time_zone", "system_time_zone", "original_task_id", "original_run_id",
    "namespace", "task_type", "status", "queue", "compatibility", "available_at", "leased_at", "lease_expires_at",
    "lease_owner_present", "attempt_count", "repair_count", "last_dispatch_attempt_at", "last_dispatched_at",
    "last_claim_failed_at", "repair_available_at", "dispatch_error_present", "claim_error_present",
    "task_error_present",
)
RUN_COLUMNS = (
    "database_time_utc", "session_time_zone", "system_time_zone", "original_run_id", "workflow_instance_id",
    "namespace", "status", "execution_deadline_at", "run_deadline_at",
)
TIMESTAMP_FIELDS = TASK_TIMES | RUN_TIMES | {"database_time_utc"}
SCALAR_LIMITS = {
    **dict.fromkeys(TIMESTAMP_FIELDS, 26),
    **dict.fromkeys(("original_task_id", "original_run_id"), 26),
    **dict.fromkeys(("attempt_count", "repair_count"), 10),
    **dict.fromkeys(("lease_owner_present", "dispatch_error_present", "claim_error_present", "task_error_present"), 1),
    "namespace": 128, "workflow_instance_id": 191, "status": 32, "task_type": 32,
    "session_time_zone": 64, "system_time_zone": 64, "queue": 255, "compatibility": 255,
}


def bind_original_query(kind: Kind, identity: OriginalIdentity) -> tuple[str, dict[str, str]]:
    """Return immutable SQL and separate values; no driver escaping/execution."""
    parameters = identity_fields(kind, identity)
    length, digest = SQL_IDENTITIES[kind]
    try:
        with (SQL_DIRECTORY / f"original-{kind}.sql").open("rb") as source:
            raw = source.read(length + 1)
    except OSError as error:
        raise ProjectionRefusal("SQL_SOURCE_UNAVAILABLE") from error
    require(len(raw) == length and hashlib.sha256(raw).hexdigest() == digest, "SQL_SOURCE_CHANGED")
    query = raw.decode("ascii")
    tokens = re.findall(r":([a-z_]+)", query)
    require(len(tokens) == len(parameters) and set(tokens) == set(parameters), "SQL_TOKEN_SET_INVALID")
    translated = re.sub(r":([a-z_]+)", lambda match: f"%({match[1]})s", query)
    return translated, parameters


def normalize_scalar(field: str, value: Any) -> str | None:
    if value is None:
        return None
    require(type(value) is bytes and len(value) <= SCALAR_LIMITS[field], "RAW_SCALAR_TYPE_OR_SIZE", field)
    try:
        normalized = cast(bytes, value).decode("ascii")
    except UnicodeDecodeError as error:
        raise ProjectionRefusal("RAW_SCALAR_ENCODING", field) from error
    if field in TIMESTAMP_FIELDS:
        if len(normalized) == 19:
            normalized += ".000000"
        timestamp(normalized, field)
    return normalized


def decode_raw_snapshot(kind: Kind, identity: OriginalIdentity, aliases: Any, rows: Any) -> SnapshotProjection:
    """Validate supplied positional bytes only; metadata/termination remain unproved."""
    try:
        identity_fields(kind, identity)
        expected = TASK_COLUMNS if kind == "task" else RUN_COLUMNS
        require(
            type(aliases) is tuple and len(aliases) == len(expected)
            and all(type(alias) is str for alias in aliases) and aliases == expected,
            "RAW_COLUMN_SET_INVALID",
        )
        require(type(rows) in (tuple, list) and len(rows) <= 1, "ROW_COUNT_OR_TYPE_INVALID")
        normalized = []
        for row in rows:
            require(type(row) is tuple and len(row) == len(expected), "RAW_ROW_SHAPE_INVALID")
            normalized.append({
                field: normalize_scalar(field, value) for field, value in zip(expected, row, strict=True)
            })
        return decode_snapshot(kind, identity, normalized)
    except ProjectionRefusal as error:
        return SnapshotProjection("invalid", error.code, error.field)
