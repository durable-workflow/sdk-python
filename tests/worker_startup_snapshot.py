"""Pure validation of proposed normalized, fixture-only historical SQL rows.

No query, driver, credential, transport, capture bound or clock conversion is
implemented. Complete projection never establishes a claim or phase verdict.
"""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any, Literal, cast

TASK_STATUSES = {"ready", "leased", "cancelled", "completed", "failed"}
RUN_STATUSES = {"pending", "running", "waiting", "cancelled", "terminated", "completed", "failed"}
COMMON_FIELDS = {"database_time_utc", "session_time_zone", "system_time_zone", "original_run_id", "namespace", "status"}
TASK_TIMES = {
    "available_at",
    "leased_at",
    "lease_expires_at",
    "last_dispatch_attempt_at",
    "last_dispatched_at",
    "last_claim_failed_at",
    "repair_available_at",
}
PRESENCE_FIELDS = {"lease_owner_present", "dispatch_error_present", "claim_error_present", "task_error_present"}
COUNTER_FIELDS = {"attempt_count", "repair_count"}
TASK_FIELDS = (
    COMMON_FIELDS
    | TASK_TIMES
    | PRESENCE_FIELDS
    | COUNTER_FIELDS
    | {
        "original_task_id",
        "task_type",
        "queue",
        "compatibility",
    }
)
RUN_TIMES = {"execution_deadline_at", "run_deadline_at"}
RUN_FIELDS = COMMON_FIELDS | RUN_TIMES | {"workflow_instance_id"}
State = Literal["complete", "missing", "truncated", "invalid"]


@dataclass(frozen=True)
class OriginalIdentity:
    namespace: str
    workflow_instance_id: str
    original_run_id: str
    original_task_id: str


@dataclass(frozen=True)
class SnapshotProjection:
    projection_state: State
    refusal_code: str | None = None
    field: str | None = None
    row: dict[str, Any] | None = None
    phase_verdict: Literal["unknown"] = "unknown"


class ProjectionRefusal(ValueError):
    def __init__(self, code: str, field: str | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.field = field


def require(value: bool, code: str, field: str | None = None) -> None:
    if not value:
        raise ProjectionRefusal(code, field)


def text(value: Any, limit: int) -> bool:
    return type(value) is str and len(value) <= limit and all(32 <= ord(char) < 127 for char in value)


def identity_fields(kind: str, identity: OriginalIdentity) -> dict[str, str]:
    require(type(kind) is str and len(kind) <= 4 and kind in {"task", "run"}, "UNKNOWN_PROJECTION")
    require(type(identity) is OriginalIdentity, "IDENTITY_INVALID")
    require(
        text(identity.namespace, 128)
        and bool(identity.namespace)
        and text(identity.workflow_instance_id, 191)
        and bool(identity.workflow_instance_id),
        "IDENTITY_INVALID",
    )
    for value in (identity.original_run_id, identity.original_task_id):
        require(
            type(value) is str
            and len(value) == 26
            and re.fullmatch(r"[0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{26}", value) is not None,
            "IDENTITY_INVALID",
        )
    return {
        "namespace": identity.namespace,
        "original_run_id": identity.original_run_id,
        ("original_task_id" if kind == "task" else "workflow_instance_id"): identity.original_task_id
        if kind == "task"
        else identity.workflow_instance_id,
    }


def timestamp(value: Any, field: str, *, nullable: bool = True) -> str | None:
    if value is None and nullable:
        return None
    require(
        type(value) is str
        and len(value) == 26
        and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}", value) is not None,
        "TIMESTAMP_INVALID",
        field,
    )
    try:
        dt.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError as error:
        raise ProjectionRefusal("TIMESTAMP_INVALID", field) from error
    return cast(str, value)


def counter(value: Any, field: str) -> int:
    if type(value) is str:
        require(len(value) <= 10 and re.fullmatch(r"0|[1-9][0-9]{0,9}", value) is not None, "COUNTER_INVALID", field)
        value = int(value)
    require(type(value) is int and 0 <= value <= 4_294_967_295, "COUNTER_INVALID", field)
    return cast(int, value)


def presence(value: Any, field: str) -> bool:
    require(
        (type(value) is int and value in (0, 1)) or (type(value) is str and value in ("0", "1")),
        "PRESENCE_BIT_INVALID",
        field,
    )
    return value in (1, "1")


def typed_row(kind: str, row: dict[str, Any], expected: dict[str, str]) -> dict[str, Any]:
    for field, value in expected.items():
        require(type(row[field]) is str and row[field] == value, "IDENTITY_MISMATCH", field)
    for field in ("session_time_zone", "system_time_zone"):
        require(text(row[field], 64) and bool(row[field]), "TIMEZONE_LABEL_INVALID", field)
    require(
        text(row["status"], 32) and row["status"] in (TASK_STATUSES if kind == "task" else RUN_STATUSES),
        "STATUS_INVALID",
        "status",
    )
    result = {field: row[field] for field in expected}
    result.update({field: row[field] for field in ("status", "session_time_zone", "system_time_zone")})
    result["database_time_utc"] = timestamp(row["database_time_utc"], "database_time_utc", nullable=False)
    if kind == "task":
        require(type(row["task_type"]) is str and row["task_type"] == "workflow", "WRONG_TASK_FAMILY", "task_type")
        result["task_type"] = row["task_type"]
        for field in ("queue", "compatibility"):
            require(row[field] is None or text(row[field], 255), "TEXT_INVALID", field)
            result[field] = row[field]
        for field in COUNTER_FIELDS:
            result[field] = counter(row[field], field)
        for field in PRESENCE_FIELDS:
            result[field] = presence(row[field], field)
    for field in TASK_TIMES if kind == "task" else RUN_TIMES:
        result[field] = timestamp(row[field], field)
    return result


def decode_snapshot(
    kind: Literal["task", "run"],
    identity: OriginalIdentity,
    rows: Any,
    *,
    source_state: State = "complete",
) -> SnapshotProjection:
    """Validate supplied normalized rows; the future driver must prove that normalization.

    Complete means one typed projection matches the supplied original identity.
    Raw capture, clock interpretation and row/phase/claim consistency stay unproved.
    """
    try:
        expected = identity_fields(kind, identity)
        require(
            type(source_state) is str
            and len(source_state) <= 9
            and source_state in {"complete", "missing", "truncated", "invalid"},
            "SOURCE_STATE_INVALID",
        )
        if source_state != "complete":
            return SnapshotProjection(source_state, "SOURCE_" + source_state.upper())
        if rows is None:
            return SnapshotProjection("missing", "INPUT_MISSING")
        require(type(rows) is list and len(rows) <= 1, "ROW_COUNT_OR_TYPE_INVALID")
        if not rows:
            return SnapshotProjection("missing", "ROW_MISSING")
        row = rows[0]
        require(
            type(row) is dict
            and len(row) == len(TASK_FIELDS if kind == "task" else RUN_FIELDS)
            and all(type(key) is str and len(key) <= 64 for key in row)
            and set(row) == (TASK_FIELDS if kind == "task" else RUN_FIELDS),
            "FIELD_SET_INVALID",
        )
        return SnapshotProjection("complete", row=typed_row(kind, row, expected))
    except ProjectionRefusal as error:
        return SnapshotProjection("invalid", error.code, error.field)
