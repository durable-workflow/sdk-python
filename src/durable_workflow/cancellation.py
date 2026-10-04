"""Immutable cancellation metadata recorded by the workflow runtime."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any


class CancellationPolicy(str, Enum):
    """Cancellation at an awaiting operation. Activities default to Try, children to Abandon."""

    TRY_CANCEL = "try_cancel"
    WAIT_CANCELLATION_COMPLETED = "wait_cancellation_completed"
    ABANDON = "abandon"


class ParentClosePolicy(str, Enum):
    """Open-child behavior after parent closure. REQUEST_CANCEL is legacy terminal cancellation."""

    ABANDON = "abandon"
    REQUEST_CANCEL = "request_cancel"
    REQUEST_CANCELLATION = "request_cancellation"
    TERMINATE = "terminate"


def _canonical_child_policies(options: Mapping[str, Any]) -> dict[str, str]:
    policies: dict[str, str] = {}
    for name, enum in (
        ("parent_close_policy", ParentClosePolicy),
        ("cancellation_policy", CancellationPolicy),
    ):
        value = options.get(name)
        if value is None:
            continue
        if isinstance(value, Enum) and not isinstance(value, enum):
            raise ValueError(f"child workflow {name} must be a supported policy")
        if not isinstance(value, str):
            raise ValueError(f"child workflow {name} must be a supported policy")
        try:
            policies[name] = enum(value).value
        except ValueError as error:
            raise ValueError(f"child workflow {name} must be a supported policy") from error
    return policies


def _canonical_activity_policy(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Enum) and not isinstance(value, CancellationPolicy):
        raise ValueError("remote activity cancellation_policy must be a supported policy")
    if not isinstance(value, str):
        raise ValueError("remote activity cancellation_policy must be a supported policy")
    try:
        return CancellationPolicy(value).value
    except ValueError as error:
        raise ValueError("remote activity cancellation_policy must be a supported policy") from error


def _text(snapshot: Mapping[str, Any], key: str) -> str:
    value = snapshot.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"cancellation {key} must be a non-empty string")
    return value


def _timestamp(value: str) -> datetime:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value):
        raise ValueError("cancellation timestamp requires an ISO date, time and timezone")
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


@dataclass(frozen=True)
class CancellationLineage:
    """One local request in the ordered root-to-descendant lineage."""

    request_id: str
    workflow_instance_id: str
    workflow_run_id: str

    def to_dict(self) -> dict[str, str]:
        return {
            "request_id": self.request_id,
            "workflow_instance_id": self.workflow_instance_id,
            "workflow_run_id": self.workflow_run_id,
        }


@dataclass(frozen=True)
class CancellationContext:
    """Original request metadata and budget, visible at committed delivery.

    Nested metadata is immutable. A child has a distinct local request ID
    while retaining its root identity and request time. A scoped child can
    narrow its cleanup deadline while preserving the original root budget.
    """

    request_id: str
    root_request_id: str
    root_workflow_instance_id: str
    root_workflow_run_id: str
    parent_request_id: str | None
    reason: str | None
    requester: Mapping[str, str]
    source: str
    requested_at: datetime
    cleanup_deadline_at: datetime
    lineage: tuple[CancellationLineage, ...]
    _replay_clock: Callable[[], datetime] | None = field(default=None, repr=False, compare=False)
    scope_origin: ScopedCancellationContext | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "requester", MappingProxyType(dict(self.requester)))
        object.__setattr__(self, "lineage", tuple(self.lineage))

    @property
    def deadline(self) -> datetime:
        """The original immutable cleanup deadline."""
        return self.cleanup_deadline_at

    def remaining(self) -> float:
        """Seconds left at the consumed replay boundary, clamped to zero.

        Available only during the workflow replay that delivered this context.
        Detached metadata has no clock. Host time never supplies this value.
        """
        if self._replay_clock is None:
            raise RuntimeError("cancellation remaining time requires active workflow replay")
        return max(0.0, (self.cleanup_deadline_at - self._replay_clock()).total_seconds())

    def _with_replay_clock(self, clock: Callable[[], datetime]) -> CancellationContext:
        return replace(self, _replay_clock=clock)

    @classmethod
    def from_dict(cls, snapshot: Mapping[str, Any]) -> CancellationContext:
        schema = snapshot.get("schema")
        if schema not in ("durable-workflow.cancellation-context/v1", "durable-workflow.cancellation-context/v2"):
            raise ValueError("unsupported cancellation context schema")
        scope_origin = None
        if schema == "durable-workflow.cancellation-context/v2":
            origin = snapshot.get("scope_origin")
            if not isinstance(origin, Mapping):
                raise ValueError("scoped run cancellation requires its original scope context")
            scope_origin = ScopedCancellationContext.from_dict(origin)
        elif "scope_origin" in snapshot or "scope_authority_deadline_at" in snapshot:
            raise ValueError("legacy cancellation context cannot discard a scoped origin")
        requester = snapshot.get("requester")
        if not isinstance(requester, Mapping) or not requester:
            raise ValueError("cancellation requester must identify its caller")
        normalized_requester: dict[str, str] = {}
        for key, value in requester.items():
            if key not in {"type", "id", "label"} or not isinstance(value, str) or value == "":
                raise ValueError("cancellation requester contains unsupported metadata")
            normalized_requester[key] = value
        lineage = snapshot.get("lineage")
        if not isinstance(lineage, list) or not lineage:
            raise ValueError("cancellation lineage must contain the root request")
        normalized = []
        for entry in lineage:
            if not isinstance(entry, Mapping):
                raise ValueError("cancellation lineage entry is invalid")
            normalized.append(CancellationLineage(
                _text(entry, "request_id"), _text(entry, "workflow_instance_id"), _text(entry, "workflow_run_id"),
            ))
        request_id = _text(snapshot, "request_id")
        root_request_id = _text(snapshot, "root_request_id")
        root_instance_id = _text(snapshot, "root_workflow_instance_id")
        root_run_id = _text(snapshot, "root_workflow_run_id")
        parent_request_id = snapshot.get("parent_request_id")
        reason = snapshot.get("reason")
        if (
            parent_request_id is not None and (not isinstance(parent_request_id, str) or not parent_request_id)
            or reason is not None and not isinstance(reason, str)
        ):
            raise ValueError("cancellation parent identity or reason is invalid")
        if len({entry.request_id for entry in normalized}) != len(normalized) or len({
            entry.workflow_run_id for entry in normalized
        }) != len(normalized):
            raise ValueError("cancellation lineage cannot contain a cycle")
        expected_parent = normalized[-2].request_id if len(normalized) > 1 else None
        if (
            normalized[0] != CancellationLineage(root_request_id, root_instance_id, root_run_id)
            or normalized[-1].request_id != request_id
            or scope_origin is None and parent_request_id != expected_parent
        ):
            raise ValueError("cancellation lineage does not match its request identities")
        requested_at = _timestamp(_text(snapshot, "requested_at"))
        deadline = _timestamp(_text(snapshot, "cleanup_deadline_at"))
        if deadline <= requested_at:
            raise ValueError("cancellation deadline must follow the original request")
        if scope_origin is not None:
            _assert_scope_origin(snapshot, scope_origin, normalized, normalized_requester, requested_at, deadline)
        return cls(
            request_id, root_request_id, root_instance_id, root_run_id, parent_request_id,
            reason, normalized_requester, _text(snapshot, "source"), requested_at, deadline, tuple(normalized),
            scope_origin=scope_origin,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return detached metadata in the portable context schema."""
        snapshot: dict[str, Any] = {
            "schema": "durable-workflow.cancellation-context/v1" if self.scope_origin is None
            else "durable-workflow.cancellation-context/v2",
            "request_id": self.request_id,
            "root_request_id": self.root_request_id,
            "root_workflow_instance_id": self.root_workflow_instance_id,
            "root_workflow_run_id": self.root_workflow_run_id,
            "parent_request_id": self.parent_request_id,
            "reason": self.reason,
            "requester": dict(self.requester),
            "source": self.source,
            "requested_at": self.requested_at.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "cleanup_deadline_at": self.cleanup_deadline_at.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "lineage": [entry.to_dict() for entry in self.lineage],
        }
        if self.scope_origin is not None:
            snapshot["scope_origin"] = self.scope_origin.to_dict()
            snapshot["scope_authority_deadline_at"] = snapshot["cleanup_deadline_at"]
        return snapshot


@dataclass(frozen=True)
class ScopedCancellationLineage:
    """One immutable original scope address and its bounded cleanup budget."""

    request_id: str
    workflow_instance_id: str
    workflow_run_id: str
    scope_id: str
    cleanup_deadline_at: datetime

    def to_dict(self) -> dict[str, str]:
        return {
            "request_id": self.request_id,
            "workflow_instance_id": self.workflow_instance_id,
            "workflow_run_id": self.workflow_run_id,
            "scope_id": self.scope_id,
            "cleanup_deadline_at": self.cleanup_deadline_at.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        }


@dataclass(frozen=True)
class ScopedCancellationContext:
    """Original scope ancestry carried by a candidate cooperative child request.

    This is immutable metadata, not permission to enter a scope body.
    """

    root_context: CancellationContext
    lineage: tuple[ScopedCancellationLineage, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "lineage", tuple(self.lineage))

    @property
    def request_id(self) -> str:
        return self.lineage[-1].request_id

    @property
    def parent_request_id(self) -> str | None:
        return self.lineage[-2].request_id if len(self.lineage) > 1 else None

    @property
    def workflow_instance_id(self) -> str:
        return self.lineage[-1].workflow_instance_id

    @property
    def workflow_run_id(self) -> str:
        return self.lineage[-1].workflow_run_id

    @property
    def scope_id(self) -> str:
        return self.lineage[-1].scope_id

    @property
    def root_scope_id(self) -> str:
        return self.lineage[0].scope_id

    @property
    def requested_at(self) -> datetime:
        return self.root_context.requested_at

    @property
    def root_deadline(self) -> datetime:
        return self.root_context.deadline

    @property
    def deadline(self) -> datetime:
        return self.lineage[-1].cleanup_deadline_at

    @classmethod
    def from_dict(cls, snapshot: Mapping[str, Any]) -> ScopedCancellationContext:
        if set(snapshot) != {"schema", "root_context", "lineage"}:
            raise ValueError("scoped cancellation context has missing or unsupported fields")
        root_snapshot = snapshot["root_context"]
        if (snapshot["schema"] != "durable-workflow.scoped-cancellation-context/v1"
            or not isinstance(root_snapshot, Mapping)
            or root_snapshot.get("schema") != "durable-workflow.cancellation-context/v1"):
            raise ValueError("scoped cancellation requires its original root context")
        root = CancellationContext.from_dict(root_snapshot)
        if (set(root_snapshot) != set(root.to_dict()) or root.request_id != root.root_request_id
            or root.parent_request_id is not None or len(root.lineage) != 1):
            raise ValueError("scoped cancellation root must contain the original root request")
        lineage = snapshot["lineage"]
        if not isinstance(lineage, list) or not lineage:
            raise ValueError("scoped cancellation lineage must contain its root address")
        requests: set[str] = set()
        addresses: set[tuple[str, str]] = set()
        instances_by_run: dict[str, str] = {}
        last_run: str | None = None
        deadline = root.deadline
        normalized = []
        for index, entry in enumerate(lineage):
            if not isinstance(entry, Mapping) or set(entry) != {
                "request_id", "workflow_instance_id", "workflow_run_id", "scope_id", "cleanup_deadline_at",
            }:
                raise ValueError("scoped cancellation address has missing or unsupported fields")
            address = ScopedCancellationLineage(
                _text(entry, "request_id"), _text(entry, "workflow_instance_id"), _text(entry, "workflow_run_id"),
                _text(entry, "scope_id"), _timestamp(_text(entry, "cleanup_deadline_at")),
            )
            if index == 0 and (
                address.request_id != root.request_id or address.workflow_instance_id != root.root_workflow_instance_id
                or address.workflow_run_id != root.root_workflow_run_id or address.cleanup_deadline_at != root.deadline
            ):
                raise ValueError("scoped cancellation root address does not match its request")
            key = (address.workflow_run_id, address.scope_id)
            if address.request_id in requests or key in addresses:
                raise ValueError("scoped cancellation cannot repeat a request or address")
            if address.workflow_run_id in instances_by_run and (
                instances_by_run[address.workflow_run_id] != address.workflow_instance_id
                or last_run != address.workflow_run_id
            ):
                raise ValueError("scoped cancellation cannot reenter or reassign an earlier run")
            if address.cleanup_deadline_at <= root.requested_at or address.cleanup_deadline_at > deadline:
                raise ValueError("scoped cancellation cannot extend a descendant budget")
            normalized.append(address)
            requests.add(address.request_id)
            addresses.add(key)
            instances_by_run[address.workflow_run_id] = address.workflow_instance_id
            last_run = address.workflow_run_id
            deadline = address.cleanup_deadline_at
        return cls(root, tuple(normalized))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "durable-workflow.scoped-cancellation-context/v1",
            "root_context": self.root_context.to_dict(),
            "lineage": [entry.to_dict() for entry in self.lineage],
        }


def _assert_scope_origin(
    snapshot: Mapping[str, Any], origin: ScopedCancellationContext, lineage: list[CancellationLineage],
    requester: dict[str, str], requested_at: datetime, deadline: datetime,
) -> None:
    root = origin.root_context
    last = lineage[-1]
    if (
        snapshot.get("parent_request_id") != origin.request_id
        or snapshot["root_request_id"] != root.root_request_id
        or snapshot["root_workflow_instance_id"] != root.root_workflow_instance_id
        or snapshot["root_workflow_run_id"] != root.root_workflow_run_id
        or snapshot.get("reason") != root.reason or requester != root.requester
        or _text(snapshot, "source") != root.source or requested_at != root.requested_at
        or deadline != _timestamp(_text(snapshot, "scope_authority_deadline_at")) or deadline > origin.deadline
        or last.request_id in {entry.request_id for entry in origin.lineage}
        or last.workflow_run_id in {entry.workflow_run_id for entry in origin.lineage}
    ):
        raise ValueError("run cancellation does not preserve its original scope context")
    expected = [root.lineage[0]]
    for entry in origin.lineage:
        if entry.workflow_run_id == root.root_workflow_run_id:
            continue
        address = CancellationLineage(entry.request_id, entry.workflow_instance_id, entry.workflow_run_id)
        if expected[-1].workflow_run_id == entry.workflow_run_id:
            expected[-1] = address
        else:
            expected.append(address)
    expected.append(last)
    if lineage != expected:
        raise ValueError("run cancellation lineage discards or replaces its original scope ancestry")
