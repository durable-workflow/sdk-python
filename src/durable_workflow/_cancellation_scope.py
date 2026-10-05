"""Canonical scope opening proof on the original worker claim.

An opening receipt identifies an authored scope. It does not grant permission
to execute scoped cancellation or change the worker's advertised capabilities.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypeGuard

from .errors import ServerError


def scope_identity(value: Any) -> TypeGuard[str]:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        return len(value.encode("utf-8")) <= 255
    except UnicodeError:
        return False


def _invalid() -> ServerError:
    return ServerError(200, {"reason": "invalid_cancellation_scope_opening"})


def _same(value: Any, expected: Any) -> bool:
    return type(value) is type(expected) and value == expected


_ADMISSIONS = {
    "ActivityScheduled", "TimerScheduled", "ChildWorkflowScheduled", "ConditionWaitOpened", "SignalWaitOpened",
}
_OPERATIONS = _ADMISSIONS | {
    "ActivityStarted", "ActivityCompleted", "ActivityFailed", "ActivityTimedOut", "ActivityCancelled",
    "ActivityRetryScheduled", "TimerFired", "TimerCancelled", "ChildRunStarted", "ChildRunCompleted",
    "ChildRunFailed", "ChildRunCancelled", "ChildRunTerminated", "ConditionWaitSatisfied",
    "ConditionWaitTimedOut", "ConditionWaitCancelled", "SignalWaitReceived", "SignalWaitTimedOut",
    "SignalWaitCancelled",
}


@dataclass(frozen=True)
class CancellationScopeHistory:
    """Original opening tree and immediate operation memberships for replay."""

    openings: dict[int, dict[str, Any]]
    memberships: dict[int, str]

    @classmethod
    def read(cls, history: Sequence[dict[str, Any]], run_id: str) -> CancellationScopeHistory:
        openings: dict[int, dict[str, Any]] = {}
        memberships: dict[int, str] = {}
        scopes: dict[str, int] = {}
        event_ids: set[str] = set()
        last_history_sequence = last_opening = 0
        namespace: str | None = None
        has_scopes = any(event.get("event_type", event.get("type")) == "CancellationScopeOpened" for event in history)
        if has_scopes:
            kinds = [event.get("event_type", event.get("type")) for event in history[:2]]
            if not kinds or not (kinds[0] == "WorkflowStarted" or kinds == ["StartAccepted", "WorkflowStarted"]):
                raise ValueError("invalid_cancellation_scope_history: missing original workflow start")
        for event in history:
            kind = event.get("event_type", event.get("type"))
            payload = event.get("payload")
            if has_scopes:
                event_id, event_sequence = event.get("id"), event.get("sequence")
                incoming_namespace = event.get("namespace")
                if (
                    not scope_identity(event_id) or event_id in event_ids
                    or type(event_sequence) is not int or event_sequence <= last_history_sequence
                    or not scope_identity(incoming_namespace)
                    or (namespace is not None and namespace != incoming_namespace)
                    or not isinstance(payload, dict) or not isinstance(kind, str) or not kind
                ):
                    raise ValueError(
                        "invalid_cancellation_scope_history: changed canonical identity, order or namespace",
                    )
                event_ids.add(event_id)
                last_history_sequence = event_sequence
                namespace = incoming_namespace
            if not isinstance(payload, dict):
                payload = {}
            sequence = payload.get("sequence")
            if kind == "CancellationScopeOpened":
                scope_id, parent = payload.get("scope_id"), payload.get("parent_scope_id")
                if (
                    payload.get("schema") != "durable-workflow.cancellation-scope/v1"
                    or not run_id or payload.get("workflow_run_id") != run_id
                    or type(sequence) is not int or sequence <= last_opening or sequence > (1 << 63) - 1
                    or sequence in memberships
                    or not scope_identity(scope_id) or scope_id == "root" or scope_id in scopes
                    or not scope_identity(parent) or (parent != "root" and parent not in scopes)
                    or type(payload.get("shield_parent")) is not bool
                ):
                    raise ValueError("invalid_cancellation_scope_history: invalid canonical opening tree")
                last_opening = sequence
                scopes[scope_id] = sequence
                openings[sequence] = {
                    "scope_id": scope_id, "parent_scope_id": parent, "shield_parent": payload["shield_parent"],
                }
                continue
            if kind not in _OPERATIONS:
                continue
            membership: str | None = None
            for snapshot in (payload, *(payload.get(name) for name in ("activity", "timer", "child_workflow"))):
                if not isinstance(snapshot, dict) or "cancellation_scope_id" not in snapshot:
                    continue
                incoming = snapshot["cancellation_scope_id"]
                if not scope_identity(incoming) or (membership is not None and membership != incoming):
                    raise ValueError("invalid_cancellation_scope_history: contradictory operation membership")
                membership = incoming
            if membership is None and kind not in _ADMISSIONS:
                continue
            membership = membership or "root"
            if membership != "root" and (
                type(sequence) is not int or sequence < 1 or membership not in scopes or scopes[membership] >= sequence
            ):
                raise ValueError("invalid_cancellation_scope_history: scope must precede original operation admission")
            if type(sequence) is not int or sequence < 1:
                continue
            if sequence in openings or (sequence in memberships and memberships[sequence] != membership):
                raise ValueError("cancellation_scope_membership_changed: operation changed its original scope")
            memberships[sequence] = membership
        return cls(openings, memberships)


@dataclass(frozen=True)
class CancellationScopeOpenReceipt:
    scope_id: str
    history_event_id: str
    sequence: int
    parent_scope_id: str
    shield_parent: bool
    duplicate: bool
    history: tuple[dict[str, Any], ...]

    @staticmethod
    def acknowledge(receipt: Any, expected: Mapping[str, Any]) -> str:
        if not isinstance(receipt, dict) or any(
            not _same(receipt.get(key), value)
            for key, value in expected.items() if key != "namespace"
        ):
            raise _invalid()
        if (
            receipt.get("opened") is not True
            or type(receipt.get("duplicate")) is not bool
            or receipt.get("claim_released") is not False
            or receipt.get("created_task_ids") != []
            or "reason" not in receipt or receipt["reason"] is not None
            or not scope_identity(receipt.get("scope_id")) or receipt["scope_id"] == "root"
            or not scope_identity(receipt.get("history_event_id"))
            or not isinstance(receipt.get("history_refresh_page_token"), str)
            or not receipt["history_refresh_page_token"].strip()
        ):
            raise _invalid()
        return str(receipt["history_refresh_page_token"])

    @classmethod
    def from_history(
        cls, receipt: dict[str, Any], history: Sequence[dict[str, Any]], expected: Mapping[str, Any],
    ) -> CancellationScopeOpenReceipt:
        cls.acknowledge(receipt, expected)
        try:
            cls_history = CancellationScopeHistory.read(history, expected["workflow_run_id"])
        except ValueError as error:
            raise _invalid() from error
        if cls_history.openings.get(expected["sequence"], {}).get("scope_id") != receipt["scope_id"]:
            raise _invalid()
        kinds = [event.get("event_type", event.get("type")) for event in history[:2]]
        if not kinds or not (kinds[0] == "WorkflowStarted" or kinds == ["StartAccepted", "WorkflowStarted"]):
            raise _invalid()
        event_ids: set[str] = set()
        scopes: set[str] = set()
        last_history_sequence = last_scope_sequence = 0
        matching: dict[str, Any] | None = None
        for event in history:
            payload = event.get("payload")
            kind = event.get("event_type", event.get("type"))
            event_id = event.get("id")
            event_sequence = event.get("sequence")
            if (
                not scope_identity(event_id) or event_id in event_ids
                or type(event_sequence) is not int or event_sequence <= last_history_sequence
                or event.get("namespace") != expected["namespace"]
                or not isinstance(kind, str) or not kind.strip()
                or not isinstance(payload, dict)
            ):
                raise _invalid()
            event_ids.add(event_id)
            last_history_sequence = event_sequence
            if kind != "CancellationScopeOpened":
                continue
            scope_id = payload.get("scope_id")
            parent = payload.get("parent_scope_id")
            sequence = payload.get("sequence")
            if (
                payload.get("schema") != "durable-workflow.cancellation-scope/v1"
                or payload.get("workflow_run_id") != expected["workflow_run_id"]
                or not scope_identity(scope_id) or scope_id == "root" or scope_id in scopes
                or not scope_identity(parent) or (parent != "root" and parent not in scopes)
                or type(payload.get("shield_parent")) is not bool
                or type(sequence) is not int or sequence <= last_scope_sequence
            ):
                raise _invalid()
            scopes.add(scope_id)
            last_scope_sequence = sequence
            if event_id == receipt["history_event_id"]:
                if any(not _same(payload.get(key), receipt[key]) for key in (
                    "scope_id", "sequence", "parent_scope_id", "shield_parent",
                )):
                    raise _invalid()
                matching = payload
        if matching is None:
            raise _invalid()
        return cls(
            matching["scope_id"], receipt["history_event_id"], matching["sequence"],
            matching["parent_scope_id"], matching["shield_parent"], receipt["duplicate"], tuple(history),
        )
