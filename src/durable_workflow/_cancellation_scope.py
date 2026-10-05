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
