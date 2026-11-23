"""Canonical input positions for condition predicate replay."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

CAPABILITY = "condition_wait_input_order"
AUTHORING = "condition_wait_input_order_authoring"
ORDER = "durable_receipt_order"
PENDING_INPUT = "condition_wait_pending_input_sequence"


def marked(payload: Mapping[str, Any]) -> bool:
    return payload.get(CAPABILITY) == ORDER


def positive_sequence(value: Any) -> int | None:
    return value if type(value) is int and value > 0 else None


@dataclass(frozen=True)
class CompetingSelection:
    terminal_id: str
    sequence: int


def competing_selection(events: Sequence[Mapping[str, Any]], group_id: str) -> CompetingSelection | None:
    candidates = [
        (event, payload) for event in events
        if event.get("event_type", event.get("type")) == "SelectionCandidateReady"
        and isinstance(payload := event.get("payload"), Mapping)
        and payload.get("selection_group_id") == group_id
    ]
    if not candidates:
        return None
    if len(candidates) != 1:
        raise ValueError("an ordered selection has more than one competing terminal event")
    candidate, payload = candidates[0]
    terminal_id = payload.get("resolution_event_id")
    sequence = positive_sequence(payload.get("resolution_event_sequence"))
    if (not isinstance(terminal_id, str) or not terminal_id or sequence is None
        or sequence >= (positive_sequence(candidate.get("sequence")) or 0)
        or not any(
            event.get("sequence") == sequence
            and (event.get("id") or event.get("event_id")) == terminal_id
            and event.get("event_type", event.get("type")) == payload.get("resolution_event_type")
            for event in events
        )):
        raise ValueError("ordered selection candidate does not match its terminal history event")
    return CompetingSelection(terminal_id, sequence)


@dataclass
class Wait:
    opening_sequence: int
    scheduled_sequence: int | None = None
    timer_id: str | None = None
    fired_sequence: int | None = None
    settled: bool | None = None
    satisfying_receipt: int | None = None


@dataclass(frozen=True)
class History:
    waits: dict[str, Wait]
    applied_signal_receipts: frozenset[int]
    update_receipts: dict[int, int]


def read(events: Sequence[Mapping[str, Any]]) -> History:
    if not any(
        event.get("event_type", event.get("type")) == "ConditionWaitOpened"
        and isinstance(event.get("payload"), Mapping) and marked(event["payload"])
        for event in events
    ):
        return History({}, frozenset(), {})
    waits: dict[str, Wait] = {}
    receipts: dict[tuple[str, str], int] = {}
    applied_signals: set[int] = set()
    update_receipts: dict[int, int] = {}
    for event in events:
        kind = event.get("event_type", event.get("type"))
        payload = event.get("payload")
        if not isinstance(payload, Mapping):
            continue
        sequence = positive_sequence(event.get("sequence"))
        command_id = event.get("workflow_command_id") or payload.get("workflow_command_id")
        if sequence is not None and isinstance(command_id, str) and command_id:
            if kind in ("SignalReceived", "UpdateAccepted"):
                receipts.setdefault((kind, command_id), sequence)
            elif kind == "SignalApplied":
                receipt = receipts.get(("SignalReceived", command_id))
                if receipt is not None:
                    applied_signals.add(receipt)
            elif kind == "UpdateApplied":
                receipt = receipts.get(("UpdateAccepted", command_id))
                if receipt is not None:
                    update_receipts[sequence] = receipt
        wait_id = payload.get("condition_wait_id")
        if not isinstance(wait_id, str) or not wait_id:
            continue
        if kind == "ConditionWaitOpened" and marked(payload):
            if sequence is None or wait_id in waits:
                raise ValueError("ordered condition has an invalid opening sequence or repeated identity")
            waits[wait_id] = Wait(sequence)
        wait = waits.get(wait_id)
        if wait is None:
            continue
        if kind == "TimerScheduled":
            timer = payload.get("timer_id")
            if (not marked(payload) or payload.get("timer_kind") != "condition_timeout"
                or sequence is None or sequence <= wait.opening_sequence or not isinstance(timer, str)
                or not timer or wait.scheduled_sequence is not None):
                raise ValueError("ordered condition timer does not match its opening")
            wait.scheduled_sequence = sequence
            wait.timer_id = timer
        elif kind == "TimerFired":
            if (not marked(payload) or payload.get("timer_id") != wait.timer_id
                or payload.get("timer_kind") != "condition_timeout" or wait.scheduled_sequence is None
                or sequence is None or sequence <= wait.scheduled_sequence or wait.fired_sequence is not None):
                raise ValueError("ordered condition fire does not match its original timer")
            wait.fired_sequence = sequence
        elif kind in ("ConditionWaitSatisfied", "ConditionWaitTimedOut"):
            satisfied = kind == "ConditionWaitSatisfied"
            receipt = positive_sequence(payload.get("input_receipt_sequence"))
            if (not marked(payload) or wait.settled is not None or sequence is None
                or sequence <= (wait.fired_sequence or wait.opening_sequence)
                or payload.get("timer_fired_sequence") != wait.fired_sequence
                or (receipt is not None and receipt >= sequence)
                or (not satisfied and (wait.fired_sequence is None or receipt is not None))
                or (satisfied and wait.fired_sequence is not None
                    and (receipt is None or receipt >= wait.fired_sequence))):
                raise ValueError("ordered condition outcome does not match its original input boundary")
            wait.settled = satisfied
            wait.satisfying_receipt = receipt
    if any(wait.scheduled_sequence is None for wait in waits.values()):
        raise ValueError("ordered condition is missing its original timer schedule")
    return History(waits, frozenset(applied_signals), update_receipts)
