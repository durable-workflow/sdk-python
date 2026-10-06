"""Original scope requests, frozen operation projections and delivery proofs.

Reading these facts grants no callback execution, stop or publication authority.
The worker must coordinate the original claim before replaying a delivery.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ._cancellation_scope import CancellationScopeHistory, _same, scope_identity
from ._cooperative_cancellation import CancellationDelivery, read_cancellation_history
from .cancellation import CancellationContext, ScopedCancellationContext, ScopedCancellationLineage

_FIELDS = ("activity_members", "timer_members", "wait_members", "child_members")
_KEYS = {
    "activity_members": ("sequence", "activity_execution_id", "descriptor_hash"),
    "timer_members": ("sequence", "timer_id", "descriptor_hash"),
    "wait_members": ("kind", "sequence", "wait_id", "timer_id", "descriptor_hash"),
    "child_members": (
        "sequence", "child_call_id", "child_workflow_instance_id", "child_workflow_run_id",
        "cancellation_policy", "descriptor_hash",
    ),
}
_ID_KEYS = dict(zip(_FIELDS, ("activity_execution_id", "timer_id", "wait_id", "child_call_id"), strict=True))
_POLICIES = {"try_cancel", "wait_cancellation_completed", "abandon"}
_ADMISSIONS = {
    "ActivityScheduled": "activity_members", "TimerScheduled": "timer_members",
    "ChildWorkflowScheduled": "child_members", "ConditionWaitOpened": "wait_members",
    "SignalWaitOpened": "wait_members",
}


def _invalid(message: str) -> ValueError:
    return ValueError("invalid_cancellation_scope_history: " + message)


def scope_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value,
    ) is None:
        raise _invalid("scope authority requires a timestamp with timezone")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as error:
        raise _invalid("invalid scope authority timestamp") from error


def _canonical_time(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _boundary_scope_states(
    context: ScopedCancellationContext, preparation: dict[str, Any],
) -> dict[str, tuple[ScopedCancellationContext, datetime]]:
    states = {context.scope_id: (context, scope_timestamp(preparation["authority_deadline_at"]))}
    for member in preparation["descendant_members"]:
        descendant = ScopedCancellationContext.from_dict(member["cancellation"])
        if (descendant.root_context != context.root_context
            or descendant.lineage[:len(context.lineage)] != context.lineage
            or descendant.scope_id in states):
            raise _invalid("scope descendant changes its original root or accepted lineage")
        states[descendant.scope_id] = (descendant, scope_timestamp(member["authority_deadline_at"]))
    return states


def _delivery_scope_states(
    delivery: dict[str, Any], prefix: Sequence[dict[str, Any]],
) -> dict[str, tuple[ScopedCancellationContext, datetime]]:
    recorded = delivery["payload"]
    preparations = [row for row in prefix if _kind(row) == "CancellationScopeDeliveryPrepared"
                    and row.get("id") == recorded.get("preparation_history_event_id")]
    if len(preparations) != 1 or preparations[0]["sequence"] >= delivery["sequence"]:
        raise _invalid("cleanup operation lacks its original canonical preparation")
    context = ScopedCancellationContext.from_dict(recorded["cancellation"])
    return _boundary_scope_states(context, preparations[0]["payload"])


def _cleanup_operation(event: dict[str, Any], prefix: Sequence[dict[str, Any]], *, local: bool = False) -> bool:
    payload = event.get("payload", {})
    operation = "cleanup local activity" if local else "cleanup timer"
    preparation = payload.get("local_preparation") if local else payload
    snapshot = preparation.get("cancellation_cleanup") if isinstance(preparation, dict) else None
    scope_id = _address(payload, "activity" if local else "timer")
    if local and isinstance(snapshot, dict) and "scope_id" not in snapshot and scope_id == "root":
        return False
    if not isinstance(preparation, dict) or "cancellation_cleanup" not in preparation:
        for delivery in prefix:
            if (_kind(delivery) != "CancellationScopeDelivered"
                or not _positive(delivery.get("sequence")) or not _positive(event.get("sequence"))
                or delivery["sequence"] >= event["sequence"]):
                continue
            if scope_id in _delivery_scope_states(delivery, prefix):
                raise _invalid(operation + " omits its original delivery snapshot")
        return False
    if not isinstance(snapshot, dict):
        raise _invalid(operation + " requires its original delivery snapshot")
    candidates = [row for row in prefix if _kind(row) == "CancellationScopeDelivered"
                  and row.get("id") == snapshot.get("delivery_history_event_id")]
    if len(candidates) != 1:
        raise _invalid(operation + " lacks its earlier canonical delivery")
    delivery = candidates[0]
    recorded = delivery["payload"]
    state = _delivery_scope_states(delivery, prefix).get(scope_id)
    if state is None:
        raise _invalid(operation + " changes its original frozen subtree membership")
    context, authority = state
    ancestor = ScopedCancellationContext.from_dict(recorded["cancellation"])
    expected = {
        "scope_id": ancestor.scope_id, "operation_scope_id": context.scope_id,
        "request_id": ancestor.request_id, "root_request_id": ancestor.root_request_id,
        "delivery_history_event_id": delivery["id"],
        "preparation_history_event_id": recorded["preparation_history_event_id"],
        "cleanup_deadline_at": _canonical_time(ancestor.deadline),
        "authority_deadline_at": _canonical_time(authority),
    }
    sequence = payload.get("sequence")
    deadline = scope_timestamp(expected["authority_deadline_at"])
    timestamp = scope_timestamp(event.get("timestamp", event.get("recorded_at")))
    if (snapshot != expected or scope_id != context.scope_id
        or not _positive(sequence) or sequence < recorded["sequence"] + recorded["sequence_span"]
        or not _positive(event.get("sequence")) or delivery["sequence"] >= event["sequence"]
        or timestamp < scope_timestamp(delivery.get("timestamp", delivery.get("recorded_at")))
        or timestamp >= deadline or not local and (
            not timestamp <= scope_timestamp(payload.get("fire_at")) < deadline
            or payload.get("timer_kind") is not None
        )):
        raise _invalid(operation + " changes its original delivery or authority ceiling")
    return True


def _cleanup_timer(event: dict[str, Any], prefix: Sequence[dict[str, Any]]) -> bool:
    return _cleanup_operation(event, prefix)


def _cleanup_local_activity(event: dict[str, Any], prefix: Sequence[dict[str, Any]]) -> bool:
    payload = event.get("payload", {})
    return (payload.get("local_activity") is True or payload.get("execution_mode") == "local") and (
        _cleanup_operation(event, prefix, local=True)
    )


def _positive(value: Any) -> bool:
    return type(value) is int and 1 <= value <= 2**63 - 1


def _kind(event: dict[str, Any]) -> Any:
    return event.get("event_type", event.get("type"))


def _hash(values: list[Any]) -> str:
    # Native/PHP hashes JSON descriptors, with PHP's default slash escaping.
    encoded = json.dumps(values, ensure_ascii=False, separators=(",", ":"), allow_nan=False).replace("/", "\\/")
    # PHP escapes non-ASCII code points but leaves ASCII DEL literal. Preserve
    # already serialized escapes, including an application string "\\u007f".
    encoded = "".join(json.dumps(char, ensure_ascii=True)[1:-1] if ord(char) > 0x7f else char for char in encoded)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def normalize_scope_members(field: str, members: Any) -> list[dict[str, Any]]:
    if field not in _KEYS or not isinstance(members, list):
        raise _invalid("unknown or non-list scope member projection")
    keys, id_key = _KEYS[field], _ID_KEYS[field]
    identities: set[str] = set()
    sequences: set[int] = set()
    normalized = []
    for member in members:
        if (not isinstance(member, dict) or set(member) != set(keys)
            or not _positive(member.get("sequence")) or not scope_identity(member.get(id_key))
            or member[id_key] in identities or not isinstance(member.get("descriptor_hash"), str)
            or re.fullmatch(r"[a-f0-9]{64}", member["descriptor_hash"]) is None):
            raise _invalid("scope projection changes an original member address")
        if field == "wait_members" and (
            member["kind"] not in {"signal", "condition"}
            or (member["timer_id"] is not None and not scope_identity(member["timer_id"]))
        ):
            raise _invalid("scope wait projection changes its kind or timeout address")
        if field == "child_members" and (
            not scope_identity(member["child_workflow_instance_id"])
            or not scope_identity(member["child_workflow_run_id"]) or member["cancellation_policy"] not in _POLICIES
        ):
            raise _invalid("scope child projection changes its target or policy")
        if field in {"activity_members", "child_members"} and member["sequence"] in sequences:
            raise _invalid("scope projection duplicates an authored position")
        identities.add(member[id_key])
        sequences.add(member["sequence"])
        normalized.append({key: member[key] for key in keys})
    return normalized


def _address(payload: dict[str, Any], descriptor_key: str) -> str:
    descriptor = payload.get(descriptor_key, {})
    if not isinstance(descriptor, dict):
        raise _invalid("scope operation needs its original descriptor")
    nested = descriptor.get("cancellation_scope_id", "root")
    address = payload.get("cancellation_scope_id", nested)
    if not scope_identity(address) or ("cancellation_scope_id" in descriptor and nested != address):
        raise _invalid("scope operation changes its immediate membership")
    return address


def _group_entry(payload: dict[str, Any]) -> dict[str, Any] | None:
    group_id = payload.get("parallel_group_id")
    kind = payload.get("parallel_group_kind")
    if not isinstance(group_id, str):
        return None
    if not isinstance(kind, str):
        prefixes = {"parallel-activities:": "activity", "parallel-calls:": "mixed", "select-calls:": "mixed",
                    "parallel-timers:": "timer", "parallel-children:": "child"}
        kind = next((value for prefix, value in prefixes.items() if group_id.startswith(prefix)), None)
    mode = payload.get("parallel_group_mode")
    if not isinstance(mode, str) or not mode:
        mode = "select" if group_id.startswith("select-calls:") else None
    key = payload.get("selection_member_key")
    if (kind is None or any(type(payload.get(field)) is not int for field in (
        "parallel_group_base_sequence", "parallel_group_size", "parallel_group_index",
    )) or payload["parallel_group_size"] < 1
        or (mode == "select" and not (type(key) is int and key >= 0 or isinstance(key, str) and bool(key)))):
        return None
    value = {
        "parallel_group_id": group_id, "parallel_group_kind": kind,
        "parallel_group_mode": mode if mode == "select" else None,
        "parallel_group_base_sequence": payload["parallel_group_base_sequence"],
        "parallel_group_size": payload["parallel_group_size"], "parallel_group_index": payload["parallel_group_index"],
        "selection_member_key": key if mode == "select" else None,
    }
    for field in ("selection_member_index", "selection_member_base_sequence", "selection_member_size"):
        value[field] = payload.get(field) if type(payload.get(field)) is int else None
    value["selection_member_kind"] = (
        payload.get("selection_member_kind") if isinstance(payload.get("selection_member_kind"), str)
        and payload["selection_member_kind"] else None
    )
    return {key: item for key, item in value.items() if item is not None}


def _group_path(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("parallel_group_path", [])
    if not isinstance(raw, list):
        raise _invalid("scope operation has a malformed original group path")
    path = [value for entry in raw if isinstance(entry, dict) and (value := _group_entry(entry)) is not None]
    if path:
        return path
    entry = _group_entry(payload)
    return [] if entry is None else [entry]


def scope_members_from_prefix(
    field: str, prefix: Sequence[dict[str, Any]], scope_id: str, run_id: str,
) -> list[dict[str, Any]]:
    members = []
    activity_ids: set[str] = set()
    activity_sequences: set[int] = set()
    timer_sequences: dict[int, Any] = {}
    for event in prefix:
        kind, payload = _kind(event), event["payload"]
        sequence = payload.get("sequence")
        if field == "activity_members" and kind == "ActivityScheduled":
            activity = payload.get("activity")
            identity = payload.get("activity_execution_id")
            if (not isinstance(activity, dict) or not scope_identity(identity) or activity.get("id") != identity
                or not _positive(sequence) or identity in activity_ids or sequence in activity_sequences
                or payload.get("cancellation_scope_id", activity.get("cancellation_scope_id", "root"))
                != activity.get("cancellation_scope_id", "root")):
                raise _invalid("scope Activity admission changes identity or membership")
            activity_ids.add(identity)
            activity_sequences.add(sequence)
            if _address(payload, "activity") != scope_id:
                continue
            if _cleanup_local_activity(event, prefix):
                continue
            for preparation in ("local_preparation", "local_group_admission"):
                if isinstance(payload.get(preparation), dict) and isinstance(
                    payload[preparation].get("cancellation_cleanup"), dict,
                ) and payload[preparation]["cancellation_cleanup"].get("scope_id") is not None:
                    raise _invalid("nested cleanup projection requires its original cleanup proof")
            policy = activity.get("cancellation_policy", "try_cancel")
            if (policy not in _POLICIES or ("local_activity" in payload and type(payload["local_activity"]) is not bool)
                or (payload.get("execution_mode") is not None and not isinstance(payload["execution_mode"], str))
                or (activity.get("schedule_to_close_deadline_at") is not None
                    and not isinstance(activity["schedule_to_close_deadline_at"], str))):
                raise _invalid("scope Activity admission changes its cancellation descriptor")
            members.append({"sequence": sequence, "activity_execution_id": identity, "descriptor_hash": _hash([
                scope_id, event["id"], policy, payload.get("local_activity", False), payload.get("execution_mode"),
                activity.get("schedule_to_close_deadline_at"),
            ])})
        if field == "timer_members" and kind == "TimerScheduled" and _address(payload, "timer") == scope_id:
            if _cleanup_timer(event, prefix):
                continue
            identity, delay, fire_at = payload.get("timer_id"), payload.get("delay_seconds"), payload.get("fire_at")
            timer_kind = payload.get("timer_kind")
            if (not _positive(sequence) or not scope_identity(identity) or type(delay) is not int or delay < 0
                or (sequence in timer_sequences and (
                    timer_kind not in {"signal_timeout", "condition_timeout"} or timer_sequences[sequence] != timer_kind
                )) or _canonical_time(scope_timestamp(fire_at)) != fire_at):
                raise _invalid("scope timer changes its original descriptor")
            timer_sequences[sequence] = timer_kind
            members.append({"sequence": sequence, "timer_id": identity, "descriptor_hash": _hash([
                scope_id, event["id"], sequence, identity, delay, fire_at, timer_kind,
                payload.get("condition_wait_id"), payload.get("condition_wait_occurrence_id"),
                payload.get("signal_wait_id"), _group_path(payload),
            ])})
        if field == "wait_members" and kind in {"SignalWaitOpened", "ConditionWaitOpened"} and (
            payload.get("cancellation_scope_id", "root") == scope_id
        ):
            wait_kind = "signal" if kind == "SignalWaitOpened" else "condition"
            identity = payload.get(wait_kind + "_wait_id")
            if not _positive(sequence) or not scope_identity(identity):
                raise _invalid("scope wait changes its original address")
            timers = [row for row in prefix if _kind(row) == "TimerScheduled"
                      and row["payload"].get(wait_kind + "_wait_id") == identity]
            if len(timers) > 1:
                raise _invalid("scope wait duplicates its timeout")
            timer = timers[0] if timers else None
            timeout = timer["payload"] if timer is not None else {}
            if timer is not None and (
                timeout.get("timer_kind") != wait_kind + "_timeout" or timeout.get("sequence") != sequence
                or timer["sequence"] <= event["sequence"] or timeout.get("cancellation_scope_id", "root") != scope_id
                or not scope_identity(timeout.get("timer_id"))
            ):
                raise _invalid("scope wait changes its original timeout")
            members.append({"kind": wait_kind, "sequence": sequence, "wait_id": identity,
                            "timer_id": timeout.get("timer_id"), "descriptor_hash": _hash([
                scope_id, event["id"], wait_kind, sequence, identity, timer["id"] if timer is not None else None,
                timeout.get("timer_id"), payload.get("timeout_seconds"), payload.get("signal_name"),
                payload.get("condition_wait_occurrence_id"), payload.get("condition_key"),
                payload.get("condition_definition_fingerprint"), _group_path(payload),
            ])})
        if field == "child_members" and kind == "ChildWorkflowScheduled" and (
            _address(payload, "child_workflow") == scope_id
        ):
            call_id, instance = payload.get("child_call_id"), payload.get("child_workflow_instance_id")
            target_run, policy = payload.get("child_workflow_run_id"), payload.get("cancellation_policy", "abandon")
            if (not _positive(sequence) or not all(scope_identity(value) for value in (call_id, instance, target_run))
                or target_run == run_id or policy not in _POLICIES):
                raise _invalid("scope child changes its original target or policy")
            last_started_id = None
            for started in prefix:
                next_payload = started["payload"]
                if _kind(started) != "ChildRunStarted" or next_payload.get("sequence") != sequence:
                    continue
                if (started["sequence"] <= event["sequence"] or next_payload.get("child_call_id") != call_id
                    or next_payload.get("child_workflow_instance_id") != instance
                    or not scope_identity(next_payload.get("child_workflow_run_id"))
                    or next_payload["child_workflow_run_id"] == run_id
                    or next_payload.get("cancellation_scope_id", scope_id) != scope_id
                    or next_payload.get("cancellation_policy", policy) != policy):
                    raise _invalid("scope child changes its original continuation")
                target_run, last_started_id = next_payload["child_workflow_run_id"], started["id"]
            members.append({"sequence": sequence, "child_call_id": call_id, "child_workflow_instance_id": instance,
                            "child_workflow_run_id": target_run, "cancellation_policy": policy,
                            "descriptor_hash": _hash([
                scope_id, event["id"], sequence, call_id, instance, target_run, policy,
                payload.get("parent_close_policy"), payload.get("child_workflow_type"), last_started_id,
                _group_path(payload),
            ])})
    return normalize_scope_members(field, members)


def scope_descendants_from_prefix(
    prefix: Sequence[dict[str, Any]], scope_id: str, run_id: str, authority_deadline: str,
) -> list[dict[str, Any]]:
    openings = {row["payload"]["scope_id"]: row for row in prefix if _kind(row) == "CancellationScopeOpened"}
    requests = {row["payload"]["scope_id"]: row for row in prefix if _kind(row) == "CancellationScopeRequested"}
    included = {scope_id}
    members = []
    for identity, opening in openings.items():
        address = opening["payload"]
        parent_id = address["parent_scope_id"]
        if identity == scope_id or address["shield_parent"] or parent_id not in included:
            continue
        included.add(identity)
        request, parent_request = requests.get(identity), requests.get(parent_id)
        if request is None or parent_request is None:
            raise _invalid("scope descendant lacks its accepted propagation")
        context = ScopedCancellationContext.from_dict(request["payload"]["cancellation"])
        parent = ScopedCancellationContext.from_dict(parent_request["payload"]["cancellation"])
        propagation: dict[str, Any] | None = request
        if context.root_context.request_id == parent.root_context.request_id:
            if request["payload"].get("parent_scope_id") != parent_id:
                raise _invalid("scope descendant changes its original parent")
        else:
            propagation = None
            for event in prefix:
                payload = event["payload"]
                if (_kind(event) != "CancellationScopeRequestConflicted" or payload.get("scope_id") != identity
                    or payload.get("parent_scope_id") != parent_id):
                    continue
                if (payload.get("schema") != "durable-workflow.cancellation-scope-request/v1"
                    or payload.get("workflow_run_id") != run_id or payload.get("reason") != "cancellation_root_conflict"
                    or event["sequence"] <= max(request["sequence"], parent_request["sequence"])):
                    raise _invalid("scope descendant changes its original conflict boundary")
                incoming = ScopedCancellationContext.from_dict(payload.get("incoming_cancellation", {}))
                accepted = ScopedCancellationContext.from_dict(payload.get("accepted_cancellation", {}))
                if (incoming.root_context == parent.root_context and incoming.lineage[:-1] == parent.lineage
                    and incoming.deadline == parent.deadline and incoming.scope_id == identity
                    and incoming.workflow_run_id == run_id
                    and incoming.workflow_instance_id == context.workflow_instance_id
                    and accepted == context):
                    propagation = event
                    break
            if propagation is None:
                raise _invalid("scope descendant lacks its original competing-root conflict")
        deadline = scope_timestamp(authority_deadline)
        ancestor = identity
        while ancestor != "root":
            if ancestor in requests:
                deadline = min(deadline, ScopedCancellationContext.from_dict(
                    requests[ancestor]["payload"]["cancellation"],
                ).deadline)
            ancestor = openings[ancestor]["payload"]["parent_scope_id"]
        if propagation is None:
            raise _invalid("scope descendant lacks its original propagation")
        members.append({
            "scope_id": identity, "parent_scope_id": parent_id, "scope_history_event_id": opening["id"],
            "request_history_event_id": request["id"], "request_id": context.request_id,
            "propagation_history_event_id": propagation["id"], "authority_deadline_at": _canonical_time(deadline),
            "cancellation": context.to_dict(),
            **{field: scope_members_from_prefix(field, prefix, identity, run_id) for field in _FIELDS},
        })
    return members


@dataclass(frozen=True)
class ScopeRequest:
    context: ScopedCancellationContext
    timestamp: datetime
    history_index: int


@dataclass(frozen=True)
class ScopeBoundary:
    context: ScopedCancellationContext
    boundary: CancellationDelivery
    authority_deadline: datetime
    event: dict[str, Any]


@dataclass(frozen=True)
class CancellationScopeDeliveryIntent:
    context: ScopedCancellationContext
    boundary: CancellationDelivery
    preparation: ScopeBoundary | None = None


@dataclass
class CancellationScopeBudget:
    expires_at: float

    @classmethod
    def start(cls, seconds: float = 5.0) -> CancellationScopeBudget:
        if isinstance(seconds, bool) or not isinstance(seconds, int | float) or not 0 < seconds <= 5:
            raise ValueError("scope request budget must be positive, finite and at most five seconds")
        return cls(asyncio.get_running_loop().time() + seconds)

    def remaining(self) -> float:
        remaining = self.expires_at - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError("scope boundary exceeded its original request budget")
        return remaining

    def restrict(self, authority_deadline: datetime) -> None:
        wall_remaining = (authority_deadline - datetime.now(timezone.utc)).total_seconds()
        self.expires_at = min(self.expires_at, asyncio.get_running_loop().time() + wall_remaining)
        self.remaining()


@dataclass(frozen=True)
class CancellationScopeDeliveryReceipt:
    preparation: ScopeBoundary
    delivery: ScopeBoundary | None
    history: tuple[dict[str, Any], ...]

    @staticmethod
    def acknowledge(receipt: Any, expected: dict[str, Any], *, delivering: bool) -> str:
        if not isinstance(receipt, dict) or any(
            key not in receipt or not _same(receipt[key], value)
            for key, value in expected.items() if key not in {"namespace", "workflow_instance_id"}
        ):
            raise _invalid("scope acknowledgement changes its original claim or authored range")
        if (receipt.get("prepared") is not True or receipt.get("delivered") is not delivering
            or receipt.get("claim_released") is not False or receipt.get("created_task_ids") != []
            or "reason" not in receipt or receipt["reason"] is not None
            or not scope_identity(receipt.get("history_event_id"))
            or not scope_identity(receipt.get("preparation_history_event_id"))
            or (receipt["history_event_id"] == receipt["preparation_history_event_id"]) is delivering
            or not isinstance(receipt.get("history_refresh_page_token"), str)
            or not receipt["history_refresh_page_token"].strip()):
            raise _invalid("scope acknowledgement lacks a complete retained claim proof")
        for field in _FIELDS:
            normalize_scope_members(field, receipt.get(field))
        context = ScopedCancellationContext.from_dict(receipt.get("cancellation", {}))
        if (context.workflow_run_id != expected["workflow_run_id"]
            or context.workflow_instance_id != expected["workflow_instance_id"]
            or context.scope_id != expected["scope_id"] or context.request_id != expected["request_id"]
            or not context.requested_at <= scope_timestamp(receipt.get("authority_deadline_at")) <= context.deadline):
            raise _invalid("scope acknowledgement changes its original cancellation authority")
        return str(receipt["history_refresh_page_token"])

    @classmethod
    def from_history(
        cls, receipt: dict[str, Any], history: Sequence[dict[str, Any]], expected: dict[str, Any], *, delivering: bool,
    ) -> CancellationScopeDeliveryReceipt:
        cls.acknowledge(receipt, expected, delivering=delivering)
        scopes = CancellationScopeHistory.read(history, expected["workflow_run_id"])
        starts: set[str] = set()
        for event in history:
            if event.get("namespace") != expected["namespace"]:
                raise _invalid("scope receipt history changes its original namespace")
            kind = _kind(event)
            if kind in {"StartAccepted", "WorkflowStarted"}:
                if (kind in starts or event["payload"].get("workflow_run_id") != expected["workflow_run_id"]
                    or event["payload"].get("workflow_instance_id") != expected["workflow_instance_id"]):
                    raise _invalid("scope receipt history changes its original workflow start")
                starts.add(kind)
        if "WorkflowStarted" not in starts:
            raise _invalid("scope receipt history omits its original workflow start")
        committed = CommittedCancellationScopeHistory.read(
            history, expected["workflow_run_id"], expected["workflow_instance_id"], scopes,
        )
        preparation = committed.preparations.get(expected["scope_id"])
        context = ScopedCancellationContext.from_dict(receipt["cancellation"])
        boundary = CancellationDelivery.from_payload({**receipt, "workflow_command_id": expected["request_id"]})
        if (preparation is None or preparation.event["id"] != receipt["preparation_history_event_id"]
            or preparation.context != context or preparation.boundary != boundary
            or preparation.authority_deadline != scope_timestamp(receipt["authority_deadline_at"])
            or any(normalize_scope_members(field, preparation.event["payload"][field])
                   != normalize_scope_members(field, receipt[field]) for field in _FIELDS)):
            raise _invalid("scope receipt differs from its original committed preparation")
        delivery = committed.deliveries.get(boundary.sequence) if delivering else None
        if delivering and (delivery is None or delivery.event["id"] != receipt["history_event_id"]
                           or delivery.context != context or delivery.boundary != boundary):
            raise _invalid("scope receipt lacks its original committed delivery")
        return cls(preparation, delivery, tuple(history))

    def assert_original_preparation(self, original: CancellationScopeDeliveryReceipt) -> None:
        before, after = original.preparation, self.preparation
        if (before.event["id"] != after.event["id"] or before.context != after.context
            or before.boundary != after.boundary or before.authority_deadline != after.authority_deadline
            or any(before.event["payload"][field] != after.event["payload"][field]
                   for field in (*_FIELDS, "descendant_members"))):
            raise _invalid("scope delivery substitutes its previously proved original preparation")


@dataclass(frozen=True)
class CommittedCancellationScopeHistory:
    """Canonical accepted requests and original v5 prepared/delivered boundaries."""

    preparations: dict[str, ScopeBoundary]
    deliveries: dict[int, ScopeBoundary]
    pending_requests: dict[str, ScopeRequest]

    @classmethod
    def read(
        cls, history: Sequence[dict[str, Any]], run_id: str, workflow_id: str,
        scopes: CancellationScopeHistory | None = None,
    ) -> CommittedCancellationScopeHistory:
        scopes = scopes if scopes is not None else CancellationScopeHistory.read(history, run_id)
        addresses = {opening["scope_id"]: {**opening, "sequence": sequence}
                     for sequence, opening in scopes.openings.items()}
        run_state = read_cancellation_history(history, run_id=run_id)
        run_root = None
        if run_state.request is not None and run_state.request.context is not None:
            run_context = run_state.request.context
            if run_context.scope_origin is None:
                root_snapshot = run_context.to_dict()
                root_snapshot.update({"request_id": run_context.root_request_id, "parent_request_id": None,
                                      "lineage": [run_context.lineage[0].to_dict()]})
                run_root = ScopedCancellationContext(CancellationContext.from_dict(root_snapshot), tuple(
                    ScopedCancellationLineage(entry.request_id, entry.workflow_instance_id, entry.workflow_run_id,
                                              "root", run_context.deadline) for entry in run_context.lineage
                ))
            else:
                local = run_context.lineage[-1]
                run_root = ScopedCancellationContext(run_context.scope_origin.root_context, (
                    *run_context.scope_origin.lineage,
                    ScopedCancellationLineage(run_context.request_id, local.workflow_instance_id,
                                              local.workflow_run_id, "root", run_context.deadline),
                ))
        requests: dict[str, ScopeRequest] = {}
        request_ids: set[str] = set()
        preparations: dict[str, ScopeBoundary] = {}
        deliveries: dict[int, ScopeBoundary] = {}
        delivered_scopes: set[str] = set()
        opened: set[str] = set()
        admissions: dict[str, dict[int, str]] = {}
        for index, event in enumerate(history):
            kind, payload = _kind(event), event.get("payload", {})
            cleanup = (kind == "TimerScheduled" and _cleanup_timer(event, history[:index])
                       or kind in {"ActivityScheduled", "ActivityStarted"}
                       and _cleanup_local_activity(event, history[:index]))
            if kind == "CancellationScopeOpened":
                opened.add(payload["scope_id"])
            if kind in _ADMISSIONS and _positive(payload.get("sequence")) and not cleanup:
                admissions.setdefault(kind, {})[payload["sequence"]] = scopes.memberships.get(
                    payload["sequence"], "root",
                )
            if kind not in {
                "CancellationScopeRequested", "CancellationScopeDeliveryPrepared", "CancellationScopeDelivered",
            }:
                continue
            if not isinstance(payload, dict) or not isinstance(payload.get("cancellation"), dict):
                raise _invalid("scope boundary omits its canonical context")
            context = ScopedCancellationContext.from_dict(payload["cancellation"])
            identity = context.scope_id
            address = addresses.get(identity)
            if (address is None or identity not in opened or not run_id or not workflow_id
                or context.workflow_run_id != run_id or context.workflow_instance_id != workflow_id
                or payload.get("workflow_run_id") != run_id or payload.get("scope_id") != identity
                or payload.get("request_id") != context.request_id):
                raise _invalid("scope cancellation changes its original run, request or address")
            timestamp = scope_timestamp(event.get("timestamp", event.get("recorded_at")))
            if timestamp < context.requested_at:
                raise _invalid("scope boundary predates the original request")
            if kind == "CancellationScopeRequested":
                if (payload.get("schema") != "durable-workflow.cancellation-scope-request/v1"
                    or "parent_scope_id" not in payload or identity in requests or context.request_id in request_ids):
                    raise _invalid("scope cancellation requires one accepted original request")
                parent_id = payload["parent_scope_id"]
                if parent_id is None:
                    if len(context.lineage) != 1 or context.request_id != context.root_context.request_id:
                        raise _invalid("direct scope request substitutes an inherited lineage")
                else:
                    parent_request = requests.get(parent_id) if isinstance(parent_id, str) else None
                    parent = run_root if parent_id == "root" and run_state.request_index < index else (
                        parent_request.context if parent_request is not None else None
                    )
                    if (parent is None or parent.workflow_run_id != run_id or parent.workflow_instance_id != workflow_id
                        or address["shield_parent"] or parent_id != address["parent_scope_id"]
                        or context.root_context != parent.root_context or context.lineage[:-1] != parent.lineage
                        or context.deadline != parent.deadline):
                        raise _invalid("inherited scope request changes its accepted parent or crosses a shield")
                requests[identity] = ScopeRequest(context, timestamp, index)
                request_ids.add(context.request_id)
                continue
            request = requests.get(identity)
            if request is None or context != request.context or timestamp < request.timestamp:
                raise _invalid("scope boundary lacks its earlier immutable accepted request")
            if any(field not in payload for field in (
                "sequence_span", "operation_sequence", "operation_sequence_span",
            )):
                raise _invalid("scope boundary omits its original operation range")
            boundary = CancellationDelivery.from_payload({**payload, "workflow_command_id": context.request_id})
            deadline = scope_timestamp(payload.get("authority_deadline_at"))
            if (boundary.sequence <= address["sequence"] or deadline < context.requested_at
                or deadline > context.deadline or timestamp > deadline):
                raise _invalid("scope boundary changes or exceeds its original authority ceiling")
            if kind == "CancellationScopeDeliveryPrepared":
                if (payload.get("schema") != "durable-workflow.cancellation-scope-preparation/v5"
                    or identity in preparations or identity in delivered_scopes):
                    raise _invalid("scope delivery requires one original v5 preparation")
                prefix = history[:index]
                for field in _FIELDS:
                    if normalize_scope_members(field, payload.get(field)) != scope_members_from_prefix(
                        field, prefix, identity, run_id,
                    ):
                        raise _invalid("scope preparation changes its original member projection")
                if payload.get("descendant_members") != scope_descendants_from_prefix(
                    prefix, identity, run_id, payload["authority_deadline_at"],
                ):
                    raise _invalid("scope preparation changes its original descendant projection")
                by_scope = {identity: payload, **{
                    member["scope_id"]: member for member in payload["descendant_members"]
                }}
                operation_scope = scopes.memberships.get(boundary.sequence, identity)
                if operation_scope not in by_scope:
                    raise _invalid("scope boundary consumes an operation outside its frozen subtree")
                if boundary.call_kind == "parallel":
                    _assert_complete_group(boundary, operation_scope, prefix, scopes)
                elif boundary.call_kind not in {"activity", "local_activity", "timer", "condition", "child"}:
                    raise _invalid("scope boundary uses an unsupported operation kind")
                for member_scope, projection in by_scope.items():
                    for admission, positions in admissions.items():
                        field = _ADMISSIONS[admission]
                        entries = projection[field]
                        if admission in {"ConditionWaitOpened", "SignalWaitOpened"}:
                            wait_kind = "condition" if admission == "ConditionWaitOpened" else "signal"
                            entries = [entry for entry in entries if entry["kind"] == wait_kind]
                        sequences = {entry["sequence"] for entry in entries}
                        if any(position not in sequences for position, owner in positions.items()
                               if owner == member_scope):
                            raise _invalid("scope preparation omits an admitted operation")
                        if positions.get(boundary.sequence) != member_scope:
                            continue
                        call_kind = {"ActivityScheduled": "activity", "TimerScheduled": "timer",
                                     "ChildWorkflowScheduled": "child", "ConditionWaitOpened": "condition",
                                     "SignalWaitOpened": "signal"}[admission]
                        if call_kind == "activity" and any(
                            _kind(row) == admission and row["payload"].get("sequence") == boundary.sequence
                            and row["payload"].get("local_activity") is True for row in prefix
                        ):
                            call_kind = "local_activity"
                        timeout = admission == "TimerScheduled" and boundary.sequence in {
                            entry["sequence"] for entry in projection["wait_members"]
                        }
                        if boundary.sequence not in sequences or (
                            not timeout and boundary.call_kind not in {"parallel", call_kind}
                        ):
                            raise _invalid("scope boundary replaces an admitted operation")
                preparations[identity] = ScopeBoundary(context, boundary, deadline, event)
                continue
            preparation = preparations.get(identity)
            if (payload.get("schema") != "durable-workflow.cancellation-scope-delivery/v1"
                or preparation is None or identity in delivered_scopes or boundary.sequence in deliveries
                or payload.get("preparation_history_event_id") != preparation.event["id"]
                or boundary != preparation.boundary or deadline != preparation.authority_deadline
                or timestamp < scope_timestamp(preparation.event.get(
                    "timestamp", preparation.event.get("recorded_at"),
                ))):
                raise _invalid("scope delivery changes its original prepared boundary")
            deliveries[boundary.sequence] = ScopeBoundary(context, boundary, deadline, event)
            delivered_scopes.add(identity)
        covered = {delivery.context.scope_id for delivery in deliveries.values()}
        for delivery in deliveries.values():
            covered.update(member["scope_id"] for member in preparations[delivery.context.scope_id].event[
                "payload"
            ]["descendant_members"])
        return cls(preparations, dict(sorted(deliveries.items())), {
            identity: request for identity, request in requests.items() if identity not in covered
        })

    def scope_states_for_boundary(
        self, boundary: ScopeBoundary,
    ) -> dict[str, tuple[ScopedCancellationContext, datetime]]:
        return _boundary_scope_states(boundary.context, self.preparations[boundary.context.scope_id].event["payload"])

    def pending_request_for_scope(self, scope_id: str, scopes: CancellationScopeHistory) -> ScopeRequest | None:
        addresses = {opening["scope_id"]: opening for opening in scopes.openings.values()}
        request = self.pending_requests.get(scope_id)
        while scope_id in addresses and not addresses[scope_id]["shield_parent"]:
            scope_id = addresses[scope_id]["parent_scope_id"]
            ancestor = self.pending_requests.get(scope_id)
            if ancestor is None:
                continue
            if request is not None and (
                ancestor.context.root_context != request.context.root_context
                or request.context.lineage[:len(ancestor.context.lineage)] != ancestor.context.lineage
            ):
                raise _invalid("pending scope selection changes its original ancestor lineage")
            request = ancestor
        return request


def _assert_complete_group(
    boundary: CancellationDelivery, scope_id: str, prefix: Sequence[dict[str, Any]], scopes: CancellationScopeHistory,
) -> None:
    members: set[int] = set()
    for event in prefix:
        kind, payload = _kind(event), event["payload"]
        sequence = payload.get("sequence")
        if (kind not in _ADMISSIONS or not _positive(sequence) or not boundary.interrupts(sequence)
            or kind == "TimerScheduled" and payload.get("timer_kind") in {"condition_timeout", "signal_timeout"}):
            continue
        if scopes.memberships.get(sequence, "root") != scope_id or sequence in members:
            raise _invalid("scope group changes its original member address")
        path = payload.get("parallel_group_path", [payload])
        if not isinstance(path, list) or not path or any(not isinstance(entry, dict) for entry in path):
            raise _invalid("scope group requires its original admitted path")
        if any(entry.get("parallel_group_mode", "all") != "all" or str(
            entry.get("parallel_group_id", ""),
        ).startswith("select-calls:") for entry in path):
            raise _invalid("scoped selection groups require their original selection proof")
        if (path[0].get("parallel_group_base_sequence") != boundary.sequence
            or path[0].get("parallel_group_size") != boundary.sequence_span
            or path[0].get("parallel_group_index") != sequence - boundary.sequence):
            raise _invalid("scope group changes its original range or member index")
        members.add(sequence)
    if len(members) != boundary.sequence_span:
        raise _invalid("scope group omits an original admitted member")
