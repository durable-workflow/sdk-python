"""Business replay time from supported consumed boundaries, without wall time."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import re
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ParamSpec, TypeGuard, TypeVar

from .errors import ReplayClockUnavailable

_P = ParamSpec("_P")
_T = TypeVar("_T")
_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])",
)
_ACTIVITY_TERMINALS = frozenset({"ActivityCompleted", "ActivityFailed", "ActivityTimedOut", "ActivityCancelled"})
_CHILD_TERMINALS = frozenset({"ChildRunCompleted", "ChildRunFailed", "ChildRunCancelled", "ChildRunTerminated"})
_CHILD_IDENTITIES = ("child_call_id", "child_workflow_instance_id", "child_workflow_run_id", "child_workflow_type")
_CHILD_EVENTS = frozenset({"ChildWorkflowScheduled", "ChildRunStarted", *_CHILD_TERMINALS})
_CHILD_FAMILY = "child workflow"
_REUSE_FIELDS = ("reused_from_run_id", "reused_recorded_at", "reused_activity_execution_id")


def _task() -> object | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


@dataclass(frozen=True)
class _Frame:
    owner: object | None
    time: datetime | None
    thread: int
    task: object | None


_ACTIVE: ContextVar[_Frame | None] = ContextVar("business_replay_clock", default=None)
_DISCARD = threading.local()


@contextlib.contextmanager
def execution_scope(owner: object | None, time: datetime | None) -> Iterator[None]:
    token = _ACTIVE.set(_Frame(owner, time, threading.get_ident(), _task()))
    try:
        yield
    finally:
        _ACTIVE.reset(token)


def isolated(function: Callable[_P, _T]) -> Callable[_P, _T]:
    """Nested reconstruction and its unqualified callbacks cannot borrow time."""
    @functools.wraps(function)
    def invoke(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        with execution_scope(None, None):
            return function(*args, **kwargs)
    return invoke


def discarded(function: Callable[_P, _T]) -> Callable[_P, _T]:
    """Synchronous disposal cannot borrow an active business clock."""
    @functools.wraps(function)
    def invoke(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        # Finalizers can interrupt a ContextVar update. Do not write another
        # ContextVar here or move generator cleanup away from its own tokens.
        previous = getattr(_DISCARD, "active", False)
        _DISCARD.active = True
        try:
            return function(*args, **kwargs)
        finally:
            _DISCARD.active = previous
    return invoke


def read(owner: object) -> datetime:
    frame = _ACTIVE.get()
    if (getattr(_DISCARD, "active", False) or frame is None or frame.owner is not owner or frame.time is None
        or frame.thread != threading.get_ident() or frame.task is not _task()):
        raise ReplayClockUnavailable()
    return frame.time


def _time(value: Any) -> datetime | None:
    if not isinstance(value, str) or _TIMESTAMP.fullmatch(value) is None:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def _kind(event: Mapping[str, Any]) -> str | None:
    value = event.get("event_type") or event.get("type")
    if "event_type" in event and "type" in event and event["event_type"] != event["type"]:
        return None
    return value if isinstance(value, str) and value else None


def _recorded_value(event: Mapping[str, Any]) -> Any:
    return event.get("timestamp") if "timestamp" in event else event.get("recorded_at")


def _sequence(payload: Mapping[str, Any]) -> int | None:
    value = payload.get("sequence")
    if type(value) is not int or value < 1:
        return None
    if "workflow_sequence" in payload:
        alias = payload["workflow_sequence"]
        if type(alias) is not int or alias != value:
            return None
    return value


def _identity(value: Any) -> TypeGuard[str]:
    return isinstance(value, str) and bool(value.strip())


def _declares_redrive(event: Mapping[str, Any]) -> bool:
    payload = event.get("payload")
    if not isinstance(payload, Mapping):
        return False
    # A conflicting kind alias invalidates authority; it must not erase a
    # recognizable redrive envelope and turn it into ordinary history.
    aliases = (event.get("event_type"), event.get("type"))
    if "StartAccepted" in aliases and payload.get("outcome") == "redriven":
        return True
    return "WorkflowStarted" in aliases and (
        payload.get("recovery_kind") == "redrive"
        or "replayed_started_at" in payload or "resume_step_sequence" in payload
    )


class BusinessReplayClock:
    """Index identities once, then read time only at actual scalar consumption."""

    def __init__(
        self, events: Sequence[Mapping[str, Any]], workflow_id: str, run_id: str,
        *, sequence_claim: Callable[[Mapping[str, Any]], int | None],
    ) -> None:
        self.events = events
        self._by_sequence: dict[int, list[int]] = {}
        self._child_calls: dict[str, list[int]] = {}
        self._source_run: str | None = None
        self._resume_step: int | None = None
        self._redrive_inserted: datetime | None = None
        self._redrive_declared = any(_declares_redrive(event) for event in events)
        starts = [index for index, event in enumerate(events) if _kind(event) == "WorkflowStarted"]
        self.initial: tuple[int | None, datetime | None] = (None, None)
        if len(starts) == 1:
            index = starts[0]
            payload = events[index].get("payload")
            if (isinstance(payload, Mapping) and self._start_prefix(index, payload)
                and self._redrive_start(index, payload, workflow_id, run_id)):
                self.initial = (index, _time(payload.get("replayed_started_at")))
        self._pending = self.initial
        self._current = self.initial
        for index, event in enumerate(events):
            self._index_event(index, event, sequence_claim)

    def _index_event(
        self, index: int, event: Mapping[str, Any], sequence_claim: Callable[[Mapping[str, Any]], int | None],
    ) -> None:
        payload = event.get("payload")
        if not isinstance(payload, Mapping):
            return
        # Visibility follows the resolver's actual claims, including legacy
        # counters. Authority still requires strict integers on every row.
        if (sequence := sequence_claim(payload)) is not None:
            self._by_sequence.setdefault(sequence, []).append(index)
        call = payload.get("child_call_id")
        if _kind(event) in _CHILD_EVENTS and _identity(call):
            self._child_calls.setdefault(call, []).append(index)

    def _start_prefix(self, index: int, payload: Mapping[str, Any]) -> bool:
        if index == 0:
            return True
        if index != 1 or _kind(self.events[0]) != "StartAccepted":
            return False
        accepted = self.events[0].get("payload")
        if not isinstance(accepted, Mapping) or accepted.get("outcome") != "redriven":
            return False
        return all(_identity(payload.get(key)) and accepted.get(key) == payload[key]
                   for key in ("workflow_instance_id", "workflow_run_id", "workflow_command_id"))

    def _redrive_start(self, index: int, payload: Mapping[str, Any], workflow_id: str, run_id: str) -> bool:
        if payload.get("recovery_kind") != "redrive":
            # Ordinary entry needs run.started_at, not history insertion time.
            return False
        keys = ("workflow_instance_id", "workflow_run_id", "workflow_command_id", "continued_from_run_id",
                "workflow_link_id")
        if not all(_identity(payload.get(key)) for key in keys):
            return False
        if payload["workflow_run_id"] == payload["continued_from_run_id"]:
            return False
        if ((workflow_id and payload["workflow_instance_id"] != workflow_id)
            or (run_id and payload["workflow_run_id"] != run_id)):
            return False
        resume_step = payload.get("resume_step_sequence")
        if type(resume_step) is not int or resume_step < 1 or _time(payload.get("replayed_started_at")) is None:
            return False
        if any(type(event.get("sequence")) is not int or event["sequence"] != index + 1
               for index, event in enumerate(self.events)):
            return False
        inserted = _time(_recorded_value(self.events[index]))
        original = _time(payload.get("replayed_started_at"))
        if inserted is None or original is None or original > inserted:
            return False
        self._source_run = payload["continued_from_run_id"]
        self._resume_step = resume_step
        self._redrive_inserted = inserted
        return True

    def resume(self, owner: object, function: Callable[..., _T], *args: Any) -> _T:
        self._current, self._pending = self._pending, (None, None)
        with execution_scope(owner, self._current[1]):
            return function(*args)

    def snapshot(self) -> datetime | None:
        # Only this final inspected prefix is proved for a query callback.
        index, time = self._current
        return time if index is not None and index == len(self.events) - 1 else None

    def unavailable(self) -> None:
        self._current = self._pending = (None, None)

    def observe(self, index: int | None, family: str, sequence: int) -> None:
        self._pending = (None, None)
        if index is None or not 0 <= index < len(self.events) or family not in {"activity", "timer", _CHILD_FAMILY}:
            return
        if any(_sequence(self.events[position]["payload"]) != sequence
               for position in self._by_sequence.get(sequence, [])):
            return
        boundary = (self._child_boundary(index, sequence) if family == _CHILD_FAMILY
                    else self._scalar_boundary(index, family, sequence))
        if not boundary:
            return
        event = self.events[index]
        payload = event["payload"]
        time = (_time(_recorded_value(event)) if family == _CHILD_FAMILY
                else self._scalar_time(event, payload, family, sequence))
        self._pending = (index, time)

    def _scalar_time(
        self, event: Mapping[str, Any], payload: Mapping[str, Any], family: str, sequence: int,
    ) -> datetime | None:
        if self._redrive_declared and self._source_run is None:
            return None
        before_resume = self._resume_step is not None and sequence < self._resume_step
        if _kind(event) in {"ActivityCompleted", "TimerFired"} and (
            before_resume or any(key in payload for key in _REUSE_FIELDS)
        ):
            # The validated resume boundary implies reuse even when both
            # markers are missing. Insertion time cannot replace its source.
            return self._reused_time(payload, family, sequence)
        return _time(_recorded_value(event))

    def _child_boundary(self, index: int, sequence: int) -> bool:
        indices = self._by_sequence.get(sequence, [])
        if len(indices) not in (2, 3) or indices[-1] != index:
            return False
        kinds = [_kind(self.events[position]) for position in indices]
        if kinds[0] != "ChildWorkflowScheduled" or kinds[-1] not in _CHILD_TERMINALS:
            return False
        if len(kinds) == 3 and kinds[1] != "ChildRunStarted":
            return False
        payload = self.events[index].get("payload")
        if not isinstance(payload, Mapping) or _sequence(payload) != sequence:
            return False
        if not all(_identity(payload.get(key)) for key in _CHILD_IDENTITIES):
            return False
        if self._child_calls.get(payload["child_call_id"]) != indices:
            return False
        return all(self._child_row(position, payload) for position in indices)

    def _child_row(self, index: int, terminal: Mapping[str, Any]) -> bool:
        payload = self.events[index].get("payload")
        return (self._scalar_row(index, _CHILD_FAMILY, "child_call_id", terminal["child_call_id"], _CHILD_EVENTS)
                and isinstance(payload, Mapping)
                and payload.get("workflow_type", terminal["child_workflow_type"]) == terminal["child_workflow_type"]
                and all(payload.get(key) == terminal[key] for key in _CHILD_IDENTITIES))

    def _reused_time(self, payload: Mapping[str, Any], family: str, sequence: int) -> datetime | None:
        if (self._source_run is None or payload.get("reused_from_run_id") != self._source_run
            or self._resume_step is None or sequence >= self._resume_step):
            return None
        if family == "activity" and (
            not _identity(payload.get("reused_activity_execution_id"))
            or payload.get("reused_activity_execution_id") == payload.get("activity_execution_id")
        ):
            return None
        original = _time(payload.get("reused_recorded_at"))
        if original is None or self._redrive_inserted is None or original > self._redrive_inserted:
            return None
        return original

    def _scalar_boundary(self, index: int, family: str, sequence: int) -> bool:
        event = self.events[index]
        payload = event.get("payload")
        if not isinstance(payload, Mapping) or _sequence(payload) != sequence:
            return False
        kinds = _ACTIVITY_TERMINALS if family == "activity" else frozenset({"TimerFired"})
        if _kind(event) not in kinds:
            return False
        indices = self._by_sequence.get(sequence, [])
        matching = [position for position in indices if _kind(self.events[position]) in kinds]
        if matching != [index]:
            return False
        resource = "activity_execution_id" if family == "activity" else "timer_id"
        identity = payload.get(resource)
        if not _identity(identity):
            return False
        opening = "ActivityScheduled" if family == "activity" else "TimerScheduled"
        allowed = {opening, *kinds}
        if family == "activity":
            allowed.add("ActivityStarted")
        if any(position > index or not self._scalar_row(position, family, resource, identity, allowed)
               for position in indices):
            return False
        return self._scalar_lifecycle(indices, index, family, opening, payload)

    def _scalar_lifecycle(
        self, indices: Sequence[int], index: int, family: str, opening: str, payload: Mapping[str, Any],
    ) -> bool:
        openings = [position for position in indices if _kind(self.events[position]) == opening]
        starts = [position for position in indices if _kind(self.events[position]) == "ActivityStarted"]
        if len(starts) > 1 or (starts and (not openings or starts[0] < openings[0])):
            return False
        reused = "reused_recorded_at" in payload and self._source_run is not None
        if len(openings) != 1 and not (family == "activity" and reused and indices == [index]):
            return False
        return not openings or openings[0] < index

    def _scalar_row(
        self, index: int, family: str, resource: str, identity: str, allowed: set[str] | frozenset[str],
    ) -> bool:
        row = self.events[index]
        body = row.get("payload")
        if not isinstance(body, Mapping) or _kind(row) not in allowed or body.get(resource) != identity:
            return False
        if (body.get("execution_mode") not in (None, "remote") or body.get("local_activity") is True
            or body.get("parallel_group") is not None or body.get("parallel_group_path") not in (None, [])
            or body.get("selection_group") is not None
            or body.get("cancellation_scope_id") not in (None, "root")):
            return False
        return family != "timer" or (
            body.get("timer_kind") in (None, "sleep", "durable_timer")
            and "condition_wait_id" not in body and "signal_wait_id" not in body
        )
