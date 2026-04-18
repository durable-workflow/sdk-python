"""Workflow authoring primitives: decorators, context, commands, and replayer.

A workflow is a Python class registered with :func:`defn`. Its ``run`` method
is a generator that yields command dataclasses (``ScheduleActivity``,
``StartTimer``, ``StartChildWorkflow``, …) — the worker's replayer drives the
generator forward by resolving each yielded command against the current
history of the workflow run. Yield a *list* of commands to run them in
parallel.

Determinism-sensitive helpers live on the :class:`WorkflowContext` passed to
``run``: :meth:`WorkflowContext.random`, :meth:`WorkflowContext.uuid4`,
:meth:`WorkflowContext.now`, and :meth:`WorkflowContext.side_effect` all
produce values that are recorded on first execution and replayed verbatim
on every subsequent replay of the same history.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import math
import random
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from . import serializer
from .errors import ChildWorkflowFailed

_REGISTRY: dict[str, type] = {}


def defn(*, name: str):  # type: ignore[no-untyped-def]
    """Register a class as a workflow type under a language-neutral name.

    Scans the class for ``@signal``-decorated methods and builds a signal
    registry at decoration time so the replayer can dispatch incoming
    signals without re-inspecting the class on every history event.
    """

    def wrap(cls: type) -> type:
        cls.__workflow_name__ = name  # type: ignore[attr-defined]
        signals: dict[str, str] = {}
        for attr in dir(cls):
            if attr.startswith("_"):
                continue
            member = getattr(cls, attr, None)
            signal_name = getattr(member, "__signal_name__", None)
            if isinstance(signal_name, str) and signal_name:
                signals[signal_name] = attr
        cls.__workflow_signals__ = signals  # type: ignore[attr-defined]
        _REGISTRY[name] = cls
        return cls

    return wrap


def signal(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a workflow method as the handler for an external signal.

    Example::

        @workflow.defn(name="approval")
        class ApprovalWorkflow:
            def __init__(self) -> None:
                self.approved: bool = False

            @workflow.signal("approve")
            def on_approve(self, by: str) -> None:
                self.approved = True

    The decorated method is called by the replayer when a matching
    ``SignalReceived`` history event is observed, with the signal's
    decoded arguments unpacked into positional parameters. Handler return
    values are ignored; to expose state back to the workflow's main run
    loop, mutate ``self.*`` attributes (as ``on_approve`` does above) and
    yield the usual commands from ``run()``.
    """

    def wrap(method: Callable[..., Any]) -> Callable[..., Any]:
        method.__signal_name__ = name  # type: ignore[attr-defined]
        return method

    return wrap


def registry() -> dict[str, type]:
    """Return a copy of workflow types registered in this process."""
    return dict(_REGISTRY)


# ── Commands yielded from a workflow ──────────────────────────────────
@dataclass
class ActivityRetryPolicy:
    """Retry policy applied to one scheduled activity call.

    The policy is snapped onto the durable activity execution when the
    workflow task completes, so later code deploys do not change the retry
    budget for an already-scheduled activity.
    """

    max_attempts: int = 3
    initial_interval_seconds: float = 1.0
    backoff_coefficient: float = 2.0
    maximum_interval_seconds: float | None = None
    non_retryable_error_types: list[str] = field(default_factory=list)
    backoff_seconds: list[int] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return the server command shape for this activity retry policy."""
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.initial_interval_seconds < 0:
            raise ValueError("initial_interval_seconds must be >= 0")
        if self.backoff_coefficient < 1:
            raise ValueError("backoff_coefficient must be >= 1")
        if self.maximum_interval_seconds is not None and self.maximum_interval_seconds < 0:
            raise ValueError("maximum_interval_seconds must be >= 0")

        return {
            "max_attempts": self.max_attempts,
            "backoff_seconds": self._backoff_seconds(),
            "non_retryable_error_types": [
                value.strip()
                for value in self.non_retryable_error_types
                if isinstance(value, str) and value.strip()
            ],
        }

    def _backoff_seconds(self) -> list[int]:
        if self.backoff_seconds is not None:
            return [max(0, int(seconds)) for seconds in self.backoff_seconds]

        seconds: list[int] = []
        current = self.initial_interval_seconds
        maximum = self.maximum_interval_seconds
        for _ in range(max(0, self.max_attempts - 1)):
            value = current if maximum is None else min(current, maximum)
            seconds.append(max(0, int(math.ceil(value))))
            current *= self.backoff_coefficient
        return seconds


ActivityRetryPolicyInput = ActivityRetryPolicy | Mapping[str, Any]


@dataclass
class ScheduleActivity:
    """Command requesting an activity task."""

    activity_type: str
    arguments: list[Any]
    queue: str | None = None
    retry_policy: ActivityRetryPolicyInput | None = None
    start_to_close_timeout: int | None = None
    schedule_to_start_timeout: int | None = None
    schedule_to_close_timeout: int | None = None
    heartbeat_timeout: int | None = None

    def to_server_command(
        self, task_queue: str, *, payload_codec: str = serializer.AVRO_CODEC
    ) -> dict[str, Any]:
        command: dict[str, Any] = {
            "type": "schedule_activity",
            "activity_type": self.activity_type,
            "arguments": serializer.envelope(self.arguments, codec=payload_codec),
            "queue": self.queue or task_queue,
        }
        if self.retry_policy is not None:
            command["retry_policy"] = (
                self.retry_policy.to_dict()
                if isinstance(self.retry_policy, ActivityRetryPolicy)
                else dict(self.retry_policy)
            )
        if self.start_to_close_timeout is not None:
            command["start_to_close_timeout"] = self.start_to_close_timeout
        if self.schedule_to_start_timeout is not None:
            command["schedule_to_start_timeout"] = self.schedule_to_start_timeout
        if self.schedule_to_close_timeout is not None:
            command["schedule_to_close_timeout"] = self.schedule_to_close_timeout
        if self.heartbeat_timeout is not None:
            command["heartbeat_timeout"] = self.heartbeat_timeout
        return command


@dataclass
class StartTimer:
    """Command requesting a durable timer."""

    delay_seconds: int

    def to_server_command(
        self, task_queue: str, *, payload_codec: str = serializer.AVRO_CODEC
    ) -> dict[str, Any]:
        return {
            "type": "start_timer",
            "delay_seconds": self.delay_seconds,
        }


@dataclass
class CompleteWorkflow:
    """Command completing a workflow with a payload result."""

    result: Any

    def to_server_command(
        self, task_queue: str, *, payload_codec: str = serializer.AVRO_CODEC
    ) -> dict[str, Any]:
        return {
            "type": "complete_workflow",
            "result": serializer.envelope(self.result, codec=payload_codec),
        }


@dataclass
class FailWorkflow:
    """Command failing a workflow with diagnostic metadata."""

    message: str
    exception_type: str | None = None
    non_retryable: bool = False

    def to_server_command(
        self, task_queue: str, *, payload_codec: str = serializer.AVRO_CODEC
    ) -> dict[str, Any]:
        cmd: dict[str, Any] = {
            "type": "fail_workflow",
            "message": self.message,
        }
        if self.exception_type is not None:
            cmd["exception_type"] = self.exception_type
        if self.non_retryable:
            cmd["non_retryable"] = True
        return cmd


@dataclass
class ContinueAsNew:
    """Workflow return value that starts a new run with fresh history."""

    workflow_type: str | None = None
    arguments: list[Any] = field(default_factory=list)
    task_queue: str | None = None

    def to_server_command(
        self, task_queue: str, *, payload_codec: str = serializer.AVRO_CODEC
    ) -> dict[str, Any]:
        cmd: dict[str, Any] = {"type": "continue_as_new"}
        if self.workflow_type is not None:
            cmd["workflow_type"] = self.workflow_type
        cmd["arguments"] = serializer.envelope(self.arguments, codec=payload_codec)
        cmd["queue"] = self.task_queue or task_queue
        return cmd


@dataclass
class RecordSideEffect:
    """Command recording the result of a non-deterministic function."""

    result: Any

    def to_server_command(
        self, task_queue: str, *, payload_codec: str = serializer.AVRO_CODEC
    ) -> dict[str, Any]:
        return {
            "type": "record_side_effect",
            "result": serializer.encode(self.result, codec=payload_codec),
        }


@dataclass
class StartChildWorkflow:
    """Command requesting a child workflow run."""

    workflow_type: str
    arguments: list[Any] = field(default_factory=list)
    task_queue: str | None = None
    parent_close_policy: str | None = None

    def to_server_command(
        self, task_queue: str, *, payload_codec: str = serializer.AVRO_CODEC
    ) -> dict[str, Any]:
        cmd: dict[str, Any] = {
            "type": "start_child_workflow",
            "workflow_type": self.workflow_type,
            "arguments": serializer.envelope(self.arguments, codec=payload_codec),
        }
        if self.task_queue is not None:
            cmd["queue"] = self.task_queue
        else:
            cmd["queue"] = task_queue
        if self.parent_close_policy is not None:
            cmd["parent_close_policy"] = self.parent_close_policy
        return cmd


@dataclass
class RecordVersionMarker:
    """Command recording a workflow code-version marker."""

    change_id: str
    version: int
    min_supported: int
    max_supported: int

    def to_server_command(
        self, task_queue: str, *, payload_codec: str = serializer.AVRO_CODEC
    ) -> dict[str, Any]:
        return {
            "type": "record_version_marker",
            "change_id": self.change_id,
            "version": self.version,
            "min_supported": self.min_supported,
            "max_supported": self.max_supported,
        }


@dataclass
class UpsertSearchAttributes:
    """Command updating workflow search attributes."""

    attributes: dict[str, Any]

    def to_server_command(
        self, task_queue: str, *, payload_codec: str = serializer.AVRO_CODEC
    ) -> dict[str, Any]:
        return {
            "type": "upsert_search_attributes",
            "attributes": self.attributes,
        }


Command = (
    ScheduleActivity | StartTimer | CompleteWorkflow | FailWorkflow
    | ContinueAsNew | RecordSideEffect | StartChildWorkflow
    | RecordVersionMarker | UpsertSearchAttributes
)


# ── Context passed to the workflow's run() ───────────────────────────

_REPLAY_LOGGER = logging.getLogger("durable_workflow.workflow.replay")


class _ReplayLogger:
    """Logger that is silent when replaying committed history."""

    def __init__(self, inner: logging.Logger) -> None:
        self._inner = inner
        self._replaying = True

    def _set_replaying(self, replaying: bool) -> None:
        self._replaying = replaying

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not self._replaying:
            self._inner.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not self._replaying:
            self._inner.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not self._replaying:
            self._inner.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if not self._replaying:
            self._inner.error(msg, *args, **kwargs)


class WorkflowContext:
    """Replay-safe helper surface passed to workflow ``run`` methods."""

    def __init__(self, *, run_id: str = "", current_time: datetime | None = None) -> None:
        self._run_id = run_id
        self._current_time = current_time or datetime.now(timezone.utc)
        seed = int(hashlib.sha256(run_id.encode()).hexdigest()[:16], 16)
        self._rng = random.Random(seed)
        self.logger = _ReplayLogger(_REPLAY_LOGGER)

    def schedule_activity(
        self,
        activity_type: str,
        arguments: list[Any],
        *,
        queue: str | None = None,
        retry_policy: ActivityRetryPolicyInput | None = None,
        start_to_close_timeout: int | None = None,
        schedule_to_start_timeout: int | None = None,
        schedule_to_close_timeout: int | None = None,
        heartbeat_timeout: int | None = None,
    ) -> ScheduleActivity:
        return ScheduleActivity(
            activity_type=activity_type,
            arguments=list(arguments),
            queue=queue,
            retry_policy=retry_policy,
            start_to_close_timeout=start_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            schedule_to_close_timeout=schedule_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
        )

    def start_timer(self, seconds: int) -> StartTimer:
        return StartTimer(delay_seconds=seconds)

    def side_effect(self, fn: Callable[[], Any]) -> RecordSideEffect:
        result = fn()
        return RecordSideEffect(result=result)

    def start_child_workflow(
        self,
        workflow_type: str,
        arguments: list[Any] | None = None,
        *,
        task_queue: str | None = None,
        parent_close_policy: str | None = None,
    ) -> StartChildWorkflow:
        return StartChildWorkflow(
            workflow_type=workflow_type,
            arguments=list(arguments) if arguments is not None else [],
            task_queue=task_queue,
            parent_close_policy=parent_close_policy,
        )

    def get_version(
        self, change_id: str, min_supported: int, max_supported: int
    ) -> RecordVersionMarker:
        return RecordVersionMarker(
            change_id=change_id,
            version=max_supported,
            min_supported=min_supported,
            max_supported=max_supported,
        )

    def upsert_search_attributes(self, attributes: dict[str, Any]) -> UpsertSearchAttributes:
        return UpsertSearchAttributes(attributes=dict(attributes))

    def continue_as_new(
        self,
        *args: Any,
        workflow_type: str | None = None,
        task_queue: str | None = None,
    ) -> ContinueAsNew:
        return ContinueAsNew(workflow_type=workflow_type, arguments=list(args), task_queue=task_queue)

    def now(self) -> datetime:
        return self._current_time

    def random(self) -> random.Random:
        return self._rng

    def uuid4(self) -> uuid.UUID:
        rand_bytes = self._rng.getrandbits(128).to_bytes(16, "big")
        return uuid.UUID(bytes=rand_bytes, version=4)


# ── Replay ───────────────────────────────────────────────────────────
@dataclass
class ReplayOutcome:
    commands: list[Command]


def _decode_history_result(payload: dict[str, Any], fallback_codec: str | None) -> Any:
    codec = payload.get("payload_codec") or fallback_codec
    return serializer.decode_envelope(payload.get("result"), codec=codec)


def _decode_signal_args(payload: dict[str, Any], fallback_codec: str | None) -> list[Any]:
    codec = payload.get("payload_codec") or fallback_codec
    raw = payload.get("value")
    if raw is None:
        raw = payload.get("input")
    if raw is None:
        raw = payload.get("arguments")
    if raw is None:
        return []
    decoded = serializer.decode_envelope(raw, codec=codec)
    if isinstance(decoded, list):
        return decoded
    return [decoded]


def replay(
    workflow_cls: type,
    history_events: Iterable[dict[str, Any]],
    start_input: list[Any],
    *,
    run_id: str = "",
    payload_codec: str | None = None,
) -> ReplayOutcome:
    events = list(history_events)

    workflow_start_time: datetime | None = None
    for ev in events:
        etype = ev.get("event_type")
        if etype in ("WorkflowStarted", "workflow_started"):
            ts = (ev.get("payload") or {}).get("timestamp")
            if ts:
                with contextlib.suppress(ValueError, TypeError):
                    workflow_start_time = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            break

    instance = workflow_cls()
    ctx = WorkflowContext(run_id=run_id, current_time=workflow_start_time)

    resolved_results: list[Any] = []
    # (resolved_result_index_before_apply, signal_name, decoded_args) —
    # signals apply before the generator consumes the resolved_result at the
    # stored index, which preserves history interleaving with activities.
    pending_signals: list[tuple[int, str, list[Any]]] = []
    for ev in events:
        etype = ev.get("event_type")
        payload = ev.get("payload") or {}
        if etype in ("ActivityCompleted", "activity_completed"):
            resolved_results.append(_decode_history_result(payload, payload_codec))
        elif etype in ("TimerFired", "timer_fired"):
            resolved_results.append(None)
        elif etype in (
            "SideEffectRecorded", "side_effect_recorded",
            "ChildRunCompleted", "child_run_completed",
        ):
            resolved_results.append(_decode_history_result(payload, payload_codec))
        elif etype in ("ChildRunFailed", "child_run_failed"):
            resolved_results.append(ChildWorkflowFailed(
                payload.get("message", "child workflow failed")
            ))
        elif etype in ("VersionMarkerRecorded", "version_marker_recorded"):
            resolved_results.append(payload.get("version", 0))
        elif etype in ("SearchAttributesUpserted", "search_attributes_upserted"):
            resolved_results.append(None)
        elif etype in ("SignalReceived", "signal_received"):
            signal_name = payload.get("signal_name")
            if isinstance(signal_name, str) and signal_name:
                pending_signals.append(
                    (len(resolved_results), signal_name, _decode_signal_args(payload, payload_codec))
                )

    signal_registry: dict[str, str] = getattr(workflow_cls, "__workflow_signals__", {}) or {}

    def _apply_due_signals() -> None:
        while pending_signals and pending_signals[0][0] <= result_cursor:
            _, name, args = pending_signals.pop(0)
            method_name = signal_registry.get(name)
            if method_name is None:
                continue
            handler = getattr(instance, method_name, None)
            if handler is None:
                continue
            ctx.logger._set_replaying(True)
            handler(*args)

    gen = instance.run(ctx, *start_input)
    if not hasattr(gen, "__next__"):
        if isinstance(gen, ContinueAsNew):
            return ReplayOutcome(commands=[gen])
        return ReplayOutcome(commands=[CompleteWorkflow(result=gen)])

    ctx.logger._set_replaying(True)

    result_cursor = 0
    next_value: Any = None
    first = True
    pending: list[Command] = []
    advanced_cmd: Any = None
    try:
        while True:
            _apply_due_signals()
            if advanced_cmd is not None:
                cmd = advanced_cmd
                advanced_cmd = None
            else:
                cmd = gen.send(None) if first else gen.send(next_value)
                first = False
            if isinstance(cmd, list):
                needed = len(cmd)
                if result_cursor + needed <= len(resolved_results):
                    vals = resolved_results[result_cursor:result_cursor + needed]
                    result_cursor += needed
                    failed = next(
                        (v for v in vals if isinstance(v, ChildWorkflowFailed)),
                        None,
                    )
                    if failed is not None:
                        try:
                            advanced_cmd = gen.throw(failed)
                            continue
                        except StopIteration as stop:
                            if isinstance(stop.value, ContinueAsNew):
                                return ReplayOutcome(commands=[stop.value])
                            return ReplayOutcome(commands=[CompleteWorkflow(result=stop.value)])
                    next_value = vals
                    continue
                ctx.logger._set_replaying(False)
                pending.extend(cmd)
                return ReplayOutcome(commands=pending)
            if isinstance(cmd, ContinueAsNew):
                return ReplayOutcome(commands=[cmd])
            if isinstance(cmd, RecordSideEffect):
                if result_cursor < len(resolved_results):
                    next_value = resolved_results[result_cursor]
                    result_cursor += 1
                    continue
                ctx.logger._set_replaying(False)
                pending.append(cmd)
                next_value = cmd.result
                continue
            if isinstance(cmd, UpsertSearchAttributes):
                if result_cursor < len(resolved_results):
                    next_value = resolved_results[result_cursor]
                    result_cursor += 1
                    continue
                ctx.logger._set_replaying(False)
                pending.append(cmd)
                next_value = None
                continue
            if isinstance(cmd, RecordVersionMarker):
                if result_cursor < len(resolved_results):
                    val = resolved_results[result_cursor]
                    result_cursor += 1
                    next_value = val
                    continue
                ctx.logger._set_replaying(False)
                pending.append(cmd)
                next_value = cmd.version
                continue
            if isinstance(cmd, (ScheduleActivity, StartTimer, StartChildWorkflow)):
                if result_cursor < len(resolved_results):
                    val = resolved_results[result_cursor]
                    result_cursor += 1
                    if isinstance(val, ChildWorkflowFailed):
                        try:
                            advanced_cmd = gen.throw(val)
                            continue
                        except StopIteration as stop:
                            if isinstance(stop.value, ContinueAsNew):
                                return ReplayOutcome(commands=[stop.value])
                            return ReplayOutcome(commands=[CompleteWorkflow(result=stop.value)])
                    next_value = val
                    continue
                ctx.logger._set_replaying(False)
                pending.append(cmd)
                return ReplayOutcome(commands=pending)
            raise TypeError(f"workflow yielded unsupported command: {cmd!r}")
    except StopIteration as stop:
        if isinstance(stop.value, ContinueAsNew):
            return ReplayOutcome(commands=pending + [stop.value])
        return ReplayOutcome(commands=pending + [CompleteWorkflow(result=stop.value)])
    except Exception as exc:
        return ReplayOutcome(commands=[FailWorkflow(
            message=str(exc),
            exception_type=type(exc).__name__,
        )])
