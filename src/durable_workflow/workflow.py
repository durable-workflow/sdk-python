from __future__ import annotations

import contextlib
import hashlib
import logging
import random
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from . import serializer

_REGISTRY: dict[str, type] = {}


def defn(*, name: str):  # type: ignore[no-untyped-def]
    def wrap(cls: type) -> type:
        cls.__workflow_name__ = name  # type: ignore[attr-defined]
        _REGISTRY[name] = cls
        return cls

    return wrap


def registry() -> dict[str, type]:
    return dict(_REGISTRY)


# ── Commands yielded from a workflow ──────────────────────────────────
@dataclass
class ScheduleActivity:
    activity_type: str
    arguments: list[Any]
    queue: str | None = None

    def to_server_command(self, task_queue: str) -> dict[str, Any]:
        return {
            "type": "schedule_activity",
            "activity_type": self.activity_type,
            "arguments": serializer.encode(self.arguments),
            "queue": self.queue or task_queue,
        }


@dataclass
class StartTimer:
    delay_seconds: int

    def to_server_command(self, task_queue: str) -> dict[str, Any]:
        return {
            "type": "start_timer",
            "delay_seconds": self.delay_seconds,
        }


@dataclass
class CompleteWorkflow:
    result: Any

    def to_server_command(self, task_queue: str) -> dict[str, Any]:
        return {
            "type": "complete_workflow",
            "result": serializer.encode(self.result),
        }


@dataclass
class FailWorkflow:
    message: str
    exception_type: str | None = None
    non_retryable: bool = False

    def to_server_command(self, task_queue: str) -> dict[str, Any]:
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
    workflow_type: str | None = None
    arguments: list[Any] = field(default_factory=list)
    task_queue: str | None = None

    def to_server_command(self, task_queue: str) -> dict[str, Any]:
        cmd: dict[str, Any] = {"type": "continue_as_new"}
        if self.workflow_type is not None:
            cmd["workflow_type"] = self.workflow_type
        cmd["arguments"] = serializer.encode(self.arguments)
        cmd["task_queue"] = self.task_queue or task_queue
        return cmd


@dataclass
class RecordSideEffect:
    result: Any

    def to_server_command(self, task_queue: str) -> dict[str, Any]:
        return {
            "type": "record_side_effect",
            "result": serializer.encode(self.result),
        }


Command = ScheduleActivity | StartTimer | CompleteWorkflow | FailWorkflow | ContinueAsNew | RecordSideEffect


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
    def __init__(self, *, run_id: str = "", current_time: datetime | None = None) -> None:
        self._run_id = run_id
        self._current_time = current_time or datetime.now(timezone.utc)
        seed = int(hashlib.sha256(run_id.encode()).hexdigest()[:16], 16)
        self._rng = random.Random(seed)
        self.logger = _ReplayLogger(_REPLAY_LOGGER)

    def schedule_activity(
        self, activity_type: str, arguments: list[Any], *, queue: str | None = None
    ) -> ScheduleActivity:
        return ScheduleActivity(activity_type=activity_type, arguments=list(arguments), queue=queue)

    def start_timer(self, seconds: int) -> StartTimer:
        return StartTimer(delay_seconds=seconds)

    def side_effect(self, fn: Callable[[], Any]) -> RecordSideEffect:
        result = fn()
        return RecordSideEffect(result=result)

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


def replay(
    workflow_cls: type,
    history_events: Iterable[dict[str, Any]],
    start_input: list[Any],
    *,
    run_id: str = "",
) -> ReplayOutcome:
    events = list(history_events)

    workflow_start_time: datetime | None = None
    for ev in events:
        etype = ev.get("event_type") or ev.get("type")
        if etype in ("WorkflowStarted", "workflow_started"):
            ts = (ev.get("details") or ev.get("payload") or {}).get("timestamp") or ev.get("timestamp")
            if ts:
                with contextlib.suppress(ValueError, TypeError):
                    workflow_start_time = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            break

    instance = workflow_cls()
    ctx = WorkflowContext(run_id=run_id, current_time=workflow_start_time)

    resolved_results: list[Any] = []
    for ev in events:
        etype = ev.get("event_type") or ev.get("type")
        details = ev.get("details") or ev.get("payload") or {}
        if etype in ("ActivityCompleted", "activity_completed"):
            raw = details.get("result") or ev.get("result")
            resolved_results.append(serializer.decode(raw))
        elif etype in ("TimerFired", "timer_fired"):
            resolved_results.append(None)
        elif etype in ("SideEffectRecorded", "side_effect_recorded"):
            raw = details.get("result") or ev.get("result")
            resolved_results.append(serializer.decode(raw))

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
    try:
        while True:
            cmd = gen.send(None) if first else gen.send(next_value)
            first = False
            if isinstance(cmd, ContinueAsNew):
                return ReplayOutcome(commands=[cmd])
            if isinstance(cmd, RecordSideEffect):
                if result_cursor < len(resolved_results):
                    next_value = resolved_results[result_cursor]
                    result_cursor += 1
                    continue
                ctx.logger._set_replaying(False)
                pending.append(cmd)
                return ReplayOutcome(commands=pending)
            if isinstance(cmd, (ScheduleActivity, StartTimer)):
                if result_cursor < len(resolved_results):
                    next_value = resolved_results[result_cursor]
                    result_cursor += 1
                    continue
                ctx.logger._set_replaying(False)
                pending.append(cmd)
                return ReplayOutcome(commands=pending)
            raise TypeError(f"workflow yielded unsupported command: {cmd!r}")
    except StopIteration as stop:
        if isinstance(stop.value, ContinueAsNew):
            return ReplayOutcome(commands=[stop.value])
        return ReplayOutcome(commands=[CompleteWorkflow(result=stop.value)])
    except Exception as exc:
        return ReplayOutcome(commands=[FailWorkflow(
            message=str(exc),
            exception_type=type(exc).__name__,
        )])
