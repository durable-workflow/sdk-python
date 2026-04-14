from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
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


Command = ScheduleActivity | StartTimer | CompleteWorkflow | FailWorkflow


# ── Context passed to the workflow's run() ───────────────────────────
class WorkflowContext:
    def schedule_activity(
        self, activity_type: str, arguments: list[Any], *, queue: str | None = None
    ) -> ScheduleActivity:
        return ScheduleActivity(activity_type=activity_type, arguments=list(arguments), queue=queue)

    def start_timer(self, seconds: int) -> StartTimer:
        return StartTimer(delay_seconds=seconds)


# ── Replay ───────────────────────────────────────────────────────────
@dataclass
class ReplayOutcome:
    commands: list[Command]


def replay(workflow_cls: type, history_events: Iterable[dict[str, Any]], start_input: list[Any]) -> ReplayOutcome:
    instance = workflow_cls()
    ctx = WorkflowContext()

    events = list(history_events)

    resolved_results: list[Any] = []
    for ev in events:
        etype = ev.get("event_type") or ev.get("type")
        details = ev.get("details") or {}
        if etype in ("ActivityCompleted", "activity_completed"):
            raw = details.get("result") or ev.get("result")
            resolved_results.append(serializer.decode(raw))
        elif etype in ("TimerFired", "timer_fired"):
            resolved_results.append(None)

    gen = instance.run(ctx, *start_input)
    if not hasattr(gen, "__next__"):
        return ReplayOutcome(commands=[CompleteWorkflow(result=gen)])

    result_cursor = 0
    next_value: Any = None
    first = True
    pending: list[Command] = []
    try:
        while True:
            cmd = gen.send(None) if first else gen.send(next_value)
            first = False
            if isinstance(cmd, (ScheduleActivity, StartTimer)):
                if result_cursor < len(resolved_results):
                    next_value = resolved_results[result_cursor]
                    result_cursor += 1
                    continue
                pending.append(cmd)
                return ReplayOutcome(commands=pending)
            raise TypeError(f"workflow yielded unsupported command: {cmd!r}")
    except StopIteration as stop:
        return ReplayOutcome(commands=[CompleteWorkflow(result=stop.value)])
    except Exception as exc:
        return ReplayOutcome(commands=[FailWorkflow(
            message=str(exc),
            exception_type=type(exc).__name__,
        )])
