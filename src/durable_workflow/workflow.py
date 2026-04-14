"""Workflow decorator and replay context.

This MVP supports deterministic workflows with a generator-based style:

    @workflow.defn(name="greeter")
    class Greeter:
        def run(self, ctx, name):
            result = yield ctx.schedule_activity("greet", [name])
            return result

The ``run`` method is a generator that yields ``Command`` objects describing
what to do next. The worker replays the workflow against the history each time
a task is received. On first yield past the history cursor, the returned
command is emitted to the server. When the server later records the outcome
(e.g. ActivityCompleted), the next replay re-runs the generator and ``yield``
returns the recorded result.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from . import serializer


_REGISTRY: dict[str, type] = {}


def defn(*, name: str):
    def wrap(cls: type) -> type:
        cls.__workflow_name__ = name
        _REGISTRY[name] = cls
        return cls

    return wrap


def registry() -> dict[str, type]:
    return dict(_REGISTRY)


# ── Commands yielded from a workflow ──────────────────────────────────
@dataclass
class ScheduleActivity:
    activity_type: str
    arguments: list
    queue: str | None = None

    def to_server_command(self, task_queue: str) -> dict:
        return {
            "type": "schedule_activity",
            "activity_type": self.activity_type,
            "arguments": serializer.encode(self.arguments),
            "queue": self.queue or task_queue,
        }


@dataclass
class CompleteWorkflow:
    result: Any

    def to_server_command(self, task_queue: str) -> dict:
        return {
            "type": "complete_workflow",
            "result": serializer.encode(self.result),
        }


# ── Context passed to the workflow's run() ───────────────────────────
class WorkflowContext:
    """Yield helpers. The worker drives the generator; the context simply
    constructs well-formed command objects."""

    def schedule_activity(self, activity_type: str, arguments: list, *, queue: str | None = None) -> ScheduleActivity:
        return ScheduleActivity(activity_type=activity_type, arguments=list(arguments), queue=queue)


# ── Replay ───────────────────────────────────────────────────────────
@dataclass
class ReplayOutcome:
    commands: list  # list[ScheduleActivity | CompleteWorkflow]


def replay(workflow_cls: type, history_events: Iterable[dict], start_input: list) -> ReplayOutcome:
    """Run the workflow generator against history.

    Returns the next command(s) to emit. For MVP we only support a single
    outstanding activity at a time; when we hit an unresolved yield, we return
    that command. If ``run`` returns, we emit ``complete_workflow``.
    """
    instance = workflow_cls()
    ctx = WorkflowContext()

    events = list(history_events)
    activity_results: list[Any] = []
    for ev in events:
        etype = ev.get("event_type") or ev.get("type")
        if etype in ("ActivityCompleted", "activity_completed"):
            details = ev.get("details") or {}
            raw = details.get("result") or ev.get("result")
            activity_results.append(serializer.decode(raw))

    gen = instance.run(ctx, *start_input)
    if not hasattr(gen, "__next__"):
        # Non-generator workflow — treat the return value as the completion.
        return ReplayOutcome(commands=[CompleteWorkflow(result=gen)])

    next_value: Any = None
    first = True
    try:
        while True:
            cmd = gen.send(None) if first else gen.send(next_value)
            first = False
            if isinstance(cmd, ScheduleActivity):
                if activity_results:
                    next_value = activity_results.pop(0)
                    continue
                return ReplayOutcome(commands=[cmd])
            raise TypeError(f"workflow yielded unsupported command: {cmd!r}")
    except StopIteration as stop:
        return ReplayOutcome(commands=[CompleteWorkflow(result=stop.value)])
