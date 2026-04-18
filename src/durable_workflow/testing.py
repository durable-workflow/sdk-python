"""In-process test harness for workflow authoring.

:class:`WorkflowEnvironment` drives a workflow to completion in a single
Python process, without a running server or worker. It reuses the same
:func:`durable_workflow.workflow.replay` machinery the worker uses, but
resolves yielded commands against user-registered activity mocks and
auto-fires timers / side-effects / search-attribute upserts so tests do
not need a real clock or Redis.

Typical use::

    def test_my_workflow():
        env = WorkflowEnvironment()
        env.register_activity_result("charge_card", {"id": "ch_1"})
        env.register_activity_result("send_receipt", None)
        result = env.execute_workflow(OrderWorkflow, "order-1", {"amount": 42})
        assert result == {"status": "complete", "charge_id": "ch_1"}

For regression-testing workflow code against production histories, use
:func:`replay_history` — it hands the real durable history straight to
the worker's replayer and surfaces any non-determinism as a raised
exception.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from . import serializer
from .errors import WorkflowCancelled, WorkflowFailed, WorkflowTerminated
from .workflow import (
    CompleteWorkflow,
    ContinueAsNew,
    FailWorkflow,
    RecordSideEffect,
    RecordVersionMarker,
    ReplayOutcome,
    ScheduleActivity,
    StartChildWorkflow,
    StartTimer,
    UpsertSearchAttributes,
    replay,
)


class WorkflowEnvironment:
    """Drives a workflow to completion against user-registered activity mocks."""

    def __init__(self, *, iteration_limit: int = 1000) -> None:
        self._activity_results: dict[str, Any] = {}
        self._activity_fns: dict[str, Callable[..., Any]] = {}
        self._child_workflow_results: dict[str, Any] = {}
        self._pending_signals: list[tuple[str, list[Any]]] = []
        self._iteration_limit = iteration_limit

    def register_activity_result(self, name: str, result: Any) -> None:
        """Canned response: every call to ``name`` returns ``result``."""
        self._activity_results[name] = result

    def register_activity(self, name: str, fn: Callable[..., Any]) -> None:
        """Callable mock: ``fn(*arguments)`` is invoked for each scheduled call.

        Use this when the test needs the mock to vary with arguments (e.g.
        look up by order id) or to capture invocations.
        """
        self._activity_fns[name] = fn

    def register_child_workflow_result(self, workflow_type: str, result: Any) -> None:
        """Canned response for child workflow completions."""
        self._child_workflow_results[workflow_type] = result

    def signal(self, name: str, args: list[Any] | None = None) -> None:
        """Queue a signal to be delivered before the next iteration.

        Signals are drained in the order they were queued and injected into
        the workflow history as ``SignalReceived`` events; the replayer then
        dispatches each to its registered ``@workflow.signal`` handler.
        """
        self._pending_signals.append((name, list(args) if args is not None else []))

    def execute_workflow(
        self,
        workflow_cls: type,
        *args: Any,
        run_id: str = "test-run",
    ) -> Any:
        """Drive ``workflow_cls`` to a terminal state and return its result.

        Raises :class:`~durable_workflow.errors.WorkflowFailed` if the workflow
        ended in the ``failed`` state. Activities that do not have a
        registered mock raise :class:`KeyError` so tests fail loudly on
        missing fixtures.
        """
        history: list[dict[str, Any]] = []

        for _ in range(self._iteration_limit):
            self._drain_pending_signals_into(history)
            outcome = replay(workflow_cls, history, list(args), run_id=run_id)
            terminal = self._apply_commands(outcome, history)
            if terminal is not _SENTINEL:
                return terminal

        raise RuntimeError(
            f"workflow did not terminate within {self._iteration_limit} iterations; "
            "check for missing activity mocks or signals that never satisfy a wait."
        )

    def _drain_pending_signals_into(self, history: list[dict[str, Any]]) -> None:
        while self._pending_signals:
            name, sig_args = self._pending_signals.pop(0)
            history.append(
                {
                    "event_type": "SignalReceived",
                    "payload": {
                        "signal_name": name,
                        "value": serializer.envelope(sig_args),
                        "payload_codec": serializer.AVRO_CODEC,
                    },
                }
            )

    def _apply_commands(
        self, outcome: ReplayOutcome, history: list[dict[str, Any]]
    ) -> Any:
        for cmd in outcome.commands:
            if isinstance(cmd, CompleteWorkflow):
                return cmd.result
            if isinstance(cmd, FailWorkflow):
                raise WorkflowFailed(cmd.message, cmd.exception_type)
            if isinstance(cmd, ContinueAsNew):
                raise NotImplementedError(
                    "continue_as_new is not yet supported by the test harness; "
                    "drive each run explicitly with a separate execute_workflow call."
                )
            if isinstance(cmd, ScheduleActivity):
                history.append(self._resolve_activity(cmd))
            elif isinstance(cmd, StartTimer):
                history.append({"event_type": "TimerFired", "payload": {}})
            elif isinstance(cmd, StartChildWorkflow):
                history.append(self._resolve_child_workflow(cmd))
            elif isinstance(cmd, RecordSideEffect):
                history.append(
                    {
                        "event_type": "SideEffectRecorded",
                        "payload": {
                            "result": serializer.envelope(cmd.result),
                            "payload_codec": serializer.AVRO_CODEC,
                        },
                    }
                )
            elif isinstance(cmd, UpsertSearchAttributes):
                history.append(
                    {"event_type": "SearchAttributesUpserted", "payload": {}}
                )
            elif isinstance(cmd, RecordVersionMarker):
                history.append(
                    {
                        "event_type": "VersionMarkerRecorded",
                        "payload": {"version": cmd.version},
                    }
                )
            else:
                raise TypeError(f"unsupported command in test harness: {cmd!r}")
        return _SENTINEL

    def _resolve_activity(self, cmd: ScheduleActivity) -> dict[str, Any]:
        if cmd.activity_type in self._activity_fns:
            result = self._activity_fns[cmd.activity_type](*cmd.arguments)
        elif cmd.activity_type in self._activity_results:
            result = self._activity_results[cmd.activity_type]
        else:
            raise KeyError(
                f"no mock registered for activity {cmd.activity_type!r}; "
                "call env.register_activity_result() or env.register_activity()."
            )
        return {
            "event_type": "ActivityCompleted",
            "payload": {
                "result": serializer.envelope(result),
                "payload_codec": serializer.AVRO_CODEC,
            },
        }

    def _resolve_child_workflow(self, cmd: StartChildWorkflow) -> dict[str, Any]:
        if cmd.workflow_type not in self._child_workflow_results:
            raise KeyError(
                f"no mock registered for child workflow {cmd.workflow_type!r}; "
                "call env.register_child_workflow_result()."
            )
        return {
            "event_type": "ChildRunCompleted",
            "payload": {
                "result": serializer.envelope(self._child_workflow_results[cmd.workflow_type]),
                "payload_codec": serializer.AVRO_CODEC,
            },
        }


# Sentinel marking "no terminal command seen this iteration".
_SENTINEL = object()


def replay_history(
    workflow_cls: type,
    history_events: Iterable[dict[str, Any]],
    start_input: list[Any] | None = None,
    *,
    run_id: str = "",
    payload_codec: str | None = None,
) -> ReplayOutcome:
    """Replay a production history against current workflow code.

    Hands the durable history directly to the worker's replayer. Raises any
    exception the workflow would raise during replay — for example a
    non-determinism failure when ``run`` yields a different command sequence
    from the one recorded in history.

    This is the supported way to regression-test a workflow change against
    real production traffic: dump the history from ``Client.get_history``,
    save the JSON, and replay it on every PR.
    """
    return replay(
        workflow_cls,
        history_events,
        list(start_input or []),
        run_id=run_id,
        payload_codec=payload_codec,
    )


def replay_history_file(
    workflow_cls: type,
    path: str | Path,
    start_input: list[Any] | None = None,
    *,
    run_id: str = "",
    payload_codec: str | None = None,
) -> ReplayOutcome:
    """Convenience wrapper: load a JSON history file and replay it.

    Accepts either a list of events at the top level or a dict with an
    ``events`` key (matching the shape of ``Client.get_history``).
    """
    data = json.loads(Path(path).read_text())
    events = data["events"] if isinstance(data, dict) else data
    return replay_history(
        workflow_cls,
        events,
        start_input,
        run_id=run_id,
        payload_codec=payload_codec,
    )


__all__ = [
    "WorkflowEnvironment",
    "replay_history",
    "replay_history_file",
]


# Re-export terminal exceptions the harness may raise so tests can catch
# them without hunting for the right import path.
_TERMINAL_EXCEPTIONS = (WorkflowFailed, WorkflowCancelled, WorkflowTerminated)
