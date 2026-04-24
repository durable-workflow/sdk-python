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

When a workflow yields :class:`~durable_workflow.workflow.ContinueAsNew`,
the harness records a ``WorkflowContinuedAsNew`` event on the completing
run, resets history, and starts a new run with the command's arguments.
:attr:`WorkflowEnvironment.runs` exposes the per-run record (input,
workflow type, history events, and terminal command) so tests can assert
on the whole chain. The chain length is bounded by
``continue_as_new_limit``; exceeding it raises :exc:`RuntimeError`.

For regression-testing workflow code against production histories, use
:func:`replay_history` — it hands the real durable history straight to
the worker's replayer and surfaces any non-determinism as a raised
exception.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
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
    registry,
    replay,
)


@dataclass
class WorkflowRunRecord:
    """One run within an ``execute_workflow`` chain.

    A single-run workflow produces one record with :attr:`terminal` of
    type :class:`~durable_workflow.workflow.CompleteWorkflow` or
    :class:`~durable_workflow.workflow.FailWorkflow`. A workflow that
    yields :class:`~durable_workflow.workflow.ContinueAsNew` produces
    one record per link; the terminal of each non-final link is the
    ``ContinueAsNew`` command itself and the final link terminates as
    usual.
    """

    workflow_type: str
    input: list[Any]
    history: list[dict[str, Any]] = field(default_factory=list)
    terminal: Any = None


class WorkflowEnvironment:
    """Drives a workflow to completion against user-registered activity mocks.

    :param iteration_limit: upper bound on per-run replay iterations
        (guard against workflows that never yield a terminal command).
    :param continue_as_new_limit: upper bound on the number of
        continue-as-new links in one ``execute_workflow`` chain. The
        first run counts as link 1; a workflow that continues five
        times runs six links. Exceeding the limit raises
        :exc:`RuntimeError`.
    """

    def __init__(
        self,
        *,
        iteration_limit: int = 1000,
        continue_as_new_limit: int = 50,
    ) -> None:
        self._activity_results: dict[str, Any] = {}
        self._activity_fns: dict[str, Callable[..., Any]] = {}
        self._child_workflow_results: dict[str, Any] = {}
        self._pending_signals: list[tuple[str, list[Any]]] = []
        self._queued_signals_by_run: dict[int, list[tuple[str, list[Any]]]] = {}
        self._iteration_limit = iteration_limit
        self._continue_as_new_limit = continue_as_new_limit
        self._registered_workflows: dict[str, type] = {}
        self._runs: list[WorkflowRunRecord] = []

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

    def register_workflow(self, workflow_cls: type) -> None:
        """Make ``workflow_cls`` resolvable by name for continue-as-new.

        ``continue_as_new(workflow_type="other-name")`` looks up the
        next workflow class by its ``@workflow.defn`` name. The class
        passed to :meth:`execute_workflow` is registered automatically;
        call this method for additional workflow classes the chain may
        continue into.
        """
        name = getattr(workflow_cls, "__workflow_name__", None)
        if not isinstance(name, str) or not name:
            raise TypeError(
                f"{workflow_cls!r} is not a workflow class; "
                "decorate it with @workflow.defn(name=...) first."
            )
        self._registered_workflows[name] = workflow_cls

    def signal(
        self,
        name: str,
        args: list[Any] | None = None,
        *,
        run: int | None = None,
    ) -> None:
        """Queue a signal to be delivered before the next iteration.

        Signals are drained in the order they were queued and injected
        into the workflow history as ``SignalReceived`` events; the
        replayer dispatches each to its registered ``@workflow.signal``
        handler.

        Pass ``run`` to target a specific link in a continue-as-new
        chain (``run=1`` is the first run, ``run=2`` the run that
        starts after the first ``ContinueAsNew``, and so on). Without
        ``run`` the signal is consumed at the current front of the
        chain — on the first run before the first iteration, and on
        later iterations in whatever link happens to be running.
        """
        normalized = (name, list(args) if args is not None else [])
        if run is None:
            self._pending_signals.append(normalized)
            return
        if run < 1:
            raise ValueError("run number must be 1 or greater")
        self._queued_signals_by_run.setdefault(run, []).append(normalized)

    @property
    def runs(self) -> list[WorkflowRunRecord]:
        """Per-run records produced by the most recent ``execute_workflow`` call.

        Empty before the first ``execute_workflow`` call. Reset at the
        start of each call.
        """
        return list(self._runs)

    @property
    def run_count(self) -> int:
        """Number of runs in the most recent ``execute_workflow`` chain."""
        return len(self._runs)

    def execute_workflow(
        self,
        workflow_cls: type,
        *args: Any,
        run_id: str = "test-run",
    ) -> Any:
        """Drive ``workflow_cls`` to a terminal state and return its result.

        Raises :class:`~durable_workflow.errors.WorkflowFailed` if the
        workflow ended in the ``failed`` state. Activities that do not
        have a registered mock raise :class:`KeyError` so tests fail
        loudly on missing fixtures.

        If the workflow yields
        :class:`~durable_workflow.workflow.ContinueAsNew`, the harness
        starts a new run with the new input and, if the command named a
        ``workflow_type``, the matching registered workflow class. The
        chain is bounded by ``continue_as_new_limit`` and returns the
        terminal result of the final run.
        """
        self._runs = []
        self.register_workflow(workflow_cls)
        current_cls = workflow_cls
        current_args: list[Any] = list(args)
        current_run_id = run_id

        for link_index in range(self._continue_as_new_limit):
            history: list[dict[str, Any]] = []
            link_number = link_index + 1
            record = WorkflowRunRecord(
                workflow_type=_workflow_name(current_cls),
                input=list(current_args),
                history=history,
            )
            self._runs.append(record)

            if link_index == 0:
                self._drain_pending_signals_into(history)
            for queued in self._queued_signals_by_run.pop(link_number, []):
                history.append(_signal_event(*queued))

            continuation: ContinueAsNew | None = None
            terminal_outcome: Any = _SENTINEL
            for _ in range(self._iteration_limit):
                self._drain_pending_signals_into(history)
                outcome = replay(
                    current_cls, history, list(current_args), run_id=current_run_id
                )
                terminal_outcome, continuation = self._apply_commands(outcome, history)
                if continuation is not None or terminal_outcome is not _SENTINEL:
                    break
            else:
                raise RuntimeError(
                    f"workflow {record.workflow_type!r} did not terminate within "
                    f"{self._iteration_limit} iterations; check for missing "
                    "activity mocks or signals that never satisfy a wait."
                )

            if continuation is not None:
                record.terminal = continuation
                history.append(_continued_as_new_event(continuation))
                current_cls = self._resolve_next_workflow(continuation, current_cls)
                current_args = list(continuation.arguments)
                current_run_id = f"{current_run_id}-continued"
                continue

            record.terminal = terminal_outcome
            if isinstance(terminal_outcome, WorkflowFailed):
                raise terminal_outcome
            if isinstance(terminal_outcome, CompleteWorkflow):
                return terminal_outcome.result
            return terminal_outcome

        raise RuntimeError(
            f"workflow chained continue-as-new past the {self._continue_as_new_limit}"
            " link limit; raise continue_as_new_limit or tighten the workflow's "
            "termination condition."
        )

    def _resolve_next_workflow(
        self, continuation: ContinueAsNew, current_cls: type
    ) -> type:
        target_name = continuation.workflow_type
        if not target_name:
            return current_cls
        current_name = getattr(current_cls, "__workflow_name__", None)
        if target_name == current_name:
            return current_cls
        if target_name in self._registered_workflows:
            return self._registered_workflows[target_name]
        global_registry = registry()
        if target_name in global_registry:
            return global_registry[target_name]
        raise KeyError(
            f"continue_as_new targeted workflow type {target_name!r} "
            "but no matching class is registered; call "
            "env.register_workflow(YourWorkflow) before execute_workflow()."
        )

    def _drain_pending_signals_into(self, history: list[dict[str, Any]]) -> None:
        while self._pending_signals:
            name, sig_args = self._pending_signals.pop(0)
            history.append(_signal_event(name, sig_args))

    def _apply_commands(
        self, outcome: ReplayOutcome, history: list[dict[str, Any]]
    ) -> tuple[Any, ContinueAsNew | None]:
        for cmd in outcome.commands:
            if isinstance(cmd, CompleteWorkflow):
                return cmd, None
            if isinstance(cmd, FailWorkflow):
                return WorkflowFailed(cmd.message, cmd.exception_type), None
            if isinstance(cmd, ContinueAsNew):
                return _SENTINEL, cmd
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
        return _SENTINEL, None

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


def _workflow_name(cls: type) -> str:
    name = getattr(cls, "__workflow_name__", None)
    return name if isinstance(name, str) and name else cls.__name__


def _signal_event(name: str, sig_args: list[Any]) -> dict[str, Any]:
    return {
        "event_type": "SignalReceived",
        "payload": {
            "signal_name": name,
            "value": serializer.envelope(sig_args),
            "payload_codec": serializer.AVRO_CODEC,
        },
    }


def _continued_as_new_event(cmd: ContinueAsNew) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "arguments": serializer.envelope(cmd.arguments),
        "payload_codec": serializer.AVRO_CODEC,
    }
    if cmd.workflow_type is not None:
        payload["workflow_type"] = cmd.workflow_type
    if cmd.task_queue is not None:
        payload["queue"] = cmd.task_queue
    return {"event_type": "WorkflowContinuedAsNew", "payload": payload}


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
    "WorkflowRunRecord",
    "replay_history",
    "replay_history_file",
]


# Re-export terminal exceptions the harness may raise so tests can catch
# them without hunting for the right import path.
_TERMINAL_EXCEPTIONS = (WorkflowFailed, WorkflowCancelled, WorkflowTerminated)
