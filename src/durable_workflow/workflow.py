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
from .errors import ChildWorkflowFailed, QueryFailed, WorkflowPayloadDecodeError

_REGISTRY: dict[str, type] = {}


def defn(*, name: str):  # type: ignore[no-untyped-def]
    """Register a class as a workflow type under a language-neutral name.

    Scans the class for ``@signal``, ``@query``, and ``@update`` decorated
    methods and builds registries at decoration time so worker-side dispatch
    can use stable receiver names without re-inspecting the class on every
    history event or control-plane request.
    """

    def wrap(cls: type) -> type:
        cls.__workflow_name__ = name  # type: ignore[attr-defined]
        signals: dict[str, str] = {}
        queries: dict[str, str] = {}
        updates: dict[str, str] = {}
        update_validators: dict[str, str] = {}
        for attr in dir(cls):
            if attr.startswith("_"):
                continue
            member = getattr(cls, attr, None)
            signal_name = getattr(member, "__signal_name__", None)
            if isinstance(signal_name, str) and signal_name:
                signals[signal_name] = attr
            query_name = getattr(member, "__query_name__", None)
            if isinstance(query_name, str) and query_name:
                queries[query_name] = attr
            update_name = getattr(member, "__update_name__", None)
            if isinstance(update_name, str) and update_name:
                updates[update_name] = attr
            update_validator_name = getattr(member, "__update_validator_name__", None)
            if isinstance(update_validator_name, str) and update_validator_name:
                update_validators[update_validator_name] = attr
        cls.__workflow_signals__ = signals  # type: ignore[attr-defined]
        cls.__workflow_queries__ = queries  # type: ignore[attr-defined]
        cls.__workflow_updates__ = updates  # type: ignore[attr-defined]
        cls.__workflow_update_validators__ = update_validators  # type: ignore[attr-defined]
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


def query(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a workflow method as a read-only query handler.

    Query methods are invoked against replayed workflow state. They must not
    mutate ``self`` or perform I/O. The server-side worker query transport is
    still implemented separately; this decorator records the Python receiver
    metadata and is used by :func:`query_state`.
    """

    def wrap(method: Callable[..., Any]) -> Callable[..., Any]:
        method.__query_name__ = name  # type: ignore[attr-defined]
        return method

    return wrap


def update(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a workflow method as an update handler.

    The returned function also exposes ``.validator`` for the common pattern::

        @workflow.update("approve")
        def approve(self, approved: bool) -> dict: ...

        @approve.validator
        def validate_approve(self, approved: bool) -> None: ...

    This release records receiver metadata only. The server-side Python update
    execution transport is tracked separately.
    """

    def wrap(method: Callable[..., Any]) -> Callable[..., Any]:
        method.__update_name__ = name  # type: ignore[attr-defined]

        def validator(validator_method: Callable[..., Any]) -> Callable[..., Any]:
            validator_method.__update_validator_name__ = name  # type: ignore[attr-defined]
            return validator_method

        method.validator = validator  # type: ignore[attr-defined]
        return method

    return wrap


def update_validator(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a workflow method as the validator for an update name."""

    def wrap(method: Callable[..., Any]) -> Callable[..., Any]:
        method.__update_validator_name__ = name  # type: ignore[attr-defined]
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
    budget for an already-scheduled activity. It is a server-side durable
    retry policy, not the SDK HTTP transport retry policy.

    ``non_retryable_error_types`` names failure types that should bypass this
    retry budget. An activity worker can also report ``non_retryable=True`` on
    a failure to stop retrying that activity execution.
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
PayloadWarningContext = serializer.PayloadSizeWarningContext | Mapping[str, Any] | None


def _payload_warning_context(
    base: PayloadWarningContext,
    *,
    kind: str,
    task_queue: str | None = None,
    activity_name: str | None = None,
    update_name: str | None = None,
) -> dict[str, str]:
    if isinstance(base, serializer.PayloadSizeWarningContext):
        context: dict[str, str] = base.to_log_context()
    elif base is None:
        context = {}
    else:
        context = {
            str(key): str(value)
            for key, value in base.items()
            if value is not None
        }

    context["kind"] = kind
    if task_queue is not None:
        context["task_queue"] = task_queue
    if activity_name is not None:
        context["activity_name"] = activity_name
    if update_name is not None:
        context["update_name"] = update_name
    return context


@dataclass
class ChildWorkflowRetryPolicy(ActivityRetryPolicy):
    """Retry policy applied to one started child workflow call.

    This is recorded with the child workflow command and controls durable
    server-side child attempts. It is separate from SDK HTTP transport retry
    and from activity retry.
    """


ChildWorkflowRetryPolicyInput = ChildWorkflowRetryPolicy | ActivityRetryPolicy | Mapping[str, Any]


@dataclass
class ScheduleActivity:
    """Command requesting an activity task.

    Timeout fields are activity budgets, not HTTP request timeouts:
    ``start_to_close_timeout`` limits one activity attempt,
    ``schedule_to_start_timeout`` limits queue wait before an attempt starts,
    ``schedule_to_close_timeout`` limits the whole activity execution including
    retries, and ``heartbeat_timeout`` limits the gap between activity
    heartbeats.
    """

    activity_type: str
    arguments: list[Any]
    queue: str | None = None
    retry_policy: ActivityRetryPolicyInput | None = None
    start_to_close_timeout: int | None = None
    schedule_to_start_timeout: int | None = None
    schedule_to_close_timeout: int | None = None
    heartbeat_timeout: int | None = None

    def to_server_command(
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
    ) -> dict[str, Any]:
        command: dict[str, Any] = {
            "type": "schedule_activity",
            "activity_type": self.activity_type,
            "arguments": serializer.envelope(
                self.arguments,
                codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="activity_input",
                    task_queue=self.queue or task_queue,
                    activity_name=self.activity_type,
                ),
            ),
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
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
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
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
    ) -> dict[str, Any]:
        return {
            "type": "complete_workflow",
            "result": serializer.envelope(
                self.result,
                codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="workflow_result",
                    task_queue=task_queue,
                ),
            ),
        }


@dataclass
class FailWorkflow:
    """Command failing a workflow with diagnostic metadata."""

    message: str
    exception_type: str | None = None
    non_retryable: bool = False

    def to_server_command(
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
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
class CompleteUpdate:
    """Worker command completing an accepted workflow update."""

    update_id: str
    result: Any

    def to_server_command(
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
    ) -> dict[str, Any]:
        return {
            "type": "complete_update",
            "update_id": self.update_id,
            "result": serializer.envelope(
                self.result,
                codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="update_result",
                    task_queue=task_queue,
                ),
            ),
        }


@dataclass
class FailUpdate:
    """Worker command failing an accepted workflow update."""

    update_id: str
    message: str
    exception_type: str | None = None
    exception_class: str | None = None
    non_retryable: bool = True

    def to_server_command(
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
    ) -> dict[str, Any]:
        cmd: dict[str, Any] = {
            "type": "fail_update",
            "update_id": self.update_id,
            "message": self.message,
        }
        if self.exception_type is not None:
            cmd["exception_type"] = self.exception_type
        if self.exception_class is not None:
            cmd["exception_class"] = self.exception_class
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
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
    ) -> dict[str, Any]:
        cmd: dict[str, Any] = {"type": "continue_as_new"}
        if self.workflow_type is not None:
            cmd["workflow_type"] = self.workflow_type
        cmd["arguments"] = serializer.envelope(
            self.arguments,
            codec=payload_codec,
            size_warning=size_warning,
            warning_context=_payload_warning_context(
                warning_context,
                kind="continue_as_new_input",
                task_queue=self.task_queue or task_queue,
            ),
        )
        cmd["queue"] = self.task_queue or task_queue
        return cmd


@dataclass
class RecordSideEffect:
    """Command recording the result of a non-deterministic function."""

    result: Any

    def to_server_command(
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
    ) -> dict[str, Any]:
        return {
            "type": "record_side_effect",
            "result": serializer.encode(
                self.result,
                codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="side_effect_result",
                    task_queue=task_queue,
                ),
            ),
        }


@dataclass
class StartChildWorkflow:
    """Command requesting a child workflow run.

    ``execution_timeout_seconds`` limits the overall child workflow execution.
    ``run_timeout_seconds`` limits one child run. These budgets are durable
    server-side workflow budgets and are separate from client HTTP timeouts.
    """

    workflow_type: str
    arguments: list[Any] = field(default_factory=list)
    task_queue: str | None = None
    parent_close_policy: str | None = None
    retry_policy: ChildWorkflowRetryPolicyInput | None = None
    execution_timeout_seconds: int | None = None
    run_timeout_seconds: int | None = None

    def to_server_command(
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
    ) -> dict[str, Any]:
        cmd: dict[str, Any] = {
            "type": "start_child_workflow",
            "workflow_type": self.workflow_type,
            "arguments": serializer.envelope(
                self.arguments,
                codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="child_workflow_input",
                    task_queue=self.task_queue or task_queue,
                ),
            ),
        }
        if self.task_queue is not None:
            cmd["queue"] = self.task_queue
        else:
            cmd["queue"] = task_queue
        if self.parent_close_policy is not None:
            cmd["parent_close_policy"] = self.parent_close_policy
        if self.retry_policy is not None:
            cmd["retry_policy"] = (
                self.retry_policy.to_dict()
                if isinstance(self.retry_policy, ActivityRetryPolicy)
                else dict(self.retry_policy)
            )
        if self.execution_timeout_seconds is not None:
            cmd["execution_timeout_seconds"] = self.execution_timeout_seconds
        if self.run_timeout_seconds is not None:
            cmd["run_timeout_seconds"] = self.run_timeout_seconds
        return cmd


@dataclass
class RecordVersionMarker:
    """Command recording a workflow code-version marker."""

    change_id: str
    version: int
    min_supported: int
    max_supported: int

    def to_server_command(
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
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
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
    ) -> dict[str, Any]:
        serializer.warn_if_json_payload_near_limit(
            self.attributes,
            size_warning=size_warning,
            warning_context=_payload_warning_context(
                warning_context,
                kind="search_attributes",
                task_queue=task_queue,
            ),
        )
        return {
            "type": "upsert_search_attributes",
            "attributes": self.attributes,
        }


@dataclass
class WaitCondition:
    """Command that yields execution until a workflow-defined predicate becomes true.

    The replayer evaluates ``predicate`` locally against in-memory workflow state
    (typically mutated by signal/update handlers). The server records a
    ``ConditionWaitOpened`` history event and re-drives the workflow when any
    signal arrives or, if ``timeout_seconds`` is provided, when the timeout
    elapses (a ``TimerFired`` history event with ``timer_kind=condition_timeout``).
    """

    predicate: Callable[[], bool]
    condition_key: str | None = None
    condition_definition_fingerprint: str | None = None
    timeout_seconds: int | None = None

    def to_server_command(
        self,
        task_queue: str,
        *,
        payload_codec: str = serializer.AVRO_CODEC,
        size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
        warning_context: PayloadWarningContext = None,
    ) -> dict[str, Any]:
        cmd: dict[str, Any] = {"type": "open_condition_wait"}
        if self.condition_key is not None:
            cmd["condition_key"] = self.condition_key
        if self.condition_definition_fingerprint is not None:
            cmd["condition_definition_fingerprint"] = self.condition_definition_fingerprint
        if self.timeout_seconds is not None:
            cmd["timeout_seconds"] = self.timeout_seconds
        return cmd


Command = (
    ScheduleActivity | StartTimer | CompleteWorkflow | FailWorkflow
    | CompleteUpdate | FailUpdate | ContinueAsNew | RecordSideEffect | StartChildWorkflow
    | RecordVersionMarker | UpsertSearchAttributes | WaitCondition
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
        """Yield a durable timer that resolves after ``seconds`` seconds."""
        return StartTimer(delay_seconds=seconds)

    def sleep(self, seconds: float) -> StartTimer:
        """Sleep for ``seconds`` seconds of durable wall time.

        Sugar over :meth:`start_timer` that accepts a float and rounds up to
        the next whole second (the server stores timer deadlines as integer
        seconds). The call is still a single yield of a durable command —
        use ``yield ctx.sleep(60)`` or bare ``yield ctx.sleep(60)`` from the
        workflow ``run`` method.
        """
        return StartTimer(delay_seconds=max(0, math.ceil(seconds)))

    def wait_condition(
        self,
        predicate: Callable[[], bool],
        *,
        key: str | None = None,
        timeout: float | None = None,
    ) -> WaitCondition:
        """Yield execution until ``predicate()`` returns truthy.

        The predicate is evaluated against the workflow's in-memory state on
        every replay tick — typically mutated by ``@signal`` / ``@update``
        handlers as external events arrive. If ``timeout`` is provided and
        elapses before the predicate becomes true, the yield resolves to
        ``False`` (otherwise ``True``). The fractional ``timeout`` is rounded
        up to the next whole second to match the server's integer-second
        timer resolution.
        """
        timeout_seconds: int | None = None
        if timeout is not None:
            timeout_seconds = max(0, math.ceil(timeout))
        return WaitCondition(
            predicate=predicate,
            condition_key=key,
            timeout_seconds=timeout_seconds,
        )

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
        retry_policy: ChildWorkflowRetryPolicyInput | None = None,
        execution_timeout_seconds: int | None = None,
        run_timeout_seconds: int | None = None,
    ) -> StartChildWorkflow:
        return StartChildWorkflow(
            workflow_type=workflow_type,
            arguments=list(arguments) if arguments is not None else [],
            task_queue=task_queue,
            parent_close_policy=parent_close_policy,
            retry_policy=retry_policy,
            execution_timeout_seconds=execution_timeout_seconds,
            run_timeout_seconds=run_timeout_seconds,
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


@dataclass
class _ReplayState:
    outcome: ReplayOutcome
    instance: Any


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


def _decode_update_args(payload: dict[str, Any], fallback_codec: str | None) -> list[Any]:
    codec = payload.get("payload_codec") or fallback_codec
    raw = payload.get("arguments")
    if raw is None:
        return []
    decoded = serializer.decode_envelope(raw, codec=codec)
    if isinstance(decoded, list):
        return decoded
    return [decoded]


def _event_id(event: Mapping[str, Any]) -> str | None:
    for key in ("event_id", "id", "sequence"):
        value = event.get(key)
        if value is not None:
            return str(value)
    return None


def _history_payload_head(payload: Mapping[str, Any], receiver_kind: str) -> str | None:
    raw = payload.get("arguments")
    if receiver_kind == "signal":
        raw = payload.get("value")
        if raw is None:
            raw = payload.get("input")
        if raw is None:
            raw = payload.get("arguments")
    if isinstance(raw, Mapping):
        blob = raw.get("blob")
        if blob is not None:
            raw = blob
    if raw is None:
        return None
    return str(raw)[:128]


def _workflow_id_from_history(events: Iterable[dict[str, Any]]) -> str | None:
    for event in events:
        payload = event.get("payload") or {}
        workflow_id = payload.get("workflow_id") or event.get("workflow_id")
        if workflow_id is not None:
            return str(workflow_id)
    return None


def _decode_receiver_args(
    event: Mapping[str, Any],
    *,
    receiver_kind: str,
    receiver_name: str,
    workflow_id: str | None,
    run_id: str,
    payload_codec: str | None,
) -> list[Any]:
    payload = event.get("payload") or {}
    if not isinstance(payload, Mapping):
        payload = {}
    codec = payload.get("payload_codec") or payload_codec
    try:
        if receiver_kind == "signal":
            return _decode_signal_args(dict(payload), payload_codec)
        return _decode_update_args(dict(payload), payload_codec)
    except Exception as exc:
        payload_head = _history_payload_head(payload, receiver_kind)
        context = {
            "workflow_id": workflow_id,
            "run_id": run_id or None,
            "event_id": _event_id(event),
            f"{receiver_kind}_name": receiver_name,
            "codec": str(codec) if codec is not None else None,
            "payload_head": payload_head,
            "exception_type": type(exc).__name__,
        }
        clean_context = {key: value for key, value in context.items() if value is not None}
        _REPLAY_LOGGER.exception(
            "workflow %s payload decode failed during replay",
            receiver_kind,
            extra={"durable_workflow_payload_decode": clean_context},
        )
        raise WorkflowPayloadDecodeError(
            f"{receiver_kind} {receiver_name!r} payload decode failed: {exc}",
            workflow_id=workflow_id,
            run_id=run_id or None,
            event_id=_event_id(event),
            receiver_kind=receiver_kind,
            receiver_name=receiver_name,
            codec=str(codec) if codec is not None else None,
            payload_head=payload_head,
            exception_type=type(exc).__name__,
        ) from exc


def replay(
    workflow_cls: type,
    history_events: Iterable[dict[str, Any]],
    start_input: list[Any],
    *,
    workflow_id: str | None = None,
    run_id: str = "",
    payload_codec: str | None = None,
) -> ReplayOutcome:
    return _replay_state(
        workflow_cls,
        history_events,
        start_input,
        workflow_id=workflow_id,
        run_id=run_id,
        payload_codec=payload_codec,
    ).outcome


def query_state(
    workflow_cls: type,
    history_events: Iterable[dict[str, Any]],
    start_input: list[Any],
    query_name: str,
    args: list[Any] | None = None,
    *,
    workflow_id: str | None = None,
    run_id: str = "",
    payload_codec: str | None = None,
) -> Any:
    """Replay a workflow to current state and invoke a registered query.

    This is the Python-side core that a future server-routed query task can
    call after fetching durable history. Unknown query names and handler
    exceptions are normalized to :class:`~durable_workflow.errors.QueryFailed`.
    """
    try:
        state = _replay_state(
            workflow_cls,
            history_events,
            start_input,
            workflow_id=workflow_id,
            run_id=run_id,
            payload_codec=payload_codec,
        )
    except Exception as exc:
        raise QueryFailed(f"workflow replay failed before query: {exc}") from exc
    if state.outcome.commands and isinstance(state.outcome.commands[0], FailWorkflow):
        failure = state.outcome.commands[0]
        raise QueryFailed(f"workflow replay failed before query: {failure.message}") from None

    query_registry: dict[str, str] = getattr(workflow_cls, "__workflow_queries__", {}) or {}
    method_name = query_registry.get(query_name)
    if method_name is None:
        raise QueryFailed(f"unknown query {query_name!r}")

    handler = getattr(state.instance, method_name, None)
    if handler is None:
        raise QueryFailed(f"query handler {query_name!r} is not available")

    try:
        return handler(*(list(args) if args is not None else []))
    except QueryFailed:
        raise
    except Exception as exc:
        raise QueryFailed(str(exc) or f"query {query_name!r} failed") from exc


def apply_update(
    workflow_cls: type,
    history_events: Iterable[dict[str, Any]],
    start_input: list[Any],
    update_id: str,
    *,
    workflow_id: str | None = None,
    run_id: str = "",
    payload_codec: str | None = None,
) -> CompleteUpdate | FailUpdate:
    """Replay current workflow state and run one accepted update handler.

    The server remains the durable authority: it accepts the update, sends a
    workflow task carrying ``workflow_update_id``, and records
    ``UpdateApplied`` / ``UpdateCompleted`` when this helper's worker command
    is submitted. Python only reconstructs in-memory state and runs the
    registered receiver method for the accepted update.
    """
    events = list(history_events)
    try:
        state = _replay_state(
            workflow_cls,
            events,
            start_input,
            workflow_id=workflow_id,
            run_id=run_id,
            payload_codec=payload_codec,
        )
    except Exception as exc:
        return _fail_update_from_exception(
            update_id,
            "workflow replay failed before update",
            exc,
        )

    if state.outcome.commands and isinstance(state.outcome.commands[0], FailWorkflow):
        failure = state.outcome.commands[0]
        return FailUpdate(
            update_id=update_id,
            message=f"workflow replay failed before update: {failure.message}",
            exception_type=failure.exception_type,
        )

    accepted = _accepted_update_payload(events, update_id)
    if accepted is None:
        return FailUpdate(
            update_id=update_id,
            message=f"accepted update {update_id!r} was not present in workflow history",
            exception_type="UpdateNotFound",
        )

    update_name = accepted.get("update_name")
    if not isinstance(update_name, str) or update_name == "":
        return FailUpdate(
            update_id=update_id,
            message=f"accepted update {update_id!r} is missing an update name",
            exception_type="InvalidUpdate",
        )

    try:
        args = _decode_receiver_args(
            {"event_type": "UpdateAccepted", "payload": accepted},
            receiver_kind="update",
            receiver_name=update_name,
            workflow_id=workflow_id or _workflow_id_from_history(events),
            run_id=run_id,
            payload_codec=payload_codec,
        )
    except Exception as exc:
        return _fail_update_from_exception(update_id, "update argument decode failed", exc)

    update_registry: dict[str, str] = getattr(workflow_cls, "__workflow_updates__", {}) or {}
    method_name = update_registry.get(update_name)
    if method_name is None:
        return FailUpdate(
            update_id=update_id,
            message=f"unknown update {update_name!r}",
            exception_type="UnknownUpdate",
        )

    validator_registry: dict[str, str] = getattr(
        workflow_cls,
        "__workflow_update_validators__",
        {},
    ) or {}
    validator_name = validator_registry.get(update_name)
    if validator_name is not None:
        validator = getattr(state.instance, validator_name, None)
        if validator is None:
            return FailUpdate(
                update_id=update_id,
                message=f"update validator {update_name!r} is not available",
                exception_type="UnknownUpdateValidator",
            )
        try:
            validation_result = validator(*args)
            if validation_result is False:
                raise ValueError(f"update validator {update_name!r} returned false")
        except Exception as exc:
            return _fail_update_from_exception(update_id, "update validator failed", exc)

    handler = getattr(state.instance, method_name, None)
    if handler is None:
        return FailUpdate(
            update_id=update_id,
            message=f"update handler {update_name!r} is not available",
            exception_type="UnknownUpdate",
        )

    try:
        return CompleteUpdate(update_id=update_id, result=handler(*args))
    except Exception as exc:
        return _fail_update_from_exception(update_id, "update handler failed", exc)


def _accepted_update_payload(
    events: list[dict[str, Any]],
    update_id: str,
) -> dict[str, Any] | None:
    for event in reversed(events):
        if event.get("event_type") not in ("UpdateAccepted", "update_accepted"):
            continue
        payload = event.get("payload") or {}
        if payload.get("update_id") == update_id:
            return payload
    return None


def _fail_update_from_exception(update_id: str, prefix: str, exc: Exception) -> FailUpdate:
    message = str(exc) or type(exc).__name__
    return FailUpdate(
        update_id=update_id,
        message=f"{prefix}: {message}",
        exception_type=type(exc).__name__,
        exception_class=f"{type(exc).__module__}.{type(exc).__qualname__}",
    )


def _replay_state(
    workflow_cls: type,
    history_events: Iterable[dict[str, Any]],
    start_input: list[Any],
    *,
    workflow_id: str | None = None,
    run_id: str = "",
    payload_codec: str | None = None,
) -> _ReplayState:
    events = list(history_events)
    workflow_id = workflow_id or _workflow_id_from_history(events)

    workflow_start_time: datetime | None = None
    for ev in events:
        etype = ev.get("event_type")
        if etype == "WorkflowStarted":
            ts = (ev.get("payload") or {}).get("timestamp")
            if ts:
                with contextlib.suppress(ValueError, TypeError):
                    workflow_start_time = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            break

    instance = workflow_cls()
    ctx = WorkflowContext(run_id=run_id, current_time=workflow_start_time)

    def _state(commands: list[Command]) -> _ReplayState:
        return _ReplayState(outcome=ReplayOutcome(commands=commands), instance=instance)

    resolved_results: list[Any] = []
    # (resolved_result_index_before_apply, receiver_kind, name, decoded_args) —
    # external receivers apply before the generator consumes the resolved_result
    # at the stored index, preserving history interleaving with activities.
    pending_receivers: list[tuple[int, str, str, list[Any]]] = []
    # Ordered list of condition_wait_id strings from ConditionWaitOpened events,
    # used by ``WaitCondition`` yields to match against their corresponding
    # opened wait in history (Nth yield ↔ Nth opened).
    wait_opened_ids: list[str] = []
    # Map condition_wait_id → resolution: 'satisfied' (from ConditionWaitSatisfied
    # in history, future server-recorded) or 'timed_out' (from a matching
    # condition_timeout TimerFired event).
    wait_resolutions: dict[str, str] = {}
    for ev in events:
        etype = ev.get("event_type")
        payload = ev.get("payload") or {}
        if etype == "ActivityCompleted":
            resolved_results.append(_decode_history_result(payload, payload_codec))
        elif etype == "TimerFired":
            timer_kind = payload.get("timer_kind")
            if timer_kind == "condition_timeout":
                wait_id = payload.get("condition_wait_id")
                if isinstance(wait_id, str) and wait_id:
                    wait_resolutions[wait_id] = "timed_out"
                continue
            if timer_kind == "signal_timeout":
                continue
            resolved_results.append(None)
        elif etype == "ConditionWaitOpened":
            wait_id = payload.get("condition_wait_id")
            if isinstance(wait_id, str) and wait_id:
                wait_opened_ids.append(wait_id)
        elif etype == "ConditionWaitSatisfied":
            wait_id = payload.get("condition_wait_id")
            if isinstance(wait_id, str) and wait_id:
                wait_resolutions[wait_id] = "satisfied"
        elif etype == "ConditionWaitTimedOut":
            wait_id = payload.get("condition_wait_id")
            if isinstance(wait_id, str) and wait_id:
                wait_resolutions[wait_id] = "timed_out"
        elif etype in ("SideEffectRecorded", "ChildRunCompleted"):
            resolved_results.append(_decode_history_result(payload, payload_codec))
        elif etype == "ChildRunFailed":
            resolved_results.append(ChildWorkflowFailed(
                payload.get("message", "child workflow failed")
            ))
        elif etype == "VersionMarkerRecorded":
            resolved_results.append(payload.get("version", 0))
        elif etype == "SearchAttributesUpserted":
            resolved_results.append(None)
        elif etype == "SignalReceived":
            signal_name = payload.get("signal_name")
            if isinstance(signal_name, str) and signal_name:
                pending_receivers.append(
                    (
                        len(resolved_results),
                        "signal",
                        signal_name,
                        _decode_receiver_args(
                            ev,
                            receiver_kind="signal",
                            receiver_name=signal_name,
                            workflow_id=workflow_id,
                            run_id=run_id,
                            payload_codec=payload_codec,
                        ),
                    )
                )
        elif etype == "UpdateApplied":
            update_name = payload.get("update_name")
            if isinstance(update_name, str) and update_name:
                pending_receivers.append(
                    (
                        len(resolved_results),
                        "update",
                        update_name,
                        _decode_receiver_args(
                            ev,
                            receiver_kind="update",
                            receiver_name=update_name,
                            workflow_id=workflow_id,
                            run_id=run_id,
                            payload_codec=payload_codec,
                        ),
                    )
                )

    signal_registry: dict[str, str] = getattr(workflow_cls, "__workflow_signals__", {}) or {}
    update_registry: dict[str, str] = getattr(workflow_cls, "__workflow_updates__", {}) or {}

    def _apply_due_receivers() -> None:
        while pending_receivers and pending_receivers[0][0] <= result_cursor:
            _, kind, name, args = pending_receivers.pop(0)
            if kind == "signal":
                method_name = signal_registry.get(name)
                if method_name is None:
                    continue
            else:
                method_name = update_registry.get(name)
                if method_name is None:
                    raise TypeError(f"unknown update {name!r} in workflow history")
            handler = getattr(instance, method_name, None)
            if handler is None:
                if kind == "signal":
                    continue
                raise TypeError(f"update handler {name!r} is not available")
            ctx.logger._set_replaying(True)
            handler(*args)

    result_cursor = 0
    _apply_due_receivers()

    gen = instance.run(ctx, *start_input)
    if not hasattr(gen, "__next__"):
        if isinstance(gen, ContinueAsNew):
            return _state([gen])
        return _state([CompleteWorkflow(result=gen)])

    ctx.logger._set_replaying(True)

    next_value: Any = None
    first = True
    pending: list[Command] = []
    advanced_cmd: Any = None
    wait_yield_count = 0
    try:
        while True:
            _apply_due_receivers()
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
                                return _state([stop.value])
                            return _state([CompleteWorkflow(result=stop.value)])
                    next_value = vals
                    continue
                ctx.logger._set_replaying(False)
                pending.extend(cmd)
                return _state(pending)
            if isinstance(cmd, ContinueAsNew):
                return _state([cmd])
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
            if isinstance(cmd, WaitCondition):
                resolution: str | None = None
                if wait_yield_count < len(wait_opened_ids):
                    resolution = wait_resolutions.get(wait_opened_ids[wait_yield_count])
                if resolution == "timed_out":
                    next_value = False
                    wait_yield_count += 1
                    continue
                try:
                    satisfied = bool(cmd.predicate())
                except Exception as exc:
                    return _state([FailWorkflow(
                        message=f"wait_condition predicate raised: {exc}",
                        exception_type=type(exc).__name__,
                    )])
                if satisfied or resolution == "satisfied":
                    next_value = True
                    wait_yield_count += 1
                    continue
                ctx.logger._set_replaying(False)
                pending.append(cmd)
                wait_yield_count += 1
                return _state(pending)
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
                                return _state([stop.value])
                            return _state([CompleteWorkflow(result=stop.value)])
                    next_value = val
                    continue
                ctx.logger._set_replaying(False)
                pending.append(cmd)
                return _state(pending)
            raise TypeError(f"workflow yielded unsupported command: {cmd!r}")
    except StopIteration as stop:
        if isinstance(stop.value, ContinueAsNew):
            return _state(pending + [stop.value])
        return _state(pending + [CompleteWorkflow(result=stop.value)])
    except Exception as exc:
        return _state([FailWorkflow(
            message=str(exc),
            exception_type=type(exc).__name__,
        )])
