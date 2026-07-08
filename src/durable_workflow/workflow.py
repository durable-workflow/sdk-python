"""Workflow authoring primitives: decorators, context, commands, and replayer.

A workflow is a Python class registered with :func:`defn`. Its ``run`` method
is a generator that yields command dataclasses (``ScheduleActivity``,
``StartTimer``, ``StartChildWorkflow``, …) — the worker's replayer drives the
generator forward by resolving each yielded command against the current
history of the workflow run. Yield a *list* of commands to run them in
parallel.

Determinism-sensitive helpers live on the :class:`WorkflowContext` passed to
``run``: :meth:`WorkflowContext.random`, :meth:`WorkflowContext.uuid4`,
:meth:`WorkflowContext.uuid7`, :meth:`WorkflowContext.now`,
:meth:`WorkflowContext.patched`, :meth:`WorkflowContext.deprecate_patch`, and
:meth:`WorkflowContext.side_effect` all produce values that are recorded on
first execution and replayed verbatim on every subsequent replay of the same
history.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import math
import random
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypeVar

from . import serializer
from .errors import (
    ActivityFailed,
    ChildWorkflowCancelled,
    ChildWorkflowFailed,
    ChildWorkflowTerminated,
    NexusOperationFailed,
    NonDeterministicReplayError,
    QueryFailed,
    WorkflowPayloadDecodeError,
)
from .external_storage import ExternalPayloadCache, ExternalStorageDriver
from .nexus import NexusOperationResult, stable_nexus_idempotency_key

_WorkflowT = TypeVar("_WorkflowT")

_REGISTRY: dict[str, type] = {}


def defn(*, name: str) -> Callable[[type[_WorkflowT]], type[_WorkflowT]]:
    """Register a class as a workflow type under a language-neutral name.

    Scans the class for ``@signal``, ``@query``, and ``@update`` decorated
    methods and builds registries at decoration time so worker-side dispatch
    can use stable receiver names without re-inspecting the class on every
    history event or control-plane request.
    """

    def wrap(cls: type[_WorkflowT]) -> type[_WorkflowT]:
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


def _should_use_external_storage(
    external_storage: ExternalStorageDriver | None,
    external_storage_threshold_bytes: int | None,
) -> bool:
    return external_storage is not None and external_storage_threshold_bytes is not None


def _payload_envelope(
    value: Any,
    *,
    payload_codec: str,
    size_warning: serializer.PayloadSizeWarningConfig | None,
    warning_context: PayloadWarningContext,
    external_storage: ExternalStorageDriver | None = None,
    external_storage_threshold_bytes: int | None = None,
) -> dict[str, Any]:
    if _should_use_external_storage(external_storage, external_storage_threshold_bytes):
        assert external_storage is not None
        assert external_storage_threshold_bytes is not None
        return serializer.external_storage_envelope(
            value,
            external_storage=external_storage,
            threshold_bytes=external_storage_threshold_bytes,
            codec=payload_codec,
            size_warning=size_warning,
            warning_context=warning_context,
        )
    return serializer.envelope(
        value,
        codec=payload_codec,
        size_warning=size_warning,
        warning_context=warning_context,
    )


def _payload_envelopes(
    values: Sequence[Any],
    *,
    payload_codec: str,
    size_warning: serializer.PayloadSizeWarningConfig | None,
    warning_contexts: Sequence[PayloadWarningContext],
    external_storage: ExternalStorageDriver | None = None,
    external_storage_threshold_bytes: int | None = None,
) -> list[dict[str, Any]]:
    if _should_use_external_storage(external_storage, external_storage_threshold_bytes):
        assert external_storage is not None
        assert external_storage_threshold_bytes is not None
        return serializer.external_storage_envelope_many(
            values,
            external_storage=external_storage,
            threshold_bytes=external_storage_threshold_bytes,
            codec=payload_codec,
            size_warning=size_warning,
            warning_context=warning_contexts,
        )
    return serializer.envelope_many(
        values,
        codec=payload_codec,
        size_warning=size_warning,
        warning_context=warning_contexts,
    )


def _payload_blob_or_external_envelope(
    value: Any,
    *,
    payload_codec: str,
    size_warning: serializer.PayloadSizeWarningConfig | None,
    warning_context: PayloadWarningContext,
    external_storage: ExternalStorageDriver | None = None,
    external_storage_threshold_bytes: int | None = None,
) -> str | dict[str, Any]:
    if _should_use_external_storage(external_storage, external_storage_threshold_bytes):
        assert external_storage is not None
        assert external_storage_threshold_bytes is not None
        envelope = serializer.external_storage_envelope(
            value,
            external_storage=external_storage,
            threshold_bytes=external_storage_threshold_bytes,
            codec=payload_codec,
            size_warning=size_warning,
            warning_context=warning_context,
        )
        if "external_storage" in envelope:
            return envelope
        blob = envelope.get("blob")
        return blob if isinstance(blob, str) else str(blob)

    return serializer.encode(
        value,
        codec=payload_codec,
        size_warning=size_warning,
        warning_context=warning_context,
    )


def _payload_blobs_or_external_envelopes(
    values: Sequence[Any],
    *,
    payload_codec: str,
    size_warning: serializer.PayloadSizeWarningConfig | None,
    warning_contexts: Sequence[PayloadWarningContext],
    external_storage: ExternalStorageDriver | None = None,
    external_storage_threshold_bytes: int | None = None,
) -> list[str | dict[str, Any]]:
    if _should_use_external_storage(external_storage, external_storage_threshold_bytes):
        assert external_storage is not None
        assert external_storage_threshold_bytes is not None
        envelopes = serializer.external_storage_envelope_many(
            values,
            external_storage=external_storage,
            threshold_bytes=external_storage_threshold_bytes,
            codec=payload_codec,
            size_warning=size_warning,
            warning_context=warning_contexts,
        )
        payloads: list[str | dict[str, Any]] = []
        for envelope in envelopes:
            if "external_storage" in envelope:
                payloads.append(envelope)
                continue
            blob = envelope.get("blob")
            payloads.append(blob if isinstance(blob, str) else str(blob))
        return payloads

    return serializer.encode_many(
        values,
        codec=payload_codec,
        size_warning=size_warning,
        warning_context=warning_contexts,
    )


@dataclass
class ChildWorkflowRetryPolicy(ActivityRetryPolicy):
    """Retry policy applied to one started child workflow call.

    This is recorded with the child workflow command and controls durable
    server-side child attempts. It is separate from SDK HTTP transport retry
    and from activity retry.
    """


ChildWorkflowRetryPolicyInput = ChildWorkflowRetryPolicy | ActivityRetryPolicy | Mapping[str, Any]


def _validate_positive_timeout(name: str, value: int | None) -> None:
    if value is None:
        return
    if value < 1:
        raise ValueError(f"{name} must be >= 1 second")


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
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
    ) -> dict[str, Any]:
        self._validate_timeouts()

        command: dict[str, Any] = {
            "type": "schedule_activity",
            "activity_type": self.activity_type,
            "arguments": _payload_envelope(
                self.arguments,
                payload_codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="activity_input",
                    task_queue=self.queue or task_queue,
                    activity_name=self.activity_type,
                ),
                external_storage=external_storage,
                external_storage_threshold_bytes=external_storage_threshold_bytes,
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

    def _validate_timeouts(self) -> None:
        timeouts = {
            "start_to_close_timeout": self.start_to_close_timeout,
            "schedule_to_start_timeout": self.schedule_to_start_timeout,
            "schedule_to_close_timeout": self.schedule_to_close_timeout,
            "heartbeat_timeout": self.heartbeat_timeout,
        }
        for name, value in timeouts.items():
            _validate_positive_timeout(name, value)

        if (
            self.heartbeat_timeout is not None
            and self.start_to_close_timeout is not None
            and self.heartbeat_timeout > self.start_to_close_timeout
        ):
            raise ValueError("heartbeat_timeout must be <= start_to_close_timeout")

        if self.schedule_to_close_timeout is None:
            return

        if (
            self.start_to_close_timeout is not None
            and self.start_to_close_timeout > self.schedule_to_close_timeout
        ):
            raise ValueError("start_to_close_timeout must be <= schedule_to_close_timeout")

        if (
            self.schedule_to_start_timeout is not None
            and self.schedule_to_start_timeout > self.schedule_to_close_timeout
        ):
            raise ValueError("schedule_to_start_timeout must be <= schedule_to_close_timeout")


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
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
    ) -> dict[str, Any]:
        return {
            "type": "complete_workflow",
            "result": _payload_envelope(
                self.result,
                payload_codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="workflow_result",
                    task_queue=task_queue,
                ),
                external_storage=external_storage,
                external_storage_threshold_bytes=external_storage_threshold_bytes,
            ),
        }


@dataclass
class FailWorkflow:
    """Command failing a workflow with diagnostic metadata."""

    message: str
    exception_type: str | None = None
    exception_class: str | None = None
    exception: dict[str, Any] | None = None
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
        if self.exception_class is not None:
            cmd["exception_class"] = self.exception_class
        if self.exception is not None:
            cmd["exception"] = self.exception
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
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
    ) -> dict[str, Any]:
        return {
            "type": "complete_update",
            "update_id": self.update_id,
            "result": _payload_envelope(
                self.result,
                payload_codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="update_result",
                    task_queue=task_queue,
                ),
                external_storage=external_storage,
                external_storage_threshold_bytes=external_storage_threshold_bytes,
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
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
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
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
    ) -> dict[str, Any]:
        cmd: dict[str, Any] = {"type": "continue_as_new"}
        if self.workflow_type is not None:
            cmd["workflow_type"] = self.workflow_type
        cmd["arguments"] = _payload_envelope(
            self.arguments,
            payload_codec=payload_codec,
            size_warning=size_warning,
            warning_context=_payload_warning_context(
                warning_context,
                kind="continue_as_new_input",
                task_queue=self.task_queue or task_queue,
            ),
            external_storage=external_storage,
            external_storage_threshold_bytes=external_storage_threshold_bytes,
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
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
    ) -> dict[str, Any]:
        return {
            "type": "record_side_effect",
            "result": _payload_blob_or_external_envelope(
                self.result,
                payload_codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="side_effect_result",
                    task_queue=task_queue,
                ),
                external_storage=external_storage,
                external_storage_threshold_bytes=external_storage_threshold_bytes,
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
        external_storage: ExternalStorageDriver | None = None,
        external_storage_threshold_bytes: int | None = None,
    ) -> dict[str, Any]:
        self._validate_timeouts()

        cmd: dict[str, Any] = {
            "type": "start_child_workflow",
            "workflow_type": self.workflow_type,
            "arguments": _payload_envelope(
                self.arguments,
                payload_codec=payload_codec,
                size_warning=size_warning,
                warning_context=_payload_warning_context(
                    warning_context,
                    kind="child_workflow_input",
                    task_queue=self.task_queue or task_queue,
                ),
                external_storage=external_storage,
                external_storage_threshold_bytes=external_storage_threshold_bytes,
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

    def _validate_timeouts(self) -> None:
        _validate_positive_timeout("execution_timeout_seconds", self.execution_timeout_seconds)
        _validate_positive_timeout("run_timeout_seconds", self.run_timeout_seconds)

        if (
            self.execution_timeout_seconds is not None
            and self.run_timeout_seconds is not None
            and self.run_timeout_seconds > self.execution_timeout_seconds
        ):
            raise ValueError("run_timeout_seconds must be <= execution_timeout_seconds")


@dataclass
class NexusServiceCall:
    """Command requesting a durable Nexus service operation from workflow code.

    The Python worker executes the operation through the service-catalog API,
    records the response or typed failure as a side-effect marker, and then
    resumes replay from that recorded marker.
    """

    endpoint_name: str
    service_name: str
    operation_name: str
    arguments: list[Any] = field(default_factory=list)
    idempotency_key: str | None = None
    payload_codec: str | None = None
    mode: str | None = "sync"
    wait_for: str | None = "completed"
    wait_timeout_seconds: int | None = None
    caller_namespace: str | None = None
    service_sdk_language: str | None = None
    artifact_tuple: dict[str, Any] | None = None
    published_artifact_worker_execution: bool | None = None
    target_workflow_instance_id: str | None = None
    target_workflow_run_id: str | None = None
    connection: str | None = None
    queue: str | None = None
    business_key: str | None = None
    labels: dict[str, Any] | None = None
    memo: dict[str, Any] | None = None
    search_attributes: dict[str, Any] | None = None
    duplicate_start_policy: str | None = None

    def to_server_command(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError(
            "NexusServiceCall is resolved by the Python worker before workflow commands "
            "are serialized to the server"
        )

    def expected_recorded_metadata(self) -> dict[str, Any]:
        return {
            "endpoint_name": self.endpoint_name,
            "service_name": self.service_name,
            "operation_name": self.operation_name,
            "idempotency_key": self.idempotency_key,
        }

    def recorded_result(self, value: Any) -> NexusOperationResult:
        return NexusOperationResult.from_recorded_payload(
            value,
            expected=self.expected_recorded_metadata(),
        )


@dataclass
class RecordVersionMarker:
    """Command recording a workflow code-version marker."""

    change_id: str
    version: int
    min_supported: int
    max_supported: int
    result_kind: str = "version"

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


def _condition_predicate_fingerprint(predicate: Callable[[], bool]) -> str:
    h = hashlib.sha256()
    h.update(b"durable-workflow-python.wait-condition.v1\0")
    h.update(f"{getattr(predicate, '__module__', '')}\0".encode())
    h.update(f"{getattr(predicate, '__qualname__', '')}\0".encode())

    code = getattr(predicate, "__code__", None)
    if code is None:
        h.update(repr(predicate).encode())
    else:
        h.update(repr((
            code.co_argcount,
            code.co_posonlyargcount,
            code.co_kwonlyargcount,
            code.co_code,
            code.co_consts,
            code.co_names,
            code.co_varnames,
            code.co_freevars,
        )).encode())

    return f"sha256:{h.hexdigest()}"


Command = (
    ScheduleActivity | StartTimer | CompleteWorkflow | FailWorkflow
    | CompleteUpdate | FailUpdate | ContinueAsNew | RecordSideEffect | StartChildWorkflow
    | NexusServiceCall | RecordVersionMarker | UpsertSearchAttributes | WaitCondition
)


def commands_to_server_commands(
    commands: Sequence[Command],
    task_queue: str,
    *,
    payload_codec: str = serializer.AVRO_CODEC,
    size_warning: serializer.PayloadSizeWarningConfig | None = serializer.DEFAULT_PAYLOAD_SIZE_WARNING,
    warning_context: PayloadWarningContext = None,
    external_storage: ExternalStorageDriver | None = None,
    external_storage_threshold_bytes: int | None = None,
) -> list[dict[str, Any]]:
    """Convert workflow commands to the server wire shape with batched payload encoding."""
    server_commands: list[dict[str, Any]] = []
    envelope_jobs: list[tuple[int, str, Any, dict[str, str]]] = []
    encode_jobs: list[tuple[int, str, Any, dict[str, str]]] = []

    for command in commands:
        if isinstance(command, ScheduleActivity):
            queue = command.queue or task_queue
            server_command: dict[str, Any] = {
                "type": "schedule_activity",
                "activity_type": command.activity_type,
                "queue": queue,
            }
            envelope_jobs.append((
                len(server_commands),
                "arguments",
                command.arguments,
                _payload_warning_context(
                    warning_context,
                    kind="activity_input",
                    task_queue=queue,
                    activity_name=command.activity_type,
                ),
            ))
            if command.retry_policy is not None:
                server_command["retry_policy"] = (
                    command.retry_policy.to_dict()
                    if isinstance(command.retry_policy, ActivityRetryPolicy)
                    else dict(command.retry_policy)
                )
            if command.start_to_close_timeout is not None:
                server_command["start_to_close_timeout"] = command.start_to_close_timeout
            if command.schedule_to_start_timeout is not None:
                server_command["schedule_to_start_timeout"] = command.schedule_to_start_timeout
            if command.schedule_to_close_timeout is not None:
                server_command["schedule_to_close_timeout"] = command.schedule_to_close_timeout
            if command.heartbeat_timeout is not None:
                server_command["heartbeat_timeout"] = command.heartbeat_timeout
            server_commands.append(server_command)
            continue

        if isinstance(command, CompleteWorkflow):
            server_commands.append({"type": "complete_workflow"})
            envelope_jobs.append((
                len(server_commands) - 1,
                "result",
                command.result,
                _payload_warning_context(
                    warning_context,
                    kind="workflow_result",
                    task_queue=task_queue,
                ),
            ))
            continue

        if isinstance(command, CompleteUpdate):
            server_commands.append({"type": "complete_update", "update_id": command.update_id})
            envelope_jobs.append((
                len(server_commands) - 1,
                "result",
                command.result,
                _payload_warning_context(
                    warning_context,
                    kind="update_result",
                    task_queue=task_queue,
                ),
            ))
            continue

        if isinstance(command, ContinueAsNew):
            queue = command.task_queue or task_queue
            server_command = {"type": "continue_as_new", "queue": queue}
            if command.workflow_type is not None:
                server_command["workflow_type"] = command.workflow_type
            server_commands.append(server_command)
            envelope_jobs.append((
                len(server_commands) - 1,
                "arguments",
                command.arguments,
                _payload_warning_context(
                    warning_context,
                    kind="continue_as_new_input",
                    task_queue=queue,
                ),
            ))
            continue

        if isinstance(command, RecordSideEffect):
            server_commands.append({"type": "record_side_effect"})
            encode_jobs.append((
                len(server_commands) - 1,
                "result",
                command.result,
                _payload_warning_context(
                    warning_context,
                    kind="side_effect_result",
                    task_queue=task_queue,
                ),
            ))
            continue

        if isinstance(command, StartChildWorkflow):
            queue = command.task_queue or task_queue
            server_command = {
                "type": "start_child_workflow",
                "workflow_type": command.workflow_type,
                "queue": queue,
            }
            envelope_jobs.append((
                len(server_commands),
                "arguments",
                command.arguments,
                _payload_warning_context(
                    warning_context,
                    kind="child_workflow_input",
                    task_queue=queue,
                ),
            ))
            if command.parent_close_policy is not None:
                server_command["parent_close_policy"] = command.parent_close_policy
            if command.retry_policy is not None:
                server_command["retry_policy"] = (
                    command.retry_policy.to_dict()
                    if isinstance(command.retry_policy, ActivityRetryPolicy)
                    else dict(command.retry_policy)
                )
            if command.execution_timeout_seconds is not None:
                server_command["execution_timeout_seconds"] = command.execution_timeout_seconds
            if command.run_timeout_seconds is not None:
                server_command["run_timeout_seconds"] = command.run_timeout_seconds
            server_commands.append(server_command)
            continue

        server_commands.append(command.to_server_command(
            task_queue,
            payload_codec=payload_codec,
            size_warning=size_warning,
            warning_context=warning_context,
        ))

    if envelope_jobs:
        envelopes = _payload_envelopes(
            [value for _, _, value, _ in envelope_jobs],
            payload_codec=payload_codec,
            size_warning=size_warning,
            warning_contexts=[context for _, _, _, context in envelope_jobs],
            external_storage=external_storage,
            external_storage_threshold_bytes=external_storage_threshold_bytes,
        )
        for (index, key, _, _), envelope_value in zip(envelope_jobs, envelopes, strict=True):
            server_commands[index][key] = envelope_value

    if encode_jobs:
        blobs = _payload_blobs_or_external_envelopes(
            [value for _, _, value, _ in encode_jobs],
            payload_codec=payload_codec,
            size_warning=size_warning,
            warning_contexts=[context for _, _, _, context in encode_jobs],
            external_storage=external_storage,
            external_storage_threshold_bytes=external_storage_threshold_bytes,
        )
        for (index, key, _, _), blob in zip(encode_jobs, blobs, strict=True):
            server_commands[index][key] = blob

    return server_commands


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

    def __init__(
        self,
        *,
        workflow_id: str = "",
        run_id: str = "",
        current_time: datetime | None = None,
    ) -> None:
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._current_time = current_time or datetime.now(timezone.utc)
        seed = int(hashlib.sha256(run_id.encode()).hexdigest()[:16], 16)
        self._rng = random.Random(seed)
        self._uuid7_counter = 0
        self._nexus_call_counter = 0
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
            condition_definition_fingerprint=_condition_predicate_fingerprint(predicate),
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

    def call_nexus_service(
        self,
        endpoint_name: str,
        service_name: str,
        operation_name: str,
        arguments: Sequence[Any] | None = None,
        *,
        idempotency_key: str | None = None,
        payload_codec: str | None = None,
        mode: str | None = "sync",
        wait_for: str | None = "completed",
        wait_timeout_seconds: int | None = None,
        caller_namespace: str | None = None,
        service_sdk_language: str | None = None,
        artifact_tuple: Mapping[str, Any] | None = None,
        published_artifact_worker_execution: bool | None = None,
        target_workflow_instance_id: str | None = None,
        target_workflow_run_id: str | None = None,
        connection: str | None = None,
        queue: str | None = None,
        business_key: str | None = None,
        labels: Mapping[str, Any] | None = None,
        memo: Mapping[str, Any] | None = None,
        search_attributes: Mapping[str, Any] | None = None,
        duplicate_start_policy: str | None = None,
    ) -> NexusServiceCall:
        """Yield a durable Nexus service operation and resume with its result.

        If the service reports a typed failure, replay raises
        :class:`~durable_workflow.errors.NexusOperationFailed` at the yield
        point so workflow code can compensate or let the workflow fail.
        """
        args = list(arguments) if arguments is not None else []
        if idempotency_key is None:
            idempotency_key = stable_nexus_idempotency_key(
                workflow_id=self._workflow_id,
                run_id=self._run_id,
                ordinal=self._nexus_call_counter,
                endpoint_name=endpoint_name,
                service_name=service_name,
                operation_name=operation_name,
                arguments=args,
            )
        self._nexus_call_counter += 1
        return NexusServiceCall(
            endpoint_name=endpoint_name,
            service_name=service_name,
            operation_name=operation_name,
            arguments=args,
            idempotency_key=idempotency_key,
            payload_codec=payload_codec,
            mode=mode,
            wait_for=wait_for,
            wait_timeout_seconds=wait_timeout_seconds,
            caller_namespace=caller_namespace,
            service_sdk_language=service_sdk_language,
            artifact_tuple=dict(artifact_tuple) if artifact_tuple is not None else None,
            published_artifact_worker_execution=published_artifact_worker_execution,
            target_workflow_instance_id=target_workflow_instance_id,
            target_workflow_run_id=target_workflow_run_id,
            connection=connection,
            queue=queue,
            business_key=business_key,
            labels=dict(labels) if labels is not None else None,
            memo=dict(memo) if memo is not None else None,
            search_attributes=dict(search_attributes) if search_attributes is not None else None,
            duplicate_start_policy=duplicate_start_policy,
        )

    def start_nexus_operation(
        self,
        endpoint_name: str,
        service_name: str,
        operation_name: str,
        arguments: Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> NexusServiceCall:
        """Yield an async Nexus service operation and resume when it is accepted."""
        kwargs.setdefault("mode", "async")
        kwargs.setdefault("wait_for", "accepted")
        return self.call_nexus_service(
            endpoint_name,
            service_name,
            operation_name,
            arguments,
            **kwargs,
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

    def patched(self, change_id: str) -> RecordVersionMarker:
        """Record or read a patch marker and resolve to ``True`` for patched runs.

        New runs record version ``1`` for ``change_id`` and replay as ``True``.
        Older runs that reached this code without a marker resolve the legacy
        default version ``-1`` and replay as ``False``.
        """
        return RecordVersionMarker(
            change_id=change_id,
            version=1,
            min_supported=-1,
            max_supported=1,
            result_kind="patched",
        )

    def deprecate_patch(self, change_id: str) -> RecordVersionMarker:
        """Keep a patch marker alive after the old branch has been removed."""
        return RecordVersionMarker(
            change_id=change_id,
            version=1,
            min_supported=-1,
            max_supported=1,
            result_kind="deprecate_patch",
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

    def uuid7(self) -> uuid.UUID:
        timestamp_ms = int(self._current_time.timestamp() * 1000)
        if timestamp_ms < 0 or timestamp_ms >= (1 << 48):
            raise ValueError("ctx.uuid7() requires ctx.now() within the UUIDv7 timestamp range")

        rand_a = self._uuid7_counter & 0xFFF
        self._uuid7_counter += 1
        rand_b = self._rng.getrandbits(62)

        value = (
            (timestamp_ms << 80)
            | (0x7 << 76)
            | (rand_a << 64)
            | (0x2 << 62)
            | rand_b
        )
        return uuid.UUID(int=value)


# ── Replay ───────────────────────────────────────────────────────────
@dataclass
class ReplayOutcome:
    commands: list[Command]


class Replayer:
    """Replay captured workflow history without a live server.

    Register one or more workflow classes, then call :meth:`replay` with a
    server-exported history list or a dictionary containing an ``events`` key.
    If the history includes a ``WorkflowStarted`` event, the replayer can infer
    the workflow type and start input from that event.
    """

    def __init__(self, *, workflows: Iterable[type]) -> None:
        self._workflows: dict[str, type] = {}
        for workflow_cls in workflows:
            workflow_type = str(getattr(workflow_cls, "__workflow_name__", workflow_cls.__name__))
            if workflow_type in self._workflows:
                raise ValueError(f"duplicate workflow type registered with Replayer: {workflow_type!r}")
            self._workflows[workflow_type] = workflow_cls
        if not self._workflows:
            raise ValueError("Replayer requires at least one workflow class")

    def replay(
        self,
        history: Iterable[dict[str, Any]] | Mapping[str, Any],
        start_input: list[Any] | None = None,
        *,
        workflow_type: str | None = None,
        workflow_id: str | None = None,
        run_id: str = "",
        payload_codec: str | None = None,
        external_storage: ExternalStorageDriver | None = None,
        external_storage_cache: ExternalPayloadCache | None = None,
    ) -> ReplayOutcome:
        events = _history_events_from_export(history)
        selected_workflow_type = workflow_type or _workflow_type_from_history(events)
        workflow_cls = self._workflow_cls(selected_workflow_type)
        replay_input = (
            list(start_input)
            if start_input is not None
            else _start_input_from_history(
                events,
                payload_codec=payload_codec,
                external_storage=external_storage,
                external_storage_cache=external_storage_cache,
            )
        )
        return replay(
            workflow_cls,
            events,
            replay_input,
            workflow_id=workflow_id,
            run_id=run_id,
            payload_codec=payload_codec,
            external_storage=external_storage,
            external_storage_cache=external_storage_cache,
        )

    def _workflow_cls(self, workflow_type: str | None) -> type:
        if workflow_type is not None:
            workflow_cls = self._workflows.get(workflow_type)
            if workflow_cls is None:
                registered = ", ".join(sorted(self._workflows))
                raise ValueError(f"workflow type {workflow_type!r} is not registered; registered: {registered}")
            return workflow_cls
        if len(self._workflows) == 1:
            return next(iter(self._workflows.values()))
        registered = ", ".join(sorted(self._workflows))
        raise ValueError(f"workflow_type is required when multiple workflows are registered: {registered}")


@dataclass
class _ReplayState:
    outcome: ReplayOutcome
    instance: Any


@dataclass
class _PendingReceiver:
    result_index: int
    kind: str
    name: str
    args: list[Any]
    condition_wait_id: str | None = None


@dataclass(frozen=True)
class _RecordedStep:
    workflow_sequence: int
    shape: str
    event_types: list[str]
    details: dict[str, Any]


def _decode_history_result(
    payload: dict[str, Any],
    fallback_codec: str | None,
    *,
    external_storage: ExternalStorageDriver | None = None,
    external_storage_cache: ExternalPayloadCache | None = None,
) -> Any:
    codec = payload.get("payload_codec") or fallback_codec
    value = payload["result"] if "result" in payload else payload.get("output")

    return serializer.decode_envelope(
        value,
        codec=codec,
        external_storage=external_storage,
        external_storage_cache=external_storage_cache,
    )


def _version_marker_result(cmd: RecordVersionMarker, version: Any) -> Any:
    if cmd.result_kind == "patched":
        return int(version) == 1
    if cmd.result_kind == "deprecate_patch":
        return None
    return version


def _decode_signal_args(
    payload: dict[str, Any],
    fallback_codec: str | None,
    *,
    external_storage: ExternalStorageDriver | None = None,
    external_storage_cache: ExternalPayloadCache | None = None,
) -> list[Any]:
    codec = payload.get("payload_codec") or fallback_codec
    raw = payload.get("value")
    if raw is None:
        raw = payload.get("input")
    if raw is None:
        raw = payload.get("arguments")
    if raw is None:
        return []
    decoded = serializer.decode_envelope(
        raw,
        codec=codec,
        external_storage=external_storage,
        external_storage_cache=external_storage_cache,
    )
    if isinstance(decoded, list):
        return decoded
    return [decoded]


def _decode_update_args(
    payload: dict[str, Any],
    fallback_codec: str | None,
    *,
    external_storage: ExternalStorageDriver | None = None,
    external_storage_cache: ExternalPayloadCache | None = None,
) -> list[Any]:
    codec = payload.get("payload_codec") or fallback_codec
    raw = payload.get("arguments")
    if raw is None:
        return []
    decoded = serializer.decode_envelope(
        raw,
        codec=codec,
        external_storage=external_storage,
        external_storage_cache=external_storage_cache,
    )
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


def _history_events_from_export(history: Iterable[dict[str, Any]] | Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_events: Any = history
    if isinstance(history, Mapping):
        if "events" in history:
            raw_events = history["events"]
        elif "history_events" in history:
            raw_events = history["history_events"]
        elif "history" in history:
            raw_events = history["history"]
        else:
            raise ValueError("history export must contain an events, history_events, or history list")

    events: list[dict[str, Any]] = []
    for event in raw_events:
        if not isinstance(event, Mapping):
            raise ValueError("history events must be dictionaries")
        events.append(dict(event))
    return events


def _workflow_type_from_history(events: Iterable[dict[str, Any]]) -> str | None:
    for event in events:
        payload = event.get("payload") or {}
        if not isinstance(payload, Mapping):
            payload = {}
        workflow_type = payload.get("workflow_type") or event.get("workflow_type")
        if workflow_type is not None:
            return str(workflow_type)
    return None


def _start_input_from_history(
    events: Iterable[dict[str, Any]],
    *,
    payload_codec: str | None,
    external_storage: ExternalStorageDriver | None = None,
    external_storage_cache: ExternalPayloadCache | None = None,
) -> list[Any]:
    for event in events:
        if event.get("event_type") != "WorkflowStarted":
            continue
        payload = event.get("payload") or {}
        if not isinstance(payload, Mapping):
            return []
        codec = payload.get("payload_codec") or payload_codec
        for key in ("input_envelope", "input", "arguments"):
            raw = payload.get(key)
            if raw is None:
                continue
            if isinstance(raw, Mapping) and "codec" in raw:
                decoded = serializer.decode_envelope(
                    raw,
                    external_storage=external_storage,
                    external_storage_cache=external_storage_cache,
                )
            elif isinstance(raw, str):
                decoded = serializer.decode(raw, codec=codec)
            else:
                decoded = raw
            if decoded is None:
                return []
            if isinstance(decoded, list):
                return decoded
            return [decoded]
        return []
    return []


def _decode_receiver_args(
    event: Mapping[str, Any],
    *,
    receiver_kind: str,
    receiver_name: str,
    workflow_id: str | None,
    run_id: str,
    payload_codec: str | None,
    external_storage: ExternalStorageDriver | None = None,
    external_storage_cache: ExternalPayloadCache | None = None,
) -> list[Any]:
    payload = event.get("payload") or {}
    if not isinstance(payload, Mapping):
        payload = {}
    codec = payload.get("payload_codec") or payload_codec
    try:
        if receiver_kind == "signal":
            return _decode_signal_args(
                dict(payload),
                payload_codec,
                external_storage=external_storage,
                external_storage_cache=external_storage_cache,
            )
        return _decode_update_args(
            dict(payload),
            payload_codec,
            external_storage=external_storage,
            external_storage_cache=external_storage_cache,
        )
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
    external_storage: ExternalStorageDriver | None = None,
    external_storage_cache: ExternalPayloadCache | None = None,
) -> ReplayOutcome:
    return _replay_state(
        workflow_cls,
        history_events,
        start_input,
        workflow_id=workflow_id,
        run_id=run_id,
        payload_codec=payload_codec,
        external_storage=external_storage,
        external_storage_cache=external_storage_cache,
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
    external_storage: ExternalStorageDriver | None = None,
    external_storage_cache: ExternalPayloadCache | None = None,
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
            external_storage=external_storage,
            external_storage_cache=external_storage_cache,
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
    external_storage: ExternalStorageDriver | None = None,
    external_storage_cache: ExternalPayloadCache | None = None,
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
            external_storage=external_storage,
            external_storage_cache=external_storage_cache,
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
            external_storage=external_storage,
            external_storage_cache=external_storage_cache,
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


def _exception_class_name(exc: BaseException) -> str:
    return f"{type(exc).__module__}.{type(exc).__qualname__}"


def _fail_workflow_from_exception(exc: BaseException, *, prefix: str | None = None) -> FailWorkflow:
    message = str(exc) or type(exc).__name__
    if prefix:
        message = f"{prefix}: {message}"

    exception_type = type(exc).__name__
    exception_class = _exception_class_name(exc)
    exception: dict[str, Any] | None = None
    cause = exc.__cause__
    activity_failure = exc if isinstance(exc, ActivityFailed) else cause if isinstance(cause, ActivityFailed) else None
    if isinstance(activity_failure, ActivityFailed):
        exception_type = activity_failure.exception_type or type(activity_failure).__name__
        exception_class = activity_failure.exception_class or _exception_class_name(activity_failure)
        exception = {
            "type": exception_type,
            "class": exception_class,
            "message": message,
        }
        if activity_failure.activity_type is not None:
            exception["activity_type"] = activity_failure.activity_type
        if activity_failure.activity_attempt_id is not None:
            exception["activity_attempt_id"] = activity_failure.activity_attempt_id

    return FailWorkflow(
        message=message,
        exception_type=exception_type,
        exception_class=exception_class,
        exception=exception,
    )


def _history_event_type(event: Mapping[str, Any]) -> str | None:
    value = event.get("event_type") or event.get("type")
    return value if isinstance(value, str) and value else None


def _workflow_sequence(payload: Mapping[str, Any]) -> int | None:
    value = payload.get("sequence") or payload.get("workflow_sequence")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _activity_type_from_payload(payload: Mapping[str, Any]) -> str | None:
    activity_type = _optional_str(payload.get("activity_type"))
    if activity_type is not None:
        return activity_type

    activity_name = _optional_str(payload.get("activity_name"))
    if activity_name is not None:
        return activity_name

    activity_payload = payload.get("activity")
    if isinstance(activity_payload, Mapping):
        return (
            _optional_str(activity_payload.get("activity_type"))
            or _optional_str(activity_payload.get("activity_name"))
            or _optional_str(activity_payload.get("type"))
            or _optional_str(activity_payload.get("name"))
        )

    return None


def _recorded_step_details(payload: Mapping[str, Any]) -> dict[str, Any]:
    details: dict[str, Any] = {}
    activity_type = _activity_type_from_payload(payload)
    if activity_type is not None:
        details["activity_type"] = activity_type

    for key in (
        "workflow_type",
        "child_workflow_type",
        "timer_kind",
        "change_id",
        "condition_key",
        "condition_definition_fingerprint",
    ):
        value = payload.get(key)
        if isinstance(value, str) and value:
            details[key] = value
    return details


def _is_resolved_step_event(event_type: str | None, payload: Mapping[str, Any]) -> bool:
    if event_type in (
        "ActivityCompleted",
        "ActivityFailed",
        "ActivityTimedOut",
        "ChildRunCompleted",
        "ChildRunFailed",
        "ChildRunCancelled",
        "ChildRunTerminated",
        "SideEffectRecorded",
        "VersionMarkerRecorded",
        "SearchAttributesUpserted",
    ):
        return True
    if event_type == "TimerFired":
        return payload.get("timer_kind") not in ("condition_timeout", "signal_timeout")
    return False


def _command_history_shape(command: Any) -> str | None:
    if isinstance(command, ScheduleActivity):
        return "activity"
    if isinstance(command, StartTimer):
        return "timer"
    if isinstance(command, StartChildWorkflow):
        return "child workflow"
    if isinstance(command, NexusServiceCall):
        return "side effect"
    if isinstance(command, RecordSideEffect):
        return "side effect"
    if isinstance(command, RecordVersionMarker):
        return "version marker"
    if isinstance(command, UpsertSearchAttributes):
        return "search attributes upsert"
    if isinstance(command, WaitCondition):
        return "condition wait"
    return None


def _command_diagnostic_shape(command: Any) -> str:
    shape = _command_history_shape(command) or type(command).__name__
    if isinstance(command, ScheduleActivity):
        return f"{shape}:{command.activity_type}"
    if isinstance(command, StartChildWorkflow):
        return f"{shape}:{command.workflow_type}"
    if isinstance(command, NexusServiceCall):
        return f"{shape}:nexus:{command.endpoint_name}/{command.service_name}/{command.operation_name}"
    if isinstance(command, RecordVersionMarker):
        return f"{shape}:{command.change_id}"
    return shape


def _recorded_detail_mismatch(command: Any, step: _RecordedStep) -> str | None:
    if isinstance(command, ScheduleActivity):
        recorded = step.details.get("activity_type")
        if isinstance(recorded, str) and recorded != command.activity_type:
            return (
                f"Recorded activity_type {recorded!r}, but current workflow "
                f"scheduled {command.activity_type!r}."
            )
    elif isinstance(command, StartChildWorkflow):
        recorded = step.details.get("workflow_type") or step.details.get("child_workflow_type")
        if isinstance(recorded, str) and recorded != command.workflow_type:
            return (
                f"Recorded child workflow_type {recorded!r}, but current workflow "
                f"started {command.workflow_type!r}."
            )
    elif isinstance(command, RecordVersionMarker):
        recorded = step.details.get("change_id")
        if isinstance(recorded, str) and recorded != command.change_id:
            return (
                f"Recorded version change_id {recorded!r}, but current workflow "
                f"requested {command.change_id!r}."
            )
    return None


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value != "" else None


_NEUTRAL_EXCEPTION_PAYLOAD_KEYS = (
    "type",
    "message",
    "code",
    "details",
    "details_payload_codec",
    "non_retryable",
)
_EXPLICIT_DIAGNOSTICS_KEYS = ("diagnostics", "runtime_diagnostics")


def _neutral_exception_payload(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None

    neutral = {key: payload[key] for key in _NEUTRAL_EXCEPTION_PAYLOAD_KEYS if key in payload}

    for key in _EXPLICIT_DIAGNOSTICS_KEYS:
        diagnostics = payload.get(key)
        if isinstance(diagnostics, Mapping):
            neutral[key] = dict(diagnostics)

    return neutral or None


def _activity_failed_from_payload(payload: Mapping[str, Any]) -> ActivityFailed:
    exception_payload = payload.get("exception")
    exception = dict(exception_payload) if isinstance(exception_payload, Mapping) else None
    exposed_exception = _neutral_exception_payload(exception_payload)
    activity_payload = payload.get("activity")
    activity = dict(activity_payload) if isinstance(activity_payload, Mapping) else None

    message = _optional_str(payload.get("message"))
    if message is None and exception is not None:
        message = _optional_str(exception.get("message"))
    if message is None:
        message = _optional_str(payload.get("closed_reason"))
    exception_type = _optional_str(payload.get("exception_type"))
    if exception_type is None and exception is not None:
        exception_type = _optional_str(exception.get("type"))
    exception_class = _optional_str(payload.get("exception_class"))
    if exception_class is None and exception is not None:
        exception_class = _optional_str(exception.get("class"))

    return ActivityFailed(
        message or "activity failed",
        activity_type=_activity_type_from_payload(payload),
        activity_execution_id=_optional_str(payload.get("activity_execution_id")),
        activity_attempt_id=_optional_str(payload.get("activity_attempt_id")),
        failure_id=_optional_str(payload.get("failure_id")),
        failure_category=_optional_str(payload.get("failure_category")),
        exception_type=exception_type,
        exception_class=exception_class,
        non_retryable=payload.get("non_retryable") is True,
        code=payload.get("code"),
        exception_payload=exposed_exception,
        activity=activity,
    )


def _child_workflow_failed_from_payload(
    payload: Mapping[str, Any],
    event_type: str,
) -> ChildWorkflowFailed:
    exception_payload = payload.get("exception")
    exception = dict(exception_payload) if isinstance(exception_payload, Mapping) else None

    message = _optional_str(payload.get("message"))
    if message is None and exception is not None:
        message = _optional_str(exception.get("message"))

    exception_class = _optional_str(payload.get("exception_class"))
    if exception_class is None and exception is not None:
        exception_class = _optional_str(exception.get("class"))
    if exception_class is None:
        exception_class = _optional_str(payload.get("exception_type"))
    if exception_class is None and exception is not None:
        exception_class = _optional_str(exception.get("type"))

    child_workflow_run_id = _optional_str(payload.get("child_workflow_run_id"))
    child_workflow_type = _optional_str(payload.get("child_workflow_type"))

    if event_type == "ChildRunCancelled":
        return ChildWorkflowCancelled(
            message or "child workflow was cancelled",
            exception_class,
            child_workflow_run_id=child_workflow_run_id,
            child_workflow_type=child_workflow_type,
        )

    if event_type == "ChildRunTerminated":
        return ChildWorkflowTerminated(
            message or "child workflow was terminated",
            exception_class,
            child_workflow_run_id=child_workflow_run_id,
            child_workflow_type=child_workflow_type,
        )

    return ChildWorkflowFailed(
        message or "child workflow failed",
        exception_class,
        failure_kind=_optional_str(payload.get("failure_category")) or "child_workflow",
        child_workflow_run_id=child_workflow_run_id,
        child_workflow_type=child_workflow_type,
    )


def _first_yield_failure(values: Iterable[Any]) -> ActivityFailed | ChildWorkflowFailed | None:
    for value in values:
        if isinstance(value, ActivityFailed):
            return value
        if isinstance(value, ChildWorkflowFailed):
            return value
    return None


def _replay_state(
    workflow_cls: type,
    history_events: Iterable[dict[str, Any]],
    start_input: list[Any],
    *,
    workflow_id: str | None = None,
    run_id: str = "",
    payload_codec: str | None = None,
    external_storage: ExternalStorageDriver | None = None,
    external_storage_cache: ExternalPayloadCache | None = None,
) -> _ReplayState:
    events = list(history_events)
    workflow_id = workflow_id or _workflow_id_from_history(events)
    event_types_by_sequence: dict[int, list[str]] = {}
    details_by_sequence: dict[int, dict[str, Any]] = {}
    resolved_sequences: set[int] = set()
    condition_wait_ids_by_sequence: dict[int, str] = {}

    for event in events:
        event_type = _history_event_type(event)
        payload = event.get("payload") or {}
        if not isinstance(payload, Mapping):
            payload = {}
        sequence = _workflow_sequence(payload)
        if sequence is None:
            continue
        if event_type is not None:
            event_types_by_sequence.setdefault(sequence, []).append(event_type)
        details_by_sequence.setdefault(sequence, {}).update(_recorded_step_details(payload))
        if _is_resolved_step_event(event_type, payload):
            resolved_sequences.add(sequence)
        if event_type == "ConditionWaitOpened":
            wait_id = payload.get("condition_wait_id")
            if isinstance(wait_id, str) and wait_id:
                condition_wait_ids_by_sequence[sequence] = wait_id

    workflow_start_time: datetime | None = None
    for ev in events:
        etype = _history_event_type(ev)
        if etype == "WorkflowStarted":
            ts = (ev.get("payload") or {}).get("timestamp")
            if ts:
                with contextlib.suppress(ValueError, TypeError):
                    workflow_start_time = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            break

    instance = workflow_cls()
    ctx = WorkflowContext(
        workflow_id=workflow_id or "",
        run_id=run_id,
        current_time=workflow_start_time,
    )

    def _state(commands: list[Command]) -> _ReplayState:
        return _ReplayState(outcome=ReplayOutcome(commands=commands), instance=instance)

    resolved_results: list[Any] = []
    recorded_steps: list[_RecordedStep] = []
    recorded_wait_steps: list[_RecordedStep] = []
    recorded_pending_steps: list[_RecordedStep] = []
    pending_sequences_added: set[int] = set()
    # External receivers normally apply by resolved-result cursor. Receivers
    # observed while a condition wait is open are pinned to that wait so
    # sequential signal-driven waits do not collapse to the same cursor.
    #
    # (resolved_result_index_before_apply, receiver_kind, name, decoded_args) —
    # external receivers apply before the generator consumes the resolved_result
    # at the stored index, preserving history interleaving with activities.
    pending_receivers: list[_PendingReceiver] = []
    # Ordered ``ConditionWaitOpened`` payloads, used by ``WaitCondition`` yields
    # to match against their corresponding opened wait in history
    # (Nth yield ↔ Nth opened).
    wait_opened: list[dict[str, Any]] = []
    # Map condition_wait_id → resolution: 'satisfied' (from ConditionWaitSatisfied
    # in history, future server-recorded) or 'timed_out' (from a matching
    # condition_timeout TimerFired event).
    wait_resolutions: dict[str, str] = {}
    def _append_resolved_result(value: Any, shape: str, event: Mapping[str, Any]) -> None:
        payload = event.get("payload") or {}
        if not isinstance(payload, Mapping):
            payload = {}
        fallback_sequence = len(recorded_steps) + 1
        workflow_sequence = _workflow_sequence(payload) or fallback_sequence
        event_type = _history_event_type(event)
        event_types = list(event_types_by_sequence.get(workflow_sequence, []))
        if not event_types and event_type is not None:
            event_types = [event_type]
        if not event_types:
            event_types = ["<unknown>"]
        details = dict(details_by_sequence.get(workflow_sequence, {}))
        details.update(_recorded_step_details(payload))
        resolved_results.append(value)
        recorded_steps.append(_RecordedStep(
            workflow_sequence=workflow_sequence,
            shape=shape,
            event_types=event_types,
            details=details,
        ))

    def _recorded_step(shape: str, event: Mapping[str, Any]) -> _RecordedStep:
        payload = event.get("payload") or {}
        if not isinstance(payload, Mapping):
            payload = {}
        fallback_sequence = len(recorded_steps) + len(recorded_wait_steps) + 1
        workflow_sequence = _workflow_sequence(payload) or fallback_sequence
        event_type = _history_event_type(event)
        event_types = list(event_types_by_sequence.get(workflow_sequence, []))
        if not event_types and event_type is not None:
            event_types = [event_type]
        if not event_types:
            event_types = ["<unknown>"]
        details = dict(details_by_sequence.get(workflow_sequence, {}))
        details.update(_recorded_step_details(payload))
        return _RecordedStep(
            workflow_sequence=workflow_sequence,
            shape=shape,
            event_types=event_types,
            details=details,
        )

    def _append_pending_step(shape: str, event: Mapping[str, Any]) -> None:
        payload = event.get("payload") or {}
        if not isinstance(payload, Mapping):
            payload = {}
        sequence = _workflow_sequence(payload)
        if sequence is None:
            return
        if sequence in resolved_sequences or sequence in pending_sequences_added:
            return
        pending_sequences_added.add(sequence)
        recorded_pending_steps.append(_recorded_step(shape, event))

    def _assert_step_matches(command: Any, step: _RecordedStep) -> None:
        expected_shape = _command_history_shape(command)
        if expected_shape is None:
            return
        if step.shape != expected_shape:
            raise NonDeterministicReplayError(
                step.workflow_sequence,
                expected_shape,
                step.event_types,
            )

        mismatch = _recorded_detail_mismatch(command, step)
        if mismatch is not None:
            raise NonDeterministicReplayError(
                step.workflow_sequence,
                _command_diagnostic_shape(command),
                step.event_types,
                detail=mismatch,
            )

    def _assert_next_step_matches(command: Any, offset: int = 0) -> None:
        step_index = result_cursor + offset
        if step_index >= len(recorded_steps):
            return
        _assert_step_matches(command, recorded_steps[step_index])

    def _unconsumed_recorded_steps() -> list[_RecordedStep]:
        candidates: list[_RecordedStep] = []
        if result_cursor < len(recorded_steps):
            candidates.extend(recorded_steps[result_cursor:])
        if wait_yield_count < len(recorded_wait_steps):
            candidates.extend(recorded_wait_steps[wait_yield_count:])
        if pending_step_cursor < len(recorded_pending_steps):
            candidates.extend(recorded_pending_steps[pending_step_cursor:])
        return sorted(candidates, key=lambda step: step.workflow_sequence)

    def _next_unconsumed_recorded_step() -> _RecordedStep | None:
        candidates = _unconsumed_recorded_steps()
        if not candidates:
            return None
        return candidates[0]

    def _assert_pending_step_matches(command: Any, offset: int = 0) -> None:
        candidates = _unconsumed_recorded_steps()
        if offset >= len(candidates):
            return
        _assert_step_matches(command, candidates[offset])

    def _assert_no_unconsumed_history(terminal_shape: str) -> None:
        step = _next_unconsumed_recorded_step()
        if step is None:
            return
        raise NonDeterministicReplayError(
            step.workflow_sequence,
            terminal_shape,
            step.event_types,
        )

    def _is_external_receiver_event(event_type: str | None) -> bool:
        return event_type in ("SignalReceived", "UpdateApplied")

    def _receiver_binding_boundary_kind(event_type: str | None, payload: Mapping[str, Any]) -> str | None:
        if event_type in (
            "ConditionWaitSatisfied",
            "ConditionWaitTimedOut",
        ):
            return "condition"
        if (
            event_type in ("TimerFired", "TimerCancelled")
            and payload.get("timer_kind") == "condition_timeout"
        ):
            return "condition"
        if event_type in (
            "WorkflowCompleted",
            "WorkflowFailed",
            "WorkflowContinuedAsNew",
            "ActivityScheduled",
            "ActivityStarted",
            "ChildWorkflowScheduled",
            "ChildRunStarted",
        ):
            return "step"
        if event_type == "TimerScheduled":
            return "step" if payload.get("timer_kind") not in ("condition_timeout", "signal_timeout") else None
        return "step" if _is_resolved_step_event(event_type, payload) else None

    def _receiver_condition_wait_bindings() -> dict[int, str | None]:
        bindings: dict[int, str | None] = {}
        prefix_receivers: list[int] = []
        prefix_can_bind_to_first_wait = True
        current_wait_id: str | None = None
        receivers_since_wait: list[int | None] = []

        for index, event in enumerate(events):
            event_type = _history_event_type(event)
            payload = event.get("payload") or {}
            if not isinstance(payload, Mapping):
                payload = {}

            if event_type == "ConditionWaitOpened":
                wait_id = payload.get("condition_wait_id")
                if not isinstance(wait_id, str) or not wait_id:
                    continue

                if current_wait_id is None:
                    for receiver_index in prefix_receivers:
                        bindings[receiver_index] = wait_id if prefix_can_bind_to_first_wait else None
                else:
                    # When multiple signals arrive while the task woken by the
                    # first signal is still leased, the server records those
                    # later SignalReceived rows before the task's next
                    # ConditionWaitOpened row. Replay them at that next wait
                    # even if their stored workflow_sequence still points at
                    # the wait that was open when the signal was accepted.
                    for receiver_index in receivers_since_wait[1:]:
                        if receiver_index is not None:
                            bindings[receiver_index] = wait_id

                prefix_receivers = []
                prefix_can_bind_to_first_wait = True
                current_wait_id = wait_id
                receivers_since_wait = []
                continue

            if _is_external_receiver_event(event_type):
                explicit_sequence = _workflow_sequence(payload) is not None
                if current_wait_id is None:
                    if prefix_can_bind_to_first_wait or not explicit_sequence:
                        prefix_receivers.append(index)
                    continue

                receivers_since_wait.append(index)
                if len(receivers_since_wait) == 1:
                    bindings[index] = current_wait_id
                continue

            boundary_kind = _receiver_binding_boundary_kind(event_type, payload)
            if boundary_kind is None:
                continue

            if current_wait_id is None:
                if boundary_kind == "step":
                    for receiver_index in prefix_receivers:
                        bindings[receiver_index] = None
                    prefix_receivers = []
                    prefix_can_bind_to_first_wait = False
                continue

            for receiver_index in receivers_since_wait:
                if receiver_index is not None and receiver_index not in bindings:
                    bindings[receiver_index] = current_wait_id
            current_wait_id = None
            receivers_since_wait = []
            prefix_can_bind_to_first_wait = boundary_kind == "condition"

        for receiver_index in prefix_receivers:
            bindings[receiver_index] = None
        if current_wait_id is not None:
            for receiver_index in receivers_since_wait:
                if receiver_index is not None and receiver_index not in bindings:
                    bindings[receiver_index] = current_wait_id

        return bindings

    receiver_condition_wait_ids = _receiver_condition_wait_bindings()

    for event_index, ev in enumerate(events):
        etype = _history_event_type(ev)
        payload = ev.get("payload") or {}
        if etype == "ActivityCompleted":
            _append_resolved_result(
                _decode_history_result(
                    payload,
                    payload_codec,
                    external_storage=external_storage,
                    external_storage_cache=external_storage_cache,
                ),
                "activity",
                ev,
            )
        elif etype in ("ActivityFailed", "ActivityTimedOut"):
            _append_resolved_result(_activity_failed_from_payload(payload), "activity", ev)
        elif etype in ("ActivityScheduled", "ActivityStarted"):
            _append_pending_step("activity", ev)
        elif etype == "TimerFired":
            timer_kind = payload.get("timer_kind")
            if timer_kind == "condition_timeout":
                wait_id = payload.get("condition_wait_id")
                if isinstance(wait_id, str) and wait_id:
                    wait_resolutions[wait_id] = "timed_out"
                continue
            if timer_kind == "signal_timeout":
                continue
            _append_resolved_result(None, "timer", ev)
        elif etype == "TimerScheduled":
            timer_kind = payload.get("timer_kind")
            if timer_kind not in ("condition_timeout", "signal_timeout"):
                _append_pending_step("timer", ev)
        elif etype == "ConditionWaitOpened":
            wait_id = payload.get("condition_wait_id")
            if isinstance(wait_id, str) and wait_id:
                recorded_wait_steps.append(_recorded_step("condition wait", ev))
                wait_opened.append(dict(payload))
        elif etype == "ConditionWaitSatisfied":
            wait_id = payload.get("condition_wait_id")
            if isinstance(wait_id, str) and wait_id:
                wait_resolutions[wait_id] = "satisfied"
        elif etype == "ConditionWaitTimedOut":
            wait_id = payload.get("condition_wait_id")
            if isinstance(wait_id, str) and wait_id:
                wait_resolutions[wait_id] = "timed_out"
        elif etype in ("SideEffectRecorded", "ChildRunCompleted"):
            shape = "side effect" if etype == "SideEffectRecorded" else "child workflow"
            _append_resolved_result(
                _decode_history_result(
                    payload,
                    payload_codec,
                    external_storage=external_storage,
                    external_storage_cache=external_storage_cache,
                ),
                shape,
                ev,
            )
        elif etype in ("ChildRunFailed", "ChildRunCancelled", "ChildRunTerminated"):
            _append_resolved_result(
                _child_workflow_failed_from_payload(payload, etype),
                "child workflow",
                ev,
            )
        elif etype in ("ChildWorkflowScheduled", "ChildRunStarted"):
            _append_pending_step("child workflow", ev)
        elif etype == "VersionMarkerRecorded":
            _append_resolved_result(payload.get("version", 0), "version marker", ev)
        elif etype == "SearchAttributesUpserted":
            _append_resolved_result(None, "search attributes upsert", ev)
        elif etype == "SignalReceived":
            signal_name = payload.get("signal_name")
            if isinstance(signal_name, str) and signal_name:
                workflow_sequence = _workflow_sequence(payload)
                if event_index in receiver_condition_wait_ids:
                    condition_wait_id = receiver_condition_wait_ids[event_index]
                elif workflow_sequence is not None:
                    condition_wait_id = condition_wait_ids_by_sequence.get(workflow_sequence)
                else:
                    condition_wait_id = None
                pending_receivers.append(_PendingReceiver(
                    result_index=len(resolved_results),
                    kind="signal",
                    name=signal_name,
                    args=_decode_receiver_args(
                        ev,
                        receiver_kind="signal",
                        receiver_name=signal_name,
                        workflow_id=workflow_id,
                        run_id=run_id,
                        payload_codec=payload_codec,
                        external_storage=external_storage,
                        external_storage_cache=external_storage_cache,
                    ),
                    condition_wait_id=condition_wait_id,
                ))
        elif etype == "UpdateApplied":
            update_name = payload.get("update_name")
            if isinstance(update_name, str) and update_name:
                workflow_sequence = _workflow_sequence(payload)
                if event_index in receiver_condition_wait_ids:
                    condition_wait_id = receiver_condition_wait_ids[event_index]
                elif workflow_sequence is not None:
                    condition_wait_id = condition_wait_ids_by_sequence.get(workflow_sequence)
                else:
                    condition_wait_id = None
                pending_receivers.append(_PendingReceiver(
                    result_index=len(resolved_results),
                    kind="update",
                    name=update_name,
                    args=_decode_receiver_args(
                        ev,
                        receiver_kind="update",
                        receiver_name=update_name,
                        workflow_id=workflow_id,
                        run_id=run_id,
                        payload_codec=payload_codec,
                        external_storage=external_storage,
                        external_storage_cache=external_storage_cache,
                    ),
                    condition_wait_id=condition_wait_id,
                ))

    signal_registry: dict[str, str] = getattr(workflow_cls, "__workflow_signals__", {}) or {}
    update_registry: dict[str, str] = getattr(workflow_cls, "__workflow_updates__", {}) or {}

    def _apply_receiver(receiver: _PendingReceiver) -> None:
        if receiver.kind == "signal":
            method_name = signal_registry.get(receiver.name)
            if method_name is None:
                return
        else:
            method_name = update_registry.get(receiver.name)
            if method_name is None:
                raise TypeError(f"unknown update {receiver.name!r} in workflow history")
        handler = getattr(instance, method_name, None)
        if handler is None:
            if receiver.kind == "signal":
                return
            raise TypeError(f"update handler {receiver.name!r} is not available")
        ctx.logger._set_replaying(True)
        handler(*receiver.args)

    def _receiver_due(receiver: _PendingReceiver, *, before_consuming_result: bool) -> bool:
        if receiver.condition_wait_id is not None:
            return False
        return (
            receiver.result_index < result_cursor
            if before_consuming_result
            else receiver.result_index <= result_cursor
        )

    def _apply_due_receivers(*, before_consuming_result: bool = False) -> None:
        while pending_receivers:
            receiver = pending_receivers[0]
            if not _receiver_due(receiver, before_consuming_result=before_consuming_result):
                break
            _apply_receiver(pending_receivers.pop(0))

    def _apply_condition_wait_receivers(condition_wait_id: str | None) -> None:
        if condition_wait_id is None:
            return
        while pending_receivers:
            receiver = pending_receivers[0]
            if receiver.condition_wait_id != condition_wait_id:
                break
            _apply_receiver(pending_receivers.pop(0))

    def _condition_wait_mismatch(opened: Mapping[str, Any], cmd: WaitCondition) -> FailWorkflow | None:
        opened_key = opened.get("condition_key")
        if isinstance(opened_key, str) and opened_key != (cmd.condition_key or ""):
            return FailWorkflow(
                message=(
                    "wait_condition key changed during replay: "
                    f"history has {opened_key!r}, workflow yielded "
                    f"{cmd.condition_key!r}"
                ),
                exception_type="NonDeterministicWaitCondition",
            )

        opened_fingerprint = opened.get("condition_definition_fingerprint")
        if (
            isinstance(opened_fingerprint, str)
            and cmd.condition_definition_fingerprint != opened_fingerprint
        ):
            return FailWorkflow(
                message=(
                    "wait_condition predicate fingerprint changed during replay: "
                    f"history has {opened_fingerprint!r}, workflow yielded "
                    f"{cmd.condition_definition_fingerprint!r}"
                ),
                exception_type="NonDeterministicWaitCondition",
            )

        return None

    def _same_logical_condition_wait(opened: Mapping[str, Any], cmd: WaitCondition) -> bool:
        if _condition_wait_mismatch(opened, cmd) is not None:
            return False

        opened_key = opened.get("condition_key")
        if isinstance(opened_key, str) and opened_key and opened_key == (cmd.condition_key or ""):
            return True

        opened_fingerprint = opened.get("condition_definition_fingerprint")
        return (
            isinstance(opened_fingerprint, str)
            and bool(opened_fingerprint)
            and opened_fingerprint == cmd.condition_definition_fingerprint
        )

    result_cursor = 0
    pending_step_cursor = 0
    wait_yield_count = 0
    try:
        gen = instance.run(ctx, *start_input)
    except NonDeterministicReplayError:
        raise
    except Exception as exc:
        return _state([_fail_workflow_from_exception(exc)])
    if not hasattr(gen, "__next__"):
        if isinstance(gen, ContinueAsNew):
            _assert_no_unconsumed_history("continue as new")
            return _state([gen])
        _assert_no_unconsumed_history("complete workflow")
        return _state([CompleteWorkflow(result=gen)])

    ctx.logger._set_replaying(True)

    next_value: Any = None
    first = True
    pending: list[Command] = []
    advanced_cmd: Any = None
    terminal_condition_reopen_cmd: WaitCondition | None = None

    def _condition_wait_has_pending_receivers(condition_wait_id: str | None) -> bool:
        if condition_wait_id is None:
            return False
        return any(receiver.condition_wait_id == condition_wait_id for receiver in pending_receivers)

    def _consume_terminal_condition_reopens() -> None:
        nonlocal wait_yield_count
        if terminal_condition_reopen_cmd is None:
            return
        while wait_yield_count < len(wait_opened):
            opened = wait_opened[wait_yield_count]
            if not _same_logical_condition_wait(opened, terminal_condition_reopen_cmd):
                break
            opened_id = opened.get("condition_wait_id")
            if not isinstance(opened_id, str):
                break
            if opened_id in wait_resolutions or _condition_wait_has_pending_receivers(opened_id):
                break
            wait_yield_count += 1

    def _terminal_state(value: Any, *, include_pending: bool) -> _ReplayState:
        _apply_due_receivers()
        _consume_terminal_condition_reopens()
        commands = list(pending) if include_pending else []
        if isinstance(value, ContinueAsNew):
            _assert_no_unconsumed_history("continue as new")
            return _state(commands + [value])
        _assert_no_unconsumed_history("complete workflow")
        return _state(commands + [CompleteWorkflow(result=value)])

    try:
        while True:
            # Cursor-0 receivers are start-boundary events. Enter run() once
            # before applying them so workflow initialization observes the
            # same WorkflowStarted-before-Signal/Update ordering the server
            # records in history.
            if not first:
                _apply_due_receivers(before_consuming_result=True)
            if advanced_cmd is not None:
                cmd = advanced_cmd
                advanced_cmd = None
            else:
                try:
                    cmd = gen.send(None) if first else gen.send(next_value)
                except StopIteration as stop:
                    return _terminal_state(stop.value, include_pending=True)
                first = False
            _apply_due_receivers()
            if isinstance(cmd, list):
                if any(isinstance(child_command, NexusServiceCall) for child_command in cmd):
                    raise TypeError("Nexus service calls must be yielded one at a time")
                needed = len(cmd)
                if result_cursor + needed <= len(resolved_results):
                    for offset, child_command in enumerate(cmd):
                        _assert_next_step_matches(child_command, offset)
                    vals = resolved_results[result_cursor:result_cursor + needed]
                    result_cursor += needed
                    failed = _first_yield_failure(vals)
                    if failed is not None:
                        try:
                            advanced_cmd = gen.throw(failed)
                            continue
                        except StopIteration as stop:
                            return _terminal_state(stop.value, include_pending=False)
                    next_value = vals
                    continue
                ctx.logger._set_replaying(False)
                for offset, child_command in enumerate(cmd):
                    _assert_pending_step_matches(child_command, offset)
                pending.extend(cmd)
                return _state(pending)
            if isinstance(cmd, ContinueAsNew):
                return _state([cmd])
            if isinstance(cmd, NexusServiceCall):
                if result_cursor < len(resolved_results):
                    _assert_next_step_matches(cmd)
                    recorded_value = resolved_results[result_cursor]
                    result_cursor += 1
                    try:
                        next_value = cmd.recorded_result(recorded_value)
                    except NexusOperationFailed as exc:
                        try:
                            advanced_cmd = gen.throw(exc)
                            continue
                        except StopIteration as stop:
                            return _terminal_state(stop.value, include_pending=False)
                    continue
                ctx.logger._set_replaying(False)
                _assert_pending_step_matches(cmd)
                pending.append(cmd)
                return _state(pending)
            if isinstance(cmd, RecordSideEffect):
                if result_cursor < len(resolved_results):
                    _assert_next_step_matches(cmd)
                    next_value = resolved_results[result_cursor]
                    result_cursor += 1
                    continue
                ctx.logger._set_replaying(False)
                _assert_pending_step_matches(cmd)
                pending.append(cmd)
                next_value = cmd.result
                continue
            if isinstance(cmd, UpsertSearchAttributes):
                if result_cursor < len(resolved_results):
                    _assert_next_step_matches(cmd)
                    next_value = resolved_results[result_cursor]
                    result_cursor += 1
                    continue
                ctx.logger._set_replaying(False)
                _assert_pending_step_matches(cmd)
                pending.append(cmd)
                next_value = None
                continue
            if isinstance(cmd, RecordVersionMarker):
                if result_cursor < len(resolved_results):
                    _assert_next_step_matches(cmd)
                    val = resolved_results[result_cursor]
                    result_cursor += 1
                    next_value = _version_marker_result(cmd, val)
                    continue
                ctx.logger._set_replaying(False)
                _assert_pending_step_matches(cmd)
                pending.append(cmd)
                next_value = _version_marker_result(cmd, cmd.version)
                continue
            if isinstance(cmd, WaitCondition):
                if wait_yield_count >= len(wait_opened):
                    step = _next_unconsumed_recorded_step()
                    if step is not None:
                        _assert_step_matches(cmd, step)
                # Terminal cleanup may only skip a later open after this
                # yielded wait has already replayed at least one false reopen.
                consumed_reopen_for_current_wait = False
                while True:
                    resolution: str | None = None
                    opened: dict[str, Any] | None = None
                    if wait_yield_count < len(wait_opened):
                        if wait_yield_count < len(recorded_wait_steps):
                            _assert_step_matches(cmd, recorded_wait_steps[wait_yield_count])
                        opened = wait_opened[wait_yield_count]
                        mismatch = _condition_wait_mismatch(opened, cmd)
                        if mismatch is not None:
                            return _state([mismatch])
                        opened_id = opened.get("condition_wait_id")
                        if isinstance(opened_id, str):
                            resolution = wait_resolutions.get(opened_id)
                            _apply_condition_wait_receivers(opened_id)
                    next_wait_index = wait_yield_count + 1
                    has_reopened_same_wait = (
                        opened is not None
                        and next_wait_index < len(wait_opened)
                        and _same_logical_condition_wait(wait_opened[next_wait_index], cmd)
                    )

                    if resolution == "timed_out":
                        terminal_condition_reopen_cmd = None
                        next_value = False
                        wait_yield_count += 1
                        break
                    try:
                        satisfied = bool(cmd.predicate())
                    except Exception as exc:
                        return _state([_fail_workflow_from_exception(
                            exc,
                            prefix="wait_condition predicate raised",
                        )])
                    if resolution == "satisfied":
                        if has_reopened_same_wait:
                            if not satisfied:
                                consumed_reopen_for_current_wait = True
                                wait_yield_count = next_wait_index
                                continue

                        terminal_condition_reopen_cmd = (
                            cmd
                            if has_reopened_same_wait and consumed_reopen_for_current_wait
                            else None
                        )
                        next_value = True
                        wait_yield_count += 1
                        break
                    if satisfied:
                        terminal_condition_reopen_cmd = (
                            cmd
                            if has_reopened_same_wait and consumed_reopen_for_current_wait
                            else None
                        )
                        next_value = True
                        wait_yield_count += 1
                        break

                    # A single logical wait can be re-opened in history after
                    # non-satisfying signals. Consume repeated physical opens
                    # with the same key/fingerprint before declaring the
                    # logical wait still pending.
                    if has_reopened_same_wait:
                        consumed_reopen_for_current_wait = True
                        wait_yield_count = next_wait_index
                        continue

                    ctx.logger._set_replaying(False)
                    pending.append(cmd)
                    wait_yield_count = next_wait_index
                    return _state(pending)
                continue
            if isinstance(cmd, (ScheduleActivity, StartTimer, StartChildWorkflow)):
                if result_cursor < len(resolved_results):
                    _assert_next_step_matches(cmd)
                    val = resolved_results[result_cursor]
                    result_cursor += 1
                    if isinstance(val, (ActivityFailed, ChildWorkflowFailed)):
                        try:
                            advanced_cmd = gen.throw(val)
                            continue
                        except StopIteration as stop:
                            return _terminal_state(stop.value, include_pending=False)
                    next_value = val
                    continue
                ctx.logger._set_replaying(False)
                _assert_pending_step_matches(cmd)
                pending.append(cmd)
                return _state(pending)
            raise TypeError(f"workflow yielded unsupported command: {cmd!r}")
    except StopIteration as stop:
        try:
            return _terminal_state(stop.value, include_pending=True)
        except Exception as exc:
            if isinstance(exc, NonDeterministicReplayError):
                raise
            return _state([_fail_workflow_from_exception(exc)])
    except NonDeterministicReplayError:
        raise
    except Exception as exc:
        return _state([_fail_workflow_from_exception(exc)])
