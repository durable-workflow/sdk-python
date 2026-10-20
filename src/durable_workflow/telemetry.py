"""Optional task spans and standard-library logging correlation.

Applications own providers, sampling, exporters and shutdown. No OpenTelemetry
dependency is imported until an enabled execution begins. This module changes
no Server messages and does not claim durable service-carrier propagation.
"""

from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Awaitable, Callable
from contextlib import suppress
from contextvars import ContextVar
from typing import Any, TypeVar

from .interceptors import (
    ActivityHandler,
    ActivityInterceptorContext,
    QueryTaskHandler,
    QueryTaskInterceptorContext,
    WorkflowTaskHandler,
    WorkflowTaskInterceptorContext,
)

_fields: ContextVar[dict[str, str | int] | None] = ContextVar("durable_execution_fields", default=None)
_execution_pid: ContextVar[int | None] = ContextVar("durable_execution_pid", default=None)
_C = TypeVar("_C")
_T = TypeVar("_T")


def current_log_context() -> dict[str, str | int]:
    """Return bounded durable IDs and the actual current application span IDs."""
    fields = _fields.get()
    if fields is None or _execution_pid.get() != os.getpid():
        return {}
    result = dict(fields)
    try:
        trace = importlib.import_module("opentelemetry.trace")
        identity = trace.get_current_span().get_span_context()
        if identity.is_valid:
            result["trace_id"] = format(identity.trace_id, "032x")
            result["span_id"] = format(identity.span_id, "016x")
    except Exception:
        pass
    return result


class LogContextFilter(logging.Filter):
    """Attach current execution IDs to ordinary application LogRecords.

    Add this filter to each application logging handler. Request IDs, messages
    and other application attributes are preserved. No handler is installed.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in current_log_context().items():
            if key == "request_id" and hasattr(record, key):
                continue
            setattr(record, key, value)
        return True


def _bounded(value: Any) -> str | None:
    if (isinstance(value, str) and value and len(value.encode("utf-8", errors="replace")) <= 256
            and not any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)):
        return value
    return None


class ExecutionTelemetry:
    """Worker interceptor with optional application-owned OpenTelemetry tracing.

    Pass one instance in ``Worker(..., interceptors=[telemetry])``. Configure the
    global provider first, or pass ``tracer_provider`` explicitly. For supervised
    callbacks, ``provider_factory="package.module:factory"`` constructs fresh
    providers inside each spawned process. The factory must be importable and
    use synchronous processing with a bounded exporter if child spans must be
    exported before process exit. Providers are never pickled or inherited by
    this interceptor, and it never flushes or shuts down shared providers.
    """

    def __init__(
        self, *, namespace: str = "default", tracer_provider: Any = None,
        provider_factory: str | None = None,
    ) -> None:
        if provider_factory is not None and (
            ":" not in provider_factory or "<locals>" in provider_factory
        ):
            raise ValueError("provider_factory must be an importable 'module:factory' name")
        self.namespace = namespace
        self.provider_factory = provider_factory
        self._provider = tracer_provider
        self._pid = os.getpid()
        self._tracer: Any = None
        self._resolved = False

    def __getstate__(self) -> dict[str, Any]:
        # SupervisedCallback uses spawn. Providers/exporter sockets never enter
        # the serialized invocation, even when the parent has already traced work.
        return {"namespace": self.namespace, "provider_factory": self.provider_factory}

    def __setstate__(self, state: dict[str, Any]) -> None:
        ExecutionTelemetry.__init__(self, namespace=state["namespace"], provider_factory=state["provider_factory"])

    def _get_tracer(self) -> Any:
        if self._pid != os.getpid():
            self._pid = os.getpid()
            self._provider = None
            self._tracer = None
            self._resolved = False
            if self.provider_factory is None:
                # A forked host may retain global exporter sockets. Only an
                # explicit factory may create providers for that child.
                try:
                    trace = importlib.import_module("opentelemetry.trace")
                    self._tracer = trace.NoOpTracerProvider().get_tracer("durable-workflow/sdk-python")
                except Exception:
                    self._tracer = None
                self._resolved = True
        if self._resolved:
            return self._tracer
        self._resolved = True
        try:
            trace = importlib.import_module("opentelemetry.trace")
            provider = self._provider
            if provider is None and self.provider_factory is not None:
                module, name = self.provider_factory.split(":", 1)
                factory: Any = importlib.import_module(module)
                for part in name.split("."):
                    factory = getattr(factory, part)
                provider = factory()
                self._provider = provider
            if provider is None:
                provider = trace.get_tracer_provider()
            self._tracer = provider.get_tracer("durable-workflow/sdk-python")
        except Exception:
            self._tracer = None
        return self._tracer

    def _task_fields(
        self, task: dict[str, Any], worker_id: str, queue: str, *, activity: bool = False,
    ) -> dict[str, str | int]:
        fields: dict[str, str | int] = {}
        sources = {
            "namespace": self.namespace, "worker_id": worker_id, "task_queue": queue,
            "workflow_instance_id": task.get("workflow_id"), "workflow_run_id": task.get("run_id"),
            "task_id": task.get("task_id", task.get("query_task_id")),
            "workflow_type": task.get("workflow_type"), "command_id": task.get("workflow_command_id"),
            "request_id": task.get("request_id"),
        }
        if activity:
            sources.update({
                "activity_type": task.get("activity_type"),
                "activity_id": task.get("activity_execution_id"),
                "activity_attempt_id": task.get("activity_attempt_id", task.get("attempt_id")),
            })
        for key, source in sources.items():
            value = _bounded(source)
            if value is not None:
                fields[key] = value
        attempt = task.get("attempt_number") if activity else task.get(
            "workflow_task_attempt", task.get("query_task_attempt"),
        )
        if type(attempt) is int and 0 < attempt <= 2**63 - 1:
            key = "activity_attempt_number" if activity else (
                "workflow_task_attempt" if "workflow_task_attempt" in task else "query_task_attempt"
            )
            fields[key] = attempt
        return fields

    async def _run(
        self, name: str, fields: dict[str, str | int], context: _C,
        next: Callable[[_C], Awaitable[_T]], *, nested: bool = False,
    ) -> _T:
        parent_fields = _fields.get() if _execution_pid.get() == os.getpid() else None
        tracer = self._get_tracer()
        span: Any = None
        otel_context: Any = None
        otel_token: Any = None
        try:
            trace = importlib.import_module("opentelemetry.trace")
            otel_context = importlib.import_module("opentelemetry.context")
            parent = otel_context.get_current() if nested and parent_fields is not None else otel_context.Context()
            if tracer is not None:
                span = tracer.start_span(
                    name, context=parent, attributes={f"durable.{k}": v for k, v in fields.items()},
                )
                parent = trace.set_span_in_context(span, parent)
            otel_token = otel_context.attach(parent)
        except Exception:
            pass
        token = _fields.set(fields)
        pid_token = _execution_pid.set(os.getpid())
        try:
            return await next(context)
        except BaseException:
            if span is not None:
                try:
                    trace = importlib.import_module("opentelemetry.trace")
                    span.set_attribute("error.type", "durable.execution.failed")
                    span.set_status(trace.Status(trace.StatusCode.ERROR, "durable execution failed"))
                except Exception:
                    pass
            raise
        finally:
            _fields.reset(token)
            _execution_pid.reset(pid_token)
            if otel_token is not None:
                with suppress(Exception):
                    otel_context.detach(otel_token)
            if span is not None:
                with suppress(Exception):
                    span.end()

    async def execute_workflow_task(
        self, context: WorkflowTaskInterceptorContext, next: WorkflowTaskHandler,
    ) -> list[dict[str, Any]] | None:
        return await self._run("durable.workflow.task", self._task_fields(
            context.task, context.worker_id, context.task_queue,
        ), context, next)

    async def execute_query_task(self, context: QueryTaskInterceptorContext, next: QueryTaskHandler) -> str:
        return await self._run("durable.query.task", self._task_fields(
            context.task, context.worker_id, context.task_queue,
        ), context, next)

    async def execute_activity(self, context: ActivityInterceptorContext, next: ActivityHandler) -> Any:
        task = {**context.task, "activity_type": context.activity_type}
        # Supervised/local callbacks already expose their admitted immutable
        # attempt through ActivityInfo, even when their task is a workflow task.
        from .activity import context as activity_context
        try:
            info = activity_context().info
            if info.task_id == task.get("task_id") and info.activity_type == context.activity_type:
                task["activity_attempt_id"] = info.activity_attempt_id
                task["attempt_number"] = info.attempt_number
        except RuntimeError:
            pass
        fields = self._task_fields(task, context.worker_id, context.task_queue, activity=True)
        return await self._run("durable.activity.attempt", fields, context, next, nested=True)
