from __future__ import annotations

import asyncio
import json
import logging
import os
import pickle
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from opentelemetry import context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from durable_workflow import activity, serializer, workflow
from durable_workflow._activity_process import CallbackInvocation, SupervisedCallback
from durable_workflow.activity import ActivityInfo
from durable_workflow.client import Client
from durable_workflow.interceptors import ActivityInterceptorContext
from durable_workflow.telemetry import ExecutionTelemetry, LogContextFilter, current_log_context
from durable_workflow.worker import Worker


def instrumentation() -> tuple[ExecutionTelemetry, TracerProvider, InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return ExecutionTelemetry(namespace="shipping", tracer_provider=provider), provider, exporter


def task(attempt: int = 1) -> dict[str, Any]:
    return {
        "task_id": "activity-task", "activity_execution_id": "logical-activity",
        "activity_type": "shipping", "activity_attempt_id": f"attempt-{attempt}",
        "workflow_id": "workflow", "run_id": "run", "attempt_number": attempt,
        "payload_codec": "avro", "arguments": serializer.encode([], codec="avro"),
    }


async def test_actual_worker_attempt_logs_follow_application_child_and_keep_request_id() -> None:
    telemetry, provider, exporter = instrumentation()
    records: list[logging.LogRecord] = []

    class Handler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = Handler()
    handler.addFilter(LogContextFilter())
    logger = logging.Logger("application")
    logger.addHandler(handler)

    @activity.defn(name="shipping")
    async def shipping() -> bytes:
        assert current_log_context()["workflow_run_id"] == "run"
        with provider.get_tracer("http-client").start_as_current_span("HTTP POST shipping") as span:
            await asyncio.sleep(0)
            logger.info("application message", extra={"request_id": "application-request"})
            assert records[-1].span_id == format(span.get_span_context().span_id, "016x")  # type: ignore[attr-defined]
        return b"shipped"

    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="shipping", workflows=[], activities=[shipping], interceptors=[telemetry])
    for attempt in (1, 2):
        assert await worker._run_activity_task(task(attempt)) == "completed"
        assert current_log_context() == {}
    assert client.complete_activity_task.call_count == 2
    assert client.complete_activity_task.call_args.kwargs["result"] == b"shipped"
    assert [record.activity_attempt_id for record in records] == ["attempt-1", "attempt-2"]  # type: ignore[attr-defined]
    assert all(record.request_id == "application-request" for record in records)  # type: ignore[attr-defined]
    spans = exporter.get_finished_spans()
    assert len(spans) == 4
    attempts = [span for span in spans if span.name == "durable.activity.attempt"]
    assert all(span.parent is None for span in attempts)
    assert attempts[0].context.trace_id != attempts[1].context.trace_id


async def test_concurrent_attempts_and_synchronous_worker_threads_keep_distinct_contexts() -> None:
    telemetry, _, exporter = instrumentation()
    ready = asyncio.Event()
    count = 0

    async def callback(ctx: ActivityInterceptorContext) -> str:
        nonlocal count
        before = current_log_context()
        count += 1
        if count == 2:
            ready.set()
        await ready.wait()
        assert await asyncio.to_thread(current_log_context) == before
        assert current_log_context() == before
        return str(ctx.task["activity_attempt_id"])

    results = await asyncio.gather(*(
        telemetry.execute_activity(
            ActivityInterceptorContext("worker", "queue", task(attempt), "shipping", ()), callback,
        )
        for attempt in (1, 2)
    ))
    assert results == ["attempt-1", "attempt-2"]
    assert current_log_context() == {}
    assert len(exporter.get_finished_spans()) == 2


@workflow.defn(name="telemetry.workflow")
class TracedWorkflow:
    def run(self, ctx: Any) -> Any:
        assert current_log_context()["workflow_run_id"] == "run"
        return (yield ctx.schedule_activity("shipping", []))


async def test_actual_workflow_replay_emits_no_completed_activity_attempt() -> None:
    telemetry, _, exporter = instrumentation()
    worker = Worker(AsyncMock(spec=Client), task_queue="shipping", workflows=[TracedWorkflow],
                    activities=[], interceptors=[telemetry])
    work = {"task_id": "workflow-task", "workflow_id": "workflow", "run_id": "run",
            "workflow_type": "telemetry.workflow", "workflow_task_attempt": 1,
            "payload_codec": "avro", "arguments": serializer.encode([], codec="avro"), "history_events": []}
    commands = await worker._run_workflow_task(work)
    assert commands is not None
    assert commands[0]["type"] == "schedule_activity"
    work["history_events"] = [{"event_type": "ActivityCompleted", "payload": {
        "sequence": 1, "activity_type": "shipping", "payload_codec": "avro",
        "result": serializer.envelope("shipped", codec="avro"),
    }}]
    await worker._run_workflow_task(work)
    assert [span.name for span in exporter.get_finished_spans()] == ["durable.workflow.task"] * 2
    assert current_log_context() == {}


async def test_failure_cancellation_and_caller_context_are_preserved_without_exception_text() -> None:
    telemetry, provider, exporter = instrumentation()
    failure = RuntimeError("private credential")

    async def failing(_: ActivityInterceptorContext) -> Any:
        raise failure

    caller = provider.get_tracer("application").start_span("caller")
    token = context.attach(trace.set_span_in_context(caller))
    try:
        invocation = ActivityInterceptorContext("worker", "queue", task(), "shipping", ())
        with pytest.raises(RuntimeError) as caught:
            await telemetry.execute_activity(invocation, failing)
        assert caught.value is failure
        assert trace.get_current_span() is caller
        started = asyncio.Event()

        async def pending(_: ActivityInterceptorContext) -> Any:
            started.set()
            await asyncio.Future()

        running = asyncio.create_task(telemetry.execute_activity(
            ActivityInterceptorContext("worker", "queue", task(2), "shipping", ()), pending,
        ))
        await started.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert current_log_context() == {}
        assert trace.get_current_span() is caller
    finally:
        context.detach(token)
        caller.end()
    spans = [span for span in exporter.get_finished_spans() if span.name != "caller"]
    assert len(spans) == 2
    assert all(span.status.status_code is trace.StatusCode.ERROR for span in spans)
    assert all(not span.events for span in spans)
    assert "private credential" not in repr([span.attributes for span in spans])


class FileExporter(SpanExporter):
    def export(self, spans: Any) -> SpanExportResult:
        with Path(os.environ["DW_TEST_TELEMETRY_FILE"]).open("a") as stream:
            for span in spans:
                stream.write(json.dumps({
                    "name": span.name, "pid": os.getpid(), "attributes": dict(span.attributes),
                }) + "\n")
        return SpanExportResult.SUCCESS


def child_provider() -> TracerProvider:
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(FileExporter()))
    return provider


def spawned_callback() -> dict[str, str | int]:
    return current_log_context()


async def test_real_supervised_child_constructs_fresh_provider_and_exports_before_exit(
    tmp_path: Path, monkeypatch: Any,
) -> None:
    destination = tmp_path / "spans.jsonl"
    monkeypatch.setenv("DW_TEST_TELEMETRY_FILE", str(destination))
    telemetry, _, _ = instrumentation()
    telemetry.provider_factory = f"{__name__}:child_provider"
    # Resolve parent provider first, proving its object/socket state is omitted.
    assert telemetry._get_tracer() is not None
    restored = pickle.loads(pickle.dumps(telemetry))
    assert restored._provider is None
    callback = SupervisedCallback(CallbackInvocation(
        spawned_callback, (), ActivityInfo("activity-task", "shipping", "attempt-child", 3, "queue", "worker"),
        task(), (telemetry,),
    ))

    async def heartbeat(_: Any) -> None:
        raise AssertionError("unauthored heartbeat")

    try:
        await callback.start()
        outcome = await callback.result(heartbeat)
        assert outcome.failure is None
        assert outcome.value["activity_attempt_id"] == "attempt-child"
        assert outcome.value["activity_attempt_number"] == 3
        assert outcome.value["trace_id"]
        assert callback.stopped
    finally:
        await callback.close()
    spans = [json.loads(line) for line in destination.read_text().splitlines()]
    assert len(spans) == 1
    assert spans[0]["pid"] != os.getpid()
    assert spans[0]["attributes"]["activity_attempt_id"] == "attempt-child"


def test_fresh_process_without_optional_api_keeps_business_result() -> None:
    script = '''
import asyncio, importlib.abc, sys
class Absent(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith("opentelemetry"):
            raise ModuleNotFoundError(fullname)
sys.meta_path.insert(0, Absent())
from durable_workflow.telemetry import ExecutionTelemetry, current_log_context
from durable_workflow.interceptors import ActivityInterceptorContext
async def callback(ctx):
    assert current_log_context()["task_id"] == "task"
    return b"original"
async def run():
    ctx = ActivityInterceptorContext("w", "q", {"task_id": "task"}, "a", ())
    assert await ExecutionTelemetry().execute_activity(ctx, callback) == b"original"
    assert current_log_context() == {}
asyncio.run(run())
'''
    subprocess.run([sys.executable, "-c", script], check=True, timeout=10)


async def test_bad_provider_and_exporter_do_not_change_original_result() -> None:
    class BadProvider:
        def get_tracer(self, *_: Any) -> Any:
            raise RuntimeError("provider unavailable")

    async def callback(_: ActivityInterceptorContext) -> object:
        return original

    original = object()
    telemetry = ExecutionTelemetry(tracer_provider=BadProvider())
    assert await telemetry.execute_activity(
        ActivityInterceptorContext("w", "q", {"task_id": "task\ninvalid"}, "shipping", ()), callback,
    ) is original
    assert current_log_context() == {}


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
async def test_forked_host_drops_inherited_context_and_provider_without_factory() -> None:
    telemetry, _, exporter = instrumentation()

    async def callback(_: ActivityInterceptorContext) -> int:
        assert current_log_context()["activity_attempt_id"] == "attempt-1"
        child = os.fork()
        if child == 0:
            try:
                if current_log_context() != {}:
                    os._exit(1)
                tracer = telemetry._get_tracer()
                span = tracer.start_span("child", context=context.Context())
                os._exit(1 if span.get_span_context().is_valid else 0)
            except BaseException:
                os._exit(1)
        _, status = await asyncio.to_thread(os.waitpid, child, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert current_log_context()["activity_attempt_id"] == "attempt-1"
        return 42

    assert await telemetry.execute_activity(
        ActivityInterceptorContext("worker", "queue", task(), "shipping", ()), callback,
    ) == 42
    assert len(exporter.get_finished_spans()) == 1
