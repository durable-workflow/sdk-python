"""Worker side of deadline-triggered timeout cleanup (server#1201).

A cooperative worker advertises ``timeout_cleanup`` version 1. Only the
delivered root timeout interruption that escapes workflow code, uncaught or
re-raised, adds ``failure.interruption: "timeout_cleanup"`` to
``fail_workflow``. Every other failure stays an ordinary failure.
"""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import Any

import pytest

from durable_workflow import (
    CancellationContext,
    WorkflowCancelled,
    WorkflowTimeoutCleanupRequested,
    serializer,
    workflow,
)
from durable_workflow._failure_reporting import bound_failure_request
from durable_workflow._timeout_cleanup import failure_interruption, manifest_entry
from durable_workflow.errors import _cancellation_requested
from durable_workflow.worker import Worker
from durable_workflow.workflow import CompleteWorkflow, FailWorkflow, WorkflowContext, replay
from tests.test_cooperative_cancellation import observation
from tests.test_cooperative_cancellation_worker import ClaimServer

MARKER = {"interruption": "timeout_cleanup"}


def context_snapshot(cause: str | None = "timeout", **overrides: Any) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "schema": "durable-workflow.cancellation-context/v1",
        "request_id": "request-1",
        "root_request_id": "request-1",
        "root_workflow_instance_id": "workflow-1",
        "root_workflow_run_id": "run-1",
        "parent_request_id": None,
        "reason": "Workflow work deadline passed; timeout cleanup requested.",
        "requester": {"type": "system", "id": "timeout_cleanup"},
        "source": "system",
        "requested_at": "2026-09-30T12:00:00.000000Z",
        "cleanup_deadline_at": "2026-09-30T12:10:00.000000Z",
        "lineage": [{"request_id": "request-1", "workflow_instance_id": "workflow-1", "workflow_run_id": "run-1"}],
        **({} if cause is None else {"cause": cause}),
    }
    snapshot.update(overrides)
    return snapshot


def cleanup_started() -> dict[str, Any]:
    return {
        "event_type": "WorkflowTimeoutCleanupStarted",
        "workflow_command_id": "request-1",
        "recorded_at": "2026-09-30T12:00:00.000100Z",
        "payload": {
            "workflow_command_id": "request-1", "timeout_kind": "execution",
            "work_deadline_at": "2026-09-30T12:00:00.000000Z", "cleanup_deadline_at": "2026-09-30T12:10:00.000000Z",
            "hard_deadline_at": "2026-09-30T12:10:00.000000Z", "detected_at": "2026-09-30T12:00:00.000000Z",
        },
    }


def requested(cause: str | None = "timeout") -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationRequested",
        "workflow_command_id": "request-1",
        "recorded_at": "2026-09-30T12:00:00.000120Z",
        "payload": {
            "workflow_command_id": "request-1", "workflow_instance_id": "workflow-1", "workflow_run_id": "run-1",
            "command_type": "request_cancellation",
            "reason": "Workflow work deadline passed; timeout cleanup requested.",
            "cleanup_deadline_at": "2026-09-30T12:10:00.000000Z", "cancellation": context_snapshot(cause),
        },
    }


def delivered(cause: str | None = "timeout", sequence: int = 1) -> dict[str, Any]:
    return {
        "event_type": "CooperativeCancellationDelivered",
        "workflow_command_id": "request-1",
        "payload": {
            "workflow_command_id": "request-1", "workflow_run_id": "run-1", "sequence": sequence,
            "call_kind": "timer", "cancellation": context_snapshot(cause),
        },
    }


def history(cause: str | None = "timeout", *, started: bool = True) -> list[dict[str, Any]]:
    """History as native records it: the cleanup event only for an engine timeout request."""
    events = [{"event_type": "TimerScheduled", "payload": {"sequence": 1, "delay_seconds": 30}}]
    if started:
        events.append(cleanup_started())
    return [*events, requested(cause), delivered(cause)]


def shielded_cleanup(sequence: int = 2) -> list[dict[str, Any]]:
    return [
        {"event_type": "TimerScheduled", "payload": {"sequence": sequence, "delay_seconds": 1}},
        {"event_type": "TimerFired", "payload": {"sequence": sequence}},
    ]


class CompensationFailed(Exception):
    pass


def _uncaught(ctx: WorkflowContext):  # type: ignore[no-untyped-def]
    yield ctx.start_timer(30)


def _rethrown_after_compensation(ctx: WorkflowContext):  # type: ignore[no-untyped-def]
    try:
        yield ctx.start_timer(30)
    except WorkflowCancelled:
        with ctx.cancellation_shield():
            yield ctx.start_timer(1)
        raise


def _rethrown_by_name(ctx: WorkflowContext):  # type: ignore[no-untyped-def]
    try:
        yield ctx.start_timer(30)
    except WorkflowTimeoutCleanupRequested as interrupted:
        raise interrupted


def _thrown_at_safe_point(ctx: WorkflowContext):  # type: ignore[no-untyped-def]
    try:
        yield ctx.start_timer(30)
    except WorkflowCancelled:
        with ctx.cancellation_shield():
            yield ctx.start_timer(1)
        ctx.throw_if_cancellation_requested()


def _compensation_failure(ctx: WorkflowContext):  # type: ignore[no-untyped-def]
    try:
        yield ctx.start_timer(30)
    except WorkflowCancelled:
        raise CompensationFailed("compensation failed") from None


def _wrapped(ctx: WorkflowContext):  # type: ignore[no-untyped-def]
    try:
        yield ctx.start_timer(30)
    except WorkflowCancelled as interrupted:
        raise CompensationFailed("wrapped") from interrupted


def _wrapped_in_the_same_type(ctx: WorkflowContext):  # type: ignore[no-untyped-def]
    try:
        yield ctx.start_timer(30)
    except WorkflowCancelled as interrupted:
        raise WorkflowTimeoutCleanupRequested(
            request_id=interrupted.request_id, context=interrupted.context,
        ) from interrupted


def _constructed(ctx: WorkflowContext):  # type: ignore[no-untyped-def]
    yield ctx.start_timer(30)
    raise WorkflowTimeoutCleanupRequested(
        request_id="request-1", context=CancellationContext.from_dict(context_snapshot()),
    )


def _returns(ctx: WorkflowContext):  # type: ignore[no-untyped-def]
    try:
        yield ctx.start_timer(30)
    except WorkflowTimeoutCleanupRequested:
        return "compensated"
    return "finished"


def _workflow(body: Callable[[WorkflowContext], Any], name: str) -> type:
    def run(self: Any, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        return (yield from body(ctx))

    return workflow.defn(name=name)(type(name.replace("-", "_"), (), {"run": run}))


def _single_failure(commands: list[Any]) -> FailWorkflow:
    assert len(commands) == 1 and isinstance(commands[0], FailWorkflow), commands
    return commands[0]


@pytest.mark.parametrize("body,events,exception_type", [
    (_uncaught, history(), "WorkflowTimeoutCleanupRequested"),
    (_rethrown_after_compensation, [*history(), *shielded_cleanup()], "WorkflowTimeoutCleanupRequested"),
    (_rethrown_by_name, history(), "WorkflowTimeoutCleanupRequested"),
    (_thrown_at_safe_point, [*history(), *shielded_cleanup()], "WorkflowTimeoutCleanupRequested"),
], ids=["uncaught", "rethrown-after-compensation", "rethrown-by-name", "safe-point"])
def test_the_escaped_delivered_timeout_interruption_is_marked(
    body: Callable[[WorkflowContext], Any], events: list[dict[str, Any]], exception_type: str,
) -> None:
    failure = _single_failure(replay(_workflow(body, "timeout-marked"), events, [], run_id="run-1").commands)

    assert failure.exception_type == exception_type
    assert failure.interruption == "timeout_cleanup"
    assert failure.to_server_command("queue")["failure"] == MARKER


@pytest.mark.parametrize("body,events,cause,exception_type", [
    (_compensation_failure, history(), "timeout", "CompensationFailed"),
    (_wrapped, history(), "timeout", "CompensationFailed"),
    (_wrapped_in_the_same_type, history(), "timeout", "WorkflowTimeoutCleanupRequested"),
    (_constructed, [
        {"event_type": "TimerScheduled", "payload": {"sequence": 1, "delay_seconds": 30}},
        {"event_type": "TimerFired", "payload": {"sequence": 1}},
    ], None, "WorkflowTimeoutCleanupRequested"),
    (_uncaught, history(None, started=False), None, "WorkflowCancelled"),
    (_uncaught, history("cancellation", started=False), "cancellation", "WorkflowCancelled"),
    (_rethrown_after_compensation, [*history(None, started=False), *shielded_cleanup()], None, "WorkflowCancelled"),
], ids=[
    "compensation-failure", "wrapped", "wrapped-in-the-same-type", "constructed",
    "ordinary-cancellation", "explicit-cancellation-cause", "rethrown-ordinary-cancellation",
])
def test_other_failures_stay_ordinary_failures(
    body: Callable[[WorkflowContext], Any], events: list[dict[str, Any]], cause: str | None, exception_type: str,
) -> None:
    failure = _single_failure(replay(_workflow(body, "timeout-unmarked"), events, [], run_id="run-1").commands)

    assert failure.exception_type == exception_type
    assert failure.interruption is None
    assert "failure" not in failure.to_server_command("queue")


def test_cleanup_that_returns_completes_and_replays_the_cleanup_event() -> None:
    outcome = replay(_workflow(_returns, "timeout-returns"), history(), [], run_id="run-1")

    assert outcome.commands == [CompleteWorkflow("compensated")]


def test_offline_replay_without_the_run_identity_does_not_mark() -> None:
    failure = _single_failure(replay(_workflow(_uncaught, "timeout-offline"), history(), []).commands)

    assert failure.exception_type == "WorkflowTimeoutCleanupRequested"
    assert failure.interruption is None


def test_the_rule_needs_the_runtime_delivered_root_timeout_object() -> None:
    timeout = CancellationContext.from_dict(context_snapshot())
    interruption = _cancellation_requested("cancelled", request_id="request-1", context=timeout)
    assert isinstance(interruption, WorkflowTimeoutCleanupRequested)
    assert failure_interruption(interruption, delivered=[interruption], run_id="run-1") == "timeout_cleanup"

    descendant = CancellationContext.from_dict(context_snapshot(
        request_id="request-child", parent_request_id="request-1",
        lineage=[*context_snapshot()["lineage"], {
            "request_id": "request-child", "workflow_instance_id": "child", "workflow_run_id": "child-run",
        }],
    ))
    another_request = WorkflowTimeoutCleanupRequested(request_id="request-2", context=timeout)
    descendant_request = WorkflowTimeoutCleanupRequested(request_id="request-child", context=descendant)
    for case, failure, run_id in [
        ("constructed without context", WorkflowTimeoutCleanupRequested(), "run-1"),
        ("constructed with only a request", WorkflowTimeoutCleanupRequested(request_id="request-1"), "run-1"),
        ("constructed copy", WorkflowTimeoutCleanupRequested(request_id="request-1", context=timeout), "run-1"),
        ("another request", another_request, "run-1"),
        ("descendant", descendant_request, "child-run"),
        ("another run", interruption, "run-2"),
        ("no run", interruption, ""),
    ]:
        assert failure_interruption(
            failure, delivered=[interruption, another_request, descendant_request], run_id=run_id,
        ) is None, case
    ordinary = _cancellation_requested(
        "cancelled", request_id="request-1", context=CancellationContext.from_dict(context_snapshot(None)),
    )
    wrapped = RuntimeError("wrapped")
    wrapped.__cause__ = interruption
    for case, failure in [("ordinary", ordinary), ("wrapped", wrapped)]:
        assert failure_interruption(failure, delivered=[ordinary, wrapped, interruption], run_id="run-1") is None, case


def test_failure_report_bounding_keeps_the_marker() -> None:
    command = FailWorkflow(
        "x" * (200 * 1024), exception_type="WorkflowTimeoutCleanupRequested",
        exception={"type": "WorkflowTimeoutCleanupRequested", "details": "y" * (200 * 1024)},
        interruption="timeout_cleanup",
    ).to_server_command("queue")
    body = {"lease_owner": "worker", "workflow_task_attempt": 1, "commands": [command]}

    bounded = bound_failure_request("/worker/workflow-tasks/task-1/complete", body)

    assert bounded["commands"][0]["failure"] == MARKER
    assert len(bounded["commands"][0]["message"].encode()) < len(command["message"].encode())


# Worker registration and delivery.


async def test_cooperative_registration_advertises_timeout_cleanup_version_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = ClaimServer()
    await server.worker(monkeypatch)
    registration = server.client.register_worker.await_args.kwargs

    assert "timeout_cleanup" in registration["capabilities"]
    assert registration["capability_manifest"]["timeout_cleanup"] == {
        "supported": True, "minimum_protocol_version": "1.20", "version": 1,
    }
    assert registration["capability_manifest"]["timeout_cleanup"] == manifest_entry()


async def test_an_explicit_capability_is_listed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer()
    await server.worker(monkeypatch, capabilities=["cooperative_cancellation", "timeout_cleanup"])

    assert server.client.register_worker.await_args.kwargs["capabilities"].count("timeout_cleanup") == 1


async def test_a_worker_without_cooperative_cancellation_advertises_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer()
    monkeypatch.delenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", raising=False)
    worker = Worker(server.client, task_queue="queue")
    await worker._register()
    registration = server.client.register_worker.await_args.kwargs

    assert "timeout_cleanup" not in registration["capabilities"]
    assert "timeout_cleanup" not in registration["capability_manifest"]


def test_timeout_cleanup_requires_cooperative_cancellation() -> None:
    with pytest.raises(ValueError, match="timeout cleanup requires cooperative_cancellation"):
        Worker(ClaimServer().client, task_queue="queue", capabilities=["timeout_cleanup"])


def _claimed(workflow_type: str, events: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "task_id": "task-1", "workflow_id": "workflow-1", "run_id": "run-1",
        "workflow_type": workflow_type, "workflow_task_attempt": 4,
        "payload_codec": "avro", "arguments": serializer.envelope([], codec="avro"),
        "history_events": deepcopy(events or []),
        **({} if events is not None else {"cancellation_request": observation()}),
    }


def _engine_request(cause: str | None) -> list[dict[str, Any]]:
    """What a worker first loads: the request, plus the cleanup event for a timeout."""
    return [*([cleanup_started()] if cause == "timeout" else []), requested(cause)]


@pytest.mark.parametrize("body,cause,exception_type,failure", [
    (_uncaught, "timeout", "WorkflowTimeoutCleanupRequested", MARKER),
    (_rethrown_by_name, "timeout", "WorkflowTimeoutCleanupRequested", MARKER),
    (_compensation_failure, "timeout", "CompensationFailed", None),
    (_wrapped, "timeout", "CompensationFailed", None),
    (_uncaught, None, "WorkflowCancelled", None),
    (_uncaught, "cancellation", "WorkflowCancelled", None),
], ids=[
    "uncaught", "rethrown", "compensation-failure", "wrapped", "ordinary-cancellation", "explicit-cancellation-cause",
])
async def test_cooperative_worker_sends_the_marker_only_for_the_escaped_interruption(
    monkeypatch: pytest.MonkeyPatch, body: Callable[[WorkflowContext], Any], cause: str | None,
    exception_type: str, failure: dict[str, str] | None,
) -> None:
    server = ClaimServer(history=_engine_request(cause))
    definition = _workflow(body, "timeout-worker")
    worker = await server.worker(monkeypatch, workflows=[definition])

    commands = await worker._run_workflow_task(_claimed("timeout-worker"))

    assert server.trace == ["history", "delivery", "history", "completion"]
    assert commands is not None and [command["type"] for command in commands] == ["fail_workflow"]
    assert commands[0]["exception_type"] == exception_type
    assert commands[0].get("failure") == failure
    assert server.client.complete_workflow_task.await_args.kwargs["commands"][0].get("failure") == failure
    server.client.fail_workflow_task.assert_not_awaited()


async def test_cooperative_cleanup_that_returns_sends_an_ordinary_completion(monkeypatch: pytest.MonkeyPatch) -> None:
    server = ClaimServer(history=_engine_request("timeout"))
    worker = await server.worker(monkeypatch, workflows=[_workflow(_returns, "timeout-worker-returns")])

    commands = await worker._run_workflow_task(_claimed("timeout-worker-returns"))

    assert commands is not None and [command["type"] for command in commands] == ["complete_workflow"]
    assert serializer.decode_envelope(commands[0]["result"]) == "compensated"
    assert "failure" not in commands[0]


async def test_a_worker_without_cooperative_cancellation_never_sends_the_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = ClaimServer()
    monkeypatch.delenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", raising=False)
    definition = _workflow(_constructed, "timeout-incapable")
    worker = Worker(server.client, task_queue="queue", worker_id="cooperative-worker", workflows=[definition])
    await worker._register()
    events = [
        {"event_type": "TimerScheduled", "payload": {"sequence": 1, "delay_seconds": 30}},
        {"event_type": "TimerFired", "payload": {"sequence": 1}},
    ]

    commands = await worker._run_workflow_task(_claimed("timeout-incapable", events))

    assert commands is not None and [command["type"] for command in commands] == ["fail_workflow"]
    assert commands[0]["exception_type"] == "WorkflowTimeoutCleanupRequested"
    assert "failure" not in commands[0]


async def test_a_worker_without_cooperative_cancellation_strips_a_replayed_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from durable_workflow import worker as worker_module

    original = worker_module.replay

    def marked(*args: Any, **kwargs: Any) -> Any:
        outcome = original(*args, **kwargs)
        outcome.commands = [FailWorkflow("forced", interruption="timeout_cleanup")]
        return outcome

    monkeypatch.setattr(worker_module, "replay", marked)
    server = ClaimServer()
    monkeypatch.delenv("DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION", raising=False)
    definition = _workflow(_uncaught, "timeout-stripped")
    worker = Worker(server.client, task_queue="queue", worker_id="cooperative-worker", workflows=[definition])
    await worker._register()

    commands = await worker._run_workflow_task(_claimed("timeout-stripped", []))

    assert commands is not None and [command["type"] for command in commands] == ["fail_workflow"]
    assert "failure" not in commands[0]
