"""Negotiated poll-to-handler controls, without a real Server."""

from __future__ import annotations

import copy
import json
import pickle
from dataclasses import FrozenInstanceError, dataclass
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from durable_workflow import (
    ActivityContext,
    ActivityHeartbeatResumeUnavailable,
    ActivityInfo,
    Client,
    TypedValueError,
    Worker,
    activity,
    serializer,
)
from durable_workflow._activity_checkpoint import capture_poll
from durable_workflow._activity_process import CallbackInvocation, SupervisedCallback

AT = "2026-11-14T09:00:00.123456789Z"


def poll(*, attempt: int = 2, details: Any = None, at: Any = None) -> dict[str, Any]:
    return {
        "task": {
            "task_id": "task", "activity_execution_id": "execution", "activity_attempt_id": "attempt",
            "workflow_id": "workflow", "run_id": "run", "namespace": "default", "attempt_number": attempt,
            "activity_type": "checkpoint.inspect", "lease_owner": "worker", "task_queue": "queue",
            "arguments": serializer.envelope([]), "payload_codec": "avro",
            "retry_policy": {"max_attempts": 3, "backoff_seconds": [1, 2]},
            "last_heartbeat_details": details, "last_heartbeat_at": at,
        },
        "server_capabilities": {"activity_heartbeat_resume": {"supported": True, "version": 1}},
    }


def context(wire: dict[str, Any]) -> ActivityContext:
    task = capture_poll(wire, "default", "worker")
    return ActivityContext(info=ActivityInfo(
        task["task_id"], "checkpoint.inspect", task["activity_attempt_id"], task["attempt_number"], "queue", "worker",
        task_metadata=task, task_namespace="default",
    ), client=AsyncMock(spec=Client))


@pytest.mark.parametrize("capability", [
    None, False, {"supported": False, "version": 1}, {"supported": True, "version": 2},
    {"supported": True, "version": "1"}, {"supported": True, "version": True}, {"supported": 1, "version": 1},
])
def test_unavailable_is_distinct_from_supported_empty(capability: Any) -> None:
    wire = poll()
    wire["server_capabilities"]["activity_heartbeat_resume"] = capability
    ctx = context(wire)
    assert ctx.heartbeat_resume_version() is None
    for accessor in (ctx.last_heartbeat_checkpoint, ctx.last_heartbeat_details, ctx.last_heartbeat_at):
        with pytest.raises(ActivityHeartbeatResumeUnavailable):
            accessor()


def test_omitted_fields_and_legacy_context_do_not_invent_empty_state() -> None:
    wire = poll()
    del wire["task"]["last_heartbeat_details"], wire["task"]["last_heartbeat_at"]
    with pytest.raises(ActivityHeartbeatResumeUnavailable):
        context(wire).last_heartbeat_checkpoint()
    info = ActivityInfo("task", "activity", "attempt", 1, "queue", "worker")
    with pytest.raises(ActivityHeartbeatResumeUnavailable):
        ActivityContext(info=info, client=AsyncMock(spec=Client)).last_heartbeat_details()


@pytest.mark.parametrize("attempt", [1, 2])
def test_explicit_empty_claim_is_supported_on_first_and_later_attempts(attempt: int) -> None:
    ctx = context(poll(attempt=attempt))
    assert ctx.heartbeat_resume_version() == 1
    assert ctx.last_heartbeat_checkpoint() is None
    assert ctx.last_heartbeat_details() is None
    assert ctx.last_heartbeat_at() is None


@dataclass
class Progress:
    cursor: int
    flags: list[bool | None]


def test_snapshot_is_typed_independent_private_and_spawn_serializable() -> None:
    details = {"cursor": 2**63 - 1, "flags": [True, None, False], "secret": "private-checkpoint"}
    wire = poll(details=details, at=AT)
    ctx = context(wire)
    details["cursor"] = 0
    first = ctx.last_heartbeat_details()
    first["flags"].clear()
    assert ctx.last_heartbeat_details(result_type=Progress) == Progress(2**63 - 1, [True, None, False])
    assert ctx.last_heartbeat_at() == AT
    checkpoint = ctx.last_heartbeat_checkpoint()
    assert checkpoint is not None
    assert checkpoint.contract_version == 1
    assert "private-checkpoint" not in repr(checkpoint)
    with pytest.raises(FrozenInstanceError):
        checkpoint.acknowledged_at = "replacement"  # type: ignore[misc]
    restored = ActivityContext(info=pickle.loads(pickle.dumps(ctx.info)), client=AsyncMock(spec=Client))
    assert restored.last_heartbeat_details() == ctx.last_heartbeat_details()
    assert restored.last_heartbeat_at() == AT
    with pytest.raises(TypedValueError):
        ctx.last_heartbeat_details(result_type=list[int])


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize(("field", "value"), [
    ("task_id", ""), ("activity_execution_id", None), ("activity_execution_id", "  "),
    ("activity_attempt_id", 3), ("workflow_id", ""), ("run_id", None), ("namespace", "other"),
    ("lease_owner", "other"), ("attempt_number", 0), ("attempt_number", -1),
    ("attempt_number", True), ("attempt_number", "2"), ("attempt_number", 2.0),
])
def test_even_empty_checkpoint_requires_the_original_claim_identity(empty: bool, field: str, value: Any) -> None:
    wire = poll(details=None if empty else {"cursor": 1}, at=None if empty else AT)
    wire["task"][field] = value
    with pytest.raises(ValueError, match="invalid checkpoint"):
        context(wire)


@pytest.mark.parametrize(("details", "at", "attempt"), [
    ({}, AT, 1), ({}, None, 2), (None, AT, 2), ({}, "not-a-time", 2),
    ({}, "2026-11-14T09:00:00", 2), ({}, "2026-02-30T09:00:00Z", 2),
    ({}, "2026-11-14T09:00:00+99:00", 2), ({"value": float("nan")}, AT, 2),
    ({}, "2026-11-14T09:00:00+01:75", 2), ({}, "2026-11-14T09:00:00.1234567890Z", 2),
    ({}, "2026-11-14T09:00:00.١٢٣Z", 2),
])
def test_invalid_pairs_and_timestamps_refuse_without_exposing_details(details: Any, at: Any, attempt: int) -> None:
    with pytest.raises(ValueError, match="^activity_heartbeat_resume v1 claim has an invalid checkpoint$"):
        context(poll(attempt=attempt, details=details, at=at))


@pytest.mark.parametrize("field", ["last_heartbeat_details", "last_heartbeat_at"])
def test_one_missing_checkpoint_field_refuses(field: str) -> None:
    wire = poll()
    del wire["task"][field]
    with pytest.raises(ValueError, match="invalid checkpoint"):
        context(wire)


@pytest.mark.parametrize("field", [
    "task_id", "activity_execution_id", "activity_attempt_id", "workflow_id", "run_id", "activity_type", "task_queue",
])
def test_task_mutation_cannot_rebind_the_captured_snapshot(field: str) -> None:
    task = capture_poll(poll(details={"cursor": 1}, at=AT), "default", "worker")
    task[field] = "replacement"
    with pytest.raises(ValueError, match="invalid checkpoint"):
        ActivityInfo(task["task_id"], "activity", task["activity_attempt_id"], 2, "queue", "worker",
                     task_metadata=task, task_namespace="default")


@pytest.mark.parametrize("outcome", ["success", "refused", "uncertain"])
async def test_heartbeat_outcomes_never_advance_the_received_checkpoint(outcome: str) -> None:
    ctx = context(poll(details={"cursor": 1}, at=AT))
    if outcome == "uncertain":
        ctx._client.heartbeat_activity_task.side_effect = httpx.ReadTimeout("response lost")  # type: ignore[attr-defined]
        with pytest.raises(httpx.ReadTimeout):
            await ctx.heartbeat({"cursor": 2})
    else:
        ctx._client.heartbeat_activity_task.return_value = {  # type: ignore[attr-defined]
            "can_continue": outcome == "success", "last_heartbeat_details": {"cursor": 2},
            "last_heartbeat_at": "2026-11-14T10:00:00Z", "reason": "stale_attempt",
        }
        if outcome == "success":
            await ctx.heartbeat({"cursor": 2})
        else:
            from durable_workflow.errors import ActivityLeaseAuthorityLost
            with pytest.raises(ActivityLeaseAuthorityLost):
                await ctx.heartbeat({"cursor": 2})
    assert ctx.last_heartbeat_details() == {"cursor": 1}
    assert ctx.last_heartbeat_at() == AT


async def test_real_client_poll_delivers_snapshot_to_handler_without_changing_task_wire() -> None:
    wire = poll(details={"cursor": 7}, at=AT)
    seen: list[Any] = []
    requests: list[str] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path.endswith("/poll"):
            return httpx.Response(200, json=wire)
        assert request.url.path.endswith("/complete")
        return httpx.Response(200, json={"recorded": True})

    @activity.defn(name="checkpoint.inspect")
    async def inspect_checkpoint() -> None:
        ctx = activity.context()
        seen.append((ctx.last_heartbeat_details(), ctx.last_heartbeat_at(), ctx.info.retry_info.policy()))

    async with Client("http://checkpoint.invalid", namespace="default") as client:
        await client._http.aclose()
        client._http = httpx.AsyncClient(transport=httpx.MockTransport(respond), base_url="http://checkpoint.invalid")
        client._cluster_info = {"worker_protocol": {"server_capabilities": {}}}
        worker = Worker(client, worker_id="worker", task_queue="queue", workflows=[], activities=[inspect_checkpoint])
        try:
            task = await client.poll_activity_task(worker_id="worker", task_queue="queue")
            assert task == wire["task"]
            assert json.loads(json.dumps(task)) == wire["task"]
            original = copy.deepcopy(wire)
            assert await worker._run_activity_task(task) == "completed"
            assert wire == original
        finally:
            await worker.stop()
    assert seen == [({"cursor": 7}, AT, {"max_attempts": 3, "backoff_seconds": [1, 2]})]
    assert len(requests) == 2


def checkpoint_in_cold_callback() -> dict[str, Any]:
    ctx = activity.context()
    return {"details": ctx.last_heartbeat_details(), "at": ctx.last_heartbeat_at(),
            "version": ctx.heartbeat_resume_version(), "attempt": ctx.info.attempt_number}


async def test_supervised_callback_receives_the_same_immutable_snapshot() -> None:
    wire = poll(details={"cursor": 3}, at=AT)
    ctx = context(wire)
    callback = SupervisedCallback(CallbackInvocation(
        handler=checkpoint_in_cold_callback, args=(), info=ctx.info,
        task=capture_poll(wire, "default", "worker"), interceptors=(),
    ))

    async def no_heartbeat(_details: Any) -> None:
        raise AssertionError("checkpoint read emitted a heartbeat")

    try:
        await callback.start()
        result = await callback.result(no_heartbeat)
        assert result.failure is None
        assert result.value == {"details": {"cursor": 3}, "at": AT, "version": 1, "attempt": 2}
    finally:
        await callback.close()


def test_namespace_or_queue_change_cannot_reuse_a_received_checkpoint() -> None:
    wire = poll(details={}, at=AT)
    with pytest.raises(ValueError, match="invalid checkpoint"):
        capture_poll(wire, "default", "worker", "other-queue")
    task = capture_poll(wire, "default", "worker", "queue")
    with pytest.raises(ValueError, match="invalid checkpoint"):
        ActivityInfo("task", "checkpoint.inspect", "attempt", 2, "queue", "worker",
                     task_metadata=task, task_namespace="other-namespace")
