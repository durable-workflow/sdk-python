from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow._activity_lease import ActivityLeaseAuthority
from durable_workflow.errors import ActivityLeaseAuthorityLost, ServerError

ORIGINAL = datetime(2026, 10, 21, tzinfo=timezone.utc)


def timestamp(seconds: float) -> str:
    return (ORIGINAL + timedelta(seconds=seconds)).isoformat(timespec="microseconds").replace("+00:00", "Z")


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    value = [100.0]
    monkeypatch.setattr("durable_workflow._activity_lease.time.monotonic", lambda: value[0])
    return value


def task(**changes: Any) -> dict[str, Any]:
    return {
        "task_id": "task-1", "workflow_id": "workflow-1", "run_id": "run-1",
        "activity_execution_id": "execution-1", "activity_attempt_id": "attempt-1", "attempt_number": 1,
        "lease_owner": "worker-1", "task_queue": "queue-1", "lease_expires_at": timestamp(5),
        "deadlines": {"start_to_close": timestamp(4)}, "retry_policy": {}, **changes,
    }


def delivery(value: dict[str, Any] | None = None, **changes: Any) -> ActivityLeaseAuthority:
    return ActivityLeaseAuthority.delivery(
        object(), "17", value if value is not None else task(), owner="worker-1",
        task_queue="queue-1", server_time=timestamp(0), request_started=100, **changes,
    )


def reply(**changes: Any) -> dict[str, Any]:
    return {
        "task_id": "task-1", "workflow_task_id": "task-1", "workflow_instance_id": "workflow-1",
        "workflow_run_id": "run-1", "activity_execution_id": "execution-1", "activity_attempt_id": "attempt-1",
        "attempt_number": 1, "lease_owner": "worker-1", "lease_renewed": True, "can_continue": True,
        "cancel_requested": False, "heartbeat_recorded": False, "reason": None,
        "server_time": timestamp(1), "lease_expires_at": timestamp(11), **changes,
    }


@pytest.mark.parametrize(("field", "value"), [
    ("task_id", ""), ("activity_attempt_id", None), ("attempt_number", True), ("attempt_number", 0),
    ("lease_owner", "other"), ("task_queue", "other"), ("namespace", "017"),
    ("lease_expires_at", "2026-02-30T00:00:10.000000Z"),
    ("lease_expires_at", "2026-10-21T00:00:10Z"), ("deadlines", []),
    ("deadlines", {"heartbeat": timestamp(3)}),
])
def test_invalid_original_delivery_never_authorizes_execution(clock: list[float], field: str, value: Any) -> None:
    with pytest.raises(ActivityLeaseAuthorityLost):
        delivery(task(**{field: value}))


@pytest.mark.parametrize(("field", "value"), [
    ("workflow_task_id", "other"), ("workflow_instance_id", "other"), ("workflow_run_id", "other"),
    ("activity_execution_id", "other"), ("activity_attempt_id", "other"), ("attempt_number", True),
    ("lease_owner", "other"), ("lease_renewed", "true"), ("can_continue", 1),
    ("cancel_requested", True), ("heartbeat_recorded", True), ("reason", "lease_expired"),
    ("server_time", "2026-02-30T00:00:00.000000Z"), ("server_time", None),
    ("lease_expires_at", timestamp(0)), ("lease_expires_at", timestamp(90000)),
    ("lease_expires_at", timestamp(4)),
])
def test_wrong_or_unbounded_renewal_cannot_extend_the_grant(
    clock: list[float], field: str, value: Any,
) -> None:
    authority = delivery()
    original = authority.lease_until
    clock[0] = 101.1
    with pytest.raises(ActivityLeaseAuthorityLost):
        authority.acknowledge(reply(**{field: value}), 101)
    assert authority.lease_until == original


def test_confirmed_renewal_preserves_every_original_business_deadline(clock: list[float]) -> None:
    authority = delivery(task(deadlines={"start_to_close": timestamp(4), "run": timestamp(3),
                                        "execution": timestamp(3.5), "schedule_to_close": timestamp(3.75)}))
    clock[0] = 101.1
    authority.acknowledge(reply(), 101)
    assert authority.lease_until == 111
    assert authority.business_until == 103
    assert authority.remaining() == pytest.approx(1.9)


def test_acknowledgement_arriving_after_expiry_cannot_revive_the_claim(clock: list[float]) -> None:
    authority = delivery(task(deadlines={}))
    clock[0] = 105.1
    with pytest.raises(ActivityLeaseAuthorityLost, match="budget_expired"):
        authority.acknowledge(reply(server_time=timestamp(4), lease_expires_at=timestamp(20)), 104)
    assert authority.lease_until == 105


def test_replaying_a_successful_reply_at_a_later_request_cannot_reset_latency(clock: list[float]) -> None:
    authority = delivery(task(deadlines={}))
    clock[0] = 101.1
    authority.acknowledge(reply(), 101)
    authority.acknowledge(reply(), 101)
    assert authority.lease_until == 111
    clock[0] = 101.2
    with pytest.raises(ActivityLeaseAuthorityLost, match="invalid_activity_renewal"):
        authority.acknowledge(reply(), 101.15)
    assert authority.lease_until == 111


def test_only_confirmed_progress_with_the_original_interval_moves_heartbeat_budget(clock: list[float]) -> None:
    authority = delivery(task(deadlines={"heartbeat": timestamp(2), "run": timestamp(3)},
                              retry_policy={"heartbeat_timeout": 2}))
    clock[0] = 101.1
    authority.acknowledge(reply(last_heartbeat_at=timestamp(0.8), heartbeat_deadline_at=timestamp(2.8)), 101)
    assert authority.heartbeat_until == pytest.approx(102.8)
    assert authority.business_until == 103


@pytest.mark.parametrize(("progress", "deadline"), [(1.1, 3.1), (0.8, 3.8), (0, 2), (None, None)])
def test_runtime_renewal_cannot_fabricate_application_progress(
    clock: list[float], progress: float | None, deadline: float | None,
) -> None:
    authority = delivery(task(deadlines={"heartbeat": timestamp(2)}, retry_policy={"heartbeat_timeout": 2}))
    clock[0] = 101.1
    authority.acknowledge(reply(last_heartbeat_at=timestamp(progress) if progress is not None else None,
                                heartbeat_deadline_at=timestamp(deadline) if deadline is not None else None), 101)
    assert authority.heartbeat_until == 102


async def test_temporary_renewal_failure_grants_nothing_but_does_not_revoke_a_live_original() -> None:
    import time

    now = datetime.now(timezone.utc)
    expires = (now + timedelta(seconds=10)).isoformat(timespec="microseconds").replace("+00:00", "Z")
    value = task(lease_expires_at=expires, deadlines={})
    client = AsyncMock()
    client.renew_activity_task_lease.side_effect = ServerError(503, {"reason": "backend_unavailable"})
    authority = ActivityLeaseAuthority.delivery(
        client, "17", value, owner="worker-1", task_queue="queue-1",
        server_time=now.isoformat(timespec="microseconds").replace("+00:00", "Z"), request_started=time.monotonic(),
    )
    original = authority.lease_until
    assert await authority.renew() is False
    assert authority.lease_until == original
    assert authority.remaining() > 0
