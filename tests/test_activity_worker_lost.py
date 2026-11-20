"""ActivityWorkerLost naming and the workflow-side lease-loss accessor."""
from __future__ import annotations

from typing import Any

import pytest

from durable_workflow import errors
from durable_workflow.errors import ActivityFailed, ActivityWorkerLost
from durable_workflow.workflow import _activity_failed_from_payload

LEASE_EXPIRED = "Workflow\\V2\\Exceptions\\ActivityLeaseExpiredException"


def test_deprecated_name_is_the_same_class() -> None:
    assert errors.ActivityLeaseAuthorityLost is ActivityWorkerLost
    with pytest.raises(errors.ActivityLeaseAuthorityLost):
        raise ActivityWorkerLost("task-1", "attempt_closed")
    assert errors.WORKER_LOST_FAILURE_TYPE == "worker_lost"
    assert errors.ACTIVITY_LEASE_EXPIRED_EXCEPTION_CLASS == LEASE_EXPIRED


@pytest.mark.parametrize(("fields", "expected"), [
    ({"exception_type": "worker_lost", "exception_class": "ActivityWorkerLost"}, "worker_lost"),
    ({"exception_type": "worker_lost", "exception_class": "WorkerLost\\Php"}, "worker_lost"),
    ({"exception_class": LEASE_EXPIRED, "failure_reason": "lease_expired"}, "lease_expired"),
    ({"exception_class": LEASE_EXPIRED, "failure_reason": "worker_deregistered"}, "worker_deregistered"),
    ({"exception_class": LEASE_EXPIRED}, "lease_expired"),
    ({"exception_type": "ValueError", "exception_class": "builtins.ValueError"}, None),
    ({"exception_type": "ActivityWorkerLost"}, None),
    ({"failure_reason": "lease_expired"}, None),
    ({}, None),
])
def test_lease_loss_reason(fields: dict[str, Any], expected: str | None) -> None:
    assert ActivityFailed("activity failed", **fields).lease_loss_reason() == expected


def test_lease_loss_reason_from_recorded_server_lease_expiry() -> None:
    # The ActivityFailed payload native Server records when it settles a lost
    # lease (activity_deadlines.rs): lease metadata merged at the top level.
    failure = _activity_failed_from_payload({
        "activity_type": "charge", "sequence": 1, "message": "Activity charge lost its original task lease.",
        "exception_class": LEASE_EXPIRED, "failure_category": "internal", "timeout_kind": "lease",
        "failure_reason": "worker_deregistered", "retry_reason": "worker_deregistered",
        "lease_owner": "worker-1", "lease_expires_at": "2026-11-20T00:00:00.000000Z",
    })
    assert failure.failure_reason == "worker_deregistered"
    assert failure.lease_loss_reason() == "worker_deregistered"


def test_lease_loss_reason_from_recorded_worker_lost_failure() -> None:
    failure = _activity_failed_from_payload({
        "activity_type": "charge", "sequence": 1,
        "exception": {"type": "worker_lost", "class": "ActivityWorkerLost",
                      "message": "activity worker released the claim: lease could not be renewed "
                                 "(activity_lease_renewal_unavailable)"},
    })
    assert failure.exception_type == "worker_lost"
    assert failure.lease_loss_reason() == "worker_lost"
