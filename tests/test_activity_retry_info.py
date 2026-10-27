from __future__ import annotations

import pickle
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from durable_workflow import ActivityInfo, ActivityRetryInfo


@pytest.mark.parametrize(("attempt", "task", "maximum", "unlimited", "last"), [
    (1, {"retry_policy": {"max_attempts": 3}}, 3, False, False),
    (3, {"retry_policy": {"max_attempts": 3}}, 3, False, True),
    (4, {"retry_policy": {"max_attempts": 3}}, 3, False, True),
    (1, {"retry_policy": {"max_attempts": 1}}, 1, False, True),
    (1, {}, None, False, None),
    (1, {"retry_policy": None}, None, False, None),
    (99, {"retry_policy": {"max_attempts": None}}, None, True, False),
    (1, {"retry_policy": {"future": [True, 2.0]}}, None, False, None),
    (1, {"retry_policy": {"max_attempts": "3"}}, None, False, None),
    (1, {"retry_policy": {"max_attempts": True}}, None, False, None),
    (1, {"retry_policy": {"max_attempts": 3.0}}, None, False, None),
    (1, {"retry_policy": {"max_attempts": 0}}, None, False, None),
    (1, {"retry_policy": "legacy"}, None, False, None),
    (0, {"retry_policy": {"max_attempts": 1}}, 1, False, None),
])
def test_received_budget_preserves_unknown_and_unlimited(
    attempt: int, task: dict[str, Any], maximum: int | None, unlimited: bool, last: bool | None,
) -> None:
    retry = ActivityRetryInfo.from_task(attempt, task)
    assert retry.attempt_number == attempt
    assert retry.policy_provided is ("retry_policy" in task)
    assert retry.policy() == task.get("retry_policy")
    assert retry.max_attempts() == maximum
    assert retry.is_unlimited() is unlimited
    assert retry.is_last_configured_attempt() is last


def test_snapshot_copies_and_original_identity_survive_spawn_serialization() -> None:
    task = {"workflow_id": "0", "run_id": "017", "retry_policy": {"max_attempts": 3, "future": [True, 2.0, None]}}
    info = ActivityInfo("task", "activity", "attempt", 3, "queue", "owner", task_metadata=task)
    task["retry_policy"]["max_attempts"] = 10
    task["workflow_id"] = "replacement"
    copy = info.retry_info.policy()
    copy["future"][0] = False
    assert info.retry_info.policy() == {"max_attempts": 3, "future": [True, 2.0, None]}
    assert info.workflow_id == "0" and info.run_id == "017"
    assert info.retry_info.is_last_configured_attempt() is True
    restored = pickle.loads(pickle.dumps(info))
    assert restored == info
    assert restored.retry_info.policy() == info.retry_info.policy()
    with pytest.raises(FrozenInstanceError):
        info.workflow_id = "replacement"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        info.retry_info.attempt_number = 100  # type: ignore[misc]


def test_legacy_constructor_has_no_invented_received_policy_or_identity() -> None:
    info = ActivityInfo("task", "activity", "attempt", 2, "queue", "owner")
    assert info.retry_info.attempt_number == 2
    assert info.retry_info.policy_provided is False
    assert info.retry_info.is_last_configured_attempt() is None
    assert info.workflow_id is None and info.run_id is None


@pytest.mark.parametrize("task", [
    {}, {"workflow_id": None, "run_id": None}, {"workflow_id": "", "run_id": 42},
    {"workflow_instance_id": "internal", "workflow_run_id": "internal-run"},
])
def test_public_claim_identity_stays_unknown_when_missing_or_invalid(task: dict[str, Any]) -> None:
    info = ActivityInfo("task", "activity", "attempt", 1, "queue", "owner", task_metadata=task)
    assert info.workflow_id is None and info.run_id is None
