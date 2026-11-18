"""Immutable heartbeat-resume metadata captured from one negotiated poll."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ._typed_values import decode_payload_value, validate_annotation
from .errors import ActivityHeartbeatResumeUnavailable

_IDENTITY = ("task_id", "activity_execution_id", "activity_attempt_id", "workflow_id", "run_id", "namespace")
_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
    r"(?:\.[0-9]{1,9})?(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])"
)


def _invalid() -> ValueError:
    return ValueError("activity_heartbeat_resume v1 claim has an invalid checkpoint")


@dataclass(frozen=True, init=False)
class ActivityHeartbeatCheckpoint:
    """The acknowledged snapshot received with this claim, never a pending write.

    ``contract_version`` describes the negotiated wire contract. Server v1
    supplies no checkpoint write sequence. Acknowledgement does not commit an
    external business effect atomically.
    """

    acknowledged_at: str
    contract_version: int = field(default=1, init=False)
    _details_json: str = field(repr=False)

    def __init__(self, details: Any, acknowledged_at: str) -> None:
        if details is None or not isinstance(acknowledged_at, str) or not _TIMESTAMP.fullmatch(acknowledged_at):
            raise _invalid()
        try:
            # Python 3.10's parser accepts fewer fractional forms. Validate the
            # calendar/offset separately and retain the original precision.
            calendar = re.sub(r"\.\d+(?=Z|[+-])", "", acknowledged_at)
            datetime.fromisoformat(calendar.replace("Z", "+00:00"))
            encoded = json.dumps(details, allow_nan=False)
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise _invalid() from None
        object.__setattr__(self, "acknowledged_at", acknowledged_at)
        object.__setattr__(self, "_details_json", encoded)

    def details(self, *, result_type: Any = None) -> Any:
        """Return an independent copy, optionally decoded using SDK typed values."""
        value = json.loads(self._details_json)
        if result_type is None:
            return value
        validate_annotation(result_type, "checkpoint")
        return decode_payload_value(value, result_type, "checkpoint")


@dataclass(frozen=True)
class _HeartbeatResume:
    available: bool = False
    checkpoint: ActivityHeartbeatCheckpoint | None = None
    identity: tuple[Any, ...] = ()

    def get(self) -> ActivityHeartbeatCheckpoint | None:
        if not self.available:
            raise ActivityHeartbeatResumeUnavailable()
        return self.checkpoint


class _ActivityTask(dict[str, Any]):
    """Keep poll metadata outside the task's unchanged public wire dictionary."""

    def __init__(self, task: Mapping[str, Any], resume: _HeartbeatResume) -> None:
        super().__init__(task)
        self.heartbeat_resume = resume


def _identity(task: Mapping[str, Any]) -> tuple[Any, ...]:
    values = tuple(task.get(name) for name in _IDENTITY)
    attempt = task.get("attempt_number")
    if (any(not isinstance(value, str) or not value.strip() for value in values)
            or type(attempt) is not int or attempt < 1):
        raise _invalid()
    return (*values, attempt, task.get("lease_owner"), task.get("activity_type"), task.get("task_queue"))


def capture_poll(data: Mapping[str, Any], namespace: str, worker_id: str, task_queue: str | None = None) -> Any:
    task = data.get("task")
    capabilities = data.get("server_capabilities")
    capability = capabilities.get("activity_heartbeat_resume") if isinstance(capabilities, Mapping) else None
    if (not isinstance(task, dict) or not isinstance(capability, Mapping)
            or capability.get("supported") is not True or type(capability.get("version")) is not int
            or capability["version"] != 1):
        return task
    if "last_heartbeat_details" not in task and "last_heartbeat_at" not in task:
        return task
    identity = _identity(task)
    if task["namespace"] != namespace or task.get("lease_owner") != worker_id:
        raise _invalid()
    if task_queue is not None and "task_queue" in task and task["task_queue"] != task_queue:
        raise _invalid()
    if "last_heartbeat_details" not in task or "last_heartbeat_at" not in task:
        raise _invalid()
    details, at = task["last_heartbeat_details"], task["last_heartbeat_at"]
    if details is None and at is None:
        checkpoint = None
    elif details is not None and task["attempt_number"] > 1:
        checkpoint = ActivityHeartbeatCheckpoint(details, at)
    else:
        raise _invalid()
    return _ActivityTask(task, _HeartbeatResume(True, checkpoint, identity))


def claim_resume(
    task: Mapping[str, Any], namespace: str | None, task_id: str, attempt_id: str, attempt: int, worker_id: str,
    activity_type: str, task_queue: str,
) -> _HeartbeatResume:
    if not isinstance(task, _ActivityTask):
        return _HeartbeatResume()
    resume = task.heartbeat_resume
    if (resume.identity != _identity(task) or task.get("namespace") != namespace
            or task.get("task_id") != task_id or task.get("activity_attempt_id") != attempt_id
            or type(attempt) is not int or task.get("attempt_number") != attempt
            or task.get("lease_owner") != worker_id or task.get("activity_type") != activity_type
            or "task_queue" in task and task["task_queue"] != task_queue):
        raise _invalid()
    return resume
