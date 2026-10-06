"""Source-only prepared local admission and supervised callback execution.

Native owns retry policy, execution deadlines and terminal history. Control
observes authority independently of application heartbeats. A lost receipt
abandons the claim and never authorizes another callback or publication.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from . import serializer
from ._activity_process import CallbackInvocation, SupervisedCallback
from .cancellation import CancellationContext
from .client import Client
from .workflow import LocalActivityExecutionAborted

_FIXED_DEADLINES = ("start_to_close_deadline_at", "schedule_to_close_deadline_at")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise LocalActivityExecutionAborted(message)


def _text(value: Any) -> str:
    _require(isinstance(value, str) and bool(value.strip()), "prepared receipt lacks a nonempty identity")
    return str(value)


def _timestamp(value: Any) -> datetime:
    _require(isinstance(value, str) and re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})", value,
    ) is not None, "prepared receipt requires an ISO timestamp with timezone")
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise LocalActivityExecutionAborted("prepared receipt timestamp is invalid") from error


def _same_deadline(actual: Any, expected: Any) -> bool:
    return actual is None if expected is None else _timestamp(actual) == _timestamp(expected)


def _cleanup_deadline(cleanup: Mapping[str, str]) -> datetime:
    deadline = _timestamp(cleanup["cleanup_deadline_at"])
    if "authority_deadline_at" in cleanup:
        authority = _timestamp(cleanup["authority_deadline_at"])
        _require(authority <= deadline, "local cleanup authority exceeds its original deadline")
        deadline = authority
    return deadline


class PreparedCancellationObserved(LocalActivityExecutionAborted):
    """Cancellation was fenced and its original callback physically joined."""


@dataclass
class PreparedAttempt:
    task_id: str
    run_id: str
    owner: str
    epoch: int
    nonce: str
    execution_id: str
    attempt_id: str
    attempt_number: int
    deadlines: dict[str, Any]
    heartbeat_timeout: int | None
    cleanup: Mapping[str, str] | None
    clock_origin: float
    authority_deadline: float

    @classmethod
    def admitted(
        cls, receipt: Mapping[str, Any], *, task_id: str, run_id: str, owner: str, epoch: int, nonce: str,
        heartbeat_timeout: int | None, cleanup: Mapping[str, str] | None, request_started: float,
    ) -> PreparedAttempt:
        _require(
            receipt.get("prepared") is True and type(receipt.get("duplicate")) is bool
            and "reason" in receipt and receipt["reason"] is None
            and receipt.get("workflow_task_id") == task_id and receipt.get("lease_owner") == owner
            and type(receipt.get("workflow_task_attempt")) is int and receipt["workflow_task_attempt"] == epoch
            and receipt.get("worker_attempt_id") == nonce
            and type(receipt.get("attempt_number")) is int and receipt["attempt_number"] > 0,
            "local admission changed its original workflow claim or nonce",
        )
        deadlines = {field: receipt.get(field) for field in (*_FIXED_DEADLINES, "heartbeat_deadline_at")}
        attempt = cls(task_id, run_id, owner, epoch, nonce, _text(receipt.get("activity_execution_id")),
                      _text(receipt.get("activity_attempt_id")), receipt["attempt_number"], deadlines,
                      heartbeat_timeout, cleanup, request_started - _timestamp(receipt.get("server_time")).timestamp(),
                      request_started)
        attempt.validate_cleanup(receipt)
        server_time = _timestamp(receipt.get("server_time"))
        _require(_timestamp(receipt.get("lease_expires_at")) > server_time, "local admission has an expired lease")
        cleanup_deadline = _cleanup_deadline(cleanup) if cleanup is not None else None
        for field, deadline in deadlines.items():
            _require(field in receipt and (deadline is None or _timestamp(deadline) > server_time),
                     "local admission omitted or exhausted an execution deadline")
            if deadline is not None and cleanup_deadline is not None:
                _require(_timestamp(deadline) <= cleanup_deadline,
                         "local admission extended its original cleanup budget")
        heartbeat = deadlines["heartbeat_deadline_at"]
        if heartbeat_timeout is not None:
            _require(heartbeat is not None
                     and _timestamp(heartbeat) <= server_time + timedelta(seconds=heartbeat_timeout),
                     "local admission changed its authored application heartbeat timeout")
        else:
            _require(_same_deadline(heartbeat, cleanup_deadline.isoformat() if cleanup_deadline is not None else None),
                     "local admission invented an application heartbeat deadline")
        if cleanup_deadline is not None:
            _require(cleanup_deadline > server_time, "local admission exhausted its original cleanup budget")
        attempt.accept_budget(receipt, request_started)
        attempt.remaining()
        return attempt

    def validate_cleanup(self, receipt: Mapping[str, Any]) -> None:
        actual = receipt.get("cancellation_cleanup")
        if self.cleanup is None:
            _require(actual is None, "local receipt invented cleanup authority")
            return
        fields = {"request_id", "root_request_id", "delivery_history_event_id", "cleanup_deadline_at"}
        if "scope_id" in self.cleanup:
            fields |= {"scope_id", "operation_scope_id", "preparation_history_event_id", "authority_deadline_at"}
        if not isinstance(actual, Mapping) or set(actual) != fields or set(self.cleanup) != fields:
            raise LocalActivityExecutionAborted("local receipt changed canonical cleanup authority")
        for field in fields:
            _require(_same_deadline(actual[field], self.cleanup[field]) if field.endswith("deadline_at")
                     else _text(actual[field]) == _text(self.cleanup[field]),
                     "local receipt changed canonical cleanup authority")
        if "scope_id" in self.cleanup:
            _require(self.cleanup["scope_id"] == self.cleanup["operation_scope_id"],
                     "local cleanup changed its original operation scope")
        _cleanup_deadline(self.cleanup)

    def validate_identity(self, receipt: Mapping[str, Any]) -> None:
        _require(
            receipt.get("workflow_task_id") == self.task_id
            and type(receipt.get("workflow_task_attempt")) is int and receipt["workflow_task_attempt"] == self.epoch
            and receipt.get("activity_execution_id") == self.execution_id
            and receipt.get("activity_attempt_id") == self.attempt_id
            and ("lease_owner" not in receipt or receipt["lease_owner"] == self.owner),
            "prepared receipt belongs to a different attempt or workflow claim",
        )

    def validate_control(self, receipt: Mapping[str, Any], *, heartbeat: bool = False) -> None:
        self.validate_identity(receipt)
        self.validate_cleanup(receipt)
        active = receipt.get("active")
        _require(
            type(active) is bool and type(receipt.get("renewed")) is bool
            and receipt.get("lease_owner") == self.owner
            and type(receipt.get("stop_required")) is bool and receipt["stop_required"] is not active
            and "reason" in receipt
            and (receipt["reason"] is None and receipt["renewed"] is (not heartbeat) if active
                 else isinstance(receipt["reason"], str) and bool(receipt["reason"]) and receipt["renewed"] is False),
            "local control lacks live authority or an explicit stop",
        )
        recorded = receipt.get("heartbeat_recorded")
        _require(
            recorded is active and (isinstance(receipt.get("heartbeat_history_event_id"), str)
                                   and bool(receipt["heartbeat_history_event_id"].strip()) if active
                                   else receipt.get("heartbeat_history_event_id") is None) if heartbeat
            else recorded is False and receipt.get("heartbeat_history_event_id") is None,
            "supervisor control and application heartbeat receipts must remain distinct",
        )
        server_time = _timestamp(receipt.get("server_time"))
        for field in _FIXED_DEADLINES:
            _require(field in receipt and _same_deadline(receipt[field], self.deadlines[field]),
                     "local control changed an original execution deadline")
        _require("heartbeat_deadline_at" in receipt, "local control omitted the application heartbeat deadline")
        updated = receipt["heartbeat_deadline_at"]
        if heartbeat and active and self.heartbeat_timeout is not None:
            _require(_timestamp(self.deadlines["heartbeat_deadline_at"]) > server_time
                     and _timestamp(updated) >= _timestamp(self.deadlines["heartbeat_deadline_at"])
                     and _timestamp(updated) <= server_time + timedelta(seconds=self.heartbeat_timeout)
                     and (self.cleanup is None
                          or _timestamp(updated) <= _cleanup_deadline(self.cleanup)),
                     "application heartbeat extended a fixed budget or revived an expired deadline")
        else:
            _require(_same_deadline(updated, self.deadlines["heartbeat_deadline_at"]),
                     "local control changed an unacknowledged application heartbeat deadline")
        if active:
            for field in ("lease_expires_at", "workflow_lease_expires_at"):
                _require(_timestamp(receipt.get(field)) > server_time, "local control returned an expired lease")
            for deadline in (receipt[field] for field in (*_FIXED_DEADLINES, "heartbeat_deadline_at")):
                _require(deadline is None or _timestamp(deadline) > server_time,
                         "local control returned active after an execution deadline")
            if self.cleanup is not None:
                _require(_cleanup_deadline(self.cleanup) > server_time,
                         "local control returned active after the original cleanup deadline")
        if heartbeat and active:
            self.deadlines["heartbeat_deadline_at"] = updated

    def validate_outcome(self, receipt: Mapping[str, Any]) -> None:
        self.validate_identity(receipt)
        retry = bool(receipt.get("event_type") == "ActivityRetryScheduled")
        created = receipt.get("created_task_ids")
        _require(
            receipt.get("recorded") is True and type(receipt.get("duplicate")) is bool
            and "reason" in receipt and receipt["reason"] is None
            and receipt.get("workflow_run_id") == self.run_id and receipt.get("worker_attempt_id") == self.nonce
            and receipt.get("event_type") in {"ActivityCompleted", "ActivityFailed", "ActivityTimedOut",
                                             "ActivityRetryScheduled"}
            and receipt.get("claim_released") is retry and isinstance(created, list) and len(created) == int(retry),
            "local outcome lacks a canonical receipt for its original attempt",
        )
        _text(receipt.get("event_id"))
        _timestamp(receipt.get("recorded_at"))
        for identity in receipt["created_task_ids"]:
            _text(identity)

    def accept_budget(self, receipt: Mapping[str, Any], started: float) -> None:
        server_time = _timestamp(receipt.get("server_time")).timestamp()
        values = [receipt.get(field) for field in (
            "lease_expires_at", "workflow_lease_expires_at", *_FIXED_DEADLINES, "heartbeat_deadline_at",
        )]
        if self.cleanup is not None:
            values.append(self.cleanup["cleanup_deadline_at"])
            if "authority_deadline_at" in self.cleanup:
                values.append(self.cleanup["authority_deadline_at"])
        self.authority_deadline = min(
            min(self.clock_origin + _timestamp(value).timestamp(),
                started + _timestamp(value).timestamp() - server_time)
            for value in values if value is not None
        )

    def remaining(self) -> float:
        remaining = self.authority_deadline - time.monotonic()
        _require(remaining > 0, "prepared local original authority budget expired")
        return min(5.0, remaining)


class PreparedLocalRunner:
    def __init__(
        self, client: Client, attempt: PreparedAttempt, *, observe: Callable[[Mapping[str, Any]], Any],
        shutdown: asyncio.Event,
    ) -> None:
        self.client = client
        self.attempt = attempt
        self.observe = observe
        self.shutdown = shutdown
        self.stop_request: CancellationContext | None = None
        self.stop_acknowledged = False
        self.lock = asyncio.Lock()
        self.callback: SupervisedCallback | None = None

    async def operation(self, operation: str, body: Mapping[str, Any]) -> dict[str, Any]:
        return await self.client.prepared_local_activity_operation(
            task_id=self.attempt.task_id, lease_owner=self.attempt.owner, workflow_task_attempt=self.attempt.epoch,
            activity_attempt_id=self.attempt.attempt_id, operation=operation, body=body,
            timeout_seconds=self.attempt.remaining(),
        )

    async def control(self, details: dict[str, Any] | None = None, *, heartbeat: bool = False) -> None:
        async with self.lock:
            _require(not self.shutdown.is_set(), "worker shutdown abandoned its prepared local claim")
            started = time.monotonic()
            receipt = await self.operation("heartbeat" if heartbeat else "control",
                                           {"progress": {"details": details} if details else {}}
                                           if heartbeat else {"renew_lease": True})
            self.attempt.validate_control(receipt, heartbeat=heartbeat)
            if receipt["active"]:
                self.attempt.accept_budget(receipt, started)
                self.attempt.remaining()
                return
            if receipt["reason"] not in {"cancellation_requested", "cancellation_deadline_expired"}:
                raise LocalActivityExecutionAborted("prepared local callback lost its original authority")
            try:
                context = CancellationContext.from_dict(receipt.get("cancellation_request", {}))
            except (ValueError, TypeError, AttributeError) as error:
                raise LocalActivityExecutionAborted("prepared stop lacks canonical cancellation context") from error
            _require(context.lineage[-1].workflow_run_id == self.attempt.run_id and receipt.get("fenced") is True,
                     "prepared cancellation did not fence this original callback")
            _text(receipt.get("cancellation_history_event_id"))
            self.observe({**context.to_dict(), "history_refresh_page_token": receipt.get("history_refresh_page_token")})
            self.stop_request = context
            raise PreparedCancellationObserved("prepared callback observed cooperative cancellation")

    async def execute(self, invocation: CallbackInvocation, processes: set[SupervisedCallback]) -> dict[str, Any]:
        async def observe() -> None:
            while True:
                await asyncio.sleep(min(1.0, self.attempt.remaining()))
                await self.control()

        async def heartbeat(details: dict[str, Any] | None) -> None:
            await self.control(details, heartbeat=True)

        try:
            await self.control()
        except PreparedCancellationObserved:
            # Admission was fenced before any process existed.
            await self.acknowledge_stop()
            raise
        callback = SupervisedCallback(invocation)
        self.callback = callback
        processes.add(callback)
        background: list[asyncio.Task[Any]] = []
        try:
            await asyncio.wait_for(callback.start(), timeout=self.attempt.remaining())
            result = asyncio.create_task(callback.result(heartbeat))
            observer = asyncio.create_task(observe())
            shutdown = asyncio.create_task(self.shutdown.wait())
            background = [result, observer, shutdown]
            done, _ = await asyncio.wait(background, return_when=asyncio.FIRST_COMPLETED)
            if shutdown in done:
                raise LocalActivityExecutionAborted("worker shutdown abandoned its prepared local claim")
            if observer in done:
                await observer
                raise LocalActivityExecutionAborted("prepared local authority observer stopped")
            outcome = await result
            await self.control()
            report: dict[str, Any]
            if outcome.failure is None:
                report = {"outcome": "completed", "result": serializer.envelope(outcome.value),
                          "payload_codec": serializer.AVRO_CODEC}
            else:
                failure = outcome.failure
                report = {"outcome": "failed", "message": failure.message, "exception_type": failure.failure_type,
                          "non_retryable": failure.non_retryable or failure.cancelled}
            # Cancel the observer before publication. No control may race a
            # committed result and mistake its closed attempt for lost authority.
            observer.cancel()
            await asyncio.gather(observer, return_exceptions=True)
            async with self.lock:
                receipt = await self.operation("outcome", {"report": report})
                self.attempt.validate_outcome(receipt)
                return receipt
        finally:
            async def finish() -> None:
                for pending in background:
                    if not pending.done():
                        pending.cancel()
                await asyncio.gather(*background, return_exceptions=True)
                if not callback.stopped:
                    await callback.stop()
                _require(callback.stopped, "prepared callback stop remains unconfirmed")
                processes.discard(callback)
                await self.acknowledge_stop()

            joined = asyncio.create_task(finish())
            while not joined.done():
                # The original cancellation still propagates after finally.
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.shield(joined)
            joined.result()

    async def acknowledge_stop(self) -> None:
        if self.stop_request is None or self.stop_acknowledged:
            return
        # A physical join, or the absence of any spawned callback, is proved
        # before this diagnostic receipt. It grants no execution authority.
        receipt = await self.client.prepared_local_activity_operation(
            task_id=self.attempt.task_id, lease_owner=self.attempt.owner,
            workflow_task_attempt=self.attempt.epoch, activity_attempt_id=self.attempt.attempt_id,
            operation="acknowledge-cancellation", body={"request_id": self.stop_request.request_id},
        )
        _require(receipt.get("acknowledged") is True and type(receipt.get("duplicate")) is bool
                 and "reason" in receipt and receipt["reason"] is None,
                 "joined prepared callback stop was not acknowledged")
        _text(receipt.get("history_event_id"))
        self.stop_acknowledged = True
