"""Runtime lease maintenance for one original activity delivery."""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, TypeVar
from urllib.parse import quote

import httpx

from .errors import WORKER_LOST_FAILURE_TYPE, ActivityWorkerLost, ServerError

_T = TypeVar("_T")
_UTC6 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\Z")
log = logging.getLogger("durable_workflow.worker")

#: Reason of the :class:`ActivityWorkerLost` raised when renewal kept
#: failing until only the release reserve of the lease was left. The worker
#: then stops the execution and, once it has confirmed the stop inside the
#: lease, releases the claim through the Server as ``worker_lost``.
ACTIVITY_LEASE_RENEWAL_EXHAUSTED = "activity_lease_renewal_unavailable"
# Part of the lease kept back to stop the execution and release the claim: at
# most this many seconds, and at most a fifth of the granted lease. The first
# half is for confirming the stop, the second for the release request.
_REPORT_RESERVE_SECONDS = 10.0
_REPORT_RESERVE_FRACTION = 0.2
# Renewal retry backoff after a transient failure, the same in every SDK:
# delay = uniform(0, min(MAX, BASE * 2**n)) for the n-th retry (full jitter).
_RETRY_BASE_SECONDS = 0.25
_RETRY_MAX_SECONDS = 4.0
# How often a stop confirmation re-checks threads and callback processes.
_STOP_POLL_SECONDS = 0.02
# Upper bound of one renewal exchange (the client accepts at most five).
_RENEWAL_TIMEOUT_SECONDS = 5.0


def renewal_retry_delay(failures: int) -> float:
    """Full-jitter backoff before the retry after ``failures`` consecutive failures."""
    exponent = min(max(failures - 1, 0), 16)
    return random.uniform(0.0, min(_RETRY_MAX_SECONDS, _RETRY_BASE_SECONDS * 2 ** exponent))


async def release_worker_lost(client: Any, *, task_id: str, activity_attempt_id: str, lease_owner: str,
                              cause: str, timeout: float) -> bool:
    """Release a claim the worker cannot maintain, by the cross-SDK contract.

    Call only after the execution is confirmed stopped (or never started):
    the Server may hand the attempt to another worker as soon as it records
    this retryable ``worker_lost`` failure.
    """
    try:
        await asyncio.wait_for(client.fail_activity_task(
            task_id=task_id, activity_attempt_id=activity_attempt_id, lease_owner=lease_owner,
            message=f"activity worker released the claim: lease could not be renewed ({cause})",
            failure_type=WORKER_LOST_FAILURE_TYPE, failure_class=ActivityWorkerLost.__name__,
            non_retryable=False, details={"reason": WORKER_LOST_FAILURE_TYPE, "cause": cause},
        ), timeout=timeout)
        return True
    except Exception as error:
        if isinstance(error, ServerError) and error.reason() == "run_closed":
            log.info("worker_lost release for activity task %s not recorded: its workflow run closed", task_id)
        else:
            log.warning("activity claim %s could not be released (%r); the Server settles it when the lease "
                        "expires", task_id, error)
        return False


def _execution_stopped(execution: Any) -> bool:
    done = getattr(execution, "done", None)
    if callable(done):
        return bool(done())
    return getattr(execution, "stopped", False) is True


def _transient(error: BaseException) -> bool:
    """A failed exchange that grants nothing but does not revoke the claim."""
    return (isinstance(error, (httpx.TransportError, asyncio.TimeoutError))
            or isinstance(error, ServerError) and (error.status >= 500 or error.status == 429))


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or _UTC6.fullmatch(value) is None:
        raise ValueError("activity timing must be UTC with six fractional digits")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def activity_lease_renewal_supported(client: Any) -> bool:
    """Use the same cached discovery that validates actual renewal requests."""
    info = getattr(client, "_cluster_info", None)
    protocol = info.get("worker_protocol") if isinstance(info, Mapping) else None
    capabilities = protocol.get("server_capabilities") if isinstance(protocol, Mapping) else None
    return isinstance(capabilities, Mapping) and capabilities.get("activity_lease_renewal") is True


def _heartbeat_terms(task: Mapping[str, Any], deadlines: Mapping[str, Any],
                     reference: datetime) -> tuple[datetime | None, int | None]:
    heartbeat = _timestamp(deadlines["heartbeat"]) if deadlines.get("heartbeat") is not None else None
    policy = task.get("retry_policy")
    interval = policy.get("heartbeat_timeout") if isinstance(policy, Mapping) else None
    if heartbeat is not None and (type(interval) is not int or interval <= 0):
        raise ValueError("missing recorded activity heartbeat interval")
    interval = interval if type(interval) is int and interval > 0 else None
    if heartbeat is not None and interval is not None and heartbeat - reference > timedelta(seconds=interval):
        raise ValueError("activity heartbeat deadline predates its claimed progress")
    return heartbeat, interval


@dataclass
class ActivityLeaseAuthority:
    client: Any
    namespace: str
    identity: dict[str, Any]
    task_identity: dict[str, Any]
    captured_at: float
    lease_until: float
    business_until: float | None
    heartbeat_until: float | None
    heartbeat_timeout: int | None
    server_time: datetime
    lease_expiry: datetime
    heartbeat_time: datetime | None
    heartbeat_deadline: datetime | None
    last_renewal: tuple[datetime, datetime, float] | None = None
    lost_reason: str | None = field(default=None, init=False)
    # Length of the latest confirmed grant, and consecutive transient renewal
    # failures since it. Neither changes recorded history.
    lease_seconds: float = field(default=0.0, init=False)
    renewal_failures: int = field(default=0, init=False)
    # Physical executions (thread futures, callback processes) started for
    # this claim. A release waits until every one has actually stopped.
    executions: list[Any] = field(default_factory=list, init=False)
    execution_started: bool = field(default=False, init=False)

    @classmethod
    async def confirm_delivery(cls, client: Any, namespace: str, task: Mapping[str, Any], *, owner: str,
                               task_queue: str, server_time: Any,
                               poll_started: float | None = None) -> ActivityLeaseAuthority:
        # A long poll may wait before a task or its lease exists. Establish the
        # first execution budget with a separate, original-claim confirmation;
        # its entire round trip is charged and it cannot revive an expired lease.
        provisional = cls.delivery(client, namespace, task, owner=owner, task_queue=task_queue,
                                   server_time=server_time, request_started=time.monotonic())
        started = time.monotonic()
        try:
            budget = provisional.renewal_budget()
            if budget <= 0:
                raise asyncio.TimeoutError("no time left to confirm the claim outside the report reserve")
            reply = await client.renew_activity_task_lease(
                task_id=provisional.task_id, activity_attempt_id=provisional.identity["activity_attempt_id"],
                lease_owner=owner, timeout=budget,
            )
            provisional.acknowledge(reply, started)
        except ActivityWorkerLost:
            raise
        except Exception as error:
            if _transient(error):
                # A slow or flaky link is not a lost claim (sdk-php#445). Keep
                # the claim under the poll's grant and let maintenance retry
                # the confirmation with backoff.
                return await cls._unconfirmed_delivery(
                    client, namespace, task, owner=owner, task_queue=task_queue, server_time=server_time,
                    poll_started=started if poll_started is None else poll_started, error=error,
                )
            reason = error.reason() if isinstance(error, ServerError) else None
            raise ActivityWorkerLost(
                provisional.task_id, reason or "activity_initial_lease_unavailable",
            ) from error
        observed = dict(task)
        observed["lease_expires_at"] = reply["lease_expires_at"]
        if provisional.heartbeat_deadline is not None:
            observed["deadlines"] = {**dict(task.get("deadlines", {})),
                                     "heartbeat": provisional.heartbeat_deadline.isoformat(
                                         timespec="microseconds").replace("+00:00", "Z")}
        return cls.delivery(client, namespace, observed, owner=owner, task_queue=task_queue,
                            server_time=reply["server_time"], request_started=started)

    @classmethod
    async def _unconfirmed_delivery(cls, client: Any, namespace: str, task: Mapping[str, Any], *, owner: str,
                                    task_queue: str, server_time: Any, poll_started: float,
                                    error: BaseException) -> ActivityLeaseAuthority:
        # The Server stamps the lease after it receives the poll, so charging
        # the grant from the poll's start is the conservative bound.
        try:
            grant = cls.delivery(client, namespace, task, owner=owner, task_queue=task_queue,
                                 server_time=server_time, request_started=poll_started)
        except ActivityWorkerLost:
            grant = None
        if grant is None or grant.lease_until - time.monotonic() <= grant.report_reserve():
            # Nothing has run, so releasing cannot cause a second concurrent
            # execution; a stale release is refused by the Server.
            task_id = str(task.get("task_id") or "")
            log.warning("activity task %s initial lease confirmation failed (%r) and the conservative grant is "
                        "spent; releasing the unstarted claim", task_id, error)
            await release_worker_lost(
                client, task_id=task_id, activity_attempt_id=str(task.get("activity_attempt_id") or ""),
                lease_owner=owner, cause=ACTIVITY_LEASE_RENEWAL_EXHAUSTED, timeout=_RENEWAL_TIMEOUT_SECONDS,
            )
            raise ActivityWorkerLost(task_id, ACTIVITY_LEASE_RENEWAL_EXHAUSTED) from error
        grant.renewal_failures = 1
        log.warning("activity task %s initial lease confirmation failed (%r); continuing under the claimed "
                    "lease and retrying", grant.task_id, error)
        return grant

    @classmethod
    def delivery(cls, client: Any, namespace: str, task: Mapping[str, Any], *, owner: str,
                 task_queue: str, server_time: Any, request_started: float) -> ActivityLeaseAuthority:
        task_id = str(task.get("task_id") or "")
        try:
            keys = ("task_id", "workflow_id", "run_id", "activity_execution_id", "activity_attempt_id",
                    "lease_owner", "task_queue")
            if (any(not isinstance(task.get(key), str) or not task[key] for key in keys)
                    or task["lease_owner"] != owner or task["task_queue"] != task_queue
                    or task.get("namespace", namespace) != namespace
                    or type(task.get("attempt_number")) is not int or task["attempt_number"] < 1):
                raise ValueError("invalid original activity identity")
            reference = _timestamp(server_time)
            expiry = _timestamp(task.get("lease_expires_at"))
            duration = (expiry - reference).total_seconds()
            if not 0 < duration <= 86400 or not 0 <= request_started <= time.monotonic():
                raise ValueError("invalid original activity lease")
            deadlines = task.get("deadlines", {})
            if not isinstance(deadlines, Mapping):
                raise ValueError("invalid activity deadlines")
            bounds = [_timestamp(deadlines[key]) for key in ("start_to_close", "schedule_to_close", "run", "execution")
                      if deadlines.get(key) is not None]
            business = request_started + (min(bounds) - reference).total_seconds() if bounds else None
            heartbeat, interval = _heartbeat_terms(task, deadlines, reference)
            result = cls(
                client, namespace,
                {"workflow_task_id": task["task_id"], "workflow_instance_id": task["workflow_id"],
                 "workflow_run_id": task["run_id"], "activity_execution_id": task["activity_execution_id"],
                 "activity_attempt_id": task["activity_attempt_id"], "attempt_number": task["attempt_number"],
                 "lease_owner": owner},
                {key: task[key] for key in (*keys, "attempt_number")},
                request_started, request_started + duration, business,
                request_started + (heartbeat - reference).total_seconds() if heartbeat is not None else None,
                interval, reference, expiry,
                heartbeat - timedelta(seconds=interval) if heartbeat is not None and interval is not None else None,
                heartbeat,
            )
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            raise ActivityWorkerLost(task_id, "invalid_activity_claim") from error
        result.lease_seconds = duration
        result.remaining()
        return result

    @property
    def task_id(self) -> str:
        return str(self.identity["workflow_task_id"])

    def matches(self, client: Any, namespace: str, task: Mapping[str, Any], owner: str, task_queue: str) -> bool:
        return (self.client is client and self.namespace == namespace and self.identity["lease_owner"] == owner
                and self.task_identity["task_queue"] == task_queue
                and type(task.get("attempt_number")) is int
                and all(task.get(key) == value for key, value in self.task_identity.items()))

    def governs(self, client: Any, namespace: str, path: str) -> bool:
        prefixes = (f"/worker/activity-tasks/{self.task_id}/",
                    f"/worker/activity-tasks/{quote(self.task_id, safe='')}/")
        return (self.client is client and self.namespace == namespace
                and (any(path == prefix + action for prefix in prefixes
                         for action in ("complete", "fail", "heartbeat", "renew-lease"))
                     or path == "/external-payloads/v1"))

    def acknowledge_progress(self, reply: Any, request_started: float) -> None:
        """Use an actual application heartbeat, never a runtime liveness ping.

        The existing heartbeat contract records last_heartbeat_at at the write
        and resets its original recorded timeout interval from that instant.
        Charging the whole request duration consumes storage/network latency.
        """
        self.remaining()
        received = time.monotonic()
        try:
            expected = {"task_id": self.task_id, "activity_attempt_id": self.identity["activity_attempt_id"],
                        "lease_owner": self.identity["lease_owner"]}
            if (not isinstance(reply, Mapping) or reply.get("heartbeat_recorded") is not True
                    or reply.get("can_continue") is not True or reply.get("cancel_requested") is not False
                    or reply.get("reason") is not None
                    or any(reply.get(key) != value for key, value in expected.items())
                    or not self.captured_at <= request_started <= received):
                raise ValueError("heartbeat did not confirm the original activity")
            progress = _timestamp(reply.get("last_heartbeat_at"))
            expiry = _timestamp(reply.get("lease_expires_at"))
            duration = (expiry - progress).total_seconds()
            if not 0 < duration <= 86400:
                raise ValueError("invalid heartbeat lease grant")
            if self.heartbeat_time is not None and progress <= self.heartbeat_time:
                return
            if request_started + duration <= received:
                raise ValueError("heartbeat lease grant was consumed in transport")
            if self.heartbeat_timeout is not None:
                until = request_started + self.heartbeat_timeout
                if until <= received:
                    raise ValueError("heartbeat progress window was consumed in transport")
                self.heartbeat_until = max(self.heartbeat_until or until, until)
                self.heartbeat_deadline = progress + timedelta(seconds=self.heartbeat_timeout)
            self.heartbeat_time = progress
            self.lease_until = max(self.lease_until, request_started + duration)
            self.lease_expiry = max(self.lease_expiry, expiry)
        except (TypeError, ValueError, OverflowError) as error:
            self.lost_reason = "invalid_activity_heartbeat"
            raise ActivityWorkerLost(self.task_id, self.lost_reason) from error

    def remaining(self) -> float:
        if self.lost_reason is not None:
            raise ActivityWorkerLost(self.task_id, self.lost_reason)
        bounds = [self.lease_until, *(value for value in (self.business_until, self.heartbeat_until)
                                     if value is not None)]
        remaining = min(bounds) - time.monotonic()
        if remaining <= 0:
            self.lost_reason = "activity_execution_budget_expired"
            raise ActivityWorkerLost(self.task_id, self.lost_reason)
        return remaining

    def report_reserve(self) -> float:
        """Lease time kept back to fail the attempt if renewal keeps failing."""
        return min(_REPORT_RESERVE_SECONDS, self.lease_seconds * _REPORT_RESERVE_FRACTION)

    def renewal_budget(self) -> float:
        """Bound one renewal so a timeout still leaves the report reserve."""
        return min(_RENEWAL_TIMEOUT_SECONDS, self.remaining(),
                   self.lease_until - time.monotonic() - self.report_reserve())

    def track(self, execution: Any) -> None:
        """Register a thread future or callback process that runs this claim."""
        self.executions.append(execution)

    async def confirm_stopped(self, deadline: float) -> bool:
        """Wait until every tracked execution has stopped, or ``deadline`` passes."""
        while not all(_execution_stopped(execution) for execution in self.executions):
            left = deadline - time.monotonic()
            if left <= 0:
                return False
            await asyncio.sleep(min(_STOP_POLL_SECONDS, left))
        return time.monotonic() < deadline

    def renewal_exhausted(self) -> bool:
        """Renewal has failed and only the report reserve of the lease is left."""
        return (self.renewal_failures > 0
                and self.lease_until - time.monotonic() <= self.report_reserve())

    def acknowledge(self, reply: Any, request_started: float) -> None:
        self.remaining()
        received = time.monotonic()
        try:
            if (not isinstance(reply, Mapping) or reply.get("lease_renewed") is not True
                    or reply.get("can_continue") is not True or reply.get("cancel_requested") is not False
                    or reply.get("heartbeat_recorded") is not False or reply.get("reason") is not None
                    or type(reply.get("attempt_number")) is not int
                    or any(reply.get(key) != value for key, value in self.identity.items())
                    or not self.captured_at <= request_started <= received):
                raise ValueError("renewal did not preserve the original activity claim")
            server = _timestamp(reply.get("server_time"))
            expiry = _timestamp(reply.get("lease_expires_at"))
            duration = (expiry - server).total_seconds()
            if not 0 < duration <= 86400 or request_started + duration <= received:
                raise ValueError("renewal did not prove a live grant")
            if (self.heartbeat_time is not None and server < self.heartbeat_time and expiry <= self.lease_expiry):
                # An independently acknowledged application heartbeat can
                # overtake an in-flight renewal. Its older reply grants nothing.
                return
            if expiry < self.lease_expiry or server < self.server_time:
                raise ValueError("renewal predates the confirmed grant")
            confirmation = (server, expiry, request_started)
            if self.last_renewal is not None and server <= self.last_renewal[0]:
                if confirmation == self.last_renewal:
                    return
                raise ValueError("replayed activity renewal")
            self.lease_until = max(self.lease_until, request_started + duration)
            self.lease_seconds = duration
            self._observe_progress(reply, server, request_started, received)
            self.server_time, self.lease_expiry, self.last_renewal = server, expiry, confirmation
        except (TypeError, ValueError, OverflowError) as error:
            self.lost_reason = "invalid_activity_renewal"
            raise ActivityWorkerLost(self.task_id, self.lost_reason) from error

    def _observe_progress(self, reply: Mapping[str, Any], server: datetime, started: float, received: float) -> None:
        if self.heartbeat_until is None or self.heartbeat_timeout is None:
            return
        try:
            progress = _timestamp(reply.get("last_heartbeat_at"))
            deadline = _timestamp(reply.get("heartbeat_deadline_at"))
        except (TypeError, ValueError):
            return
        until = started + (deadline - server).total_seconds()
        if (progress <= server < deadline and until > received
                and (self.heartbeat_time is None or progress > self.heartbeat_time)
                and (self.heartbeat_deadline is None or deadline > self.heartbeat_deadline)
                and deadline - progress == timedelta(seconds=self.heartbeat_timeout)):
            self.heartbeat_until = max(self.heartbeat_until, until)
            self.heartbeat_time, self.heartbeat_deadline = progress, deadline

    async def renew(self) -> bool:
        started = time.monotonic()
        try:
            budget = self.renewal_budget()
            if budget <= 0:
                # No time for a renewal that would still leave the reserve.
                self.renewal_failures += 1
                return False
            reply = await asyncio.wait_for(self.client.renew_activity_task_lease(
                task_id=self.task_id, activity_attempt_id=self.identity["activity_attempt_id"],
                lease_owner=self.identity["lease_owner"], timeout=budget,
            ), timeout=budget)
            self.acknowledge(reply, started)
            self.renewal_failures = 0
            return True
        except ActivityWorkerLost:
            raise
        except Exception as error:
            if _transient(error):
                # A failed exchange grants nothing. The existing lease may
                # still authorize work until its original local bound.
                self.renewal_failures += 1
                self.remaining()
                return False
            self.lost_reason = "activity_renewal_unavailable"
            raise ActivityWorkerLost(self.task_id, self.lost_reason) from error

    async def _maintain(self, stopped: asyncio.Event) -> None:
        while not stopped.is_set():
            lease_left = max(0.0, self.lease_until - time.monotonic())
            if self.renewal_failures:
                # Back off with full jitter on a struggling link, but keep
                # retrying inside the grant, ahead of the release reserve.
                delay = min(renewal_retry_delay(self.renewal_failures),
                            max(0.0, lease_left - self.report_reserve()) / 3)
            else:
                delay = min(30.0, lease_left / 3)
            if self.heartbeat_timeout is not None:
                delay = min(delay, self.heartbeat_timeout / 3)
            await asyncio.sleep(min(delay, self.remaining()))
            if stopped.is_set():
                return
            if self.renewal_exhausted():
                self.lost_reason = ACTIVITY_LEASE_RENEWAL_EXHAUSTED
                return
            await self.renew()

    async def run(self, operation: Callable[[], Awaitable[_T]]) -> _T:
        self.remaining()
        if self.renewal_exhausted():
            # Never start work that could not be confirmed stopped in time.
            self.lost_reason = ACTIVITY_LEASE_RENEWAL_EXHAUSTED
            raise ActivityWorkerLost(self.task_id, self.lost_reason)
        stopped = asyncio.Event()
        self.execution_started = True
        execution = asyncio.ensure_future(operation())
        maintenance = asyncio.create_task(self._maintain(stopped))
        try:
            while True:
                done, _ = await asyncio.wait({execution, maintenance}, timeout=self.remaining(),
                                             return_when=asyncio.FIRST_COMPLETED)
                if maintenance in done:
                    await maintenance
                    raise ActivityWorkerLost(self.task_id, self.lost_reason or "activity_renewal_stopped")
                if execution in done:
                    result = await execution
                    self.remaining()
                    return result
        except BaseException:
            self.lost_reason = self.lost_reason or "activity_execution_abandoned"
            raise
        finally:
            # An I/O wrapper may complete concurrently with cancel() and consume
            # that cancellation. The settled scope must still stop renewing.
            stopped.set()
            # A Python thread may continue after cancellation. Its existing
            # executor keeps the physical slot until it actually exits, while
            # this context independently fences every late HTTP publication.
            for future in (execution, maintenance):
                future.cancel()
            await asyncio.gather(execution, maintenance, return_exceptions=True)


current_activity_authority: ContextVar[ActivityLeaseAuthority | None] = ContextVar(
    "activity_lease_authority", default=None,
)
