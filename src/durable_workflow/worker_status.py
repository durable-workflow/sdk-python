"""Read-only worker readiness and liveness status.

:meth:`durable_workflow.Worker.status` returns a :class:`WorkerStatus`
snapshot that an application health endpoint or orchestrator probe can serve.
Taking a snapshot never performs network I/O, claims tasks, renews leases or
waits on the worker's event loop, so it is safe to call from another thread,
including when the worker's event loop itself is stalled.

Readiness and liveness answer different questions:

- **Ready** means this worker holds a current registration for its own
  namespace and task queue, every configured poller is running, the most
  recent Server exchange of every role succeeded, and no outstanding request
  is overdue (unanswered beyond one HTTP attempt deadline). A transient Server
  or network outage makes the worker unready without making it unlive, and
  the next successful exchange of each affected role restores readiness
  without a restart.
- **Live** means the worker's local machinery is progressing: its event loop
  is advancing, no configured poller has exited, and no outstanding Server
  request has outlived its stall bound, which exceeds the transport retry
  budget. An idle long poll, a Server outage within that budget, or a
  long-running activity handler is not a liveness failure.

Missing observations are reported as unavailable (``None``), never as
success.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

__all__ = ["WorkerRoleStatus", "WorkerState", "WorkerStatus"]

# Event-loop progress is sampled at this cadence by a local task that performs
# no I/O. Probes compare the age of the latest sample with their stall bound.
EVENT_LOOP_TICK_SECONDS = 1.0
DEFAULT_EVENT_LOOP_STALL_SECONDS = 30.0


class WorkerState(str, Enum):
    """Lifecycle state of a :class:`~durable_workflow.Worker`."""

    STARTING = "starting"
    """Constructed or registering; not yet admitted by the Server."""

    REGISTERED = "registered"
    """Registered with the Server; pollers and heartbeats are running."""

    DRAINING = "draining"
    """``stop()`` was requested; pollers stop and in-flight tasks drain."""

    STOPPED = "stopped"
    """Shutdown completed and the registration was removed."""

    FAILED = "failed"
    """Registration was refused, a poller failed, or shutdown could not confirm cleanup."""


@dataclass(frozen=True)
class WorkerRoleStatus:
    """Progress of one configured worker role.

    Ages are seconds on the local monotonic clock, measured when the snapshot
    was taken. ``None`` means the observation has not happened yet.
    """

    role: str
    """``workflow_poll``, ``activity_poll``, ``query_poll`` or ``heartbeat``."""

    running: bool
    """Whether the role's loop or thread is still running."""

    in_flight_seconds: float | None
    """Age of the role's outstanding Server request, or ``None`` when idle."""

    last_success_age_seconds: float | None
    """Age of the role's most recent successful Server exchange."""

    last_failure_age_seconds: float | None
    """Age of the role's most recent failed Server exchange."""

    consecutive_failures: int
    """Failed exchanges since the role's last success."""

    last_failure_type: str | None
    """Exception class name of the most recent failure; never its message."""

    overdue: bool
    """Whether the outstanding request is unanswered beyond one HTTP attempt deadline."""

    stalled: bool
    """Whether the outstanding request has outlived the snapshot's stall bound."""

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable mapping of this role status."""

        return {
            "role": self.role,
            "running": self.running,
            "in_flight_seconds": self.in_flight_seconds,
            "last_success_age_seconds": self.last_success_age_seconds,
            "last_failure_age_seconds": self.last_failure_age_seconds,
            "consecutive_failures": self.consecutive_failures,
            "last_failure_type": self.last_failure_type,
            "overdue": self.overdue,
            "stalled": self.stalled,
        }


@dataclass(frozen=True)
class WorkerStatus:
    """Point-in-time readiness and liveness of a worker.

    The snapshot carries identity and progress only: no payloads, task
    arguments, exception messages or credentials.
    """

    worker_id: str
    namespace: str | None
    task_queue: str
    state: WorkerState
    ready: bool
    """Whether an orchestrator should route work to, or count, this worker."""

    ready_reason: str
    """``ready`` or the first reason the worker is not ready."""

    live: bool
    """Whether the worker's local machinery is progressing."""

    live_reason: str
    """``live`` or the first reason the worker is not live."""

    registered_age_seconds: float | None
    """Seconds since the Server acknowledged this worker's registration."""

    event_loop_progress_age_seconds: float | None
    """Seconds since the worker's event loop last advanced its progress tick."""

    roles: tuple[WorkerRoleStatus, ...]

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable mapping suitable for a health endpoint."""

        return {
            "worker_id": self.worker_id,
            "namespace": self.namespace,
            "task_queue": self.task_queue,
            "state": self.state.value,
            "ready": self.ready,
            "ready_reason": self.ready_reason,
            "live": self.live,
            "live_reason": self.live_reason,
            "registered_age_seconds": self.registered_age_seconds,
            "event_loop_progress_age_seconds": self.event_loop_progress_age_seconds,
            "roles": [role.as_dict() for role in self.roles],
        }


class _RoleProgress:
    __slots__ = (
        "consecutive_failures",
        "in_flight_since",
        "last_failure_at",
        "last_failure_type",
        "last_success_at",
        "running",
    )

    def __init__(self) -> None:
        self.running = True
        self.in_flight_since: float | None = None
        self.last_success_at: float | None = None
        self.last_failure_at: float | None = None
        self.last_failure_type: str | None = None
        self.consecutive_failures = 0


class _WorkerStatusTracker:
    """Thread-safe progress recorder owned by one worker.

    Pollers record from the worker's event loop and from the query thread;
    snapshots are read from any thread. Every operation holds one lock briefly
    and never blocks on I/O.
    """

    def __init__(self, *, clock: Any = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._state = WorkerState.STARTING
        self._started = False
        self._registered_at: float | None = None
        self._loop_tick_at: float | None = None
        self._failure_reason: str | None = None
        self._roles: dict[str, _RoleProgress] = {}

    def now(self) -> float:
        return float(self._clock())

    def run_started(self) -> None:
        with self._lock:
            self._started = True
            self._loop_tick_at = self.now()

    def loop_tick(self) -> None:
        with self._lock:
            self._loop_tick_at = self.now()

    def registered(self) -> None:
        with self._lock:
            if self._state is WorkerState.STARTING:
                self._state = WorkerState.REGISTERED
                self._registered_at = self.now()

    def roles_started(self, roles: Iterable[str]) -> None:
        with self._lock:
            for role in roles:
                self._roles[role] = _RoleProgress()

    def request_started(self, role: str) -> None:
        with self._lock:
            progress = self._roles.get(role)
            if progress is not None:
                progress.in_flight_since = self.now()

    def request_succeeded(self, role: str) -> None:
        with self._lock:
            progress = self._roles.get(role)
            if progress is not None:
                progress.in_flight_since = None
                progress.last_success_at = self.now()
                progress.consecutive_failures = 0

    def request_failed(self, role: str, error: BaseException) -> None:
        with self._lock:
            progress = self._roles.get(role)
            if progress is not None:
                progress.in_flight_since = None
                progress.last_failure_at = self.now()
                progress.last_failure_type = type(error).__name__
                progress.consecutive_failures += 1

    def request_abandoned(self, role: str) -> None:
        with self._lock:
            progress = self._roles.get(role)
            if progress is not None:
                progress.in_flight_since = None

    def role_exited(self, role: str) -> None:
        with self._lock:
            progress = self._roles.get(role)
            if progress is not None:
                progress.running = False
                progress.in_flight_since = None

    def draining(self) -> None:
        with self._lock:
            if self._state in (WorkerState.STARTING, WorkerState.REGISTERED):
                self._state = WorkerState.DRAINING

    def failed(self, reason: str) -> None:
        with self._lock:
            if self._state is not WorkerState.FAILED:
                self._state = WorkerState.FAILED
                self._failure_reason = reason

    def stopped(self) -> None:
        with self._lock:
            if self._state is not WorkerState.FAILED:
                self._state = WorkerState.STOPPED

    def snapshot(
        self,
        *,
        worker_id: str,
        namespace: str | None,
        task_queue: str,
        event_loop_stall_seconds: float,
        request_overdue_seconds: float,
        request_stall_seconds: float,
    ) -> WorkerStatus:
        with self._lock:
            now = self.now()
            state = self._state
            started = self._started
            failure_reason = self._failure_reason
            registered_age = _age(now, self._registered_at)
            loop_age = _age(now, self._loop_tick_at)
            roles = tuple(
                WorkerRoleStatus(
                    role=role,
                    running=progress.running,
                    in_flight_seconds=_age(now, progress.in_flight_since),
                    last_success_age_seconds=_age(now, progress.last_success_at),
                    last_failure_age_seconds=_age(now, progress.last_failure_at),
                    consecutive_failures=progress.consecutive_failures,
                    last_failure_type=progress.last_failure_type,
                    overdue=(
                        progress.in_flight_since is not None
                        and now - progress.in_flight_since > request_overdue_seconds
                    ),
                    stalled=(
                        progress.in_flight_since is not None
                        and now - progress.in_flight_since > request_stall_seconds
                    ),
                )
                for role, progress in self._roles.items()
            )

        live, live_reason = _liveness(state, started, failure_reason, loop_age, event_loop_stall_seconds, roles)
        ready, ready_reason = _readiness(state, started, live, live_reason, roles)
        return WorkerStatus(
            worker_id=worker_id,
            namespace=namespace,
            task_queue=task_queue,
            state=state,
            ready=ready,
            ready_reason=ready_reason,
            live=live,
            live_reason=live_reason,
            registered_age_seconds=registered_age if state is WorkerState.REGISTERED else None,
            event_loop_progress_age_seconds=loop_age,
            roles=roles,
        )


def _age(now: float, at: float | None) -> float | None:
    return None if at is None else max(0.0, now - at)


def _liveness(
    state: WorkerState,
    started: bool,
    failure_reason: str | None,
    loop_age: float | None,
    event_loop_stall_seconds: float,
    roles: tuple[WorkerRoleStatus, ...],
) -> tuple[bool, str]:
    if not started:
        return False, "not_started"
    if state is WorkerState.FAILED:
        return False, f"failed:{failure_reason}" if failure_reason else "failed"
    if state is WorkerState.STOPPED:
        return False, "stopped"
    if loop_age is None or loop_age > event_loop_stall_seconds:
        return False, "event_loop_stalled"
    if state is WorkerState.REGISTERED:
        return _role_liveness(roles)
    return True, "live"


def _role_liveness(roles: tuple[WorkerRoleStatus, ...]) -> tuple[bool, str]:
    stopped = next((role.role for role in roles if not role.running), None)
    if stopped is not None:
        return False, f"role_stopped:{stopped}"
    stalled = next((role.role for role in roles if role.stalled), None)
    if stalled is not None:
        return False, f"role_stalled:{stalled}"
    return True, "live"


def _readiness(
    state: WorkerState,
    started: bool,
    live: bool,
    live_reason: str,
    roles: tuple[WorkerRoleStatus, ...],
) -> tuple[bool, str]:
    if not started:
        return False, "not_started"
    if state is WorkerState.STARTING:
        return False, "registering"
    if state is WorkerState.DRAINING:
        return False, "draining"
    if state is not WorkerState.REGISTERED or not live:
        return False, live_reason
    for role in roles:
        if role.consecutive_failures or role.overdue:
            return False, f"server_unavailable:{role.role}"
    return True, "ready"
