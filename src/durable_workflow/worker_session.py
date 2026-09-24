"""Typed worker-held session leases for activity routing."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .client import Client


@dataclass(frozen=True)
class WorkerSessionOptions:
    session_id: str
    queue: str | None = None
    requirements: tuple[str, ...] = ()
    lease_seconds: int = 120
    ttl_seconds: int = 1800
    max_concurrent_activities: int = 1
    create_if_missing: bool = True
    allow_reacquire_after_failure: bool = True

    def __post_init__(self) -> None:
        session_id = self.session_id.strip()
        if not session_id:
            raise ValueError("worker session id must be a non-empty string")
        object.__setattr__(self, "session_id", session_id)
        if self.queue is not None and not self.queue.strip():
            raise ValueError("worker session queue must be a non-empty string when provided")
        for name in ("lease_seconds", "ttl_seconds", "max_concurrent_activities"):
            if getattr(self, name) < 1:
                raise ValueError(f"worker session {name} must be positive")
        requirements = tuple(dict.fromkeys(requirement.strip() for requirement in self.requirements))
        if any(not requirement for requirement in requirements):
            raise ValueError("worker session requirements must be non-empty strings")
        object.__setattr__(self, "requirements", requirements)

    def to_wire(self) -> dict[str, Any]:
        wire: dict[str, Any] = {
            "session_id": self.session_id,
            "requirements": list(self.requirements),
            "lease_seconds": self.lease_seconds,
            "ttl_seconds": self.ttl_seconds,
            "max_concurrent_activities": self.max_concurrent_activities,
            "create_if_missing": self.create_if_missing,
            "allow_reacquire_after_failure": self.allow_reacquire_after_failure,
        }
        if self.queue is not None:
            wire["queue"] = self.queue
        return wire

    @classmethod
    def from_wire(cls, wire: dict[str, Any]) -> WorkerSessionOptions:
        return cls(
            session_id=wire["session_id"],
            queue=wire.get("queue"),
            requirements=tuple(wire.get("requirements") or ()),
            lease_seconds=wire.get("lease_seconds", 120),
            ttl_seconds=wire.get("ttl_seconds", 1800),
            max_concurrent_activities=wire.get("max_concurrent_activities", 1),
            create_if_missing=wire.get("create_if_missing", True),
            allow_reacquire_after_failure=wire.get("allow_reacquire_after_failure", True),
        )


class WorkerSession:
    """A lease owned by one registered worker; state must be rebuilt after holder loss."""

    def __init__(self, client: Client, worker_id: str, options: WorkerSessionOptions) -> None:
        self.client = client
        self.worker_id = worker_id
        self.options = options
        self._active = False
        self._close_response: dict[str, Any] | None = None
        self._lock = asyncio.Lock()

    @property
    def active(self) -> bool:
        return self._active

    async def create(self) -> dict[str, Any]:
        async with self._lock:
            response = await self.client.create_worker_session(self.worker_id, self.options)
            self._active = True
            self._close_response = None
            return response

    async def renew(self, lease_seconds: int | None = None) -> dict[str, Any]:
        async with self._lock:
            if not self._active:
                raise RuntimeError("a closed or uncreated worker session cannot be renewed")
            requested_lease = self.options.lease_seconds if lease_seconds is None else lease_seconds
            return await self.client.renew_worker_session(
                self.worker_id, self.options.session_id, requested_lease
            )

    async def close(self, reason: str = "worker_shutdown") -> dict[str, Any]:
        async with self._lock:
            if self._close_response is not None:
                return self._close_response
            response = await self.client.close_worker_session(self.worker_id, self.options.session_id, reason)
            self._active = False
            self._close_response = response
            return response

    def activity_options(self) -> dict[str, Any]:
        return {"worker_session": self.options.to_wire()}

    def rebuild_required_after_holder_loss(self) -> bool:
        return True

    def _track_leased_task(self) -> None:
        self._active = True
        self._close_response = None
