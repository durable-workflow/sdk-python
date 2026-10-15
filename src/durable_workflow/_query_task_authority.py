"""A query delivery's original, non-renewing execution budget."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypeVar

from .errors import QueryTaskAuthorityLost

_T = TypeVar("_T")
_UTC6 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\Z")
_OBSERVATION_INTERVAL_SECONDS = 5.0


def _timestamp(value: Any, *, canonical: bool) -> datetime:
    if not isinstance(value, str) or (canonical and _UTC6.fullmatch(value) is None):
        raise ValueError("missing or malformed query timing")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("query timing must include a timezone")
    return parsed


@dataclass
class QueryTaskAuthority:
    client: object
    namespace: str
    task_id: str
    attempt: int
    owner: str
    observed: bool
    timing: dict[str, str]
    until: float
    identity: dict[str, Any]
    observer: Callable[[], Awaitable[Mapping[str, Any]]] | None = field(default=None, repr=False)

    @classmethod
    def delivery(cls, client: object, namespace: str, task: Mapping[str, Any], *,
                 owner: str, task_queue: str, observed: bool) -> QueryTaskAuthority:
        task_id, attempt = task.get("query_task_id"), task.get("query_task_attempt")
        if (not isinstance(owner, str) or not owner or not isinstance(task_queue, str) or not task_queue
                or not isinstance(task_id, str) or not task_id or type(attempt) is not int or attempt < 1
                or task.get("lease_owner") != owner or task.get("task_queue") != task_queue
                or task.get("namespace", namespace) != namespace):
            raise QueryTaskAuthorityLost(str(task_id or ""), "invalid_query_claim")
        try:
            lease = _timestamp(task.get("lease_expires_at"), canonical=observed)
            timing = {"lease_expires_at": task["lease_expires_at"]}
            bounds = [lease]
            if observed or "query_admitted_at" in task or "query_deadline_at" in task:
                admitted = _timestamp(task.get("query_admitted_at"), canonical=True)
                deadline = _timestamp(task.get("query_deadline_at"), canonical=True)
                if deadline < admitted or (observed and lease > deadline):
                    raise ValueError("inconsistent original query timing")
                timing.update({"query_admitted_at": task["query_admitted_at"],
                               "query_deadline_at": task["query_deadline_at"]})
                bounds.append(deadline)
            # Legacy compatibility has no non-renewing observation. Its local
            # clock check cannot establish an undisclosed caller deadline.
            reference = (_timestamp(task.get("server_time"), canonical=True) if observed
                         else datetime.now(timezone.utc))
            until = time.monotonic() + (min(bounds) - reference).total_seconds()
        except (ValueError, TypeError, OverflowError) as error:
            raise QueryTaskAuthorityLost(task_id, "invalid_query_timing") from error
        result = cls(client, namespace, task_id, attempt, owner, observed, timing, until,
                     {key: task.get(key) for key in ("workflow_id", "run_id", "task_queue")})
        result.remaining()
        return result

    def matches(self, client: object, namespace: str, task: Mapping[str, Any], owner: str) -> bool:
        return (self.client is client and self.namespace == namespace and self.owner == owner
                and task.get("query_task_id") == self.task_id
                and type(task.get("query_task_attempt")) is int and task.get("query_task_attempt") == self.attempt
                and task.get("lease_owner") == owner
                and all(task.get(key) == value for key, value in self.identity.items())
                and all(task.get(key) == value for key, value in self.timing.items()))

    def remaining(self) -> float:
        remaining = self.until - time.monotonic()
        if remaining <= 0:
            raise QueryTaskAuthorityLost(self.task_id, "query_execution_budget_expired")
        return remaining

    async def confirm(self) -> None:
        remaining = self.remaining()
        if not self.observed:
            return
        started = time.monotonic()
        try:
            if self.observer is None:
                raise ValueError("query authority observer is unavailable")
            reply = await asyncio.wait_for(self.observer(), timeout=min(5.0, remaining))
            if (not isinstance(reply, Mapping) or reply.get("query_task_id") != self.task_id
                    or type(reply.get("query_task_attempt")) is not int
                    or reply.get("query_task_attempt") != self.attempt or reply.get("lease_owner") != self.owner
                    or any(reply.get(key) != value for key, value in self.timing.items())):
                raise ValueError("query authority observation changed its original claim")
            if (reply.get("can_continue") is not True or reply.get("query_task_status") != "leased"
                    or reply.get("reason") is not None):
                raise QueryTaskAuthorityLost(self.task_id, str(reply.get("reason") or "query_claim_unavailable"))
            server_time = _timestamp(reply.get("server_time"), canonical=True)
            bound = min(_timestamp(value, canonical=True) for key, value in self.timing.items()
                        if key != "query_admitted_at")
            # Charging the entire observation round trip is conservative. A
            # later observation may shorten this bound but can never reset it.
            self.until = min(self.until, started + (bound - server_time).total_seconds())
        except QueryTaskAuthorityLost:
            raise
        except Exception as error:
            raise QueryTaskAuthorityLost(self.task_id, "query_authority_unavailable") from error
        self.remaining()

    async def run(self, operation: Callable[[], Awaitable[_T]], *, observe: bool = True,
                  confirm_after: bool = True) -> _T:
        await self.confirm()
        execution = asyncio.ensure_future(operation())

        async def watch() -> None:
            while True:
                await asyncio.sleep(min(_OBSERVATION_INTERVAL_SECONDS, self.remaining()))
                await self.confirm()

        observation = asyncio.create_task(watch()) if observe and self.observed else None
        pending: set[asyncio.Future[Any]] = {execution}
        if observation is not None:
            pending.add(observation)
        try:
            done, _ = await asyncio.wait(pending, timeout=self.remaining(), return_when=asyncio.FIRST_COMPLETED)
            if not done:
                raise QueryTaskAuthorityLost(self.task_id, "query_execution_budget_expired")
            if observation is not None and observation in done:
                await observation
                raise QueryTaskAuthorityLost(self.task_id, "query_authority_unavailable")
            result = await execution
            if confirm_after:
                await self.confirm()
            return result
        finally:
            for future in pending:
                future.cancel()
            await asyncio.gather(*pending, return_exceptions=True)


# Async task creation copies this context. Each dispatched query keeps the
# loading budget without adding private fields to the public wire dictionary
# or retaining an unbounded task-ID registry on Client/Worker.
current_query_authority: ContextVar[QueryTaskAuthority | None] = ContextVar("query_task_authority", default=None)
