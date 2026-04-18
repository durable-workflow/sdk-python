"""HTTP transport retry policy used inside :class:`~durable_workflow.Client`.

.. warning::

   This :class:`RetryPolicy` covers **only client-side HTTP retries** for
   transient transport errors (connection failures, timeouts, 5xx responses,
   429 rate-limiting). It is **not** the activity retry policy. Activity-level
   retry and timeout configuration is tracked in
   https://github.com/zorporation/durable-workflow/issues/392 and will land on
   ``ctx.schedule_activity(..., retry_policy=...)``.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

import httpx

T = TypeVar("T")


@dataclass
class RetryPolicy:
    """
    Retry policy for transient server errors.

    Retries requests that fail with transient errors (connection errors,
    timeouts, 5xx server errors, 429 rate limit). Does not retry client
    errors (4xx except 429).

    Uses exponential backoff with jitter to avoid thundering herd.
    """

    max_attempts: int = 3
    initial_backoff_seconds: float = 0.1
    max_backoff_seconds: float = 5.0
    backoff_multiplier: float = 2.0
    jitter: bool = True

    def should_retry(self, exc: Exception, attempt: int) -> bool:
        """Check if the error is retryable and we haven't exceeded max attempts."""
        if attempt >= self.max_attempts:
            return False

        # Retry connection errors and timeouts
        if isinstance(exc, (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError)):
            return True

        # Retry 5xx server errors and 429 rate limit
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code >= 500 or exc.response.status_code == 429

        return False

    def backoff_seconds(self, attempt: int) -> float:
        """Calculate backoff duration for the given attempt number (0-indexed)."""
        backoff = min(
            self.initial_backoff_seconds * (self.backoff_multiplier**attempt),
            self.max_backoff_seconds,
        )
        if self.jitter:
            # Add ±25% jitter
            backoff *= random.uniform(0.75, 1.25)
        return backoff

    async def execute(self, fn: Callable[[], Awaitable[T]]) -> T:
        """
        Execute the given async function with retries.

        Raises the last exception if all retries are exhausted.
        """
        attempt = 0
        last_exc: Exception | None = None

        while attempt < self.max_attempts:
            try:
                result = await fn()
                return result
            except Exception as exc:
                last_exc = exc
                if not self.should_retry(exc, attempt):
                    raise

                if attempt + 1 < self.max_attempts:
                    backoff = self.backoff_seconds(attempt)
                    await asyncio.sleep(backoff)

                attempt += 1

        # All retries exhausted
        if last_exc:
            raise last_exc
        raise RuntimeError("retry loop exhausted with no exception")
