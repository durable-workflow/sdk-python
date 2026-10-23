"""Heartbeat a bounded, cooperatively cancellable call from a sync activity."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from queue import SimpleQueue
from threading import Event, Thread
from typing import TypeVar

from durable_workflow import activity
from durable_workflow.errors import ActivityCancelled

T = TypeVar("T")


def call_with_heartbeats(work: Callable[[Event], T], *, interval: float = 5.0) -> T:
    """Run work(cancelled), which must enforce its own finite deadline.

    Work must return or raise promptly when cancelled is set. This helper cannot
    interrupt an arbitrary blocking call. Call it from a synchronous activity,
    never from the worker event loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError("call_with_heartbeats requires a synchronous activity thread")
    if not 0 < interval < float("inf"):
        raise ValueError("interval must be finite and positive")

    ctx = activity.context()
    finished = Event()
    cancelled = Event()
    failures: SimpleQueue[BaseException] = SimpleQueue()

    def heartbeat() -> None:
        while not finished.wait(interval):
            try:
                ctx.heartbeat_sync({"message": "Waiting for remote operation"})
            except (ActivityCancelled, Exception) as error:
                # ActivityCancelled inherits BaseException, not Exception.
                failures.put(error)
                cancelled.set()
                return

    helper = Thread(target=heartbeat, name="activity-heartbeat")
    helper.start()
    try:
        return work(cancelled)
    finally:
        finished.set()
        # Keep the captured context and client alive until the helper exits.
        helper.join()
        if not failures.empty():
            raise failures.get()
