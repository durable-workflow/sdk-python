"""Exercise the public example with real ActivityContext thread/loop behavior."""
from __future__ import annotations

import asyncio
import importlib.util
import unittest
from pathlib import Path
from threading import Event, Thread
from threading import enumerate as threads
from typing import Any

from durable_workflow import activity
from durable_workflow.activity import ActivityContext, ActivityInfo, _set_context
from durable_workflow.errors import ActivityCancelled, ActivityLeaseAuthorityLost

SPEC = importlib.util.spec_from_file_location(
    "threaded_heartbeats_example",
    Path(__file__).resolve().parents[1] / "examples" / "threaded_heartbeats.py",
)
assert SPEC is not None and SPEC.loader is not None
EXAMPLE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXAMPLE)


class HeartbeatClient:
    def __init__(self, response: Any = None) -> None:
        self.response = response if response is not None else {"can_continue": True}
        self.calls: list[dict[str, Any]] = []
        self.observed = Event()

    async def heartbeat_activity_task(self, **request: Any) -> Any:
        self.calls.append(request)
        self.observed.set()
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class ThreadedHeartbeatExampleTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client = HeartbeatClient()
        self.ctx = ActivityContext(
            info=ActivityInfo(
                "task-original", "export", "attempt-original", 2, "reports", "worker-original",
            ),
            client=self.client,  # type: ignore[arg-type]
        )
        _set_context(self.ctx)

    async def asyncTearDown(self) -> None:
        _set_context(None)
        self.assertFalse([thread for thread in threads() if thread.name == "activity-heartbeat"])

    async def run_work(self, work: Any) -> Any:
        return await asyncio.wait_for(
            asyncio.to_thread(EXAMPLE.call_with_heartbeats, work, interval=0.01),
            timeout=3,
        )

    async def test_new_thread_needs_the_captured_context(self) -> None:
        errors: list[BaseException] = []

        def look_up_context() -> None:
            try:
                activity.context()
            except RuntimeError as error:
                errors.append(error)

        helper = Thread(target=look_up_context)
        helper.start()
        helper.join()
        self.assertEqual(len(errors), 1)
        self.assertIn("outside of an activity", str(errors[0]))

    async def test_return_keeps_original_attempt_and_joins(self) -> None:
        def work(cancelled: Event) -> dict[str, int]:
            self.assertTrue(self.client.observed.wait(2))
            self.assertFalse(cancelled.is_set())
            return {"count": 7}

        self.assertEqual(await self.run_work(work), {"count": 7})
        self.assertTrue(self.client.calls)
        for request in self.client.calls:
            self.assertEqual(request["task_id"], "task-original")
            self.assertEqual(request["activity_attempt_id"], "attempt-original")
            self.assertEqual(request["lease_owner"], "worker-original")

    async def test_work_error_also_joins(self) -> None:
        original = ValueError("business call failed")

        def work(cancelled: Event) -> None:
            self.assertTrue(self.client.observed.wait(2))
            raise original

        with self.assertRaises(ValueError) as caught:
            await self.run_work(work)
        self.assertIs(caught.exception, original)

    def cancelled_work(self, cancelled: Event) -> str:
        self.assertTrue(cancelled.wait(2))
        return "must not be returned"

    async def test_cancellation_stops_work_and_preserves_type(self) -> None:
        self.client.response = {"cancel_requested": True}
        with self.assertRaises(ActivityCancelled):
            await self.run_work(self.cancelled_work)

    async def test_authority_loss_keeps_original_identity_and_reason(self) -> None:
        self.client.response = {"can_continue": False, "reason": "activity_attempt_replaced"}
        with self.assertRaises(ActivityLeaseAuthorityLost) as caught:
            await self.run_work(self.cancelled_work)
        self.assertEqual(caught.exception.task_id, "task-original")
        self.assertEqual(caught.exception.reason, "activity_attempt_replaced")

    async def test_transport_error_is_not_silently_lost(self) -> None:
        original = OSError("transport unavailable")
        self.client.response = original
        with self.assertRaises(OSError) as caught:
            await self.run_work(self.cancelled_work)
        self.assertIs(caught.exception, original)

    async def test_event_loop_call_refuses_before_work(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "synchronous activity thread"):
            EXAMPLE.call_with_heartbeats(lambda cancelled: self.fail("must not run"))
