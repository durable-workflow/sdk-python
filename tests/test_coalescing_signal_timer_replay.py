"""Coalescing signals that arrive while an activity is held (sdk-python#369).

The installed trace on #369 recorded two timers at sequences 3 and 4 where the
workflow authors one. Native Server leased two workflow tasks for the same run
with byte-equivalent histories, and accepted the one timer each replay returned
(server#1282). These controls pin the SDK side of that boundary:

- One replay of the shared history authors exactly one timer, and concurrent
  claims on that history return identical commands.
- The correct history reaches the second batch and applies the highest
  revision once. Every prefix authors the command the full history records.
- A signal admitted while the timer is open applies at the preceding result. A
  signal admitted after the timer fires joins the next batch. Both hold for the
  live task and the cold replay.
- The producer-duplicated history stays a visible replay failure.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from durable_workflow import Worker, serializer, workflow
from durable_workflow.client import Client
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.workflow import WorkflowContext, query_state, replay

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "replay_regressions"
RECORDED_COMMANDS = {"TimerScheduled": "StartTimer", "ActivityScheduled": "ScheduleActivity"}


@workflow.defn(name="tests.replay.coalescing-refresh")
class CoalescingRefresh:
    """The monotone coalescing recipe from #369, plus a query of handler order."""

    def __init__(self) -> None:
        self.latest = 0
        self.seen: list[int] = []

    @workflow.signal("changed")
    def changed(self, revision: int) -> None:
        self.seen.append(revision)
        self.latest = max(self.latest, revision)

    @workflow.query("seen")
    def seen_revisions(self) -> list[int]:
        return list(self.seen)

    def run(self, ctx: WorkflowContext, entity: str, pending: int = 0, applied: int = 0):  # type: ignore[no-untyped-def]
        self.latest = max(self.latest, pending)
        for _ in range(2):
            yield ctx.wait_condition(lambda: self.latest > applied, key="has-work")  # noqa: B023 (recipe as published)
            while True:
                selected = self.latest
                yield ctx.start_timer(1)
                if self.latest == selected:
                    break
            yield ctx.schedule_activity("refresh", [entity, selected])
            applied = selected
        return ctx.continue_as_new(entity, self.latest, applied)


def _history(fixture_id: str) -> list[dict[str, Any]]:
    fixture = json.loads((FIXTURE_DIR / f"{fixture_id}.json").read_text(encoding="utf-8"))
    history = fixture["history"]
    assert isinstance(history, list)
    return history


def _replay(history: list[dict[str, Any]]) -> list[Any]:
    return list(replay(CoalescingRefresh, history, ["entity-1"], payload_codec="avro").commands)


def _kinds(commands: list[Any]) -> list[str]:
    return [type(command).__name__ for command in commands]


def test_held_activity_signals_reach_handler_in_recorded_order() -> None:
    history = _history("coalescing-held-activity-signals-reach-second-batch")

    seen = query_state(CoalescingRefresh, history, ["entity-1"], "seen", payload_codec="avro")

    assert seen == [1, 2, 3, 4]


def test_held_activity_signals_author_one_timer_then_highest_revision_once() -> None:
    prefix = _history("coalescing-held-activity-signals-author-one-timer")
    complete = _history("coalescing-held-activity-signals-reach-second-batch")

    assert _kinds(_replay(prefix)) == ["StartTimer"]

    batch_commands = _replay(complete)
    assert _kinds(batch_commands) == ["ScheduleActivity"]
    assert batch_commands[0].activity_type == "refresh"
    assert batch_commands[0].arguments == ["entity-1", 4]
    # The first activity stays recorded once. Only the second batch is new.
    assert sum(event["event_type"] == "ActivityScheduled" for event in complete) == 1


@pytest.mark.parametrize(
    "fixture_id",
    [
        "coalescing-held-activity-signals-reach-second-batch",
        "coalescing-signal-during-open-timer-applies-before-timer",
        "coalescing-signal-after-timer-cold-continues-with-latest",
    ],
)
def test_every_prefix_authors_the_command_history_records_next(fixture_id: str) -> None:
    """A task at any delivered boundary agrees with the cold replay of the whole run."""

    history = _history(fixture_id)
    for end in range(1, len(history) + 1):
        authored = [kind for kind in _kinds(_replay(history[:end])) if kind in RECORDED_COMMANDS.values()]
        recorded = next(
            (RECORDED_COMMANDS[event["event_type"]] for event in history[end:]
             if event["event_type"] in RECORDED_COMMANDS),
            None,
        )
        if authored and recorded is not None:
            assert authored[0] == recorded, f"prefix {end} of {fixture_id}"


def test_signal_admitted_while_timer_open_applies_at_prior_result() -> None:
    """Every task replays full history, so live and cold workers agree here.

    A receiver recorded between ``TimerScheduled`` and ``TimerFired`` applies at
    the preceding result boundary. Existing histories depend on that rule.
    """

    commands = _replay(_history("coalescing-signal-during-open-timer-applies-before-timer"))

    assert _kinds(commands) == ["ScheduleActivity"]
    assert commands[0].arguments == ["entity-1", 4]


def test_signal_admitted_after_timer_fires_joins_next_batch() -> None:
    live = _replay(_history("coalescing-signal-after-timer-joins-next-batch"))
    cold = _replay(_history("coalescing-signal-after-timer-cold-continues-with-latest"))

    assert _kinds(live) == ["ScheduleActivity"]
    assert live[0].arguments == ["entity-1", 3]
    assert _kinds(cold) == ["ContinueAsNew"]
    assert cold[0].arguments == ["entity-1", 4, 3]


def test_producer_duplicate_timer_remains_visible_replay_failure() -> None:
    with pytest.raises(NonDeterministicReplayError) as captured:
        _replay(_history("coalescing-producer-duplicate-timer-refused"))

    assert captured.value.workflow_sequence == 4


def test_concurrent_claims_on_shared_history_each_author_one_timer() -> None:
    """Two same-run claims with one history return one timer each.

    Committing both is the producer duplicate tracked by server#1282. The SDK
    cannot see the other claim, so each replay must stay a pure function of its
    delivered history.
    """

    prefix = _history("coalescing-held-activity-signals-author-one-timer")

    async def run_claims() -> list[list[dict[str, Any]]]:
        client = AsyncMock(spec=Client)
        worker = Worker(client, task_queue="q", worker_id="coalescing-worker", workflows=[CoalescingRefresh])
        tasks = [
            {
                "task_id": f"workflow-task-{claim}",
                "workflow_id": "entity-1",
                "run_id": "run-1",
                "workflow_type": "tests.replay.coalescing-refresh",
                "workflow_task_attempt": 1,
                "lease_owner": "coalescing-worker",
                "task_queue": "q",
                "payload_codec": "avro",
                "arguments": serializer.envelope(["entity-1"], codec="avro"),
                "history_events": prefix,
            }
            for claim in ("signal", "activity")
        ]
        results = await asyncio.gather(*(worker._run_workflow_task(task) for task in tasks))
        assert all(result is not None for result in results)
        return [result for result in results if result is not None]

    signal_claim, activity_claim = asyncio.run(run_claims())

    assert [command["type"] for command in signal_claim] == ["start_timer"]
    assert signal_claim == activity_claim
