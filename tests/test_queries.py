from __future__ import annotations

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import QueryFailed
from durable_workflow.workflow import CompleteWorkflow, WorkflowContext, query_state, replay


@workflow.defn(name="queryable-counter")
class QueryableCounter:
    def __init__(self) -> None:
        self.count = 0

    @workflow.signal("increment")
    def on_increment(self, amount: int) -> None:
        self.count += amount

    @workflow.query("current_count")
    def current_count(self) -> int:
        return self.count

    @workflow.query("is_at_least")
    def is_at_least(self, threshold: int) -> bool:
        return self.count >= threshold

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("wait", [])
        return self.count


def _activity_completed_event(result: object) -> dict[str, object]:
    return {
        "event_type": "ActivityCompleted",
        "payload": {
            "result": serializer.envelope(result),
            "payload_codec": serializer.AVRO_CODEC,
        },
    }


def _signal_received_event(name: str, args: list[object]) -> dict[str, object]:
    return {
        "event_type": "SignalReceived",
        "payload": {
            "signal_name": name,
            "value": serializer.envelope(args),
            "payload_codec": serializer.AVRO_CODEC,
        },
    }


class TestQueryDecoratorRegistry:
    def test_defn_collects_query_methods_into_registry(self) -> None:
        queries = QueryableCounter.__workflow_queries__  # type: ignore[attr-defined]

        assert queries == {
            "current_count": "current_count",
            "is_at_least": "is_at_least",
        }

    def test_query_decorator_sets_query_name_marker(self) -> None:
        assert QueryableCounter.current_count.__query_name__ == "current_count"  # type: ignore[attr-defined]

    def test_workflow_without_query_methods_has_empty_registry(self) -> None:
        @workflow.defn(name="no-queries")
        class PlainWorkflow:
            def run(self, ctx: WorkflowContext) -> str:
                return "done"

        assert PlainWorkflow.__workflow_queries__ == {}  # type: ignore[attr-defined]


class TestQueryStateReplay:
    def test_query_state_replays_signal_mutations_before_invoking_handler(self) -> None:
        history = [
            _signal_received_event("increment", [3]),
            _activity_completed_event(None),
            _signal_received_event("increment", [4]),
        ]

        assert query_state(QueryableCounter, history, [], "current_count") == 7

    def test_query_state_passes_query_arguments(self) -> None:
        history = [
            _signal_received_event("increment", [5]),
        ]

        assert query_state(QueryableCounter, history, [], "is_at_least", [5]) is True
        assert query_state(QueryableCounter, history, [], "is_at_least", [6]) is False

    def test_query_state_does_not_change_regular_replay_result(self) -> None:
        history = [
            _signal_received_event("increment", [2]),
            _activity_completed_event(None),
        ]

        outcome = replay(QueryableCounter, history, [])

        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == 2

    def test_unknown_query_raises_query_failed(self) -> None:
        with pytest.raises(QueryFailed, match="unknown query"):
            query_state(QueryableCounter, [], [], "missing_query")

    def test_query_handler_exceptions_raise_query_failed(self) -> None:
        @workflow.defn(name="failing-query")
        class FailingQuery:
            @workflow.query("boom")
            def boom(self) -> str:
                raise RuntimeError("query exploded")

            def run(self, ctx: WorkflowContext) -> str:
                return "done"

        with pytest.raises(QueryFailed, match="query exploded"):
            query_state(FailingQuery, [], [], "boom")

    def test_replay_failure_raises_query_failed_before_invoking_handler(self) -> None:
        @workflow.defn(name="replay-failing-query")
        class ReplayFailingQuery:
            @workflow.query("status")
            def status(self) -> str:
                return "unreachable"

            def run(self, ctx: WorkflowContext) -> str:
                raise RuntimeError("replay cannot continue")

        with pytest.raises(QueryFailed, match="workflow replay failed"):
            query_state(ReplayFailingQuery, [], [], "status")
