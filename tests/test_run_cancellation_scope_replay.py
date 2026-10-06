from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.cancellation import CancellationContext, ScopedCancellationContext
from durable_workflow.errors import NonDeterministicReplayError, WorkflowCancelled
from durable_workflow.workflow import LocalActivityExecutionAborted


def fixture() -> dict[str, Any]:
    return json.loads((Path(__file__).parent / "fixtures/run-inherited-scope-delivery.json").read_text())  # type: ignore[no-any-return]


def probe(seen: list[CancellationContext | ScopedCancellationContext]) -> type:
    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            def scoped():  # type: ignore[no-untyped-def]
                try:
                    yield ctx.start_timer(10)
                    pytest.fail("original scope cancellation was skipped")
                except WorkflowCancelled as error:
                    assert isinstance(error.context, ScopedCancellationContext)
                    seen.append(error.context)
                    with ctx.cancellation_shield():
                        assert (yield ctx.local_activity("tests.scoped-cleanup", [])) == "scope-cleaned"

            yield from ctx.cancellation_scope(scoped)
            assert ctx.cancellation_context is None
            try:
                yield ctx.start_timer(10)
                pytest.fail("original root cancellation was skipped")
            except WorkflowCancelled as error:
                assert isinstance(error.context, CancellationContext)
                seen.append(error.context)
                with ctx.cancellation_shield():
                    assert (yield ctx.local_activity("tests.root-cleanup", [])) == "root-cleaned"
            return "finished"

    return Probe


def run(cls: type, value: dict[str, Any], count: int) -> workflow.ReplayOutcome:
    return workflow.replay(
        cls, value["history"][:count], [], workflow_id=value["task"]["workflow_id"],
        run_id=value["task"]["run_id"], payload_codec="avro", prepare_local_activities=True,
        local_activity_executor=lambda _: pytest.fail("callback executed before canonical admission"),
        allow_cancellation_scope_authoring=True, allow_cancellation_scope_delivery=True,
    )


@pytest.mark.parametrize("prefix", [
    "requested", "prepared", "scope_delivered", "scope_cleaned", "root_delivered", "root_cleaned",
])
def test_original_root_request_finishes_scoped_cleanup_before_root_delivery(prefix: str) -> None:
    value = fixture()
    count = value["history_ranges"][prefix]
    seen: list[CancellationContext | ScopedCancellationContext] = []
    cls = probe(seen)
    result = run(cls, value, count)
    root = next(row["payload"]["cancellation"] for row in value["history"]
                if row["event_type"] == "CooperativeCancellationRequested")
    if prefix in {"requested", "prepared"}:
        intent = result.cancellation_scope_delivery
        assert intent is not None and intent.boundary.sequence == 2
        assert intent.context.root_request_id == root["root_request_id"]
        assert (intent.preparation is not None) is (prefix == "prepared")
        assert result.cancellation_delivery is None and seen == []
    elif prefix in {"scope_delivered", "root_delivered"}:
        call = result.prepared_local_activity
        assert call is not None and call.sequence == (3 if prefix == "scope_delivered" else 5)
        assert call.recover is False
        descriptor = call.descriptor("avro")
        scheduled = value["history"][7 if prefix == "scope_delivered" else 11]
        expected = scheduled["payload"]["local_preparation"]["cancellation_cleanup"]
        assert call.cleanup == expected
        if prefix == "scope_delivered":
            assert descriptor["cancellation_scope_id"] == value["scope_id"]
            assert descriptor["cancellation_cleanup"] == {
                field: expected[field] for field in ("scope_id", "request_id", "delivery_history_event_id")
            }
        else:
            assert "cancellation_scope_id" not in descriptor
            assert descriptor["cancellation_cleanup"] == {
                field: expected[field] for field in ("request_id", "delivery_history_event_id")
            }
    elif prefix == "scope_cleaned":
        assert result.cancellation_delivery is not None
        assert result.cancellation_delivery.sequence == 4
        assert result.cancellation_delivery.request_id == root["request_id"]
        assert result.cancellation_scope_delivery is None
    else:
        assert result.commands == [workflow.CompleteWorkflow("finished")]
        assert value["native_terminal_status"] == "cancelled"
    for context in seen:
        assert context.root_request_id == root["root_request_id"]
        assert context.deadline.isoformat(timespec="microseconds").replace("+00:00", "Z") == root["cleanup_deadline_at"]
    seen.clear()
    assert run(cls, value, count) == result


@pytest.mark.parametrize("prefix", ["scope_cleaned", "root_cleaned"])
def test_replacement_recovers_started_cleanup_without_reexecuting_callback(prefix: str) -> None:
    value = fixture()
    result = run(probe([]), value, value["history_ranges"][prefix] - 1)
    call = result.prepared_local_activity
    assert call is not None and call.recover is True
    assert call.sequence == (3 if prefix == "scope_cleaned" else 5)


@pytest.mark.parametrize("index", [7, 8])
@pytest.mark.parametrize("field", [
    "scope_id", "operation_scope_id", "request_id", "root_request_id", "delivery_history_event_id",
    "preparation_history_event_id", "cleanup_deadline_at", "authority_deadline_at", "omitted", "null",
])
def test_forged_scoped_cleanup_authority_refuses_before_workflow_construction(index: int, field: str) -> None:
    value = fixture()
    preparation = value["history"][index]["payload"]["local_preparation"]
    if field == "omitted":
        del preparation["cancellation_cleanup"]
    elif field == "null":
        preparation["cancellation_cleanup"] = None
    else:
        preparation["cancellation_cleanup"][field] = "changed"
    entered: list[bool] = []

    class Probe:
        def __init__(self) -> None:
            entered.append(True)

        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.start_timer(10)

    with pytest.raises(LocalActivityExecutionAborted, match="cleanup local activity"):
        run(Probe, value, len(value["history"]))
    assert entered == []


def test_committed_root_delivery_cannot_replace_active_scope_or_emit_another_scope_intent() -> None:
    value = fixture()
    delivery = value["history"][10]
    delivery["payload"]["sequence"] = 2
    delivery["sequence"] = 6
    value["history"] = value["history"][:5] + [delivery]
    seen: list[CancellationContext | ScopedCancellationContext] = []
    with pytest.raises(NonDeterministicReplayError, match="root cancellation cannot replace"):
        run(probe(seen), value, len(value["history"]))
    assert seen == []


@pytest.mark.parametrize("mode", ["all", "select", "scalar"])
def test_pending_root_request_cannot_borrow_captured_scoped_membership(mode: str) -> None:
    value = fixture()

    class Probe:
        def run(self, ctx: workflow.WorkflowContext):  # type: ignore[no-untyped-def]
            scoped = yield from ctx.cancellation_scope(lambda: ctx.start_timer(10))
            if mode == "all":
                yield [scoped, ctx.start_timer(10)]
            elif mode == "select":
                yield ctx.select({"scoped": scoped, "root": ctx.start_timer(10)})
            else:
                yield scoped
            pytest.fail("root cancellation cannot consume a captured scoped operation")

    with pytest.raises((LocalActivityExecutionAborted, NonDeterministicReplayError), match="scoped|scope"):
        run(Probe, value, value["history_ranges"]["requested"])
