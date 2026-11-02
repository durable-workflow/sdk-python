from __future__ import annotations

import pickle
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any, Optional
from unittest.mock import AsyncMock

import pytest

from durable_workflow import TypedValueError, activity, serializer, workflow
from durable_workflow._typed_values import from_payload_value, to_payload_value
from durable_workflow.client import Client
from durable_workflow.worker import Worker
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, WorkflowContext, replay
from tests.test_cooperative_remote_worker import make_owner, task


class Color(Enum):
    RED = "red"
    BLUE = "blue"


class Priority(IntEnum):
    LOW = 1
    HIGH = 2


@dataclass
class Item:
    sku: str
    qty: int = 1


@dataclass
class Order:
    order_id: str
    color: Color
    items: list[Item]
    priority: Priority = Priority.LOW
    note: Optional[str] = None  # noqa: UP045 - typing.Optional is part of the contract
    tags: dict[str, int] = field(default_factory=dict)


ORDER = Order("o-1", Color.BLUE, [Item("a", 2), Item("b")], Priority.HIGH, "gift", {"x": 1})
WIRE = {
    "order_id": "o-1", "color": "blue", "items": [{"sku": "a", "qty": 2}, {"sku": "b", "qty": 1}],
    "priority": 2, "note": "gift", "tags": {"x": 1},
}


def test_typed_values_round_trip_through_the_neutral_wire_format() -> None:
    assert to_payload_value(ORDER) == WIRE
    blob = serializer.encode(to_payload_value(ORDER))
    assert from_payload_value(serializer.decode(blob), Order) == ORDER


def test_newer_payload_keys_are_ignored_and_older_payloads_take_defaults() -> None:
    newer = {**WIRE, "added_in_v2": True, "items": [{"sku": "a", "qty": 2, "weight": 3}]}
    assert from_payload_value(newer, Order).items == [Item("a", 2)]
    older = {"order_id": "o-1", "color": "red", "items": []}
    assert from_payload_value(older, Order) == Order("o-1", Color.RED, [])


@pytest.mark.parametrize(("payload", "path"), [
    ({**WIRE, "items": [{"qty": 2}]}, "order.items[0].sku"),
    ({**WIRE, "color": "green"}, "order.color"),
    ({**WIRE, "items": "a"}, "order.items"),
    ({**WIRE, "tags": {"x": "one"}}, "order.tags['x']"),
    ({**WIRE, "order_id": None}, "order.order_id"),
    ("o-1", "order"),
])
def test_invalid_values_name_the_field_path(payload: Any, path: str) -> None:
    with pytest.raises(TypedValueError) as raised:
        from_payload_value(payload, Order, "order")
    assert raised.value.path == path


def test_unannotated_targets_keep_plain_values() -> None:
    assert from_payload_value(WIRE, Any) is WIRE
    assert from_payload_value([1, 2], list) == [1, 2]
    assert from_payload_value(3, float) == 3.0


async def typed_charge(order: Order, attempts: Optional[int] = None) -> Order:  # noqa: UP045
    return Order(order.order_id, Color.RED, order.items[:1], order.priority, note=f"charged:{attempts}")


async def untyped_charge(order: Any) -> Any:
    return {"type": type(order).__name__}


def test_typed_activity_wrapper_keeps_its_name_and_pickles_by_reference() -> None:
    wrapped = activity.defn(name="tests.typed-charge-pickle", typed=True)(typed_charge)
    assert wrapped.__activity_name__ == "tests.typed-charge-pickle"  # type: ignore[attr-defined]
    assert wrapped.__qualname__ == typed_charge.__qualname__
    assert wrapped.__typed_activity__ is typed_charge  # type: ignore[attr-defined]
    globals()["typed_charge"] = wrapped
    try:
        assert pickle.loads(pickle.dumps(wrapped)) is wrapped
    finally:
        globals()["typed_charge"] = wrapped.__typed_activity__  # type: ignore[attr-defined]


@pytest.mark.parametrize("supervised", [False, True])
async def test_worker_runs_typed_activities_on_neutral_payloads(
    monkeypatch: pytest.MonkeyPatch, supervised: bool,
) -> None:
    wrapped = activity.defn(name="tests.typed-charge", typed=True)(typed_charge)
    monkeypatch.setitem(globals(), "typed_charge", wrapped)
    if supervised:
        worker, client = await make_owner(monkeypatch)
    else:
        client = AsyncMock(spec=Client)
        worker = Worker(client, task_queue="queue", worker_id="owner")
    worker.activities["remote"] = wrapped
    try:
        outcome = await worker._run_activity_task(task({**WIRE, "future_field": 1}, 3))
        assert outcome == "completed"
        result = client.complete_activity_task.await_args.kwargs["result"]
        assert result == {**WIRE, "color": "red", "items": [WIRE["items"][0]], "note": "charged:3"}
        client.fail_activity_task.assert_not_awaited()
    finally:
        await worker.stop()


async def test_untyped_activities_keep_receiving_plain_maps() -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="queue", worker_id="owner")
    worker.activities["remote"] = untyped_charge
    try:
        assert await worker._run_activity_task(task(WIRE)) == "completed"
        assert client.complete_activity_task.await_args.kwargs["result"] == {"type": "dict"}
    finally:
        await worker.stop()


async def test_typed_activity_conversion_failure_names_the_parameter() -> None:
    client = AsyncMock(spec=Client)
    worker = Worker(client, task_queue="queue", worker_id="owner")
    worker.activities["remote"] = activity.defn(name="tests.typed-charge-invalid", typed=True)(typed_charge)
    try:
        assert await worker._run_activity_task(task({**WIRE, "color": "green"})) == "failed"
        report = client.fail_activity_task.await_args.kwargs
        assert report["failure_type"] == "TypedValueError"
        assert report["message"].startswith("order.color: expected Color")
    finally:
        await worker.stop()


@workflow.defn(name="tests.typed-activity-result")
class TypedResultWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        order = yield ctx.schedule_activity("charge", [to_payload_value(ORDER)], result_type=Order)
        return {"is_order": isinstance(order, Order), "color": order.color.value, "items": len(order.items)}


@workflow.defn(name="tests.typed-activity-result-invalid")
class InvalidTypedResultWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.schedule_activity("charge", [], result_type=Order)
        except TypedValueError as error:
            return error.path
        return "converted"


def completed(value: Any) -> list[dict[str, Any]]:
    return [{"event_type": "ActivityCompleted", "payload": {
        "result": serializer.envelope(value, codec=serializer.AVRO_CODEC),
    }}]


def test_workflow_receives_the_declared_activity_result_type_without_changing_the_command() -> None:
    first = replay(TypedResultWorkflow, [], [])
    command = first.commands[0]
    assert isinstance(command, ScheduleActivity)
    assert command.arguments == [WIRE]
    assert command == ScheduleActivity("charge", [WIRE])

    resumed = replay(TypedResultWorkflow, completed({**WIRE, "color": "red", "v2": True}), [])
    assert resumed.commands == [CompleteWorkflow(result={"is_order": True, "color": "red", "items": 2})]


def test_workflow_can_handle_an_activity_result_that_does_not_match() -> None:
    resumed = replay(InvalidTypedResultWorkflow, completed({"order_id": "o-1"}), [])
    assert resumed.commands == [CompleteWorkflow(result="activity result.color")]
