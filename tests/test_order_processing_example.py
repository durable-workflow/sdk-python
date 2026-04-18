from __future__ import annotations

from examples.order_processing.app import (
    ProcessOrderWorkflow,
    charge_payment,
    create_shipment,
    reserve_inventory,
    sample_order,
    send_confirmation,
    total_order_cents,
)

from durable_workflow import serializer
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, replay


def _completed(result: object) -> dict[str, object]:
    return {
        "event_type": "ActivityCompleted",
        "payload": {
            "result": serializer.encode(result, codec="json"),
            "payload_codec": "json",
        },
    }


def test_order_workflow_schedules_inventory_first() -> None:
    order = sample_order("order-test")

    outcome = replay(ProcessOrderWorkflow, [], [order])

    assert len(outcome.commands) == 1
    cmd = outcome.commands[0]
    assert isinstance(cmd, ScheduleActivity)
    assert cmd.activity_type == "orders.reserve_inventory"
    assert cmd.arguments == [order["order_id"], order["items"]]


def test_order_workflow_advances_through_all_activities() -> None:
    order = sample_order("order-test")
    inventory = reserve_inventory(order["order_id"], order["items"])
    payment = charge_payment(order["order_id"], total_order_cents(order), order["payment"])
    shipment = create_shipment(order["order_id"], order["customer"], inventory)
    confirmation = send_confirmation(order["order_id"], order["customer"], payment, shipment)

    first = replay(ProcessOrderWorkflow, [_completed(inventory)], [order]).commands[0]
    assert isinstance(first, ScheduleActivity)
    assert first.activity_type == "orders.charge_payment"

    second = replay(ProcessOrderWorkflow, [_completed(inventory), _completed(payment)], [order]).commands[0]
    assert isinstance(second, ScheduleActivity)
    assert second.activity_type == "orders.create_shipment"

    third = replay(
        ProcessOrderWorkflow,
        [_completed(inventory), _completed(payment), _completed(shipment)],
        [order],
    ).commands[0]
    assert isinstance(third, ScheduleActivity)
    assert third.activity_type == "orders.send_confirmation"

    final = replay(
        ProcessOrderWorkflow,
        [_completed(inventory), _completed(payment), _completed(shipment), _completed(confirmation)],
        [order],
    ).commands[0]
    assert isinstance(final, CompleteWorkflow)
    assert final.result["status"] == "confirmed"
    assert final.result["amount_cents"] == 8200
    assert final.result["confirmation"]["email"] == "ada@example.com"

