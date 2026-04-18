"""Non-trivial Python SDK example: process an ecommerce order.

The workflow uses four sequential activities so replay has to advance through
several task completions before producing a final result.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import uuid
from typing import Any

from durable_workflow import Client, Worker, activity, workflow

TASK_QUEUE = "orders-python"


def sample_order(order_id: str | None = None) -> dict[str, Any]:
    oid = order_id or f"order-{uuid.uuid4().hex[:8]}"
    return {
        "order_id": oid,
        "customer": {
            "id": "cust-1001",
            "email": "ada@example.com",
            "tier": "gold",
            "shipping_region": "US-CA",
        },
        "payment": {
            "token": "tok_sample_visa",
            "currency": "USD",
        },
        "items": [
            {"sku": "dwf-shirt", "quantity": 2, "unit_price_cents": 3200},
            {"sku": "dwf-mug", "quantity": 1, "unit_price_cents": 1800},
        ],
    }


def total_order_cents(order: dict[str, Any]) -> int:
    return sum(int(item["quantity"]) * int(item["unit_price_cents"]) for item in order["items"])


@activity.defn(name="orders.reserve_inventory")
def reserve_inventory(order_id: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    reservations = [
        {
            "sku": item["sku"],
            "quantity": int(item["quantity"]),
            "reservation_id": f"res-{order_id}-{item['sku']}",
        }
        for item in items
    ]
    return {
        "order_id": order_id,
        "status": "reserved",
        "reservations": reservations,
        "total_units": sum(item["quantity"] for item in reservations),
    }


@activity.defn(name="orders.charge_payment")
def charge_payment(order_id: str, amount_cents: int, payment: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "status": "authorized",
        "amount_cents": amount_cents,
        "currency": payment["currency"],
        "authorization_id": f"auth-{order_id}",
    }


@activity.defn(name="orders.create_shipment")
def create_shipment(
    order_id: str,
    customer: dict[str, Any],
    inventory: dict[str, Any],
) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "status": "label_created",
        "shipment_id": f"ship-{order_id}",
        "region": customer["shipping_region"],
        "packages": max(1, int(inventory["total_units"])),
    }


@activity.defn(name="orders.send_confirmation")
def send_confirmation(
    order_id: str,
    customer: dict[str, Any],
    payment: dict[str, Any],
    shipment: dict[str, Any],
) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "status": "sent",
        "email": customer["email"],
        "template": "order-confirmed",
        "summary": f"{payment['currency']} {payment['amount_cents'] / 100:.2f} via {shipment['shipment_id']}",
    }


@workflow.defn(name="orders.process")
class ProcessOrderWorkflow:
    def run(self, ctx, order):  # type: ignore[no-untyped-def]
        amount_cents = total_order_cents(order)
        inventory = yield ctx.schedule_activity(
            "orders.reserve_inventory",
            [order["order_id"], order["items"]],
        )
        payment = yield ctx.schedule_activity(
            "orders.charge_payment",
            [order["order_id"], amount_cents, order["payment"]],
        )
        shipment = yield ctx.schedule_activity(
            "orders.create_shipment",
            [order["order_id"], order["customer"], inventory],
        )
        confirmation = yield ctx.schedule_activity(
            "orders.send_confirmation",
            [order["order_id"], order["customer"], payment, shipment],
        )
        return {
            "order_id": order["order_id"],
            "status": "confirmed",
            "amount_cents": amount_cents,
            "inventory": inventory,
            "payment": payment,
            "shipment": shipment,
            "confirmation": confirmation,
        }


async def run_order() -> dict[str, Any]:
    server_url = os.environ.get("SERVER_URL", "http://localhost:8080")
    token = os.environ.get("WORKFLOW_TOKEN", "sample-token")
    namespace = os.environ.get("WORKFLOW_NAMESPACE", "default")
    task_queue = os.environ.get("TASK_QUEUE", TASK_QUEUE)
    order = sample_order(os.environ.get("ORDER_ID"))
    workflow_id = os.environ.get("WORKFLOW_ID", order["order_id"])

    async with Client(server_url, token=token, namespace=namespace) as client:
        handle = await client.start_workflow(
            workflow_type="orders.process",
            workflow_id=workflow_id,
            task_queue=task_queue,
            input=[order],
            memo={
                "sample": "order_processing",
                "customer_id": order["customer"]["id"],
                "total_cents": total_order_cents(order),
            },
        )

        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[ProcessOrderWorkflow],
            activities=[
                reserve_inventory,
                charge_payment,
                create_shipment,
                send_confirmation,
            ],
            shutdown_timeout=10.0,
        )
        await worker.run_until(workflow_id=workflow_id, timeout=90.0, poll_interval=0.5)
        result = await handle.result(timeout=10.0, poll_interval=0.5)
        if not isinstance(result, dict):
            raise TypeError(f"expected workflow result object, got {type(result).__name__}")
        return result


async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log = logging.getLogger("order_processing")
    try:
        result = await run_order()
    except Exception:
        log.exception("order workflow failed")
        return 1

    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
