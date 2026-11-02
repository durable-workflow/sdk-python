#!/usr/bin/env python3
"""Qualify a normally installed Python SDK against one running Server.

Run this with the interpreter of a virtualenv that installed ``durable-workflow``
from the registry, from outside any source checkout, with ``python -I``:

    python -I qualify_installed_lifecycle.py --server-url URL --token TOKEN --output report.json

Scenarios:

- ``lifecycle``: start, activity, update, signal, query and completion.
- ``cancellation``: cancel a waiting workflow and observe ``WorkflowCancelled``.
- ``failure``: a non-retryable activity failure surfaces as typed ``WorkflowFailed``.
- ``server_compatibility_resource``: the packaged Server compatibility record
  (added in 2.12.4) is checked when the installed version ships it.

Each scenario reports ``pass``, ``product_fail`` (the SDK or Server behaved
wrongly) or ``runner_blocked`` (the runner could not reach a verdict).
``not_applicable`` is reserved for the resource check on releases that predate
it. Only ``pass`` and ``not_applicable`` count as success.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import importlib.resources
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from durable_workflow import Client, NonRetryableError, Worker, activity, workflow
from durable_workflow.errors import WorkflowCancelled, WorkflowFailed

EXPECTED_SERVER_COMPATIBILITY = {
    "supported-server-versions": "2.8.0",
    "supported-server-release-baselines": {"2": "2.8.0", "3": "3.0.0"},
}
RESOURCE_MINIMUM_VERSION = (2, 12, 4)


class PaymentDeclined(NonRetryableError):
    """Typed, non-retryable activity failure used by the failure scenario."""


@activity.defn(name="q_greet")
def greet(name: str) -> str:
    return f"hello, {name}"


@activity.defn(name="q_charge")
def charge(amount: int) -> str:
    raise PaymentDeclined(f"card declined for {amount}")


@workflow.defn(name="q_order")
class OrderWorkflow:
    def __init__(self) -> None:
        self.stage = "started"
        self.approved_by: str | None = None
        self.note: str | None = None

    @workflow.signal("approve")
    def approve(self, by: str) -> None:
        self.approved_by = by

    @workflow.update("set_note")
    def set_note(self, note: str) -> dict[str, str]:
        self.note = note
        return {"note": note}

    @workflow.query("state")
    def state(self) -> dict[str, Any]:
        return {"stage": self.stage, "approved_by": self.approved_by, "note": self.note}

    def run(self, ctx, name):  # type: ignore[no-untyped-def]
        greeting = yield ctx.schedule_activity("q_greet", [name])
        self.stage = "awaiting_approval"
        yield ctx.wait_condition(lambda: self.approved_by is not None and self.note is not None, key="approved")
        self.stage = "done"
        return {"greeting": greeting, "approved_by": self.approved_by, "note": self.note}


@workflow.defn(name="q_waiter")
class WaiterWorkflow:
    def __init__(self) -> None:
        self.released = False

    @workflow.signal("release")
    def release(self) -> None:
        self.released = True

    @workflow.query("waiting")
    def waiting(self) -> bool:
        return not self.released

    def run(self, ctx):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.released, key="released")
        return "released"


@workflow.defn(name="q_payment")
class PaymentWorkflow:
    def run(self, ctx, amount):  # type: ignore[no-untyped-def]
        result = yield ctx.schedule_activity("q_charge", [amount])
        return result


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


QUERY_REFUSALS: dict[str, int] = {}


def query_result(response: Any) -> Any:
    """Return the decoded query result from the control-plane response."""

    if isinstance(response, dict) and "query_name" in response and "result" in response:
        return response["result"]
    return response


async def wait_for_query(client: Client, workflow_id: str, query: str, predicate: Any, timeout: float = 30.0) -> Any:
    deadline = time.monotonic() + timeout
    last: Any = None
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            last = query_result(await client.query_workflow(workflow_id, query))
            if predicate(last):
                return last
        except Exception as error:  # queries can race the first workflow task
            last_error = error
            reason = getattr(error, "body", None)
            key = reason.get("reason") if isinstance(reason, dict) else type(error).__name__
            QUERY_REFUSALS[str(key)] = QUERY_REFUSALS.get(str(key), 0) + 1
        await asyncio.sleep(1.0)
    raise TimeoutError(f"query {query} did not reach the expected state; last={last!r} error={last_error!r}")


async def scenario_lifecycle(client: Client, queue: str, suffix: str) -> dict[str, Any]:
    workflow_id = f"q-order-{suffix}"
    handle = await client.start_workflow(
        workflow_type="q_order", workflow_id=workflow_id, task_queue=queue, input=["world"],
    )
    state = await wait_for_query(client, workflow_id, "state", lambda s: s.get("stage") == "awaiting_approval")
    update = await client.update_workflow(workflow_id, "set_note", args=["ship it"], wait_for="completed")
    await client.signal_workflow(workflow_id, "approve", args=["qa"])
    result = await client.get_result(handle, timeout=60.0)
    expected = {"greeting": "hello, world", "approved_by": "qa", "note": "ship it"}
    description = await client.describe_workflow(workflow_id)
    ok = result == expected and str(description.status).lower() == "completed"
    return {
        "outcome": "pass" if ok else "product_fail",
        "workflow_id": workflow_id,
        "run_id": handle.run_id,
        "query_before_signal": state,
        "update_response": update,
        "result": result,
        "status": description.status,
    }


async def scenario_cancellation(client: Client, queue: str, suffix: str) -> dict[str, Any]:
    workflow_id = f"q-waiter-{suffix}"
    handle = await client.start_workflow(
        workflow_type="q_waiter", workflow_id=workflow_id, task_queue=queue, input=[],
    )
    await wait_for_query(client, workflow_id, "waiting", lambda waiting: waiting is True)
    await client.cancel_workflow(workflow_id, reason="qualification cancellation")
    observed: str
    try:
        result = await client.get_result(handle, timeout=60.0)
        observed = f"completed with {result!r}"
    except WorkflowCancelled as cancelled:
        observed = f"WorkflowCancelled: {cancelled}"
    description = await client.describe_workflow(workflow_id)
    status = str(description.status).lower()
    ok = observed.startswith("WorkflowCancelled") and status in ("cancelled", "canceled")
    return {
        "outcome": "pass" if ok else "product_fail",
        "workflow_id": workflow_id,
        "run_id": handle.run_id,
        "caller_observed": observed,
        "status": description.status,
    }


async def scenario_failure(client: Client, queue: str, suffix: str) -> dict[str, Any]:
    workflow_id = f"q-payment-{suffix}"
    handle = await client.start_workflow(
        workflow_type="q_payment", workflow_id=workflow_id, task_queue=queue, input=[42],
    )
    try:
        result = await client.get_result(handle, timeout=60.0)
    except WorkflowFailed as failed:
        description = await client.describe_workflow(workflow_id)
        ok = (
            str(description.status).lower() == "failed"
            and bool(str(failed))
            and "declined" in str(failed)
        )
        return {
            "outcome": "pass" if ok else "product_fail",
            "workflow_id": workflow_id,
            "run_id": handle.run_id,
            "error_type": type(failed).__name__,
            "exception_class": failed.exception_class,
            "message": str(failed),
            "status": description.status,
        }
    return {"outcome": "product_fail", "workflow_id": workflow_id, "unexpected_result": result}


def scenario_server_compatibility_resource(sdk_version: str, server_version: str | None) -> dict[str, Any]:
    numeric = tuple(int(part) for part in sdk_version.split(".")[:3] if part.isdigit())
    resource = importlib.resources.files("durable_workflow").joinpath("server_compatibility.json")
    present = resource.is_file()
    if numeric < RESOURCE_MINIMUM_VERSION:
        return {
            "outcome": "not_applicable" if not present else "product_fail",
            "detail": f"durable-workflow {sdk_version} predates the packaged record; present={present}",
        }
    if not present:
        return {"outcome": "product_fail", "detail": "installed package lacks server_compatibility.json"}
    record = json.loads(resource.read_bytes())
    baselines = record.get("supported-server-release-baselines", {})
    server_major = server_version.split(".", 1)[0] if server_version else None
    floor = baselines.get(server_major) if server_major else None
    selected = (
        floor is not None and server_version is not None
        and tuple(int(p) for p in server_version.split(".")[:3]) >= tuple(int(p) for p in floor.split("."))
    )
    ok = record == EXPECTED_SERVER_COMPATIBILITY and selected
    return {
        "outcome": "pass" if ok else "product_fail",
        "record": record,
        "server_version": server_version,
        "selected_major_floor": floor,
        "server_meets_floor": selected,
    }


async def run(server_url: str, token: str, namespace: str) -> dict[str, Any]:
    sdk_version = importlib.metadata.version("durable-workflow")
    import durable_workflow

    report: dict[str, Any] = {
        "sdk_version": sdk_version,
        "sdk_package_file": durable_workflow.__file__,
        "python": sys.version.split()[0],
        "started_at_utc": utc_now(),
        "scenarios": {},
    }
    suffix = uuid4().hex[:10]
    queue = f"q-installed-{suffix}"
    async with Client(server_url, token=token, namespace=namespace) as client:
        info = await client.get_cluster_info()
        server_version = info.get("version") if isinstance(info, dict) else None
        report["server_version"] = server_version
        report["server_implementation"] = info.get("implementation") if isinstance(info, dict) else None
        worker = Worker(
            client,
            task_queue=queue,
            workflows=[OrderWorkflow, WaiterWorkflow, PaymentWorkflow],
            activities=[greet, charge],
            shutdown_timeout=15.0,
        )
        run_task = asyncio.create_task(worker.run())
        try:
            for name, scenario in (
                ("lifecycle", scenario_lifecycle),
                ("cancellation", scenario_cancellation),
                ("failure", scenario_failure),
            ):
                started = time.monotonic()
                try:
                    if run_task.done():
                        raise RuntimeError(f"worker stopped early: {run_task.exception()!r}")
                    result = await asyncio.wait_for(scenario(client, queue, suffix), timeout=120.0)
                except Exception as error:
                    result = {
                        "outcome": "runner_blocked",
                        "error": f"{type(error).__name__}: {error}",
                        "traceback": traceback.format_exc(limit=5),
                    }
                result["seconds"] = round(time.monotonic() - started, 2)
                result["query_refusals"] = dict(QUERY_REFUSALS)
                QUERY_REFUSALS.clear()
                report["scenarios"][name] = result
        finally:
            await worker.stop()
            try:
                await run_task
            except Exception as error:
                report["worker_run_error"] = f"{type(error).__name__}: {error}"
    report["scenarios"]["server_compatibility_resource"] = scenario_server_compatibility_resource(
        sdk_version, server_version if isinstance(server_version, str) else None,
    )
    report["ended_at_utc"] = utc_now()
    report["passed"] = all(
        scenario["outcome"] in ("pass", "not_applicable") for scenario in report["scenarios"].values()
    ) and "worker_run_error" not in report
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(run(args.server_url, args.token, args.namespace))
    args.output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    for name, scenario in report["scenarios"].items():
        print(f"{name}: {scenario['outcome']}")
    print("passed" if report["passed"] else "FAILED")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
