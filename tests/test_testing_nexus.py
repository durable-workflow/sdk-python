from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.errors import NexusOperationFailed, WorkflowFailed
from durable_workflow.testing import WorkflowEnvironment
from durable_workflow.workflow import WorkflowContext

METHODS = ["call_nexus_service", "start_nexus_operation"]


@workflow.defn(name="tests.harness.nexus")
class NexusCaller:
    def run(
        self, ctx: WorkflowContext, method: str, endpoint: str = "billing",
        service: str = "invoices", operation: str = "create",
    ) -> Generator[Any, Any, Any]:
        result = yield getattr(ctx, method)(endpoint, service, operation, ["a"], target_namespace="shared")
        yield ctx.sleep(1)
        return {
            "accepted": result.accepted, "status": result.status, "result": result.result,
            "endpoint": result.endpoint_name, "service": result.service_name,
            "operation": result.operation_name, "namespace": result.target_namespace,
            "call_id": result.service_call_id,
        }


@workflow.defn(name="tests.harness.nexus-failure")
class CatchingNexusCaller:
    def run(self, ctx: WorkflowContext, method: str) -> Generator[Any, Any, Any]:
        try:
            yield getattr(ctx, method)("billing", "invoices", "create", ["a"])
        except NexusOperationFailed as error:
            yield ctx.schedule_activity("compensate", [error.service_error_type])
            return {
                "message": str(error), "type": error.service_error_type, "reason": error.reason,
                "endpoint": error.endpoint_name, "service": error.service_name,
                "operation": error.operation_name,
            }
        return "unexpected success"


@pytest.mark.parametrize("method", METHODS)
def test_canned_operation_result_uses_production_delivery_semantics(method: str) -> None:
    env = WorkflowEnvironment()
    env.register_nexus_operation_result("billing", "invoices", "create", {"id": "invoice-1"})
    result = env.execute_workflow(NexusCaller, method)
    assert result == {
        "accepted": True,
        "status": "started" if method == "start_nexus_operation" else "completed",
        "result": None if method == "start_nexus_operation" else {"id": "invoice-1"},
        "endpoint": "billing", "service": "invoices", "operation": "create", "namespace": "shared",
        "call_id": result["call_id"],
    }
    assert result["call_id"]
    assert [event["event_type"] for event in env.runs[0].history] == [
        "ServiceCallStarted" if method == "start_nexus_operation" else "ServiceCallCompleted", "TimerFired",
    ]


@pytest.mark.parametrize("method", METHODS)
def test_callable_mock_runs_once_despite_later_task_replay(method: str) -> None:
    env = WorkflowEnvironment()
    calls: list[str] = []

    def create(customer: str) -> str:
        calls.append(customer)
        return "invoice-1"

    env.register_nexus_operation("billing", "invoices", "create", create)
    result = env.execute_workflow(NexusCaller, method)
    assert calls == ["a"]
    assert result["result"] == (None if method == "start_nexus_operation" else "invoice-1")


@pytest.mark.parametrize("endpoint,service,operation", [
    ("billing", "invoices", "create"), ("other", "invoices", "create"),
    ("billing", "other", "create"), ("billing", "invoices", "other"),
])
def test_mock_identity_includes_all_three_names(endpoint: str, service: str, operation: str) -> None:
    env = WorkflowEnvironment()
    env.register_nexus_operation_result("billing", "invoices", "create", "default")
    env.register_nexus_operation_result(endpoint, service, operation, "selected")
    result = env.execute_workflow(NexusCaller, "call_nexus_service", endpoint, service, operation)
    assert result["result"] == "selected"
    if (endpoint, service, operation) != ("billing", "invoices", "create"):
        assert env.execute_workflow(NexusCaller, "call_nexus_service")["result"] == "default"


@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("typed", [False, True], ids=["ordinary", "typed"])
def test_operation_failure_reaches_workflow_and_can_compensate(method: str, typed: bool) -> None:
    env = WorkflowEnvironment()
    compensations: list[str] = []

    def refuse(customer: str) -> None:
        assert customer == "a"
        if typed:
            raise NexusOperationFailed("declined", service_error_type="CardDeclined", reason="insufficient_funds")
        raise ValueError("declined")

    env.register_nexus_operation("billing", "invoices", "create", refuse)
    env.register_activity("compensate", compensations.append)
    result = env.execute_workflow(CatchingNexusCaller, method)
    assert result == {
        "message": "declined", "type": "CardDeclined" if typed else "ValueError",
        "reason": "insufficient_funds" if typed else None,
        "endpoint": "billing", "service": "invoices", "operation": "create",
    }
    assert compensations == ["CardDeclined" if typed else "ValueError"]
    assert env.runs[0].history[0]["event_type"] == "ServiceCallFailed"


@pytest.mark.parametrize("method", METHODS)
def test_unhandled_mock_failure_fails_the_workflow(method: str) -> None:
    env = WorkflowEnvironment()

    def refuse(customer: str) -> None:
        raise ValueError("service unavailable")

    env.register_nexus_operation("billing", "invoices", "create", refuse)
    with pytest.raises(WorkflowFailed, match="service unavailable") as caught:
        env.execute_workflow(NexusCaller, method)
    assert caught.value.exception_type == "NexusOperationFailed"


@pytest.mark.parametrize("method", METHODS)
def test_missing_operation_mock_names_identity_and_registration(method: str) -> None:
    env = WorkflowEnvironment()
    with pytest.raises(KeyError) as caught:
        env.execute_workflow(NexusCaller, method)
    message = str(caught.value)
    assert all(name in message for name in ("billing", "invoices", "create"))
    assert "register_nexus_operation_result" in message
    assert "register_nexus_operation" in message


@pytest.mark.parametrize("last_registration", ["result", "callable"])
def test_latest_mock_registration_wins(last_registration: str) -> None:
    env = WorkflowEnvironment()
    env.register_nexus_operation_result("billing", "invoices", "create", "old")
    env.register_nexus_operation("billing", "invoices", "create", lambda _: "callable")
    if last_registration == "result":
        env.register_nexus_operation_result("billing", "invoices", "create", "result")
    assert env.execute_workflow(NexusCaller, "call_nexus_service")["result"] == last_registration
