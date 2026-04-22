import copy
import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import serializer
from durable_workflow.errors import NonRetryableError
from durable_workflow.external_task_result import parse_external_task_result
from durable_workflow.invocable import InvocableActivityHandler, handle_invocable_activity

FIXTURES = Path(__file__).parent / "fixtures" / "external-task-input"


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())


def activity_input(*args: Any) -> dict[str, Any]:
    envelope = copy.deepcopy(load_fixture("activity-task.v1.json"))
    envelope["payloads"]["arguments"] = serializer.envelope(list(args), codec=serializer.JSON_CODEC)
    return envelope


async def test_invocable_activity_handler_returns_success_result_envelope() -> None:
    handler = InvocableActivityHandler(
        {"billing.charge-card": lambda amount, currency: {"approved": True, "amount": amount, "currency": currency}},
        carrier="lambda-adapter",
        result_codec=serializer.JSON_CODEC,
    )

    output = await handler.handle(activity_input(4200, "USD"))
    result = parse_external_task_result(output)

    assert result.succeeded is True
    assert result.task.id == "acttask_01HV7D3G3G61TAH2YB5RK45XJS"
    assert result.metadata["carrier"] == "lambda-adapter"
    assert result.result is not None
    assert serializer.decode_envelope(result.result["payload"]) == {
        "approved": True,
        "amount": 4200,
        "currency": "USD",
    }


async def test_invocable_activity_handler_awaits_async_handlers() -> None:
    async def charge(amount: int) -> dict[str, int]:
        return {"amount": amount}

    output = await handle_invocable_activity(
        activity_input(42),
        {"billing.charge-card": charge},
        result_codec=serializer.JSON_CODEC,
    )
    result = parse_external_task_result(output)

    assert result.succeeded is True
    assert result.result is not None
    assert serializer.decode_envelope(result.result["payload"]) == {"amount": 42}


async def test_invocable_activity_handler_fails_closed_for_unknown_handler() -> None:
    output = await InvocableActivityHandler({}, result_codec=serializer.JSON_CODEC).handle(activity_input("x"))
    result = parse_external_task_result(output)

    assert result.failed is True
    assert result.retryable is False
    assert result.failure_kind == "application"
    assert result.failure_classification == "application_error"
    assert result.failure is not None
    assert "no invocable activity handler registered" in result.failure.message


async def test_invocable_activity_handler_maps_non_retryable_errors() -> None:
    def handler() -> None:
        raise NonRetryableError("card rejected")

    output = await InvocableActivityHandler(
        {"billing.charge-card": handler},
        result_codec=serializer.JSON_CODEC,
    ).handle(activity_input())
    result = parse_external_task_result(output)

    assert result.failed is True
    assert result.retryable is False
    assert result.failure_kind == "application"
    assert result.failure is not None
    assert result.failure.message == "card rejected"


async def test_invocable_activity_handler_rejects_workflow_task_inputs() -> None:
    output = await InvocableActivityHandler(
        {"billing.invoice.workflow": lambda: {"ignored": True}},
        result_codec=serializer.JSON_CODEC,
    ).handle(load_fixture("workflow-task.v1.json"))
    result = parse_external_task_result(output)

    assert result.failed is True
    assert result.retryable is False
    assert result.task.kind == "workflow_task"
    assert result.failure is not None
    assert "only accept activity_task" in result.failure.message


async def test_invocable_activity_handler_validates_result_codec() -> None:
    with pytest.raises(ValueError, match="unsupported invocable result codec"):
        InvocableActivityHandler({}, result_codec="protobuf")
