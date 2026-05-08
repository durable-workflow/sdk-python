import asyncio
import base64
import copy
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import serializer
from durable_workflow.errors import ActivityCancelled, NonRetryableError
from durable_workflow.external_task_result import parse_external_task_result
from durable_workflow.invocable import (
    InvocableActivityHandler,
    handle_invocable_activity,
    handle_invocable_http_request,
    lambda_invocable_activity_handler,
)

FIXTURES = Path(__file__).parent / "fixtures" / "external-task-input"


def load_fixture(name: str) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads((FIXTURES / name).read_text())
    return loaded


def _iso(timestamp: datetime) -> str:
    return timestamp.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _refresh_bounds(envelope: dict[str, Any], *, expires_in: timedelta = timedelta(minutes=5)) -> dict[str, Any]:
    expires_at = _iso(datetime.now(timezone.utc) + expires_in)
    envelope["lease"]["expires_at"] = expires_at
    for key in envelope["deadlines"]:
        envelope["deadlines"][key] = expires_at
    return envelope


def activity_input(*args: Any, expires_in: timedelta = timedelta(minutes=5)) -> dict[str, Any]:
    envelope = copy.deepcopy(load_fixture("activity-task.v1.json"))
    _refresh_bounds(envelope, expires_in=expires_in)
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


async def test_invocable_activity_handler_fails_closed_for_expired_lease_without_invoking_handler() -> None:
    invoked = False

    def charge() -> dict[str, bool]:
        nonlocal invoked
        invoked = True
        return {"approved": True}

    envelope = activity_input()
    envelope["lease"]["expires_at"] = _iso(datetime.now(timezone.utc) - timedelta(seconds=1))

    output = await InvocableActivityHandler(
        {"billing.charge-card": charge},
        result_codec=serializer.JSON_CODEC,
    ).handle(envelope)
    result = parse_external_task_result(output)

    assert invoked is False
    assert result.failed is True
    assert result.retryable is True
    assert result.deadline_exceeded is True
    assert result.failure is not None
    assert result.failure.details == {
        "deadline": "lease.expires_at",
        "expires_at": envelope["lease"]["expires_at"],
    }


async def test_invocable_activity_handler_times_out_async_handler_before_success() -> None:
    async def slow_charge() -> dict[str, bool]:
        await asyncio.sleep(1)
        return {"approved": True}

    output = await InvocableActivityHandler(
        {"billing.charge-card": slow_charge},
        result_codec=serializer.JSON_CODEC,
    ).handle(activity_input(expires_in=timedelta(milliseconds=100)))
    result = parse_external_task_result(output)

    assert result.failed is True
    assert result.retryable is True
    assert result.deadline_exceeded is True
    assert result.failure is not None
    assert result.failure.timeout_type == "deadline_exceeded"
    assert result.result is None


async def test_invocable_activity_handler_rejects_sync_success_after_deadline_overrun() -> None:
    def slow_charge() -> dict[str, bool]:
        time.sleep(0.05)
        return {"approved": True}

    output = await InvocableActivityHandler(
        {"billing.charge-card": slow_charge},
        result_codec=serializer.JSON_CODEC,
    ).handle(activity_input(expires_in=timedelta(milliseconds=10)))
    result = parse_external_task_result(output)

    assert result.failed is True
    assert result.retryable is True
    assert result.deadline_exceeded is True
    assert result.result is None


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


async def test_invocable_http_request_helper_returns_result_response() -> None:
    response = await handle_invocable_http_request(
        json.dumps(activity_input(42)),
        {"billing.charge-card": lambda amount: {"amount": amount}},
        result_codec=serializer.JSON_CODEC,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/vnd.durable-workflow.external-task-result+json"

    result = parse_external_task_result(response.json())
    assert result.succeeded is True
    assert result.result is not None
    assert serializer.decode_envelope(result.result["payload"]) == {"amount": 42}


async def test_invocable_http_request_helper_accepts_bytes_body() -> None:
    response = await handle_invocable_http_request(
        json.dumps(activity_input(7)).encode(),
        {"billing.charge-card": lambda amount: {"amount": amount}},
        result_codec=serializer.JSON_CODEC,
    )

    assert response.status_code == 200
    result = parse_external_task_result(response.json())
    assert result.succeeded is True


async def test_invocable_http_request_helper_rejects_invalid_json_body() -> None:
    response = await handle_invocable_http_request("{", {})

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_invocable_request"


def test_lambda_invocable_activity_handler_accepts_base64_api_gateway_body() -> None:
    handler = lambda_invocable_activity_handler(
        {"billing.charge-card": lambda amount: {"amount": amount}},
        result_codec=serializer.JSON_CODEC,
    )
    body = json.dumps(activity_input(99)).encode()

    response = handler(
        {
            "body": base64.b64encode(body).decode(),
            "isBase64Encoded": True,
        },
        None,
    )

    assert response["statusCode"] == 200
    assert response["headers"]["Content-Type"] == "application/vnd.durable-workflow.external-task-result+json"

    result = parse_external_task_result(json.loads(response["body"]))
    assert result.succeeded is True
    assert result.result is not None
    assert serializer.decode_envelope(result.result["payload"]) == {"amount": 99}


def test_lambda_invocable_activity_handler_rejects_missing_body() -> None:
    handler = lambda_invocable_activity_handler({})
    response = handler({"body": None}, None)

    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "invalid_invocable_request"


# HTTP carrier contract: activity-level failures must return HTTP 200 with a
# valid external-task result envelope, not HTTP 400. HTTP 400 is reserved for
# requests that cannot be parsed into a task envelope at all.


async def test_invocable_http_request_propagates_non_retryable_error_as_200() -> None:
    def handler() -> None:
        raise NonRetryableError("card rejected")

    response = await handle_invocable_http_request(
        json.dumps(activity_input()),
        {"billing.charge-card": handler},
        result_codec=serializer.JSON_CODEC,
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/vnd.durable-workflow.external-task-result+json"
    result = parse_external_task_result(response.json())
    assert result.failed is True
    assert result.retryable is False
    assert result.failure_kind == "application"
    assert result.failure is not None
    assert result.failure.message == "card rejected"


async def test_invocable_http_request_propagates_generic_exception_as_200() -> None:
    def handler() -> None:
        raise RuntimeError("unexpected crash")

    response = await handle_invocable_http_request(
        json.dumps(activity_input()),
        {"billing.charge-card": handler},
        result_codec=serializer.JSON_CODEC,
    )

    assert response.status_code == 200
    result = parse_external_task_result(response.json())
    assert result.failed is True
    assert result.retryable is True
    assert result.failure_kind == "application"
    assert result.failure is not None
    assert "unexpected crash" in result.failure.message


async def test_invocable_http_request_propagates_cancellation_as_200() -> None:
    def handler() -> None:
        raise ActivityCancelled("cancelled by workflow")

    response = await handle_invocable_http_request(
        json.dumps(activity_input()),
        {"billing.charge-card": handler},
        result_codec=serializer.JSON_CODEC,
    )

    assert response.status_code == 200
    result = parse_external_task_result(response.json())
    assert result.failed is True
    assert result.retryable is False
    assert result.cancelled is True
    assert result.failure_classification == "cancelled"


async def test_invocable_http_request_propagates_expired_deadline_as_200() -> None:
    envelope = activity_input()
    envelope["lease"]["expires_at"] = _iso(datetime.now(timezone.utc) - timedelta(seconds=1))

    response = await handle_invocable_http_request(
        json.dumps(envelope),
        {"billing.charge-card": lambda: {"approved": True}},
        result_codec=serializer.JSON_CODEC,
    )

    assert response.status_code == 200
    result = parse_external_task_result(response.json())
    assert result.failed is True
    assert result.retryable is True
    assert result.deadline_exceeded is True


async def test_invocable_http_request_propagates_handler_timeout_as_200() -> None:
    async def slow_handler() -> None:
        await asyncio.sleep(1)

    response = await handle_invocable_http_request(
        json.dumps(activity_input(expires_in=timedelta(milliseconds=100))),
        {"billing.charge-card": slow_handler},
        result_codec=serializer.JSON_CODEC,
    )

    assert response.status_code == 200
    result = parse_external_task_result(response.json())
    assert result.failed is True
    assert result.retryable is True
    assert result.deadline_exceeded is True


async def test_invocable_http_request_propagates_unknown_handler_as_200() -> None:
    response = await handle_invocable_http_request(
        json.dumps(activity_input()),
        {},
        result_codec=serializer.JSON_CODEC,
    )

    assert response.status_code == 200
    result = parse_external_task_result(response.json())
    assert result.failed is True
    assert result.retryable is False
    assert result.failure_kind == "application"
    assert result.failure is not None
    assert "no invocable activity handler registered" in result.failure.message


async def test_invocable_http_request_propagates_workflow_task_rejection_as_200() -> None:
    response = await handle_invocable_http_request(
        json.dumps(load_fixture("workflow-task.v1.json")),
        {"billing.invoice.workflow": lambda: {"ignored": True}},
        result_codec=serializer.JSON_CODEC,
    )

    assert response.status_code == 200
    result = parse_external_task_result(response.json())
    assert result.failed is True
    assert result.retryable is False
    assert result.failure is not None
    assert "only accept activity_task" in result.failure.message
