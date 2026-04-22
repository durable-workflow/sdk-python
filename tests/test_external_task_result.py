import copy
import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow.external_task_result import (
    EXTERNAL_TASK_RESULT_SCHEMA,
    EXTERNAL_TASK_RESULT_VERSION,
    ExternalTaskResultError,
    parse_external_task_result,
)

FIXTURES = Path(__file__).parent / "fixtures" / "external-task-result"


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())


def test_success_fixture_parses_carrier_neutral_result_envelope() -> None:
    result = parse_external_task_result(load_fixture("success.v1.json"))

    assert result.kind == "success"
    assert result.succeeded is True
    assert result.failed is False
    assert result.recorded is True
    assert result.retryable is False
    assert result.task.kind == "activity_task"
    assert result.task.attempt == 1
    assert result.result is not None
    assert result.result["payload"]["codec"] == "avro"


def test_failure_fixture_exposes_retryability_timeout_and_details() -> None:
    result = parse_external_task_result(load_fixture("failure.v1.json"))

    assert result.kind == "failure"
    assert result.failed is True
    assert result.retryable is True
    assert result.cancelled is False
    assert result.failure is not None
    assert result.failure.kind == "timeout"
    assert result.failure.classification == "deadline_exceeded"
    assert result.failure.deadline_exceeded is True
    assert result.failure.details is not None
    assert result.failure.details["codec"] == "avro"


def test_malformed_output_fixture_preserves_raw_output_as_logs_only_evidence() -> None:
    result = parse_external_task_result(load_fixture("malformed-output.v1.json"))

    assert result.kind == "malformed_output"
    assert result.malformed_output is True
    assert result.retryable is False
    assert result.recorded is False
    assert result.failure is not None
    assert result.failure.kind == "malformed_output"
    assert result.failure.classification == "malformed_output"
    assert result.raw_output is not None
    assert result.raw_output["exit_code"] == 64


def test_unknown_optional_fields_are_ignored() -> None:
    envelope = load_fixture("success.v1.json")
    envelope["carrier_debug"] = {"ignored": True}
    envelope["outcome"]["carrier_debug"] = "ignored"

    result = parse_external_task_result(envelope)

    assert result.kind == "success"


def test_missing_required_fields_are_rejected() -> None:
    envelope = load_fixture("failure.v1.json")
    del envelope["failure"]["classification"]

    with pytest.raises(ExternalTaskResultError, match="classification"):
        parse_external_task_result(envelope)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema", "wrong.schema"),
        ("version", 2),
    ],
)
def test_schema_and_version_are_pinned(field: str, value: object) -> None:
    envelope = load_fixture("success.v1.json")
    envelope[field] = value

    with pytest.raises(ExternalTaskResultError):
        parse_external_task_result(envelope)

    assert EXTERNAL_TASK_RESULT_SCHEMA == "durable-workflow.v2.external-task-result"
    assert EXTERNAL_TASK_RESULT_VERSION == 1


def test_cancelled_failures_must_set_cancelled_flag() -> None:
    envelope = copy.deepcopy(load_fixture("failure.v1.json"))
    envelope["failure"]["kind"] = "cancellation"
    envelope["failure"]["classification"] = "cancelled"
    envelope["failure"]["cancelled"] = False

    with pytest.raises(ExternalTaskResultError, match="cancelled=true"):
        parse_external_task_result(envelope)
