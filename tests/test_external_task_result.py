import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow.external_task_result import (
    EXTERNAL_TASK_RESULT_MEDIA_TYPE,
    EXTERNAL_TASK_RESULT_SCHEMA,
    EXTERNAL_TASK_RESULT_VERSION,
    ExternalTaskResultError,
    parse_external_task_result,
    parse_external_task_result_artifact,
)

FIXTURES = Path(__file__).parent / "fixtures" / "external-task-result"


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())


def artifact_for(name: str, example: dict[str, Any]) -> dict[str, Any]:
    encoded = json.dumps(example, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return {
        "artifact": f"durable-workflow.v2.external-task-result.{name}",
        "media_type": EXTERNAL_TASK_RESULT_MEDIA_TYPE,
        "schema": EXTERNAL_TASK_RESULT_SCHEMA,
        "version": EXTERNAL_TASK_RESULT_VERSION,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "example": example,
    }


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
    assert result.deadline_exceeded is True
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


@pytest.mark.parametrize(
    ("fixture", "classification", "failure_kind", "retryable", "recorded"),
    [
        ("cancellation.v1.json", "cancelled", "cancellation", False, True),
        ("handler-crash.v1.json", "handler_crash", "handler_crash", True, True),
        ("decode-failure.v1.json", "decode_failure", "decode_failure", False, True),
        (
            "unsupported-payload-codec.v1.json",
            "unsupported_payload_codec",
            "unsupported_payload",
            False,
            True,
        ),
        (
            "unsupported-payload-reference.v1.json",
            "unsupported_payload_reference",
            "unsupported_payload",
            False,
            True,
        ),
    ],
)
def test_full_result_fixture_set_exposes_machine_outcome_decisions(
    fixture: str,
    classification: str,
    failure_kind: str,
    retryable: bool,
    recorded: bool,
) -> None:
    result = parse_external_task_result(load_fixture(fixture))

    assert result.kind == "failure"
    assert result.failed is True
    assert result.failure_kind == failure_kind
    assert result.failure_classification == classification
    assert result.retryable is retryable
    assert result.recorded is recorded
    assert result.cancelled is (classification == "cancelled")
    assert result.handler_crash is (classification == "handler_crash")
    assert result.decode_failure is (classification == "decode_failure")
    assert result.unsupported_payload is classification.startswith("unsupported_payload_")


@pytest.mark.parametrize(
    ("fixture", "artifact_name"),
    [
        ("success.v1.json", "success.v1"),
        ("failure.v1.json", "failure.v1"),
        ("malformed-output.v1.json", "malformed-output.v1"),
        ("cancellation.v1.json", "cancellation.v1"),
        ("handler-crash.v1.json", "handler-crash.v1"),
        ("decode-failure.v1.json", "decode-failure.v1"),
        ("unsupported-payload-codec.v1.json", "unsupported-payload-codec.v1"),
        ("unsupported-payload-reference.v1.json", "unsupported-payload-reference.v1"),
    ],
)
def test_cluster_info_fixture_artifacts_validate_sha_and_embedded_examples(
    fixture: str, artifact_name: str
) -> None:
    example = load_fixture(fixture)
    result = parse_external_task_result_artifact(artifact_for(artifact_name, example))

    assert result.task.kind == "activity_task"


def test_fixture_artifact_rejects_checksum_drift() -> None:
    example = load_fixture("handler-crash.v1.json")
    artifact = artifact_for("handler-crash.v1", example)
    artifact["example"]["failure"]["message"] = "rewritten"

    with pytest.raises(ExternalTaskResultError, match="sha256 mismatch"):
        parse_external_task_result_artifact(artifact)


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
