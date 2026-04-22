import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow.external_task_input import (
    EXTERNAL_TASK_INPUT_MEDIA_TYPE,
    EXTERNAL_TASK_INPUT_SCHEMA,
    EXTERNAL_TASK_INPUT_VERSION,
    ExternalTaskInputError,
    parse_external_task_input,
    parse_external_task_input_artifact,
)

FIXTURES = Path(__file__).parent / "fixtures" / "external-task-input"


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())


def artifact_for(name: str, example: dict[str, Any]) -> dict[str, Any]:
    encoded = json.dumps(example, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return {
        "artifact": f"durable-workflow.v2.external-task-input.{name}",
        "media_type": EXTERNAL_TASK_INPUT_MEDIA_TYPE,
        "schema": EXTERNAL_TASK_INPUT_SCHEMA,
        "version": EXTERNAL_TASK_INPUT_VERSION,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "example": example,
    }


def test_activity_task_fixture_parses_carrier_neutral_input_envelope() -> None:
    task_input = parse_external_task_input(load_fixture("activity-task.v1.json"))

    assert task_input.kind == "activity_task"
    assert task_input.is_activity_task is True
    assert task_input.is_workflow_task is False
    assert task_input.task.activity_attempt_id == "attempt_01HV7D3KJ1C8WQNNY8MVM8J40X"
    assert task_input.task.handler == "billing.charge-card"
    assert task_input.task.connection is None
    assert task_input.workflow.id == "invoice-2026-0001"
    assert task_input.lease.heartbeat_endpoint.endswith("/heartbeat")
    assert task_input.deadlines is not None
    assert task_input.deadlines["heartbeat"] == "2026-04-22T01:04:00.000000Z"
    assert task_input.history is None


def test_workflow_task_fixture_parses_history_and_resume_context() -> None:
    task_input = parse_external_task_input(load_fixture("workflow-task.v1.json"))

    assert task_input.kind == "workflow_task"
    assert task_input.is_workflow_task is True
    assert task_input.task.attempt == 2
    assert task_input.task.compatibility == "build-2026-04-22"
    assert task_input.workflow.status == "running"
    assert task_input.workflow.resume is not None
    assert task_input.workflow.resume["workflow_sequence"] == 42
    assert task_input.history is not None
    assert task_input.history["last_sequence"] == 42
    assert task_input.deadlines is None


def test_cluster_info_fixture_artifact_validates_sha_and_embedded_example() -> None:
    example = load_fixture("activity-task.v1.json")
    task_input = parse_external_task_input_artifact(artifact_for("activity-task.v1", example))

    assert task_input.kind == "activity_task"
    assert task_input.task.idempotency_key == "attempt_01HV7D3KJ1C8WQNNY8MVM8J40X"


def test_fixture_artifact_rejects_checksum_drift() -> None:
    example = load_fixture("activity-task.v1.json")
    artifact = artifact_for("activity-task.v1", example)
    artifact["example"]["task"]["handler"] = "billing.refund-card"

    with pytest.raises(ExternalTaskInputError, match="sha256 mismatch"):
        parse_external_task_input_artifact(artifact)


def test_unknown_optional_fields_are_ignored() -> None:
    envelope = load_fixture("activity-task.v1.json")
    envelope["carrier_debug"] = {"ignored": True}
    envelope["task"]["carrier_debug"] = "ignored"

    task_input = parse_external_task_input(envelope)

    assert task_input.kind == "activity_task"


def test_missing_required_nested_fields_are_rejected() -> None:
    envelope = copy.deepcopy(load_fixture("workflow-task.v1.json"))
    del envelope["history"]["last_sequence"]

    with pytest.raises(ExternalTaskInputError, match="last_sequence"):
        parse_external_task_input(envelope)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema", "wrong.schema"),
        ("version", 2),
    ],
)
def test_schema_and_version_are_pinned(field: str, value: object) -> None:
    envelope = load_fixture("activity-task.v1.json")
    envelope[field] = value

    with pytest.raises(ExternalTaskInputError):
        parse_external_task_input(envelope)

    assert EXTERNAL_TASK_INPUT_SCHEMA == "durable-workflow.v2.external-task-input"
    assert EXTERNAL_TASK_INPUT_VERSION == 1


def test_activity_attempts_must_be_positive() -> None:
    envelope = load_fixture("activity-task.v1.json")
    envelope["task"]["attempt"] = 0

    with pytest.raises(ExternalTaskInputError, match="attempt must be >= 1"):
        parse_external_task_input(envelope)
