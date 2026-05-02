"""Tests for the offline replay verification surface."""

from __future__ import annotations

import json
from pathlib import Path

from durable_workflow import workflow
from durable_workflow.replay_verify import (
    FIXTURE_SCHEMA,
    REASON_BUNDLE_INVALID,
    REASON_EXPECTATION_MISMATCH,
    REASON_NONE,
    REPORT_SCHEMA,
    REPORT_SCHEMA_VERSION,
    STATUS_DRIFTED,
    STATUS_FAILED,
    STATUS_REPLAYED,
    main as replay_verify_main,
    verify_golden_history,
    verify_replay,
)
from durable_workflow.workflow import WorkflowContext


@workflow.defn(name="replay-verify.greet")
class GreetWorkflow:
    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        return (yield ctx.schedule_activity("greet", [name]))


def test_verify_replay_reports_clean_replay_with_expectation_match() -> None:
    history = [
        {"event_type": "ActivityCompleted", "payload": {"result": "\"hello Ada\""}},
    ]

    report = verify_replay(
        GreetWorkflow,
        history,
        start_input=["Ada"],
        expected={"command_type": "CompleteWorkflow", "result": "hello Ada"},
        case_id="greet-clean",
    )

    assert report.status == STATUS_REPLAYED
    assert report.reason == REASON_NONE
    assert report.workflow_type == "replay-verify.greet"
    assert report.observed == {"command_type": "CompleteWorkflow", "result": "hello Ada"}
    assert report.error is None


def test_verify_replay_surfaces_expectation_mismatch_as_drift() -> None:
    history = [
        {"event_type": "ActivityCompleted", "payload": {"result": "\"hello Ada\""}},
    ]

    report = verify_replay(
        GreetWorkflow,
        history,
        start_input=["Ada"],
        expected={"command_type": "CompleteWorkflow", "result": "different"},
        case_id="greet-expected-mismatch",
    )

    assert report.status == STATUS_DRIFTED
    assert report.reason == REASON_EXPECTATION_MISMATCH
    assert report.error is not None
    assert "does not match expected" in report.error["message"]


def test_verify_golden_history_reports_missing_fixtures(tmp_path: Path) -> None:
    report = verify_golden_history(tmp_path, [GreetWorkflow])

    assert report.status == STATUS_FAILED
    assert report.summary["fixtures"] == 0
    assert any(case.reason == REASON_BUNDLE_INVALID for case in report.cases)


def test_verify_golden_history_replays_clean_fixture(tmp_path: Path) -> None:
    fixture = {
        "fixture_schema": FIXTURE_SCHEMA,
        "source": {
            "runtime": "sdk-python",
            "package": "durable-workflow",
            "version": "0.0.0-test",
            "worker_protocol_version": "1.0",
            "control_plane_version": "2",
        },
        "cases": [
            {
                "name": "single_activity",
                "family": "activity",
                "workflow_type": "replay-verify.greet",
                "start_input": ["Ada"],
                "history": [
                    {
                        "event_type": "ActivityCompleted",
                        "payload": {"result": "\"hello Ada\""},
                    }
                ],
                "expected": {
                    "command_type": "CompleteWorkflow",
                    "result": "hello Ada",
                },
            }
        ],
    }

    fixture_path = tmp_path / "test-runtime-0.0.0.json"
    fixture_path.write_text(json.dumps(fixture), encoding="utf-8")

    report = verify_golden_history(
        tmp_path,
        [GreetWorkflow],
        required_families={"activity"},
    )

    assert report.status == STATUS_REPLAYED
    assert report.summary == {
        "fixtures": 1,
        "cases": 1,
        "replayed": 1,
        "drifted": 0,
        "failed": 0,
    }
    assert report.missing_families == []
    assert report.cases[0].status == STATUS_REPLAYED
    assert report.cases[0].family == "activity"


def test_verify_golden_history_flags_missing_required_family(tmp_path: Path) -> None:
    fixture = {
        "fixture_schema": FIXTURE_SCHEMA,
        "source": {
            "runtime": "sdk-python",
            "package": "durable-workflow",
            "version": "0.0.0-test",
        },
        "cases": [
            {
                "name": "single_activity",
                "family": "activity",
                "workflow_type": "replay-verify.greet",
                "start_input": ["Ada"],
                "history": [
                    {
                        "event_type": "ActivityCompleted",
                        "payload": {"result": "\"hello Ada\""},
                    }
                ],
                "expected": {
                    "command_type": "CompleteWorkflow",
                    "result": "hello Ada",
                },
            }
        ],
    }

    fixture_path = tmp_path / "test-runtime-0.0.0.json"
    fixture_path.write_text(json.dumps(fixture), encoding="utf-8")

    report = verify_golden_history(
        tmp_path,
        [GreetWorkflow],
        required_families={"activity", "saga-compensation"},
    )

    assert report.status == STATUS_FAILED
    assert "saga-compensation" in report.missing_families


def test_verify_golden_history_drift_blocks_promotion(tmp_path: Path) -> None:
    fixture = {
        "fixture_schema": FIXTURE_SCHEMA,
        "source": {
            "runtime": "sdk-python",
            "package": "durable-workflow",
            "version": "0.0.0-test",
        },
        "cases": [
            {
                "name": "expected_drift",
                "family": "activity",
                "workflow_type": "replay-verify.greet",
                "start_input": ["Ada"],
                "history": [
                    {
                        "event_type": "ActivityCompleted",
                        "payload": {"result": "\"hello Ada\""},
                    }
                ],
                "expected": {
                    "command_type": "CompleteWorkflow",
                    "result": "different",
                },
            }
        ],
    }

    fixture_path = tmp_path / "test-runtime-0.0.0.json"
    fixture_path.write_text(json.dumps(fixture), encoding="utf-8")

    report = verify_golden_history(tmp_path, [GreetWorkflow], required_families={"activity"})

    assert report.status == STATUS_DRIFTED
    assert report.cases[0].status == STATUS_DRIFTED
    assert report.cases[0].reason == REASON_EXPECTATION_MISMATCH


def test_report_to_dict_uses_published_schema(tmp_path: Path) -> None:
    fixture = {
        "fixture_schema": FIXTURE_SCHEMA,
        "source": {
            "runtime": "sdk-python",
            "package": "durable-workflow",
            "version": "0.0.0-test",
        },
        "cases": [],
    }
    fixture_path = tmp_path / "empty.json"
    fixture_path.write_text(json.dumps(fixture), encoding="utf-8")

    report = verify_golden_history(tmp_path, [GreetWorkflow], required_families=set())

    payload = report.to_dict()
    assert payload["schema"] == REPORT_SCHEMA
    assert payload["schema_version"] == REPORT_SCHEMA_VERSION


def _greet_workflows() -> list[type]:
    return [GreetWorkflow]


def test_cli_writes_json_report(tmp_path: Path) -> None:
    fixture = {
        "fixture_schema": FIXTURE_SCHEMA,
        "source": {
            "runtime": "sdk-python",
            "package": "durable-workflow",
            "version": "0.0.0-test",
        },
        "cases": [
            {
                "name": "single_activity",
                "family": "activity",
                "workflow_type": "replay-verify.greet",
                "start_input": ["Ada"],
                "history": [
                    {
                        "event_type": "ActivityCompleted",
                        "payload": {"result": "\"hello Ada\""},
                    }
                ],
                "expected": {"command_type": "CompleteWorkflow", "result": "hello Ada"},
            }
        ],
    }

    fixture_dir = tmp_path / "fixtures"
    fixture_dir.mkdir()
    (fixture_dir / "test-runtime-0.0.0.json").write_text(json.dumps(fixture), encoding="utf-8")
    output_path = tmp_path / "report.json"

    exit_code = replay_verify_main(
        [
            str(fixture_dir),
            "--workflows",
            f"{__name__}:_greet_workflows",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 0
    payload = json.loads(output_path.read_text())
    assert payload["status"] == STATUS_REPLAYED
    assert payload["schema"] == REPORT_SCHEMA


def test_cli_returns_failure_for_drifted_replay(tmp_path: Path) -> None:
    fixture = {
        "fixture_schema": FIXTURE_SCHEMA,
        "source": {
            "runtime": "sdk-python",
            "package": "durable-workflow",
            "version": "0.0.0-test",
        },
        "cases": [
            {
                "name": "expected_drift",
                "family": "activity",
                "workflow_type": "replay-verify.greet",
                "start_input": ["Ada"],
                "history": [
                    {
                        "event_type": "ActivityCompleted",
                        "payload": {"result": "\"hello Ada\""},
                    }
                ],
                "expected": {"command_type": "CompleteWorkflow", "result": "different"},
            }
        ],
    }

    fixture_dir = tmp_path / "fixtures"
    fixture_dir.mkdir()
    (fixture_dir / "test-runtime-0.0.0.json").write_text(json.dumps(fixture), encoding="utf-8")
    output_path = tmp_path / "report.json"

    exit_code = replay_verify_main(
        [
            str(fixture_dir),
            "--workflows",
            f"{__name__}:_greet_workflows",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 1
    payload = json.loads(output_path.read_text())
    assert payload["status"] == STATUS_DRIFTED
