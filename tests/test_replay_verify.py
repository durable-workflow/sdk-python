"""Tests for the offline replay verification surface."""

from __future__ import annotations

import json
from pathlib import Path

from durable_workflow import workflow
from durable_workflow.replay_verify import (
    FIXTURE_SCHEMA,
    PROMOTION_BLOCK_AND_INVESTIGATE,
    PROMOTION_BLOCK_UNTIL_COMPATIBLE,
    PROMOTION_REVIEW_BEFORE_PROMOTE,
    PROMOTION_SAFE_TO_PROMOTE,
    REASON_BUNDLE_INVALID,
    REASON_EXPECTATION_MISMATCH,
    REASON_NONE,
    REPORT_SCHEMA,
    REPORT_SCHEMA_VERSION,
    SIMULATION_REPORT_SCHEMA,
    SIMULATION_REPORT_SCHEMA_VERSION,
    STATUS_DRIFTED,
    STATUS_FAILED,
    STATUS_REPLAYED,
    VERDICT_DRIFTED,
    VERDICT_FAILED,
    VERDICT_OK,
    aggregate_verdicts,
    main as replay_verify_main,
    promotion_decision_for,
    simulate_bundles,
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


def test_promotion_decision_for_known_verdicts() -> None:
    assert promotion_decision_for(VERDICT_OK) == PROMOTION_SAFE_TO_PROMOTE
    assert promotion_decision_for("warning") == PROMOTION_REVIEW_BEFORE_PROMOTE
    assert promotion_decision_for(VERDICT_DRIFTED) == PROMOTION_BLOCK_UNTIL_COMPATIBLE
    assert promotion_decision_for(VERDICT_FAILED) == PROMOTION_BLOCK_AND_INVESTIGATE


def test_promotion_decision_for_unknown_verdict_blocks() -> None:
    assert promotion_decision_for("totally-bogus") == PROMOTION_BLOCK_AND_INVESTIGATE


def test_aggregate_verdicts_picks_strictest() -> None:
    assert aggregate_verdicts([VERDICT_OK, VERDICT_OK]) == VERDICT_OK
    assert aggregate_verdicts([VERDICT_OK, "warning"]) == "warning"
    assert aggregate_verdicts([VERDICT_OK, VERDICT_DRIFTED, "warning"]) == VERDICT_DRIFTED
    assert aggregate_verdicts([VERDICT_OK, VERDICT_FAILED]) == VERDICT_FAILED
    assert aggregate_verdicts([]) == VERDICT_FAILED
    assert aggregate_verdicts([VERDICT_OK, "totally-new"]) == VERDICT_FAILED


def test_golden_history_report_to_dict_includes_promotion_decision(tmp_path: Path) -> None:
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

    (tmp_path / "fixture.json").write_text(json.dumps(fixture), encoding="utf-8")

    report = verify_golden_history(tmp_path, [GreetWorkflow], required_families={"activity"})

    payload = report.to_dict()
    assert payload["verdict"] == VERDICT_OK
    assert payload["promotion_decision"] == PROMOTION_SAFE_TO_PROMOTE
    assert payload["cases"][0]["promotion_decision"] == PROMOTION_SAFE_TO_PROMOTE


def test_golden_history_drift_yields_block_until_compatible(tmp_path: Path) -> None:
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

    (tmp_path / "fixture.json").write_text(json.dumps(fixture), encoding="utf-8")

    report = verify_golden_history(tmp_path, [GreetWorkflow], required_families={"activity"})

    payload = report.to_dict()
    assert payload["verdict"] == VERDICT_DRIFTED
    assert payload["promotion_decision"] == PROMOTION_BLOCK_UNTIL_COMPATIBLE


def test_simulate_bundles_returns_failed_for_missing_directory(tmp_path: Path) -> None:
    report = simulate_bundles(tmp_path / "no-such-dir")

    assert report.verdict == VERDICT_FAILED
    assert report.promotion_decision == PROMOTION_BLOCK_AND_INVESTIGATE
    assert report.error is not None


def test_simulate_bundles_aggregates_per_bundle_verdicts(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()

    # Bundle 1: completely empty — fails integrity (missing required sections etc.)
    (bundle_dir / "bad.json").write_text("{}", encoding="utf-8")

    # Bundle 2: malformed JSON — also fails
    (bundle_dir / "broken.json").write_text("not json", encoding="utf-8")

    report = simulate_bundles(bundle_dir)
    payload = report.to_dict()

    assert payload["schema"] == SIMULATION_REPORT_SCHEMA
    assert payload["schema_version"] == SIMULATION_REPORT_SCHEMA_VERSION
    assert payload["verdict"] == VERDICT_FAILED
    assert payload["promotion_decision"] == PROMOTION_BLOCK_AND_INVESTIGATE
    assert payload["summary"]["total"] == 2
    assert payload["summary"][VERDICT_FAILED] == 2
    for entry in payload["bundles"]:
        assert entry["verdict"] == VERDICT_FAILED
        assert entry["promotion_decision"] == PROMOTION_BLOCK_AND_INVESTIGATE


def test_simulate_bundles_cli(tmp_path: Path) -> None:
    bundle_dir = tmp_path / "bundles"
    bundle_dir.mkdir()
    (bundle_dir / "bad.json").write_text("{}", encoding="utf-8")
    output_path = tmp_path / "sim.json"

    exit_code = replay_verify_main(
        [
            str(bundle_dir),
            "--simulate-bundles",
            "--output",
            str(output_path),
        ]
    )

    assert exit_code == 1
    payload = json.loads(output_path.read_text())
    assert payload["schema"] == SIMULATION_REPORT_SCHEMA
    assert payload["verdict"] == VERDICT_FAILED
    assert payload["promotion_decision"] == PROMOTION_BLOCK_AND_INVESTIGATE


def test_cli_requires_workflows_when_not_simulating(tmp_path: Path) -> None:
    import pytest

    fixture_dir = tmp_path / "fixtures"
    fixture_dir.mkdir()

    with pytest.raises(SystemExit) as excinfo:
        replay_verify_main([str(fixture_dir)])

    assert excinfo.value.code == 2
