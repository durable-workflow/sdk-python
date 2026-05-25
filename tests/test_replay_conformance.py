from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from durable_workflow.replay_conformance import (
    COVERAGE_SCOPE,
    RESULT_SCHEMA,
    RESULT_VERSION,
    compose_report,
    main as replay_conformance_main,
)


def artifact_versions() -> dict[str, str]:
    return {
        "server": "0.2.171",
        "cli": "0.1.55",
        "workflow": "2.0.0-alpha.175",
        "sdk-python": "0.4.73",
        "waterline": "2.0.0-alpha.57",
    }


def artifact_sources() -> dict[str, str]:
    return {
        "server": "docker_image",
        "cli": "official_install_script",
        "workflow": "composer_package",
        "sdk-python": "pypi_package",
        "waterline": "packagist_package",
    }


def scenarios(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(scenario["scenario_id"]): scenario
        for scenario in report["scenario_results"]
    }


def test_compose_report_emits_python_replay_conformance_shard() -> None:
    report = compose_report(
        artifact_versions=artifact_versions(),
        artifact_sources=artifact_sources(),
    )
    indexed = scenarios(report)

    assert report["schema"] == RESULT_SCHEMA
    assert report["schema_version"] == RESULT_VERSION
    assert report["coverage_scope"] == COVERAGE_SCOPE
    assert report["outcome"] == "pass"
    assert report["runtime_matrix"]["runtimes"] == ["sdk-python"]
    assert report["artifact_versions"]["workflow-php"] == "2.0.0-alpha.175"
    assert report["artifact_sources"]["workflow-php"] == "composer_package"
    assert {scenario["status"] for scenario in report["scenario_results"]} == {"pass"}
    assert report["findings"] == []
    assert report["finding_links"] == {}

    for scenario_id in [
        "published_artifact_install_only",
        "python_completed_history_activity_replay",
        "python_completed_history_signal_update_replay",
        "python_completed_history_wait_condition_replay",
        "python_completed_history_version_marker_replay",
        "python_completed_history_saga_compensation_replay",
        "python_worker_restart_completed_query",
        "python_worker_restart_activity_state",
        "python_worker_restart_signal_update_state",
        "python_worker_restart_wait_condition_state",
        "python_worker_restart_version_marker_state",
        "python_worker_restart_saga_compensation_state",
        "python_code_divergence_refusal",
        "server_history_mutation_refusal",
        "malformed_history_refusal",
        "python_in_flight_signal_restart_timing",
    ]:
        assert indexed[scenario_id]["status"] == "pass"


def test_report_proves_published_artifact_install_policy() -> None:
    report = compose_report(
        artifact_versions=artifact_versions(),
        artifact_sources=artifact_sources(),
    )
    published = scenarios(report)["published_artifact_install_only"]
    observed = published["observed_outputs"]

    assert published["status"] == "pass"
    assert observed["published_artifacts_only"] is True
    assert observed["published_install_tuple_proven"] is True
    assert observed["missing_artifacts"] == []
    assert observed["artifact_sources"]["server"] == "docker_image"
    assert observed["artifact_sources"]["sdk-python"] == "pypi_package"


def test_report_rejects_missing_versions_and_sources() -> None:
    report = compose_report(
        artifact_versions={"sdk-python": "0.4.73"},
        artifact_sources={"sdk-python": "pypi_package"},
    )
    published = scenarios(report)["published_artifact_install_only"]
    observed = published["observed_outputs"]

    assert report["outcome"] == "fail"
    assert published["status"] == "fail"
    assert "server" in observed["missing_artifacts"]
    assert "workflow-php" in observed["missing_artifact_versions"]
    assert "waterline" in observed["missing_artifact_sources"]
    assert report["finding_links"]["published_artifact_install_only"]


def test_report_rejects_dev_versions_and_local_sources() -> None:
    versions = artifact_versions()
    versions["workflow"] = "dev-main"
    sources = artifact_sources()
    sources["server"] = "workspace_repo_as_artifact_under_test"

    report = compose_report(artifact_versions=versions, artifact_sources=sources)
    published = scenarios(report)["published_artifact_install_only"]
    observed = published["observed_outputs"]

    assert report["outcome"] == "fail"
    assert published["status"] == "fail"
    assert observed["rejected_versions"]["workflow-php"]["version"] == "dev-main"
    assert observed["forbidden_sources"]["server"] == "workspace_repo_as_artifact_under_test"
    assert observed["published_artifacts_only"] is False


def test_code_divergence_refusal_has_actionable_diagnostics() -> None:
    report = compose_report(
        artifact_versions=artifact_versions(),
        artifact_sources=artifact_sources(),
    )
    observed = scenarios(report)["python_code_divergence_refusal"]["observed_outputs"]

    assert observed["observed_outcome"] == "non_determinism_error"
    assert observed["workflow_sequence"] == 1
    assert observed["expected_shape"] == "activity:python-replay.farewell"
    assert observed["recorded_event_types"] == ["ActivityCompleted"]
    assert observed["message"]


def test_history_refusal_scenarios_surface_integrity_findings() -> None:
    report = compose_report(
        artifact_versions=artifact_versions(),
        artifact_sources=artifact_sources(),
    )
    indexed = scenarios(report)
    mutated = indexed["server_history_mutation_refusal"]["observed_outputs"]
    malformed = indexed["malformed_history_refusal"]["observed_outputs"]

    assert mutated["observed_outcome"] == "bundle_invalid_or_drifted"
    assert mutated["integrity"]["rule"] == "integrity.checksum_mismatch"
    assert mutated["replay_diff"]["reason"] == "bundle_invalid"
    assert malformed["observed_outcome"] == "bundle_invalid_or_failed"
    assert malformed["integrity"]["rule"] == "workflow.run_id_missing"


def test_in_flight_signal_replay_records_same_next_decision() -> None:
    report = compose_report(
        artifact_versions=artifact_versions(),
        artifact_sources=artifact_sources(),
    )
    observed = scenarios(report)["python_in_flight_signal_restart_timing"]["observed_outputs"]

    assert observed["observed_outcome"] == "same_next_decision_after_replay"
    assert observed["worker_restart_at"] == "after_signal_history_reload"
    assert observed["signal_sent_at"] == "history_event:SignalReceived"
    assert observed["replayed_next_decision"] == {
        "command_type": "ScheduleActivity",
        "activity_type": "python-replay.greet",
        "arguments": ["Grace"],
    }


def test_cli_writes_json_report(tmp_path: Path) -> None:
    output_path = tmp_path / "report.json"

    exit_code = replay_conformance_main([
        "--artifact-version=server=0.2.171",
        "--artifact-version=cli=0.1.55",
        "--artifact-version=workflow=2.0.0-alpha.175",
        "--artifact-version=sdk-python=0.4.73",
        "--artifact-version=waterline=2.0.0-alpha.57",
        "--artifact-source=server=docker_image",
        "--artifact-source=cli=official_install_script",
        "--artifact-source=workflow=composer_package",
        "--artifact-source=sdk-python=pypi_package",
        "--artifact-source=waterline=packagist_package",
        "--output",
        str(output_path),
    ])

    assert exit_code == 0
    payload = json.loads(output_path.read_text())
    assert payload["schema"] == RESULT_SCHEMA
    assert payload["coverage_scope"] == COVERAGE_SCOPE
    assert payload["outcome"] == "pass"


def test_cli_exits_nonzero_when_required_artifact_evidence_is_missing(tmp_path: Path) -> None:
    output_path = tmp_path / "report.json"

    exit_code = replay_conformance_main([
        "--artifact-version=sdk-python=0.4.73",
        "--artifact-source=sdk-python=pypi_package",
        "--output",
        str(output_path),
    ])

    assert exit_code == 1
    payload = json.loads(output_path.read_text())
    assert payload["outcome"] == "fail"
    assert scenarios(payload)["published_artifact_install_only"]["status"] == "fail"
