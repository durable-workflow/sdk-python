from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from durable_workflow.python_conformance import (
    REQUIRED_ARTIFACTS,
    REQUIRED_CAPABILITIES,
    REQUIRED_SCENARIOS,
    evaluate_result,
    manifest,
)


def complete_result() -> dict[str, Any]:
    artifacts = {
        "server": "0.2.168",
        "cli": "0.1.55",
        "sdk-python": "0.4.71",
        "workflow": "2.0.0-alpha.172",
        "waterline": "2.0.0-alpha.57",
    }
    scenario_results = {
        scenario: {
            "scenario_id": scenario,
            "status": "pass",
            "observed_outputs": {"summary": f"{scenario} passed"},
            "linked_findings": [],
        }
        for scenario in REQUIRED_SCENARIOS
    }
    scenario_results["published_artifact_install_only"].update(
        {
            "install_channels": {
                "server": "docker",
                "cli": "install script",
                "sdk-python": "pypi",
            },
            "artifact_versions": artifacts,
            "source_policy": {"artifact_source": "published_artifacts", "local_product_sources_used": False},
        }
    )
    scenario_results["official_cli_install_start_result_path"].update(
        {
            "cli_install": {"command": "curl -fsSL https://durable-workflow.com/install.sh | sh"},
            "cli_start": {"command": "dw workflow:start --json"},
            "cli_result": {"command": "dw workflow:result --json"},
            "cli_evidence": {
                "install_command": "curl -fsSL https://durable-workflow.com/install.sh | sh",
                "start_command": "dw workflow:start --json",
                "result_command": "dw workflow:result --json",
                "json_outputs": [{"workflow_id": "py-parity"}],
            },
        }
    )
    scenario_results["cold_first_user_setup"].update(
        {
            "fresh_state": True,
            "first_user_flow": {"created": True},
            "cold_setup": {
                "fresh_state": True,
                "namespace_created": "default",
                "first_workflow_started": "py-parity",
                "result_observed": {"status": "completed"},
            },
        }
    )
    scenario_results["python_worker_registration"].update(
        {
            "registered_workflows": ["py.parity.workflow"],
            "registered_activities": ["py.parity.activity"],
            "worker_identity": "python-worker",
        }
    )
    scenario_results["activity_backed_workflow_execution"].update(
        {
            "workflow_execution": {"status": "completed"},
            "activity_execution": {"status": "completed"},
        }
    )
    scenario_results["workflow_result_surface"].update(
        {
            "result_observed": True,
            "result_value": {"ok": True},
        }
    )
    scenario_results["worker_restart_activity_and_signal_state"].update(
        {
            "restart_boundary": {"worker": "restarted"},
            "activity_state_after_restart": {"activity": "replayed"},
            "signal_state_after_restart": {"signal": "replayed"},
        }
    )
    scenario_results["protocol_trace_capture"].update(
        {
            "control_plane_traces": [{"method": "POST", "path": "/workflows"}],
            "worker_protocol_traces": [{"method": "POST", "path": "/worker/workflow-tasks/poll"}],
            "protocol_traces": [{"request": "POST /workflows", "response": 201}],
        }
    )
    scenario_results["php_assumption_audit"].update(
        {
            "server_cli_audit": {"status": "pass"},
            "sdk_runtime_audit": {"status": "pass"},
            "php_assumption_audit": {
                "status": "pass",
                "checks": {
                    "no_php_runtime_required": True,
                    "no_php_paths_required": True,
                    "no_php_serializer_required": True,
                    "no_php_only_error_shapes": True,
                },
            },
        }
    )
    scenario_results["capability_table_complete"].update(
        {
            "capability_table": [
                {"id": capability, "status": "pass"}
                for capability in REQUIRED_CAPABILITIES
            ],
        }
    )

    return {
        "schema": "durable-workflow.v2.python-sdk-parity.result",
        "version": 1,
        "outcome": "pass",
        "started_at": "2026-05-21T01:00:00Z",
        "finished_at": "2026-05-21T01:05:00Z",
        "generated_at": "2026-05-21T01:05:01Z",
        "artifact_versions": artifacts,
        "source_policy": {"artifact_source": "published_artifacts", "local_product_sources_used": False},
        "scenario_results": scenario_results,
        "capability_table": [
            {"id": capability, "status": "pass", "evidence": {"observed": True}}
            for capability in REQUIRED_CAPABILITIES
        ],
        "protocol_traces": [{"request": "POST /workflows", "response": 201}],
        "php_assumption_audit": {
            "status": "pass",
            "checks": {
                "no_php_runtime_required": True,
                "no_php_paths_required": True,
                "no_php_serializer_required": True,
                "no_php_only_error_shapes": True,
            },
        },
        "findings": [],
        "finding_links": [],
    }


def test_manifest_names_full_python_parity_surface() -> None:
    payload = manifest()

    assert payload["schema"] == "durable-workflow.v2.python-sdk-parity.contract"
    assert payload["required_scenarios"] == list(REQUIRED_SCENARIOS)
    assert payload["artifact_policy"]["required_artifacts"] == list(REQUIRED_ARTIFACTS)
    assert payload["coverage_gate"]["smoke_subset_outcome"] == "non_passing"
    assert "official_cli_install_start_result_path" in payload["required_scenarios"]
    assert "cold_first_user_setup" in payload["required_scenarios"]
    assert "protocol_trace_capture" in payload["required_scenarios"]
    assert "php_assumption_audit" in payload["required_scenarios"]
    assert "capability_table_complete" in payload["required_scenarios"]


def test_evaluate_result_accepts_complete_published_artifact_evidence() -> None:
    evaluation = evaluate_result(complete_result())

    assert evaluation["status"] == "pass"
    assert evaluation["missing_scenarios"] == []
    assert evaluation["missing_capabilities"] == []
    assert evaluation["gate_failures"] == []


def test_evaluate_result_accepts_advertised_artifact_version_run_record_alias() -> None:
    result = complete_result()
    result["publishedArtifactVersions"] = result.pop("artifact_versions")

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "pass"
    assert {
        "code": "missing_run_record_field",
        "field": "artifact_versions",
    } not in evaluation["gate_failures"]


def test_evaluate_result_accepts_advertised_declared_outcome_aliases() -> None:
    for field in ("status", "verdict"):
        result = complete_result()
        result[field] = result.pop("outcome")

        evaluation = evaluate_result(result)

        assert evaluation["status"] == "pass"
        assert {
            "code": "missing_run_record_field",
            "field": "outcome",
        } not in evaluation["gate_failures"]


def test_evaluate_result_rejects_smoke_only_evidence() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    result["scenario_results"] = {
        "python_worker_registration": result["scenario_results"]["python_worker_registration"],
        "activity_backed_workflow_execution": result["scenario_results"]["activity_backed_workflow_execution"],
        "workflow_result_surface": result["scenario_results"]["workflow_result_surface"],
        "worker_restart_activity_and_signal_state": result["scenario_results"][
            "worker_restart_activity_and_signal_state"
        ],
    }
    result["capability_table"] = [
        {
            "id": "python_workflow_runs",
            "status": "pass",
            "evidence": {"observed": True},
        },
        {
            "id": "python_activity_runs",
            "status": "pass",
            "evidence": {"observed": True},
        },
        {
            "id": "workflow_result_returned",
            "status": "pass",
            "evidence": {"observed": True},
        },
    ]

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert evaluation["smoke_subset_detected"] is True
    failure_codes = {failure["code"] for failure in evaluation["gate_failures"]}
    assert "missing_required_scenario" in failure_codes
    assert "missing_required_capability" in failure_codes
    assert "smoke_subset_cannot_pass" in failure_codes


def test_evaluate_result_rejects_placeholder_artifact_versions() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    result["artifact_versions"]["server"] = "latest"
    result["scenario_results"]["published_artifact_install_only"]["artifact_versions"]["server"] = "latest"

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert {
        "code": "placeholder_artifact_version",
        "artifact": "server",
        "version": "latest",
    } in evaluation["gate_failures"]


def test_evaluate_result_rejects_scenario_local_product_source_evidence() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    result.pop("source_policy")
    install_scenario = result["scenario_results"]["published_artifact_install_only"]
    install_scenario["source_policy"] = {
        "artifact_source": "published_artifacts",
        "local_product_sources_used": True,
    }
    install_scenario["local_product_source_artifacts"] = {
        "sdk-python": "workspace_repo_as_artifact_under_test",
    }

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert {
        "code": "local_product_source_artifacts_used",
        "scope": "scenario",
        "scenario_id": "published_artifact_install_only",
        "value": True,
    } in evaluation["gate_failures"]
    assert {
        "code": "local_product_source_artifacts_reported",
        "scope": "scenario",
        "scenario_id": "published_artifact_install_only",
        "value": {"sdk-python": "workspace_repo_as_artifact_under_test"},
    } in evaluation["gate_failures"]


def test_evaluate_result_rejects_non_install_scenario_local_product_source_artifact() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    worker_scenario = result["scenario_results"]["python_worker_registration"]
    worker_scenario["productSourceArtifacts"] = {
        "sdk-python": "workspace_repo_as_artifact_under_test",
    }

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert {
        "code": "local_product_source_artifacts_reported",
        "scope": "scenario",
        "scenario_id": "python_worker_registration",
        "value": {"sdk-python": "workspace_repo_as_artifact_under_test"},
    } in evaluation["gate_failures"]


def test_evaluate_result_rejects_extra_non_pass_scenario_without_linked_finding() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    result["scenario_results"]["optional_protocol_probe"] = {
        "scenario_id": "optional_protocol_probe",
        "status": "unsupported",
        "observed_outputs": {"summary": "probe is not supported"},
    }

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert {
        "code": "missing_non_pass_finding",
        "scenario_id": "optional_protocol_probe",
        "status": "unsupported",
    } in evaluation["gate_failures"]


def test_evaluate_result_rejects_install_scenario_source_policy_without_artifact_proof() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    result.pop("source_policy")
    install_scenario = result["scenario_results"]["published_artifact_install_only"]
    install_scenario["source_policy"] = {"policy": "install from release artifacts"}

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert {
        "code": "missing_published_artifact_source",
        "scope": "scenario",
        "scenario_id": "published_artifact_install_only",
        "value": "",
    } in evaluation["gate_failures"]
    assert {
        "code": "missing_local_product_sources_used",
        "scope": "scenario",
        "scenario_id": "published_artifact_install_only",
        "value": None,
    } in evaluation["gate_failures"]


def test_evaluate_result_rejects_install_scenario_source_policy_without_local_source_proof() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    result.pop("source_policy")
    install_scenario = result["scenario_results"]["published_artifact_install_only"]
    install_scenario["source_policy"] = {"artifact_source": "published_artifacts"}

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert {
        "code": "missing_local_product_sources_used",
        "scope": "scenario",
        "scenario_id": "published_artifact_install_only",
        "value": None,
    } in evaluation["gate_failures"]


def test_cli_evaluate_exits_nonzero_for_incomplete_result(tmp_path: Path) -> None:
    result_file = tmp_path / "result.json"
    result_file.write_text(json.dumps({"outcome": "pass"}), encoding="utf-8")

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "durable_workflow.python_conformance",
            "--evaluate",
            str(result_file),
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 1
    assert "missing_required_scenario" in proc.stdout
