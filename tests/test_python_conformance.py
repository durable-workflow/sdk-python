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
    compose_result,
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
            "cli_result": {"command": "dw workflow:describe py-parity --json"},
            "cli_evidence": {
                "install_command": "curl -fsSL https://durable-workflow.com/install.sh | sh",
                "start_command": "dw workflow:start --json",
                "describe_command": "dw workflow:describe py-parity --json",
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


def complete_host_evidence() -> dict[str, Any]:
    result = complete_result()
    return {
        "started_at": result["started_at"],
        "finished_at": result["finished_at"],
        "generated_at": result["generated_at"],
        "artifact_versions": result["artifact_versions"],
        "source_policy": result["source_policy"],
        "install_channels": result["scenario_results"]["published_artifact_install_only"]["install_channels"],
        "cli_evidence": result["scenario_results"]["official_cli_install_start_result_path"]["cli_evidence"],
        "cold_setup": result["scenario_results"]["cold_first_user_setup"]["cold_setup"],
        "protocol_traces": [
            {"plane": "control", "request": "POST /workflows", "response": 201},
            {"plane": "worker", "request": "POST /worker/workflow-tasks/poll", "response": 200},
        ],
        "php_assumption_audit": {
            **result["php_assumption_audit"],
            "server_cli_audit": {"status": "pass"},
            "sdk_runtime_audit": {"status": "pass"},
        },
        "scenario_results": {
            scenario: scenario_result
            for scenario, scenario_result in result["scenario_results"].items()
            if scenario
            not in {
                "published_artifact_install_only",
                "official_cli_install_start_result_path",
                "cold_first_user_setup",
                "protocol_trace_capture",
                "php_assumption_audit",
                "capability_table_complete",
            }
        },
        "capabilities": {
            capability: {"status": "pass", "evidence": {"observed": True}}
            for capability in REQUIRED_CAPABILITIES
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
    assert payload["host_evidence"]["schema"] == "durable-workflow.v2.python-sdk-parity.host-evidence"
    assert payload["host_evidence"]["missing_scenario_status"] == "not_covered"
    assert "server-up" in payload["host_evidence"]["entry_id_aliases"]["capability_ids"]["server_up"]
    assert "local_product_source_checkouts_used" in payload["host_evidence"]["top_level_evidence_fields"]


def test_evaluate_result_accepts_complete_published_artifact_evidence() -> None:
    evaluation = evaluate_result(complete_result())

    assert evaluation["status"] == "pass"
    assert evaluation["missing_scenarios"] == []
    assert evaluation["missing_capabilities"] == []
    assert evaluation["gate_failures"] == []


def test_compose_result_accepts_full_host_runner_evidence() -> None:
    evidence = complete_host_evidence()

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["schema"] == "durable-workflow.v2.python-sdk-parity.result"
    assert composed["outcome"] == "pass"
    assert composed["scenario_results"]["official_cli_install_start_result_path"]["status"] == "pass"
    assert composed["scenario_results"]["protocol_trace_capture"]["status"] == "pass"
    assert len(composed["capability_table"]) == len(REQUIRED_CAPABILITIES)
    assert evaluation["status"] == "pass"
    assert evaluation["gate_failures"] == []


def test_compose_result_accepts_runner_native_host_evidence_aliases() -> None:
    result = complete_result()
    evidence = {
        "startedAt": result["started_at"],
        "finishedAt": result["finished_at"],
        "generatedAt": result["generated_at"],
        "publishedArtifactVersions": result["artifact_versions"],
        "sourcePolicy": result["source_policy"],
        "installChannels": result["scenario_results"]["published_artifact_install_only"]["install_channels"],
        "officialCli": {
            "install": {"command": "curl -fsSL https://durable-workflow.com/install.sh | sh"},
            "workflowStart": {"command": "dw workflow:start --json"},
            "workflowDescribe": {"command": "dw workflow:describe py-parity --json"},
            "outputs": [{"workflow_id": "py-parity", "status": "completed"}],
        },
        "firstUserFlow": {
            "freshState": True,
            "namespace_created": "default",
            "first_workflow_started": "py-parity",
            "result_observed": {"status": "completed"},
        },
        "traces": [
            {"plane": "control", "request": "POST /workflows", "response": 201},
            {"plane": "worker", "request": "POST /worker/workflow-tasks/poll", "response": 200},
        ],
        "languageNeutralityAudit": {
            "status": "pass",
            "serverAudit": {"status": "pass"},
            "sdkAudit": {"status": "pass"},
            "checks": {
                "no_php_runtime_required": True,
                "no_php_paths_required": True,
                "no_php_serializer_required": True,
                "no_php_only_error_shapes": True,
            },
        },
        "scenarioEvidence": {
            scenario: scenario_result
            for scenario, scenario_result in result["scenario_results"].items()
            if scenario
            not in {
                "published_artifact_install_only",
                "official_cli_install_start_result_path",
                "cold_first_user_setup",
                "protocol_trace_capture",
                "php_assumption_audit",
                "capability_table_complete",
            }
        },
        "capabilityResults": {
            capability: {"status": "pass", "evidence": {"observed": True}}
            for capability in REQUIRED_CAPABILITIES
        },
        "findings": [],
        "findingLinks": [],
    }

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["outcome"] == "pass"
    assert composed["scenario_results"]["official_cli_install_start_result_path"]["cli_install"] == {
        "command": "curl -fsSL https://durable-workflow.com/install.sh | sh"
    }
    assert composed["scenario_results"]["official_cli_install_start_result_path"]["cli_result"] == {
        "command": "dw workflow:describe py-parity --json"
    }
    assert composed["scenario_results"]["cold_first_user_setup"]["fresh_state"] is True
    assert composed["scenario_results"]["php_assumption_audit"]["server_cli_audit"] == {"status": "pass"}
    assert evaluation["status"] == "pass"
    assert evaluation["gate_failures"] == []


def test_compose_result_accepts_runbook_style_ids_and_statuses() -> None:
    result = complete_result()
    derived_scenarios = {
        "published_artifact_install_only",
        "official_cli_install_start_result_path",
        "cold_first_user_setup",
        "protocol_trace_capture",
        "php_assumption_audit",
        "capability_table_complete",
    }
    scenario_evidence = []
    for scenario in REQUIRED_SCENARIOS:
        if scenario in derived_scenarios:
            continue
        entry = dict(result["scenario_results"][scenario])
        entry.pop("scenario_id", None)
        entry["scenario"] = scenario.replace("_", "-")
        entry["status"] = "succeeded"
        scenario_evidence.append(entry)

    def capability_alias(capability: str) -> str:
        if capability == "official_cli_installed":
            return "cli-installed"
        if capability == "workflow_result_returned":
            return "result-returned"
        return capability.replace("_", "-")

    evidence = {
        "startedAt": result["started_at"],
        "finishedAt": result["finished_at"],
        "generatedAt": result["generated_at"],
        "artifactVersions": result["artifact_versions"],
        "sourcePolicy": result["source_policy"],
        "installChannels": result["scenario_results"]["published_artifact_install_only"]["install_channels"],
        "officialCli": {
            "installCommand": "curl -fsSL https://durable-workflow.com/install.sh | sh",
            "startCommand": "dw workflow:start --json",
            "showRunCommand": "dw workflow:show-run py-parity 01JTEST --follow --json",
            "jsonOutputs": [{"workflow_id": "py-parity", "status": "completed"}],
        },
        "firstUserFlow": {
            "freshState": True,
            "namespaceCreated": "default",
            "firstWorkflowStarted": "py-parity",
            "resultObserved": {"status": "completed"},
        },
        "traces": [
            {"plane": "control-plane", "request": "POST /workflows", "response": 201},
            {"plane": "worker-protocol", "request": "POST /worker/workflow-tasks/poll", "response": 200},
        ],
        "languageNeutralityAudit": {
            "status": "succeeded",
            "serverAudit": {"status": "ok"},
            "sdkAudit": {"status": "ok"},
            "checks": {
                "noPhpRuntimeRequired": True,
                "noPhpPathsRequired": True,
                "noPhpSerializerRequired": True,
                "noPhpOnlyErrorShapes": True,
            },
        },
        "scenarioEvidence": scenario_evidence,
        "capabilityResults": [
            {
                "capability": capability_alias(capability),
                "status": "passed",
                "evidence": {"observed": True},
            }
            for capability in REQUIRED_CAPABILITIES
        ],
        "findings": [],
        "findingLinks": [],
    }

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["outcome"] == "pass"
    assert composed["scenario_results"]["worker_restart_activity_and_signal_state"]["status"] == "pass"
    assert (
        composed["scenario_results"]["official_cli_install_start_result_path"]["cli_result"]
        == "dw workflow:show-run py-parity 01JTEST --follow --json"
    )
    assert composed["scenario_results"]["protocol_trace_capture"]["worker_protocol_traces"] == [evidence["traces"][1]]
    assert {entry["id"] for entry in composed["capability_table"]} == set(REQUIRED_CAPABILITIES)
    assert evaluation["status"] == "pass"
    assert evaluation["gate_failures"] == []


def test_compose_result_accepts_nested_runner_tables_and_audit_checks() -> None:
    result = complete_result()
    derived_scenarios = {
        "published_artifact_install_only",
        "official_cli_install_start_result_path",
        "cold_first_user_setup",
        "protocol_trace_capture",
        "php_assumption_audit",
        "capability_table_complete",
    }
    evidence = {
        "startedAt": result["started_at"],
        "finishedAt": result["finished_at"],
        "generatedAt": result["generated_at"],
        "resolvedArtifactVersions": result["artifact_versions"],
        "artifactSources": {
            "server": "docker image",
            "cli": "official install script",
            "sdk-python": "pypi",
            "workflow": "composer",
            "waterline": "published package",
        },
        "localProductSourcesUsed": False,
        "installChannels": result["scenario_results"]["published_artifact_install_only"]["install_channels"],
        "officialCli": {
            "installCommand": "curl -fsSL https://durable-workflow.com/install.sh | sh",
            "startCommand": "dw workflow:start py-parity --json",
            "startWaitCommand": "dw workflow:start py-parity --wait --json",
            "terminalOutput": [{"workflow_id": "py-parity", "status": "completed"}],
        },
        "firstUserSetup": {
            "freshState": True,
            "namespaceCreated": "default",
            "firstWorkflowStarted": "py-parity",
            "resultObserved": {"status": "completed"},
        },
        "protocolTraces": {
            "controlPlane": [{"method": "POST", "path": "/workflows"}],
            "workerProtocol": [{"method": "POST", "path": "/worker/workflow-tasks/poll"}],
        },
        "languageNeutralityAudit": {
            "checks": {
                "noPhpRuntime": True,
                "noPhpPaths": True,
                "noPhpSerializer": True,
                "noPhpErrorShapes": True,
            },
        },
        "scenarioEvidence": [
            {
                **scenario_result,
                "scenario": scenario_id.replace("_", "-"),
                "passed": True,
            }
            for scenario_id, scenario_result in result["scenario_results"].items()
            if scenario_id not in derived_scenarios
        ],
        "capabilityTable": {
            "rows": [
                {
                    "name": capability.replace("_", "-"),
                    "passed": True,
                    "observed": {"capability": capability},
                }
                for capability in REQUIRED_CAPABILITIES
            ],
        },
        "findings": [],
        "findingLinks": [],
    }

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["outcome"] == "pass"
    assert composed["source_policy"]["artifact_source"] == "published_artifacts"
    assert composed["scenario_results"]["protocol_trace_capture"]["control_plane_traces"] == [
        {"method": "POST", "path": "/workflows"}
    ]
    assert composed["scenario_results"]["php_assumption_audit"]["server_cli_audit"]["status"] == "pass"
    assert {entry["id"] for entry in composed["capability_table"]} == set(REQUIRED_CAPABILITIES)
    assert evaluation["status"] == "pass"
    assert evaluation["gate_failures"] == []


def test_compose_result_rejects_cli_command_only_evidence_without_real_output() -> None:
    evidence = complete_host_evidence()
    evidence["cli_evidence"].pop("json_outputs")

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["outcome"] == "fail"
    cli_scenario = composed["scenario_results"]["official_cli_install_start_result_path"]
    assert cli_scenario["status"] == "pass"
    assert cli_scenario["observed_outputs"]["summary"] == "required Python SDK conformance evidence recorded"
    assert {
        "code": "missing_cli_evidence",
        "field": "json_outputs",
    } in evaluation["gate_failures"]


def test_compose_result_accepts_nested_cli_command_output_as_real_output() -> None:
    evidence = complete_host_evidence()
    evidence["cli_evidence"] = {
        "install": {
            "command": "curl -fsSL https://durable-workflow.com/install.sh | sh",
            "stdout": "dw 0.1.69 installed to ~/.local/bin",
        },
        "workflowStart": {
            "command": "dw workflow:start py-parity --wait --json",
            "json": {"workflow_id": "py-parity", "run_id": "01JTEST", "status": "completed"},
        },
        "workflowShowRun": {
            "command": "dw workflow:show-run py-parity 01JTEST --follow --json",
            "stdoutJson": {"workflow_id": "py-parity", "run_id": "01JTEST", "status": "completed"},
        },
    }

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["outcome"] == "pass"
    assert (
        composed["scenario_results"]["official_cli_install_start_result_path"]["cli_result"]["command"]
        == "dw workflow:show-run py-parity 01JTEST --follow --json"
    )
    assert evaluation["status"] == "pass"
    assert evaluation["gate_failures"] == []


def test_compose_result_requires_explicit_no_local_source_evidence() -> None:
    evidence = complete_host_evidence()
    evidence.pop("source_policy")
    evidence["artifactSources"] = {
        "server": "docker image",
        "cli": "official install script",
        "sdk-python": "pypi",
        "workflow": "composer",
        "waterline": "published package",
    }

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["outcome"] == "fail"
    assert composed["source_policy"]["artifact_source"] == "published_artifacts"
    assert "local_product_sources_used" not in composed["source_policy"]
    assert {
        "code": "missing_local_product_sources_used",
        "scope": "scenario",
        "scenario_id": "published_artifact_install_only",
        "value": None,
    } in evaluation["gate_failures"]


def test_compose_result_accepts_no_local_product_source_checkout_alias() -> None:
    evidence = complete_host_evidence()
    evidence.pop("source_policy")
    evidence["artifactSources"] = {
        "server": "docker image",
        "cli": "official install script",
        "sdk-python": "pypi",
        "workflow": "composer",
        "waterline": "published package",
    }
    evidence["local_product_source_checkouts_used"] = False

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["source_policy"]["artifact_source"] == "published_artifacts"
    assert composed["source_policy"]["local_product_sources_used"] is False
    assert evaluation["status"] == "pass"
    assert evaluation["gate_failures"] == []


def test_compose_result_rejects_protocol_trace_evidence_without_both_planes() -> None:
    evidence = complete_host_evidence()
    evidence["protocol_traces"] = [
        {"plane": "control", "request": "POST /workflows", "response": 201},
    ]
    evidence["finding_links"] = {
        "protocol_trace_capture": [{"summary": "worker protocol trace plane was not captured"}],
    }

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["outcome"] == "fail"
    protocol_scenario = composed["scenario_results"]["protocol_trace_capture"]
    assert protocol_scenario["status"] == "not_covered"
    assert protocol_scenario["control_plane_traces"] == evidence["protocol_traces"]
    assert "worker_protocol_traces" not in protocol_scenario
    assert evaluation["status"] == "non_passing"
    assert "protocol_trace_capture" in evaluation["non_pass_scenarios"]
    assert {"code": "missing_protocol_trace_plane", "plane": "worker"} in evaluation["gate_failures"]


def test_compose_result_rejects_pass_capabilities_without_recorded_evidence() -> None:
    evidence = complete_host_evidence()
    evidence["capabilities"] = {
        capability: {"status": "pass"}
        for capability in REQUIRED_CAPABILITIES
    }

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["outcome"] == "fail"
    assert composed["capability_table"][0] == {"id": REQUIRED_CAPABILITIES[0], "status": "pass"}
    assert evaluation["status"] == "non_passing"
    assert {
        "code": "missing_capability_evidence",
        "capability_id": REQUIRED_CAPABILITIES[0],
    } in evaluation["gate_failures"]


def test_compose_result_keeps_smoke_only_host_evidence_non_passing() -> None:
    result = complete_result()
    evidence = {
        "started_at": result["started_at"],
        "finished_at": result["finished_at"],
        "generated_at": result["generated_at"],
        "artifact_versions": result["artifact_versions"],
        "source_policy": result["source_policy"],
        "scenario_results": {
            "python_worker_registration": result["scenario_results"]["python_worker_registration"],
            "activity_backed_workflow_execution": result["scenario_results"]["activity_backed_workflow_execution"],
            "workflow_result_surface": result["scenario_results"]["workflow_result_surface"],
            "worker_restart_activity_and_signal_state": result["scenario_results"][
                "worker_restart_activity_and_signal_state"
            ],
        },
        "capabilities": {
            "python_workflow_runs": {"status": "pass", "evidence": {"observed": True}},
            "python_activity_runs": {"status": "pass", "evidence": {"observed": True}},
            "workflow_result_returned": {"status": "pass", "evidence": {"observed": True}},
        },
        "findings": {
            scenario: [{"summary": "expanded Python conformance scenario was not covered"}]
            for scenario in REQUIRED_SCENARIOS
        },
        "finding_links": {
            scenario: [{"summary": "expanded Python conformance scenario was not covered"}]
            for scenario in REQUIRED_SCENARIOS
        },
    }

    composed = compose_result(evidence)
    evaluation = evaluate_result(composed)

    assert composed["outcome"] == "fail"
    assert composed["scenario_results"]["official_cli_install_start_result_path"]["status"] == "not_covered"
    assert evaluation["status"] == "non_passing"
    assert "official_cli_install_start_result_path" in evaluation["non_pass_scenarios"]
    assert "official_cli_installed" in evaluation["non_pass_capabilities"]


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


def test_evaluate_result_rejects_cli_observed_outputs_without_real_output_alias() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    cli_scenario = result["scenario_results"]["official_cli_install_start_result_path"]
    cli_scenario["cli_evidence"].pop("json_outputs")

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert cli_scenario["observed_outputs"]["summary"] == "official_cli_install_start_result_path passed"
    assert {
        "code": "missing_cli_evidence",
        "field": "json_outputs",
    } in evaluation["gate_failures"]


def test_evaluate_result_rejects_cli_scenario_generic_output_as_cli_output_proof() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    cli_scenario = result["scenario_results"]["official_cli_install_start_result_path"]
    cli_scenario["cli_evidence"].pop("json_outputs")
    cli_scenario["output"] = {"workflow_id": "py-parity", "status": "completed"}

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert {
        "code": "missing_cli_evidence",
        "field": "json_outputs",
    } in evaluation["gate_failures"]


def test_evaluate_result_rejects_top_level_outputs_as_cli_output_proof() -> None:
    result = complete_result()
    result["outcome"] = "fail"
    result["outputs"] = [{"workflow_id": "py-parity", "status": "completed"}]
    result["scenario_results"]["official_cli_install_start_result_path"]["cli_evidence"].pop("json_outputs")

    evaluation = evaluate_result(result)

    assert evaluation["status"] == "non_passing"
    assert {
        "code": "missing_cli_evidence",
        "field": "json_outputs",
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
