"""Python SDK published-artifact conformance contract.

This module exposes the machine-readable contract the host conformance runner
uses to decide whether a Python SDK run proves the full published-artifact
property or only a smoke subset. It intentionally has no dependency on the
server, CLI, or worker runtime so a runner can import it from the installed
``durable-workflow`` package before starting any product processes.
"""

from __future__ import annotations

import argparse
import copy
import json
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

SCHEMA = "durable-workflow.v2.python-sdk-parity.contract"
VERSION = 1
RESULT_SCHEMA = "durable-workflow.v2.python-sdk-parity.result"
RESULT_VERSION = 1
RESULT_GATE_SCHEMA = "durable-workflow.v2.python-sdk-parity.result-gate"
RESULT_GATE_VERSION = 1
HOST_EVIDENCE_SCHEMA = "durable-workflow.v2.python-sdk-parity.host-evidence"
HOST_EVIDENCE_VERSION = 1
PUBLISHED_ARTIFACT_INSTALL_ONLY_SCENARIO = "published_artifact_install_only"

ALLOWED_SCENARIO_STATUSES = ("pass", "fail", "unsupported", "not_covered", "runner_blocked")
NON_PASS_SCENARIO_STATUSES = ("fail", "unsupported", "not_covered", "runner_blocked")
REQUIRED_ARTIFACTS = ("server", "cli", "sdk-python", "workflow", "waterline")
ARTIFACT_VERSION_FIELDS = (
    "artifact_versions",
    "artifactVersions",
    "published_artifact_versions",
    "publishedArtifactVersions",
)
SCENARIO_RESULTS_FIELDS = (
    "scenario_results",
    "scenarioResults",
    "scenario_evidence",
    "scenarioEvidence",
    "scenarios",
)
CAPABILITY_TABLE_FIELDS = (
    "capability_table",
    "capabilityTable",
    "capabilities",
    "capability_results",
    "capabilityResults",
    "capability_evidence",
    "capabilityEvidence",
    "capability_checks",
    "capabilityChecks",
)
DECLARED_OUTCOME_FIELDS = (
    "outcome",
    "status",
    "verdict",
)
PLACEHOLDER_VERSION_TOKENS = {
    "",
    "current",
    "head",
    "latest",
    "main",
    "placeholder",
    "tip",
    "unresolved",
}

REQUIRED_SCENARIOS = (
    PUBLISHED_ARTIFACT_INSTALL_ONLY_SCENARIO,
    "official_cli_install_start_result_path",
    "cold_first_user_setup",
    "python_worker_registration",
    "activity_backed_workflow_execution",
    "workflow_result_surface",
    "worker_restart_activity_and_signal_state",
    "protocol_trace_capture",
    "php_assumption_audit",
    "capability_table_complete",
)

REQUIRED_CAPABILITIES = (
    "server_up",
    "official_cli_installed",
    "cli_reaches_server",
    "cli_starts_workflow",
    "cli_reads_workflow_result",
    "cold_first_user_setup",
    "python_sdk_installed_from_pypi",
    "python_worker_connects",
    "python_worker_registers_workflows",
    "python_worker_registers_activities",
    "python_workflow_runs",
    "python_activity_runs",
    "workflow_result_returned",
    "worker_restart_replays_activity_state",
    "worker_restart_replays_signal_state",
    "protocol_traces_recorded",
    "php_assumptions_absent",
)

SCENARIO_REQUIRED_EVIDENCE: Mapping[str, tuple[str, ...]] = {
    "published_artifact_install_only": (
        "install_channels",
        "artifact_versions",
        "source_policy",
    ),
    "official_cli_install_start_result_path": (
        "cli_install",
        "cli_start",
        "cli_result",
    ),
    "cold_first_user_setup": (
        "fresh_state",
        "first_user_flow",
    ),
    "python_worker_registration": (
        "registered_workflows",
        "registered_activities",
        "worker_identity",
    ),
    "activity_backed_workflow_execution": (
        "workflow_execution",
        "activity_execution",
    ),
    "workflow_result_surface": (
        "result_observed",
        "result_value",
    ),
    "worker_restart_activity_and_signal_state": (
        "restart_boundary",
        "activity_state_after_restart",
        "signal_state_after_restart",
    ),
    "protocol_trace_capture": (
        "control_plane_traces",
        "worker_protocol_traces",
    ),
    "php_assumption_audit": (
        "server_cli_audit",
        "sdk_runtime_audit",
    ),
    "capability_table_complete": (
        "capability_table",
    ),
}


def manifest() -> dict[str, Any]:
    """Return the Python SDK conformance contract."""

    return {
        "schema": SCHEMA,
        "version": VERSION,
        "result_schema": RESULT_SCHEMA,
        "result_version": RESULT_VERSION,
        "fixture_category": "python_sdk_published_artifact_parity",
        "platform_conformance_suite_schema": "durable-workflow.v2.platform-conformance.suite",
        "claimed_targets": [
            "official_sdk",
            "worker_protocol_implementation",
        ],
        "artifact_policy": {
            "version_source": "latest_published_artifacts_at_run_time",
            "published_artifacts_only": True,
            "requires_resolved_versions": True,
            "required_artifacts": list(REQUIRED_ARTIFACTS),
            "install_channels": {
                "server": "docker image durableworkflow/server:<resolved-version>",
                "cli": "official dw install script pinned to its resolved release tag",
                "sdk-python": "PyPI package durable-workflow==<resolved-version>",
                "workflow": "Composer package durable-workflow/workflow:<resolved-version>",
                "waterline": "published Waterline package or image matching the resolved release tag",
            },
            "forbidden_sources": [
                "local_product_source_checkout",
                "workspace_repo_as_artifact_under_test",
                "editable_python_sdk_install",
            ],
            "required_run_record_fields": [
                "artifact_versions",
                "started_at",
                "finished_at",
                "generated_at",
                "outcome",
                "scenario_results",
                "capability_table",
                "protocol_traces",
                "php_assumption_audit",
                "findings",
                "finding_links",
            ],
        },
        "scenario_statuses": list(ALLOWED_SCENARIO_STATUSES),
        "required_scenarios": list(REQUIRED_SCENARIOS),
        "scenario_required_evidence": {
            scenario: list(fields)
            for scenario, fields in SCENARIO_REQUIRED_EVIDENCE.items()
        },
        "capability_table": [
            {
                "id": capability,
                "required_status": "pass",
            }
            for capability in REQUIRED_CAPABILITIES
        ],
        "coverage_gate": {
            "passing_outcome_requires": [
                "all_required_scenarios_reported",
                "all_required_scenarios_pass",
                "all_required_capabilities_reported",
                "all_required_capabilities_pass",
                "artifact_versions_recorded_and_pinned",
                "run_timestamps_recorded",
                "official_cli_install_start_and_result_path_reported",
                "cold_first_user_setup_reported",
                "protocol_traces_reported",
                "php_assumption_audit_passes",
                "no_local_product_source_artifacts",
                "findings_linked_for_non_pass_scenarios",
                "declared_outcome_matches_evaluated_status",
            ],
            "smoke_subset_outcome": "non_passing",
            "uncovered_required_scenario_outcome": "non_passing",
            "runner_blocked_outcome": "non_passing_runner_blocked",
        },
        "result_gate": result_gate_spec(),
        "host_evidence": host_evidence_spec(),
    }


def result_gate_spec() -> dict[str, Any]:
    """Return the result-gate contract consumed by runners."""

    return {
        "schema": RESULT_GATE_SCHEMA,
        "version": RESULT_GATE_VERSION,
        "evaluates_result_schema": RESULT_SCHEMA,
        "required_scenarios_source": "python_sdk_published_artifact_parity.required_scenarios",
        "required_capabilities_source": "python_sdk_published_artifact_parity.capability_table",
        "artifact_versions_fields": list(ARTIFACT_VERSION_FIELDS),
        "scenario_results_fields": list(SCENARIO_RESULTS_FIELDS),
        "capability_table_fields": list(CAPABILITY_TABLE_FIELDS),
        "declared_outcome_fields": list(DECLARED_OUTCOME_FIELDS),
        "non_pass_statuses": list(NON_PASS_SCENARIO_STATUSES),
        "pass_requires": manifest_pass_requirements(),
        "smoke_subset_outcome": "non_passing",
    }


def host_evidence_spec() -> dict[str, Any]:
    """Return the host-evidence composition contract consumed by runners."""

    return {
        "schema": HOST_EVIDENCE_SCHEMA,
        "version": HOST_EVIDENCE_VERSION,
        "composes_result_schema": RESULT_SCHEMA,
        "compose_command": "durable-workflow-python-conformance --compose host-evidence.json",
        "evaluate_command": "durable-workflow-python-conformance --evaluate python-conformance-result.json",
        "input_fields": {
            "artifact_versions": list(ARTIFACT_VERSION_FIELDS),
            "scenario_results": list(SCENARIO_RESULTS_FIELDS),
            "capability_table": list(CAPABILITY_TABLE_FIELDS),
            "declared_outcome": list(DECLARED_OUTCOME_FIELDS),
        },
        "top_level_evidence_fields": [
            "install_channels",
            "source_policy",
            "cli_evidence",
            "official_cli",
            "cold_setup",
            "first_user_flow",
            "protocol_traces",
            "control_plane_traces",
            "worker_protocol_traces",
            "traces",
            "php_assumption_audit",
            "language_neutrality_audit",
            "findings",
            "finding_links",
        ],
        "scenario_evidence_fields": {
            scenario: list(fields)
            for scenario, fields in SCENARIO_REQUIRED_EVIDENCE.items()
        },
        "missing_scenario_status": "not_covered",
        "missing_capability_status": "not_covered",
    }


def manifest_pass_requirements() -> list[str]:
    """Return the ordered list of gate requirements for a passing result."""

    return [
        "every_required_scenario_has_one_result",
        "every_required_scenario_status_is_pass",
        "every_result_uses_a_published_status",
        "every_pass_scenario_has_observed_outputs",
        "every_pass_scenario_has_required_evidence",
        "each_non_pass_scenario_has_linked_findings",
        "every_required_capability_has_one_result",
        "every_required_capability_status_is_pass",
        "run_timestamps_outcome_and_finding_links_are_recorded",
        "published_artifact_versions_are_recorded_and_pinned",
        "official_cli_evidence_is_recorded",
        "cold_first_user_setup_evidence_is_recorded",
        "protocol_traces_are_recorded",
        "php_assumption_audit_is_recorded_and_passing",
        "no_local_product_source_artifacts_are_reported",
        "overall_outcome_matches_gate_status",
    ]


def compose_result(evidence: Mapping[str, Any], contract: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Compose a full Python conformance result from host-runner evidence.

    Host runners collect observations from published artifacts. This function
    gives them one stable shape to emit: every required scenario and
    capability is represented, pass cells carry their evidence, and omitted
    cells are explicit ``not_covered`` results that the gate rejects with
    focused failures instead of silently accepting smoke-only output.
    """

    contract = contract or manifest()
    artifacts = dict(_artifact_versions(evidence))
    source_policy = _copy_mapping(_first_present(evidence, ("source_policy", "sourcePolicy")))
    scenario_results = _compose_scenario_results(evidence, contract, artifacts, source_policy)
    capability_table = _compose_capability_table(evidence, contract)
    protocol_traces = _deepcopy_json_like(
        _first_present(evidence, _protocol_trace_fields())
    )
    php_assumption_audit = _deepcopy_json_like(
        _first_present(evidence, _php_assumption_audit_fields())
    )

    result: dict[str, Any] = {
        "schema": RESULT_SCHEMA,
        "version": RESULT_VERSION,
        "started_at": _timestamp_value(evidence, "started_at"),
        "finished_at": _timestamp_value(evidence, "finished_at"),
        "generated_at": _timestamp_value(evidence, "generated_at"),
        "artifact_versions": artifacts,
        "source_policy": source_policy,
        "scenario_results": scenario_results,
        "capability_table": capability_table,
        "protocol_traces": protocol_traces,
        "php_assumption_audit": php_assumption_audit,
        "findings": _deepcopy_json_like(_first_present(evidence, ("findings",))),
        "finding_links": _deepcopy_json_like(_first_present(evidence, ("finding_links", "findingLinks"))),
    }

    declared = _string_value(_first_present(evidence, DECLARED_OUTCOME_FIELDS))
    result["outcome"] = declared or _composed_outcome(result, contract)
    return result


def _compose_scenario_results(
    evidence: Mapping[str, Any],
    contract: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    source_policy: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    duplicates: dict[str, int] = {}
    raw_scenarios = _entries_by_id(evidence, SCENARIO_RESULTS_FIELDS, duplicates)
    required_scenarios = _string_list(contract.get("required_scenarios", []))
    required_evidence = _scenario_required_evidence(contract)
    scenario_results: dict[str, dict[str, Any]] = {}

    for scenario_id in required_scenarios:
        scenario = _copy_mapping(raw_scenarios.get(scenario_id))
        scenario["scenario_id"] = scenario_id
        _apply_top_level_scenario_evidence(scenario_id, scenario, evidence, artifacts, source_policy, contract)
        status = _string_value(scenario.get("status"))
        if status == "":
            status = (
                "pass"
                if _scenario_has_required_evidence(scenario, required_evidence.get(scenario_id, []))
                else "not_covered"
            )
            scenario["status"] = status
        if status == "pass" and not _has_observed_outputs(scenario):
            scenario["observed_outputs"] = {
                "summary": "required Python SDK conformance evidence recorded",
                "evidence_fields": [
                    field
                    for field in required_evidence.get(scenario_id, [])
                    if _has_non_empty_field(scenario, field)
                ],
            }
        _attach_scenario_findings(scenario, evidence)
        scenario_results[scenario_id] = scenario

    for scenario_id, scenario in raw_scenarios.items():
        if scenario_id in scenario_results:
            continue
        extra = _copy_mapping(scenario)
        extra.setdefault("scenario_id", scenario_id)
        _attach_scenario_findings(extra, evidence)
        scenario_results[scenario_id] = extra

    return scenario_results


def _apply_top_level_scenario_evidence(
    scenario_id: str,
    scenario: dict[str, Any],
    evidence: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    source_policy: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> None:
    if scenario_id == PUBLISHED_ARTIFACT_INSTALL_ONLY_SCENARIO:
        _setdefault_non_empty(
            scenario,
            "install_channels",
            _first_present(evidence, ("install_channels", "installChannels")),
        )
        _setdefault_non_empty(scenario, "artifact_versions", artifacts)
        _setdefault_non_empty(scenario, "source_policy", source_policy)
        return

    if scenario_id == "official_cli_install_start_result_path":
        cli_evidence = _copy_mapping(_first_present(evidence, _cli_evidence_fields()))
        _setdefault_non_empty(scenario, "cli_evidence", cli_evidence)
        _setdefault_non_empty(
            scenario,
            "cli_install",
            _first_present(cli_evidence, _cli_install_fields())
            or _first_present(evidence, _cli_install_fields()),
        )
        _setdefault_non_empty(
            scenario,
            "cli_start",
            _first_present(cli_evidence, _cli_start_fields())
            or _first_present(evidence, _cli_start_fields()),
        )
        _setdefault_non_empty(
            scenario,
            "cli_result",
            _first_present(cli_evidence, _cli_result_fields())
            or _first_present(evidence, _cli_result_fields()),
        )
        return

    if scenario_id == "cold_first_user_setup":
        cold_setup = _copy_mapping(_first_present(evidence, _cold_setup_fields()))
        _setdefault_non_empty(scenario, "cold_setup", cold_setup)
        _setdefault_non_empty(
            scenario,
            "fresh_state",
            _first_present(cold_setup, ("fresh_state", "freshState"))
            or _first_present(evidence, ("fresh_state", "freshState")),
        )
        _setdefault_non_empty(
            scenario,
            "first_user_flow",
            _first_present(evidence, ("first_user_flow", "firstUserFlow")) or cold_setup,
        )
        return

    if scenario_id == "protocol_trace_capture":
        traces = _deepcopy_json_like(_first_present(evidence, _protocol_trace_fields()))
        control_traces = _first_present(evidence, _control_trace_fields())
        worker_traces = _first_present(evidence, _worker_trace_fields())
        _setdefault_non_empty(scenario, "protocol_traces", traces)
        _setdefault_non_empty(scenario, "control_plane_traces", control_traces or _filtered_traces(traces, "control"))
        _setdefault_non_empty(
            scenario,
            "worker_protocol_traces",
            worker_traces or _filtered_traces(traces, "worker"),
        )
        return

    if scenario_id == "php_assumption_audit":
        audit = _copy_mapping(_first_present(evidence, _php_assumption_audit_fields()))
        _setdefault_non_empty(scenario, "php_assumption_audit", audit)
        _setdefault_non_empty(
            scenario,
            "server_cli_audit",
            _first_present(audit, _server_cli_audit_fields()),
        )
        _setdefault_non_empty(
            scenario,
            "sdk_runtime_audit",
            _first_present(audit, _sdk_runtime_audit_fields()),
        )
        return

    if scenario_id == "capability_table_complete":
        _setdefault_non_empty(scenario, "capability_table", _compose_capability_table(evidence, contract))


def _compose_capability_table(evidence: Mapping[str, Any], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_capabilities = _raw_capability_entries(evidence)
    required_capabilities = _required_capabilities(contract)
    capability_table: list[dict[str, Any]] = []

    for capability_id in required_capabilities:
        capability = _copy_mapping(raw_capabilities.get(capability_id))
        capability["id"] = capability_id
        status = _string_value(
            capability.get("status")
            or capability.get("result")
            or capability.get("outcome")
        )
        if status == "":
            status = "not_covered"
            capability["status"] = status
        else:
            capability.setdefault("status", status)
        capability_table.append(capability)

    for capability_id, capability in raw_capabilities.items():
        if capability_id in required_capabilities:
            continue
        extra = _copy_mapping(capability)
        extra.setdefault("id", capability_id)
        capability_table.append(extra)

    return capability_table


def _raw_capability_entries(evidence: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = _first_present(evidence, CAPABILITY_TABLE_FIELDS)
    if isinstance(raw, Mapping):
        entries: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            if not isinstance(key, str) or key == "":
                continue
            if isinstance(value, Mapping):
                entry = dict(value)
            elif isinstance(value, bool):
                entry = {"status": "pass" if value else "fail", "evidence": {"observed": value}}
            elif isinstance(value, str):
                entry = {"status": value}
            else:
                continue
            entry.setdefault("id", key)
            entries[key] = entry
        return entries

    duplicates: dict[str, int] = {}
    return _entries_by_id(evidence, CAPABILITY_TABLE_FIELDS, duplicates)


def _scenario_has_required_evidence(scenario: Mapping[str, Any], required_fields: Iterable[str]) -> bool:
    return all(_has_non_empty_field(scenario, field) for field in required_fields)


def _attach_scenario_findings(scenario: dict[str, Any], evidence: Mapping[str, Any]) -> None:
    if any(
        _has_non_empty_field(scenario, field)
        for field in ("linked_findings", "linkedFindings", "finding_links", "findingLinks")
    ):
        return

    scenario_id = _entry_id(scenario)
    links = _first_present(evidence, ("finding_links", "findingLinks", "findings"))
    linked: Any = None
    if isinstance(links, Mapping):
        linked = links.get(scenario_id)
    elif isinstance(links, list):
        linked = [
            item
            for item in links
            if isinstance(item, Mapping)
            and _string_value(item.get("scenario_id") or item.get("scenario") or item.get("id")) == scenario_id
        ]
    if linked not in (None, "", [], {}):
        scenario["linked_findings"] = _deepcopy_json_like(linked)


def _filtered_traces(traces: Any, plane: str) -> Any:
    if not isinstance(traces, list):
        return []
    return [
        trace
        for trace in traces
        if isinstance(trace, Mapping)
        and _string_value(trace.get("plane")).lower() == plane
    ]


def _timestamp_value(evidence: Mapping[str, Any], field: str) -> str:
    return _string_value(_first_present(evidence, (field, _camelize(field))))


def _composed_outcome(result: Mapping[str, Any], contract: Mapping[str, Any]) -> str:
    provisional = dict(result)
    provisional["outcome"] = "pass"
    if evaluate_result(provisional, contract).get("status") == "pass":
        return "pass"
    return "fail"


def _setdefault_non_empty(mapping: dict[str, Any], key: str, value: Any) -> None:
    if mapping.get(key) in (None, "", [], {}) and value not in (None, "", [], {}):
        mapping[key] = _deepcopy_json_like(value)


def _copy_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _deepcopy_json_like(value: Any) -> Any:
    return copy.deepcopy(value)


def evaluate_result(result: Mapping[str, Any], contract: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Evaluate a Python SDK conformance result against the full parity gate."""

    contract = contract or manifest()
    failures: list[dict[str, Any]] = []
    required_scenarios = _string_list(contract.get("required_scenarios", []))
    allowed_statuses = _string_list(contract.get("scenario_statuses", []))
    required_capabilities = _required_capabilities(contract)

    duplicate_scenarios: dict[str, int] = {}
    scenario_results = _entries_by_id(result, SCENARIO_RESULTS_FIELDS, duplicate_scenarios)
    scenario_statuses: dict[str, str] = {}
    missing_scenarios: list[str] = []
    non_pass_scenarios: list[str] = []

    for scenario_id, count in duplicate_scenarios.items():
        failures.append({"code": "duplicate_scenario_result", "scenario_id": scenario_id, "count": count})

    required_evidence = _scenario_required_evidence(contract)
    for scenario_id in required_scenarios:
        scenario_result = scenario_results.get(scenario_id)
        if scenario_result is None:
            missing_scenarios.append(scenario_id)
            failures.append({"code": "missing_required_scenario", "scenario_id": scenario_id})
            continue

        status = _string_value(scenario_result.get("status"))
        scenario_statuses[scenario_id] = status
        if status not in allowed_statuses:
            failures.append(
                {
                    "code": "invalid_scenario_status",
                    "scenario_id": scenario_id,
                    "status": status,
                    "allowed_statuses": allowed_statuses,
                }
            )
            continue

        if status == "pass":
            if not _has_observed_outputs(scenario_result):
                failures.append({"code": "missing_pass_observed_outputs", "scenario_id": scenario_id})
            for evidence_field in required_evidence.get(scenario_id, []):
                if not _has_non_empty_field(scenario_result, evidence_field):
                    failures.append(
                        {
                            "code": "missing_required_scenario_evidence",
                            "scenario_id": scenario_id,
                            "field": evidence_field,
                        }
                    )
        else:
            non_pass_scenarios.append(scenario_id)

    for scenario_id, scenario_result in scenario_results.items():
        status = _string_value(scenario_result.get("status"))
        if status in NON_PASS_SCENARIO_STATUSES and not _has_linked_findings(scenario_result, result):
            failures.append(
                {
                    "code": "missing_non_pass_finding",
                    "scenario_id": scenario_id,
                    "status": status,
                }
            )
        if scenario_id not in required_scenarios and status not in allowed_statuses:
            failures.append(
                {
                    "code": "invalid_extra_scenario_status",
                    "scenario_id": scenario_id,
                    "status": status,
                    "allowed_statuses": allowed_statuses,
                }
            )

    duplicate_capabilities: dict[str, int] = {}
    capability_results = _entries_by_id(
        result,
        CAPABILITY_TABLE_FIELDS,
        duplicate_capabilities,
    )
    capability_statuses: dict[str, str] = {}
    missing_capabilities: list[str] = []
    non_pass_capabilities: list[str] = []

    for capability_id, count in duplicate_capabilities.items():
        failures.append({"code": "duplicate_capability_result", "capability_id": capability_id, "count": count})

    for capability_id in required_capabilities:
        capability = capability_results.get(capability_id)
        if capability is None:
            missing_capabilities.append(capability_id)
            failures.append({"code": "missing_required_capability", "capability_id": capability_id})
            continue

        status = _string_value(capability.get("status") or capability.get("result") or capability.get("outcome"))
        capability_statuses[capability_id] = status
        if status != "pass":
            non_pass_capabilities.append(capability_id)
            failures.append({"code": "non_pass_required_capability", "capability_id": capability_id, "status": status})
            continue
        if not _has_observed_outputs(capability) and not _has_non_empty_field(capability, "evidence"):
            failures.append({"code": "missing_capability_evidence", "capability_id": capability_id})

    failures.extend(_run_record_failures(result, contract))
    failures.extend(_artifact_version_failures(result, contract))
    failures.extend(_source_policy_failures(result, scenario_results))
    failures.extend(_cli_evidence_failures(result, scenario_results))
    failures.extend(_cold_setup_failures(result, scenario_results))
    failures.extend(_protocol_trace_failures(result, scenario_results))
    failures.extend(_php_assumption_audit_failures(result, scenario_results))

    smoke_subset_detected = _is_smoke_subset(scenario_statuses, capability_statuses, required_scenarios)
    if smoke_subset_detected:
        failures.append(
            {
                "code": "smoke_subset_cannot_pass",
                "reason": "Python worker smoke evidence is not a complete Python SDK conformance result.",
            }
        )

    evaluated_status = (
        "pass"
        if not failures
        and not missing_scenarios
        and not non_pass_scenarios
        and not missing_capabilities
        and not non_pass_capabilities
        else "non_passing"
    )
    failures.extend(_declared_outcome_failures(result, evaluated_status))
    passes = evaluated_status == "pass" and not failures

    return {
        "schema": RESULT_GATE_SCHEMA,
        "version": RESULT_GATE_VERSION,
        "status": "pass" if passes else "non_passing",
        "required_scenarios": required_scenarios,
        "reported_scenarios": list(scenario_results.keys()),
        "missing_scenarios": missing_scenarios,
        "non_pass_scenarios": non_pass_scenarios,
        "duplicate_scenarios": duplicate_scenarios,
        "scenario_statuses": scenario_statuses,
        "required_capabilities": required_capabilities,
        "reported_capabilities": list(capability_results.keys()),
        "missing_capabilities": missing_capabilities,
        "non_pass_capabilities": non_pass_capabilities,
        "duplicate_capabilities": duplicate_capabilities,
        "capability_statuses": capability_statuses,
        "smoke_subset_detected": smoke_subset_detected,
        "gate_failures": failures,
    }


def _required_capabilities(contract: Mapping[str, Any]) -> list[str]:
    raw = contract.get("capability_table")
    capabilities: list[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, Mapping):
                capability_id = _string_value(item.get("id") or item.get("capability_id"))
                if capability_id:
                    capabilities.append(capability_id)
            elif isinstance(item, str):
                capabilities.append(item)
    return capabilities


def _scenario_required_evidence(contract: Mapping[str, Any]) -> dict[str, list[str]]:
    raw = contract.get("scenario_required_evidence")
    if not isinstance(raw, Mapping):
        return {}

    evidence: dict[str, list[str]] = {}
    for scenario_id, fields in raw.items():
        if isinstance(scenario_id, str):
            evidence[scenario_id] = _string_list(fields)
    return evidence


def _entries_by_id(
    result: Mapping[str, Any],
    field_names: Iterable[str],
    duplicates: dict[str, int],
) -> dict[str, dict[str, Any]]:
    entries: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    raw = _first_present(result, field_names)
    if isinstance(raw, Mapping):
        iterable: Iterable[tuple[Any, Any]] = raw.items()
    elif isinstance(raw, list):
        iterable = enumerate(raw)
    else:
        iterable = ()

    for key, value in iterable:
        if isinstance(value, Mapping):
            entry = dict(value)
        elif isinstance(value, bool):
            entry = {"status": "pass" if value else "fail", "evidence": {"observed": value}}
        elif isinstance(value, str):
            entry = {"status": value}
        else:
            continue
        entry_id = key if isinstance(key, str) else _entry_id(entry)
        if not isinstance(entry_id, str) or entry_id == "":
            continue
        if entry_id in seen:
            duplicates[entry_id] = duplicates.get(entry_id, 1) + 1
        seen.add(entry_id)
        entry.setdefault("id", entry_id)
        entries[entry_id] = entry
    return entries


def _entry_id(entry: Mapping[str, Any]) -> str:
    return _string_value(
        entry.get("scenario_id")
        or entry.get("scenarioId")
        or entry.get("capability_id")
        or entry.get("capabilityId")
        or entry.get("id")
    )


def _run_record_failures(result: Mapping[str, Any], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    required = _string_list(
        _mapping_value(contract.get("artifact_policy")).get("required_run_record_fields", [])
    )
    failures: list[dict[str, Any]] = []
    aliases = {
        "artifact_versions": ARTIFACT_VERSION_FIELDS,
        "scenario_results": SCENARIO_RESULTS_FIELDS,
        "capability_table": CAPABILITY_TABLE_FIELDS,
        "outcome": DECLARED_OUTCOME_FIELDS,
        "protocol_traces": _protocol_trace_fields(),
        "php_assumption_audit": _php_assumption_audit_fields(),
        "finding_links": ("finding_links", "findingLinks"),
    }
    for field in required:
        field_aliases = aliases.get(field, (field,))
        value = _first_present(result, field_aliases)
        if field in {"findings", "finding_links"}:
            if value is None:
                failures.append({"code": "missing_run_record_field", "field": field})
            continue
        if value in (None, "", [], {}):
            failures.append({"code": "missing_run_record_field", "field": field})

    for field in ("started_at", "finished_at", "generated_at"):
        value = result.get(field) or result.get(_camelize(field))
        if not isinstance(value, str) or value == "":
            failures.append({"code": "missing_run_timestamp", "field": field})
    return failures


def _artifact_version_failures(result: Mapping[str, Any], contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    versions = _artifact_versions(result)
    required = _string_list(
        _mapping_value(contract.get("artifact_policy")).get("required_artifacts", REQUIRED_ARTIFACTS)
    )
    failures: list[dict[str, Any]] = []
    if not versions:
        return [{"code": "missing_artifact_versions"}]

    for artifact in required:
        version = _string_value(versions.get(artifact))
        if version == "":
            failures.append({"code": "missing_artifact_version", "artifact": artifact})
        elif _is_placeholder_version(version):
            failures.append(
                {
                    "code": "placeholder_artifact_version",
                    "artifact": artifact,
                    "version": version,
                }
            )
    return failures


def _artifact_versions(result: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = _first_present(
        result,
        ARTIFACT_VERSION_FIELDS,
    )
    return raw if isinstance(raw, Mapping) else {}


def _is_placeholder_version(version: str) -> bool:
    normalized = version.strip().lower()
    if normalized in PLACEHOLDER_VERSION_TOKENS:
        return True
    return any(token in normalized for token in ("<", ">", "${", "{{", "}}"))


def _source_policy_failures(
    result: Mapping[str, Any],
    scenario_results: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    failures = _source_policy_scope_failures(result, scope="result")
    for scenario_id, scenario in scenario_results.items():
        failures.extend(
            _source_policy_scope_failures(
                scenario,
                scope="scenario",
                scenario_id=scenario_id,
                require_published_artifact_policy=(
                    scenario_id == PUBLISHED_ARTIFACT_INSTALL_ONLY_SCENARIO
                    and _string_value(scenario.get("status")) == "pass"
                ),
            )
        )
    return failures


def _source_policy_scope_failures(
    entry: Mapping[str, Any],
    *,
    scope: str,
    scenario_id: str | None = None,
    require_published_artifact_policy: bool = False,
) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    raw_source_policy = _first_present(entry, ("source_policy", "sourcePolicy"))
    source_policy = _mapping_value(raw_source_policy)
    local_sources = _first_present(entry, _local_source_artifact_fields())

    if require_published_artifact_policy:
        if not source_policy:
            failures.append(_source_policy_failure("missing_source_policy", scope, scenario_id, raw_source_policy))

        used = _first_present(source_policy, ("local_product_sources_used", "localProductSourcesUsed"))
        if used is not False:
            code = (
                "missing_local_product_sources_used"
                if used in (None, [], {})
                else "local_product_source_artifacts_used"
            )
            failures.append(_source_policy_failure(code, scope, scenario_id, used))

        artifact_source = _string_value(_first_present(source_policy, ("artifact_source", "artifactSource")))
        if artifact_source != "published_artifacts":
            code = "missing_published_artifact_source" if artifact_source == "" else "non_published_artifact_source"
            failures.append(_source_policy_failure(code, scope, scenario_id, artifact_source))
    elif source_policy:
        used = _first_present(source_policy, ("local_product_sources_used", "localProductSourcesUsed"))
        if used not in (False, None, [], {}):
            failures.append(_source_policy_failure("local_product_source_artifacts_used", scope, scenario_id, used))

        artifact_source = _string_value(_first_present(source_policy, ("artifact_source", "artifactSource")))
        if artifact_source and artifact_source != "published_artifacts":
            failures.append(
                _source_policy_failure("non_published_artifact_source", scope, scenario_id, artifact_source)
            )

    if source_policy:
        policy_local_sources = _first_present(source_policy, _local_source_artifact_fields())
        if policy_local_sources not in (None, [], {}, False):
            failures.append(
                _source_policy_failure(
                    "local_product_source_artifacts_reported",
                    scope,
                    scenario_id,
                    policy_local_sources,
                )
            )

    if local_sources not in (None, [], {}, False):
        failures.append(
            _source_policy_failure("local_product_source_artifacts_reported", scope, scenario_id, local_sources)
        )
    return failures


def _source_policy_failure(code: str, scope: str, scenario_id: str | None, value: Any) -> dict[str, Any]:
    failure = {"code": code, "scope": scope, "value": value}
    if scenario_id is not None:
        failure["scenario_id"] = scenario_id
    return failure


def _local_source_artifact_fields() -> tuple[str, ...]:
    return (
        "local_product_source_artifacts",
        "localProductSourceArtifacts",
        "product_source_artifacts",
        "productSourceArtifacts",
    )


def _cli_evidence_failures(
    result: Mapping[str, Any],
    scenario_results: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    scenario = scenario_results.get("official_cli_install_start_result_path", {})
    evidence = _mapping_value(
        _first_present(scenario, _cli_evidence_fields())
        or _first_present(result, _cli_evidence_fields())
    )
    failures: list[dict[str, Any]] = []
    if (
        not _first_present(evidence, _cli_install_fields())
        and not _first_present(scenario, ("cli_install", "cliInstall"))
    ):
        failures.append({"code": "missing_cli_evidence", "field": "install_command"})
    if not _first_present(evidence, _cli_start_fields()) and not _first_present(scenario, ("cli_start", "cliStart")):
        failures.append({"code": "missing_cli_evidence", "field": "start_command"})
    if (
        not _first_present(evidence, _cli_result_fields())
        and not _first_present(scenario, ("cli_result", "cliResult"))
    ):
        failures.append({"code": "missing_cli_evidence", "field": "result_command"})
    if not _has_non_empty_field(evidence, "json_outputs") and not _has_non_empty_field(evidence, "outputs"):
        failures.append({"code": "missing_cli_evidence", "field": "json_outputs"})
    return failures


def _cold_setup_failures(
    result: Mapping[str, Any],
    scenario_results: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    scenario = scenario_results.get("cold_first_user_setup", {})
    evidence = _mapping_value(
        _first_present(scenario, _cold_setup_fields())
        or _first_present(result, _cold_setup_fields())
    )
    required = ("fresh_state", "namespace_created", "first_workflow_started", "result_observed")
    return [
        {"code": "missing_cold_setup_evidence", "field": field}
        for field in required
        if not _has_non_empty_field(evidence, field)
    ]


def _protocol_trace_failures(
    result: Mapping[str, Any],
    scenario_results: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    scenario = scenario_results.get("protocol_trace_capture", {})
    traces = _first_present(
        scenario,
        _protocol_trace_fields() + ("observed_outputs",),
    )
    if traces in (None, "", [], {}):
        traces = _first_present(result, _protocol_trace_fields())
    control_traces = _first_present(scenario, _control_trace_fields())
    worker_traces = _first_present(scenario, _worker_trace_fields())

    if control_traces in (None, "", [], {}):
        control_traces = _first_present(result, _control_trace_fields())
    if worker_traces in (None, "", [], {}):
        worker_traces = _first_present(result, _worker_trace_fields())
    if control_traces in (None, "", [], {}):
        control_traces = _filtered_traces(traces, "control")
    if worker_traces in (None, "", [], {}):
        worker_traces = _filtered_traces(traces, "worker")

    failures: list[dict[str, Any]] = []
    if traces in (None, "", [], {}):
        failures.append({"code": "missing_protocol_traces"})
    if control_traces in (None, "", [], {}):
        failures.append({"code": "missing_protocol_trace_plane", "plane": "control"})
    if worker_traces in (None, "", [], {}):
        failures.append({"code": "missing_protocol_trace_plane", "plane": "worker"})
    return failures


def _php_assumption_audit_failures(
    result: Mapping[str, Any],
    scenario_results: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    scenario = scenario_results.get("php_assumption_audit", {})
    audit = _mapping_value(
        _first_present(scenario, _php_assumption_audit_fields() + ("observed_outputs",))
        or _first_present(result, _php_assumption_audit_fields())
    )
    if not audit:
        return [{"code": "missing_php_assumption_audit"}]

    status = _string_value(audit.get("status") or audit.get("outcome") or audit.get("verdict"))
    failures: list[dict[str, Any]] = []
    if status != "pass":
        failures.append({"code": "non_pass_php_assumption_audit", "status": status})

    checks = _mapping_value(audit.get("checks"))
    required_checks = (
        "no_php_runtime_required",
        "no_php_paths_required",
        "no_php_serializer_required",
        "no_php_only_error_shapes",
    )
    for check in required_checks:
        if checks.get(check) is not True:
            failures.append({"code": "missing_php_assumption_check", "check": check})
    return failures


def _cli_evidence_fields() -> tuple[str, ...]:
    return ("cli_evidence", "cliEvidence", "official_cli", "officialCli", "cli")


def _cli_install_fields() -> tuple[str, ...]:
    return (
        "cli_install",
        "cliInstall",
        "install_command",
        "installCommand",
        "install",
        "installed",
        "install_output",
        "installOutput",
    )


def _cli_start_fields() -> tuple[str, ...]:
    return (
        "cli_start",
        "cliStart",
        "start_command",
        "startCommand",
        "workflow_start",
        "workflowStart",
        "start_workflow",
        "startWorkflow",
        "start_output",
        "startOutput",
    )


def _cli_result_fields() -> tuple[str, ...]:
    return (
        "cli_result",
        "cliResult",
        "result_command",
        "resultCommand",
        "workflow_result",
        "workflowResult",
        "read_result",
        "readResult",
        "result_output",
        "resultOutput",
    )


def _cold_setup_fields() -> tuple[str, ...]:
    return (
        "cold_setup",
        "coldSetup",
        "first_user_setup",
        "firstUserSetup",
        "first_user_flow",
        "firstUserFlow",
        "cold_start",
        "coldStart",
        "fresh_setup",
        "freshSetup",
    )


def _protocol_trace_fields() -> tuple[str, ...]:
    return (
        "protocol_traces",
        "protocolTraces",
        "api_captures",
        "apiCaptures",
        "http_traces",
        "httpTraces",
        "traces",
    )


def _control_trace_fields() -> tuple[str, ...]:
    return (
        "control_plane_traces",
        "controlPlaneTraces",
        "control_traces",
        "controlTraces",
        "control_plane_protocol_traces",
        "controlPlaneProtocolTraces",
    )


def _worker_trace_fields() -> tuple[str, ...]:
    return (
        "worker_protocol_traces",
        "workerProtocolTraces",
        "worker_traces",
        "workerTraces",
        "worker_plane_traces",
        "workerPlaneTraces",
    )


def _php_assumption_audit_fields() -> tuple[str, ...]:
    return (
        "php_assumption_audit",
        "phpAssumptionAudit",
        "language_neutrality_audit",
        "languageNeutralityAudit",
        "php_audit",
        "phpAudit",
    )


def _server_cli_audit_fields() -> tuple[str, ...]:
    return (
        "server_cli_audit",
        "serverCliAudit",
        "server_audit",
        "serverAudit",
        "cli_audit",
        "cliAudit",
    )


def _sdk_runtime_audit_fields() -> tuple[str, ...]:
    return (
        "sdk_runtime_audit",
        "sdkRuntimeAudit",
        "sdk_audit",
        "sdkAudit",
        "runtime_audit",
        "runtimeAudit",
    )


def _declared_outcome_failures(result: Mapping[str, Any], evaluated_status: str) -> list[dict[str, Any]]:
    declared = _string_value(
        _first_present(result, DECLARED_OUTCOME_FIELDS)
    )
    if declared == "":
        return [{"code": "missing_declared_outcome"}]
    if _normal_outcome(declared) != evaluated_status:
        return [
            {
                "code": "declared_outcome_mismatch",
                "declared": declared,
                "evaluated": evaluated_status,
            }
        ]
    return []


def _normal_outcome(value: str) -> str:
    return "pass" if value == "pass" else "non_passing"


def _has_observed_outputs(entry: Mapping[str, Any]) -> bool:
    return any(
        _has_non_empty_field(entry, field)
        for field in (
            "observed_outputs",
            "observedOutputs",
            "evidence",
            "api_captures",
            "apiCaptures",
            "protocol_traces",
            "protocolTraces",
            "runtime_matrix",
            "runtimeMatrix",
        )
    )


def _has_linked_findings(scenario_result: Mapping[str, Any], result: Mapping[str, Any]) -> bool:
    if any(
        _has_non_empty_field(scenario_result, field)
        for field in ("linked_findings", "linkedFindings", "finding_links", "findingLinks")
    ):
        return True

    scenario_id = _entry_id(scenario_result)
    for field in ("finding_links", "findingLinks", "findings"):
        links = _first_present(result, (field,))
        if isinstance(links, Mapping):
            value = links.get(scenario_id)
            if value not in (None, "", [], {}):
                return True
        elif isinstance(links, list):
            for item in links:
                if not isinstance(item, Mapping):
                    continue
                linked = _string_value(item.get("scenario_id") or item.get("scenario") or item.get("id"))
                if linked == scenario_id:
                    return True
    return False


def _is_smoke_subset(
    scenario_statuses: Mapping[str, str],
    capability_statuses: Mapping[str, str],
    required_scenarios: Iterable[str],
) -> bool:
    if not scenario_statuses:
        return False
    if set(scenario_statuses) >= set(required_scenarios):
        return False

    smoke_capabilities = {
        "python_worker_registers_workflows",
        "python_worker_registers_activities",
        "python_activity_runs",
        "python_workflow_runs",
        "workflow_result_returned",
        "worker_restart_replays_activity_state",
        "worker_restart_replays_signal_state",
    }
    reported = {capability for capability, status in capability_statuses.items() if status == "pass"}
    return bool(reported & smoke_capabilities)


def _first_present(mapping: Mapping[str, Any], field_names: Iterable[str]) -> Any:
    for field in field_names:
        if field in mapping:
            return mapping[field]
    return None


def _mapping_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, Mapping)):
        return []
    return [item for item in value if isinstance(item, str)]


def _string_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    return ""


def _has_non_empty_field(mapping: Mapping[str, Any], field: str) -> bool:
    value = _first_present(mapping, (field, _camelize(field)))
    return value not in (None, "", [], {})


def _camelize(value: str) -> str:
    parts = value.split("_")
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for runners and release jobs."""

    parser = argparse.ArgumentParser(description="Inspect or evaluate the Python SDK conformance contract.")
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--manifest", action="store_true", help="Print the Python SDK conformance manifest.")
    action.add_argument("--result-gate", action="store_true", help="Print the result-gate specification.")
    action.add_argument("--host-evidence", action="store_true", help="Print the host-evidence composition contract.")
    action.add_argument(
        "--compose",
        type=Path,
        metavar="EVIDENCE.json",
        help="Compose a result document from host evidence.",
    )
    action.add_argument("--evaluate", type=Path, metavar="RESULT.json", help="Evaluate a conformance result document.")
    parser.add_argument("--evaluate-composed", action="store_true", help="Evaluate the result produced by --compose.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    args = parser.parse_args(argv)

    if args.evaluate_composed and args.compose is None:
        parser.error("--evaluate-composed requires --compose")

    if args.result_gate:
        payload = result_gate_spec()
    elif args.host_evidence:
        payload = host_evidence_spec()
    elif args.compose is not None:
        composed = compose_result(json.loads(args.compose.read_text(encoding="utf-8")))
        payload = evaluate_result(composed) if args.evaluate_composed else composed
    elif args.evaluate is not None:
        payload = evaluate_result(json.loads(args.evaluate.read_text(encoding="utf-8")))
    else:
        payload = manifest()

    json.dump(payload, sys.stdout, indent=2 if args.pretty else None, sort_keys=True)
    sys.stdout.write("\n")
    if (args.evaluate is not None or args.evaluate_composed) and payload.get("status") != "pass":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
