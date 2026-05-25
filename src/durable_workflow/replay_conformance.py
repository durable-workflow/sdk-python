"""Replay conformance shard for the published Python SDK artifact.

The host replay runner composes runtime shards into the platform-level
``durable-workflow.v2.replay-conformance.result`` document. This module is the
Python SDK shard: it exercises the replay verifier and in-process replayer
without requiring a local product source checkout or a running server.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import serializer, workflow
from .errors import ChildWorkflowFailed
from .history_bundle_verify import STATUS_FAILED as INTEGRITY_STATUS_FAILED
from .history_bundle_verify import verify_bundle
from .replay_verify import (
    REASON_BUNDLE_INVALID,
    REASON_SHAPE_MISMATCH,
    STATUS_DRIFTED,
    verify_replay,
)
from .workflow import ScheduleActivity, WorkflowContext, query_state, replay

RESULT_SCHEMA = "durable-workflow.v2.replay-conformance.result"
RESULT_VERSION = 1
COVERAGE_SCOPE = "sdk-python-runtime-shard"

REQUIRED_ARTIFACTS = (
    "server",
    "cli",
    "workflow-php",
    "sdk-python",
    "waterline",
)

PUBLISHED_ARTIFACT_SOURCES = {
    "server": {
        "docker_image",
        "docker_registry",
        "oci_image",
        "published_docker_image",
        "registry_image",
    },
    "cli": {
        "github_release",
        "github_release_asset",
        "official_install_script",
        "release_asset",
    },
    "workflow-php": {
        "composer",
        "composer_package",
        "packagist",
        "packagist_package",
    },
    "sdk-python": {
        "pip_package",
        "pypi",
        "pypi_package",
        "python_package",
    },
    "waterline": {
        "composer",
        "composer_package",
        "packagist",
        "packagist_package",
    },
}

COMPLETED_SCENARIOS = {
    "activity": "python_completed_history_activity_replay",
    "signal-update": "python_completed_history_signal_update_replay",
    "wait-condition": "python_completed_history_wait_condition_replay",
    "version-marker": "python_completed_history_version_marker_replay",
    "saga-compensation": "python_completed_history_saga_compensation_replay",
}

RESTART_SCENARIOS = {
    "completed-query": "python_worker_restart_completed_query",
    "activity": "python_worker_restart_activity_state",
    "signal-update": "python_worker_restart_signal_update_state",
    "wait-condition": "python_worker_restart_wait_condition_state",
    "version-marker": "python_worker_restart_version_marker_state",
    "saga-compensation": "python_worker_restart_saga_compensation_state",
}


@workflow.defn(name="python-replay-conformance")
class ReplayConformanceWorkflow:
    def __init__(self) -> None:
        self.name: str | None = None
        self.greeting: str | None = None
        self.approved = False
        self.version = -1
        self.version_result: str | None = None
        self.reservation_id: str | None = None
        self.stage = "started"
        self.events: list[str] = []

    @workflow.signal("name-provided")
    def name_provided(self, name: str) -> None:
        self.name = name
        self.events.append(f"signal:{name}")

    @workflow.update("approve")
    def approve(self, approved: bool) -> bool:
        self.approved = bool(approved)
        if self.approved:
            self.events.append("approved")
        return self.approved

    @workflow.query("currentState")
    def current_state(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "name": self.name,
            "greeting": self.greeting,
            "approved": self.approved,
            "version": self.version,
            "version_result": self.version_result,
            "reservation_id": self.reservation_id,
            "events": list(self.events),
        }

    def run(self, ctx: WorkflowContext, scenario: str):  # type: ignore[no-untyped-def]
        if scenario == "single-activity":
            self.greeting = yield ctx.schedule_activity("python-replay.greet", ["Ada"])
            self.events.append(f"activity:{self.greeting}")
            self.stage = "completed"
            return self.current_state()

        if scenario == "signal-activity":
            yield ctx.wait_condition(lambda: self.name is not None, key="name-provided")
            self.greeting = yield ctx.schedule_activity("python-replay.greet", [self.name])
            self.events.append(f"activity:{self.greeting}")
            self.stage = "completed"
            return self.current_state()

        if scenario == "wait-condition":
            yield ctx.wait_condition(lambda: self.approved, key="approval")
            self.events.append("condition-satisfied")
            self.stage = "approved"
            return self.current_state()

        if scenario == "version-marker":
            self.version = yield ctx.get_version("python-replay-version", -1, 2)
            activity = (
                "python-replay.versioned-v3"
                if self.version >= 2
                else "python-replay.versioned-v2"
            )
            self.version_result = yield ctx.schedule_activity(activity, [])
            self.events.append(f"version:{self.version}")
            self.stage = "completed"
            return self.current_state()

        if scenario == "saga-compensation":
            self.reservation_id = yield ctx.schedule_activity(
                "python-replay.reserve-inventory",
                ["order-123"],
            )
            try:
                yield ctx.start_child_workflow(
                    "python-replay.charge-customer",
                    ["order-123", self.reservation_id],
                )
            except ChildWorkflowFailed as exc:
                yield ctx.schedule_activity(
                    "python-replay.release-inventory",
                    ["order-123", self.reservation_id, str(exc)],
                )
                self.events.append(f"compensated:{exc}")
                self.stage = "compensated"
                return self.current_state()

            self.stage = "charged"
            return self.current_state()

        raise ValueError(f"unknown replay conformance scenario {scenario!r}")


@workflow.defn(name="python-replay-conformance")
class ReplayConformanceDivergentWorkflow:
    def run(self, ctx: WorkflowContext, scenario: str):  # type: ignore[no-untyped-def]
        if scenario == "single-activity":
            return (yield ctx.schedule_activity("python-replay.farewell", []))
        raise ValueError(f"unknown replay conformance scenario {scenario!r}")


def compose_report(
    *,
    artifact_versions: Mapping[str, str] | None = None,
    artifact_sources: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    started_at = _timestamp()
    versions = _canonical_artifact_metadata(dict(artifact_versions or {}))
    sources = _canonical_artifact_metadata(dict(artifact_sources or {}))
    scenario_results: dict[str, dict[str, Any]] = {}
    findings: list[dict[str, Any]] = []
    finding_links: dict[str, list[dict[str, Any]]] = {}

    _append_scenario(
        scenario_results,
        findings,
        finding_links,
        _published_artifact_scenario(versions, sources),
    )

    for family, scenario_id in COMPLETED_SCENARIOS.items():
        _append_scenario(
            scenario_results,
            findings,
            finding_links,
            _completed_history_scenario(scenario_id, family, versions),
        )

    for family, scenario_id in RESTART_SCENARIOS.items():
        _append_scenario(
            scenario_results,
            findings,
            finding_links,
            _worker_restart_scenario(scenario_id, family, versions),
        )

    for scenario in (
        _code_divergence_scenario(versions),
        _server_history_mutation_scenario(versions),
        _malformed_history_scenario(versions),
        _in_flight_signal_timing_scenario(versions),
    ):
        _append_scenario(scenario_results, findings, finding_links, scenario)

    has_failures = any(
        scenario.get("status") != "pass" for scenario in scenario_results.values()
    )

    return {
        "schema": RESULT_SCHEMA,
        "schema_version": RESULT_VERSION,
        "coverage_scope": COVERAGE_SCOPE,
        "outcome": "fail" if has_failures else "pass",
        "started_at": started_at,
        "finished_at": _timestamp(),
        "artifact_versions": versions,
        "artifact_sources": sources,
        "runtime_matrix": {
            "runtimes": ["sdk-python"],
        },
        "scenario_results": list(scenario_results.values()),
        "completed_history_replay": _section_summary(
            scenario_results,
            list(COMPLETED_SCENARIOS.values()),
        ),
        "worker_restart_replay": _section_summary(
            scenario_results,
            list(RESTART_SCENARIOS.values()),
        ),
        "adversarial_replay": _section_summary(
            scenario_results,
            [
                "python_code_divergence_refusal",
                "server_history_mutation_refusal",
                "malformed_history_refusal",
            ],
        ),
        "in_flight_timing": _section_summary(
            scenario_results,
            ["python_in_flight_signal_restart_timing"],
        ),
        "findings": findings,
        "finding_links": finding_links,
    }


def _append_scenario(
    scenario_results: dict[str, dict[str, Any]],
    findings: list[dict[str, Any]],
    finding_links: dict[str, list[dict[str, Any]]],
    scenario: dict[str, Any],
) -> None:
    scenario_id = _string_value(scenario.get("scenario_id"))
    if not scenario_id:
        return

    scenario_results[scenario_id] = scenario
    if scenario.get("status") == "pass":
        return

    finding = {
        "scenario_id": scenario_id,
        "title": f"Python replay conformance scenario [{scenario_id}] did not pass",
        "evidence": scenario.get("observed_outputs") or scenario.get("replay_diagnostics") or {},
    }
    findings.append(finding)
    finding_links[scenario_id] = [finding]


def _published_artifact_scenario(
    artifact_versions: Mapping[str, str],
    artifact_sources: Mapping[str, str],
) -> dict[str, Any]:
    missing_versions: list[str] = []
    missing_sources: list[str] = []
    placeholder: dict[str, str] = {}
    rejected_versions: dict[str, dict[str, str]] = {}
    forbidden_sources: dict[str, str] = {}
    untrusted_sources: dict[str, str] = {}

    for artifact in REQUIRED_ARTIFACTS:
        version = _artifact_metadata(artifact_versions, artifact)
        if version is None:
            missing_versions.append(artifact)
        else:
            reason = _unpublished_version_reason(version)
            if reason is not None:
                rejected_versions[artifact] = {
                    "version": version,
                    "reason": reason,
                }
                if reason.startswith("placeholder"):
                    placeholder[artifact] = version

        source = _artifact_metadata(artifact_sources, artifact)
        if source is None:
            missing_sources.append(artifact)
        elif _is_local_artifact_source(source):
            forbidden_sources[artifact] = source
        elif not _is_published_artifact_source(artifact, source):
            untrusted_sources[artifact] = source

    missing = sorted(set(missing_versions + missing_sources))
    status = (
        "pass"
        if not missing
        and not rejected_versions
        and not forbidden_sources
        and not untrusted_sources
        else "fail"
    )

    return _scenario(
        "published_artifact_install_only",
        status,
        artifact_versions,
        {
            "artifact_versions_recorded": dict(artifact_versions),
            "artifact_sources": dict(artifact_sources),
            "missing_artifacts": missing,
            "missing_artifact_versions": missing_versions,
            "missing_artifact_sources": missing_sources,
            "placeholder_versions": placeholder,
            "rejected_versions": rejected_versions,
            "forbidden_sources": forbidden_sources,
            "untrusted_sources": untrusted_sources,
            "published_artifacts_only": status == "pass",
            "published_install_tuple_proven": status == "pass",
        },
    )


def _completed_history_scenario(
    scenario_id: str,
    family: str,
    artifact_versions: Mapping[str, str],
) -> dict[str, Any]:
    try:
        case = _golden_case(family)
        state = query_state(
            ReplayConformanceWorkflow,
            _history(case["scenario"]),
            [case["scenario"]],
            "currentState",
        )
        matches = state == case["expected_state"]
        outcome = replay(
            ReplayConformanceWorkflow,
            _history(case["scenario"]),
            [case["scenario"]],
        )

        return _scenario(
            scenario_id,
            "pass" if matches else "fail",
            artifact_versions,
            {
                "family": family,
                "history_event_types": _history_event_types(_history(case["scenario"])),
                "expected_state": case["expected_state"],
                "replayed_state": state,
                "state_matches_fixture": matches,
                "replayed_command_sequence": [_command_summary(command) for command in outcome.commands],
            },
        )
    except Exception as exc:
        return _exception_scenario(scenario_id, artifact_versions, exc)


def _worker_restart_scenario(
    scenario_id: str,
    family: str,
    artifact_versions: Mapping[str, str],
) -> dict[str, Any]:
    try:
        case = _golden_case("activity" if family == "completed-query" else family)
        state = query_state(
            ReplayConformanceWorkflow,
            _history(case["scenario"]),
            [case["scenario"]],
            "currentState",
        )
        matches = state == case["expected_state"]

        return _scenario(
            scenario_id,
            "pass" if matches else "fail",
            artifact_versions,
            {
                "family": family,
                "worker_restart_simulated": True,
                "original_query_result": case["expected_state"],
                "replay_query_result": state,
                "query_result_matches_original": matches,
                "history_event_types": _history_event_types(_history(case["scenario"])),
            },
        )
    except Exception as exc:
        return _exception_scenario(scenario_id, artifact_versions, exc)


def _code_divergence_scenario(artifact_versions: Mapping[str, str]) -> dict[str, Any]:
    try:
        report = verify_replay(
            ReplayConformanceDivergentWorkflow,
            _history("single-activity"),
            start_input=["single-activity"],
            case_id="python_code_divergence_refusal",
            workflow_type="python-replay-conformance",
        )
        diagnostics = dict(report.divergence or {})
        passes = (
            report.status == STATUS_DRIFTED
            and report.reason == REASON_SHAPE_MISMATCH
            and diagnostics != {}
        )

        return _scenario(
            "python_code_divergence_refusal",
            "pass" if passes else "fail",
            artifact_versions,
            {
                "observed_outcome": "non_determinism_error",
                "workflow_sequence": diagnostics.get("workflow_sequence"),
                "expected_shape": diagnostics.get("expected_shape"),
                "recorded_event_types": diagnostics.get("recorded_event_types", []),
                "message": diagnostics.get("message"),
                "replay_diff": report.to_dict(),
            },
        )
    except Exception as exc:
        return _exception_scenario("python_code_divergence_refusal", artifact_versions, exc)


def _server_history_mutation_scenario(
    artifact_versions: Mapping[str, str],
) -> dict[str, Any]:
    try:
        bundle = _history_bundle("single-activity")
        bundle["history_events"][1]["payload"]["result"] = _payload("Hello, Grace!")

        integrity = verify_bundle(bundle)
        finding = _first_finding(integrity)
        passes = integrity.get("status") == INTEGRITY_STATUS_FAILED and finding != {}

        return _scenario(
            "server_history_mutation_refusal",
            "pass" if passes else "fail",
            artifact_versions,
            {
                "observed_outcome": "bundle_invalid_or_drifted",
                "integrity": {
                    "rule": finding.get("rule", "integrity.checksum_mismatch"),
                    "path": finding.get("path", "integrity.checksum"),
                    "status": integrity.get("status"),
                },
                "replay_diff": {
                    "reason": REASON_BUNDLE_INVALID,
                    "status": "failed",
                },
                "message": finding.get(
                    "message",
                    "Mutated history bundle was refused by integrity verification.",
                ),
            },
        )
    except Exception as exc:
        return _exception_scenario("server_history_mutation_refusal", artifact_versions, exc)


def _malformed_history_scenario(artifact_versions: Mapping[str, str]) -> dict[str, Any]:
    try:
        bundle = _history_bundle("single-activity")
        del bundle["workflow"]["run_id"]

        integrity = verify_bundle(bundle)
        finding = _first_finding(integrity, "workflow.run_id_missing") or _first_finding(integrity)
        passes = integrity.get("status") == INTEGRITY_STATUS_FAILED and finding != {}

        return _scenario(
            "malformed_history_refusal",
            "pass" if passes else "fail",
            artifact_versions,
            {
                "observed_outcome": "bundle_invalid_or_failed",
                "integrity": {
                    "rule": finding.get("rule", "workflow.run_id_missing"),
                    "path": finding.get("path", "workflow.run_id"),
                    "status": integrity.get("status"),
                },
                "message": finding.get(
                    "message",
                    "Malformed history bundle was refused by integrity verification.",
                ),
            },
        )
    except Exception as exc:
        return _exception_scenario("malformed_history_refusal", artifact_versions, exc)


def _in_flight_signal_timing_scenario(
    artifact_versions: Mapping[str, str],
) -> dict[str, Any]:
    try:
        history = _history("signal-activity", complete=False)
        outcome = replay(
            ReplayConformanceWorkflow,
            history,
            ["signal-activity"],
        )
        command = outcome.commands[0] if outcome.commands else None
        next_decision = _command_summary(command)
        passes = (
            isinstance(command, ScheduleActivity)
            and command.activity_type == "python-replay.greet"
            and command.arguments == ["Grace"]
        )

        return _scenario(
            "python_in_flight_signal_restart_timing",
            "pass" if passes else "fail",
            artifact_versions,
            {
                "observed_outcome": "same_next_decision_after_replay",
                "worker_restart_at": "after_signal_history_reload",
                "signal_sent_at": "history_event:SignalReceived",
                "history_reloaded_at": "durable_workflow.workflow.replay",
                "replayed_next_decision": next_decision,
                "expected_next_decision": {
                    "command_type": "ScheduleActivity",
                    "activity_type": "python-replay.greet",
                    "arguments": ["Grace"],
                },
                "history_event_types": _history_event_types(history),
            },
        )
    except Exception as exc:
        return _exception_scenario(
            "python_in_flight_signal_restart_timing",
            artifact_versions,
            exc,
        )


def _scenario(
    scenario_id: str,
    status: str,
    artifact_versions: Mapping[str, str],
    observed_outputs: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "status": status,
        "published_artifact_versions": dict(artifact_versions),
        "implementation_identity": {
            "runtime": "sdk-python",
            "package": "durable-workflow",
            "version": _artifact_metadata(artifact_versions, "sdk-python"),
        },
        "runtime_matrix": {
            "runtimes": ["sdk-python"],
        },
        "observed_outputs": dict(observed_outputs),
        "replay_diagnostics": dict(observed_outputs),
    }


def _exception_scenario(
    scenario_id: str,
    artifact_versions: Mapping[str, str],
    exc: Exception,
) -> dict[str, Any]:
    return _scenario(
        scenario_id,
        "fail",
        artifact_versions,
        {
            "exception_class": f"{type(exc).__module__}.{type(exc).__qualname__}",
            "message": str(exc),
        },
    )


def _golden_case(family: str) -> dict[str, Any]:
    cases: dict[str, dict[str, Any]] = {
        "activity": {
            "scenario": "single-activity",
            "expected_state": {
                "stage": "completed",
                "name": None,
                "greeting": "Hello, Ada!",
                "approved": False,
                "version": -1,
                "version_result": None,
                "reservation_id": None,
                "events": ["activity:Hello, Ada!"],
            },
        },
        "signal-update": {
            "scenario": "signal-activity",
            "expected_state": {
                "stage": "completed",
                "name": "Grace",
                "greeting": "Hello, Grace!",
                "approved": False,
                "version": -1,
                "version_result": None,
                "reservation_id": None,
                "events": ["signal:Grace", "activity:Hello, Grace!"],
            },
        },
        "wait-condition": {
            "scenario": "wait-condition",
            "expected_state": {
                "stage": "approved",
                "name": None,
                "greeting": None,
                "approved": True,
                "version": -1,
                "version_result": None,
                "reservation_id": None,
                "events": ["approved", "condition-satisfied"],
            },
        },
        "version-marker": {
            "scenario": "version-marker",
            "expected_state": {
                "stage": "completed",
                "name": None,
                "greeting": None,
                "approved": False,
                "version": 2,
                "version_result": "v3_result",
                "reservation_id": None,
                "events": ["version:2"],
            },
        },
        "saga-compensation": {
            "scenario": "saga-compensation",
            "expected_state": {
                "stage": "compensated",
                "name": None,
                "greeting": None,
                "approved": False,
                "version": -1,
                "version_result": None,
                "reservation_id": "inventory-id-456",
                "events": ["compensated:payment declined"],
            },
        },
    }
    if family not in cases:
        raise ValueError(f"unknown replay conformance family {family!r}")
    return cases[family]


def _history(scenario: str, *, complete: bool = True) -> list[dict[str, Any]]:
    if scenario == "single-activity":
        return [
            _event(1, "ActivityCompleted", {
                "sequence": 1,
                "activity_type": "python-replay.greet",
                "result": _payload("Hello, Ada!"),
                "payload_codec": serializer.JSON_CODEC,
            }),
        ]

    if scenario == "signal-activity":
        events = [
            _event(1, "ConditionWaitOpened", {
                "sequence": 1,
                "condition_wait_id": "wait-name-1",
                "condition_key": "name-provided",
            }),
            _event(2, "SignalReceived", {
                "sequence": 1,
                "signal_name": "name-provided",
                "arguments": _payload(["Grace"]),
                "payload_codec": serializer.JSON_CODEC,
            }),
            _event(3, "ConditionWaitSatisfied", {
                "sequence": 1,
                "condition_wait_id": "wait-name-1",
            }),
        ]
        if complete:
            events.append(
                _event(4, "ActivityCompleted", {
                    "sequence": 2,
                    "activity_type": "python-replay.greet",
                    "result": _payload("Hello, Grace!"),
                    "payload_codec": serializer.JSON_CODEC,
                })
            )
        return events

    if scenario == "wait-condition":
        return [
            _event(1, "ConditionWaitOpened", {
                "sequence": 1,
                "condition_wait_id": "wait-approval-1",
                "condition_key": "approval",
            }),
            _event(2, "UpdateApplied", {
                "sequence": 1,
                "update_name": "approve",
                "arguments": _payload([True]),
                "payload_codec": serializer.JSON_CODEC,
            }),
            _event(3, "ConditionWaitSatisfied", {
                "sequence": 1,
                "condition_wait_id": "wait-approval-1",
            }),
        ]

    if scenario == "version-marker":
        return [
            _event(1, "VersionMarkerRecorded", {
                "sequence": 1,
                "change_id": "python-replay-version",
                "version": 2,
                "min_supported": -1,
                "max_supported": 2,
            }),
            _event(2, "ActivityCompleted", {
                "sequence": 2,
                "activity_type": "python-replay.versioned-v3",
                "result": _payload("v3_result"),
                "payload_codec": serializer.JSON_CODEC,
            }),
        ]

    if scenario == "saga-compensation":
        return [
            _event(1, "ActivityCompleted", {
                "sequence": 1,
                "activity_type": "python-replay.reserve-inventory",
                "result": _payload("inventory-id-456"),
                "payload_codec": serializer.JSON_CODEC,
            }),
            _event(2, "ChildRunFailed", {
                "sequence": 2,
                "child_workflow_type": "python-replay.charge-customer",
                "message": "payment declined",
                "exception_class": "RuntimeError",
            }),
            _event(3, "ActivityCompleted", {
                "sequence": 3,
                "activity_type": "python-replay.release-inventory",
                "result": _payload("cancelled-inventory-id-456"),
                "payload_codec": serializer.JSON_CODEC,
            }),
        ]

    raise ValueError(f"unknown replay conformance scenario {scenario!r}")


def _event(sequence: int, event_type: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": f"python-replay-conformance-event-{sequence:02d}",
        "sequence": sequence,
        "event_type": event_type,
        "type": event_type,
        "workflow_task_id": None,
        "workflow_command_id": None,
        "recorded_at": f"2026-05-21T00:00:{sequence:02d}.000000Z",
        "payload": dict(payload),
    }


def _history_bundle(scenario: str) -> dict[str, Any]:
    run_id = "python-replay-conformance-" + scenario.replace("-", "_")
    events = [
        _event(1, "WorkflowStarted", {
            "workflow_type": "python-replay-conformance",
            "arguments": _payload([scenario]),
            "payload_codec": serializer.JSON_CODEC,
        }),
    ]
    for event in _history(scenario):
        cloned = copy.deepcopy(event)
        cloned["sequence"] = int(cloned["sequence"]) + 1
        cloned["id"] = f"python-replay-conformance-event-{cloned['sequence']:02d}"
        events.append(cloned)

    bundle: dict[str, Any] = {
        "schema": "durable-workflow.v2.history-export",
        "schema_version": 1,
        "exported_at": "2026-05-21T00:00:00.000000Z",
        "dedupe_key": f"{run_id}:1:2026-05-21T00:00:00.000000Z",
        "history_complete": True,
        "workflow": {
            "instance_id": f"{run_id}-instance",
            "run_id": run_id,
            "run_number": 1,
            "workflow_type": "python-replay-conformance",
            "workflow_class": "durable_workflow.replay_conformance.ReplayConformanceWorkflow",
            "status": "completed",
            "last_history_sequence": len(events),
            "started_at": "2026-05-21T00:00:00.000000Z",
            "closed_at": "2026-05-21T00:01:00.000000Z",
        },
        "payloads": {
            "codec": serializer.JSON_CODEC,
            "arguments": {
                "available": True,
                "data": _payload([scenario]),
            },
            "output": {
                "available": False,
                "data": None,
            },
        },
        "history_events": events,
        "commands": [],
        "signals": [],
        "updates": [],
        "tasks": [],
        "activities": [],
        "timers": [],
        "failures": [],
        "links": {
            "projection_source": "rebuilt",
            "parents": [],
            "children": [],
        },
        "redaction": {
            "applied": False,
            "policy": None,
            "paths": [],
        },
        "codec_schemas": {},
        "payload_manifest": {
            "version": 1,
            "entries": [],
        },
    }
    bundle["integrity"] = _integrity(bundle)
    return bundle


def _integrity(bundle: Mapping[str, Any]) -> dict[str, Any]:
    cloned = {key: value for key, value in bundle.items() if key != "integrity"}
    canonical = json.dumps(
        _canonicalize(cloned),
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return {
        "canonicalization": "json-recursive-ksort-v1",
        "checksum_algorithm": "sha256",
        "checksum": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "signature_algorithm": None,
        "signature": None,
        "key_id": None,
    }


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def _first_finding(report: Mapping[str, Any], rule: str | None = None) -> dict[str, Any]:
    findings = report.get("findings")
    if not isinstance(findings, list):
        return {}
    for finding in findings:
        if not isinstance(finding, Mapping):
            continue
        if rule is None or finding.get("rule") == rule:
            return dict(finding)
    return {}


def _section_summary(
    scenario_results: Mapping[str, Mapping[str, Any]],
    scenario_ids: Sequence[str],
) -> dict[str, Any]:
    statuses = {
        scenario_id: _string_value(scenario_results.get(scenario_id, {}).get("status")) or "not_covered"
        for scenario_id in scenario_ids
    }
    return {
        "scenario_statuses": statuses,
        "passed": sum(1 for status in statuses.values() if status == "pass"),
        "total": len(statuses),
    }


def _command_summary(command: Any) -> dict[str, Any] | None:
    if command is None:
        return None
    if isinstance(command, ScheduleActivity):
        return {
            "command_type": "ScheduleActivity",
            "activity_type": command.activity_type,
            "arguments": list(command.arguments),
        }
    return {
        "command_type": type(command).__name__,
    }


def _history_event_types(events: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        _string_value(event.get("event_type") or event.get("type")) or "<unknown>"
        for event in events
    ]


def _payload(value: Any) -> str:
    return serializer.encode(value, codec=serializer.JSON_CODEC, size_warning=None)


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_artifact_metadata(metadata: Mapping[str, str]) -> dict[str, str]:
    canonical: dict[str, str] = {}
    for artifact, value in metadata.items():
        key = _canonical_artifact_key(artifact)
        canonical[key or artifact] = value
    return canonical


def _canonical_artifact_key(artifact: str) -> str | None:
    normalized = artifact.strip().lower().replace("_", "-")
    if normalized in {"server", "cli", "waterline"}:
        return normalized
    if normalized in {"workflow", "workflow-php"}:
        return "workflow-php"
    if normalized in {"python", "sdk-python"}:
        return "sdk-python"
    return None


def _artifact_metadata(metadata: Mapping[str, str], artifact: str) -> str | None:
    aliases = {
        "workflow-php": ("workflow-php", "workflow_php", "workflow"),
        "sdk-python": ("sdk-python", "sdk_python", "python"),
    }
    for key in aliases.get(artifact, (artifact,)):
        value = metadata.get(key)
        if value:
            return value
    return None


def _unpublished_version_reason(version: str) -> str | None:
    normalized = version.strip().lower()
    if normalized == "":
        return "empty_version"
    if any(token in normalized for token in ("<", ">", "${", "{{", "}}")):
        return "placeholder_template"
    if normalized in {"latest", "current", "head", "unresolved", "placeholder"}:
        return "placeholder_label"
    if "*" in normalized:
        return "wildcard_version"
    if any(token in normalized for token in ("local", "workspace", "source", "checkout")):
        return "local_or_source_version"
    if (
        normalized.startswith("dev-")
        or normalized.endswith("-dev")
        or normalized.endswith(".x-dev")
        or normalized in {"main", "master", "trunk", "v2"}
    ):
        return "dev_or_branch_version"
    return None


def _is_local_artifact_source(source: str) -> bool:
    normalized = _normalize_artifact_source(source)
    return any(
        f"_{token}_" in f"_{normalized}_"
        for token in ("dev", "editable", "local", "path", "repo", "source", "workspace", "checkout")
    )


def _is_published_artifact_source(artifact: str, source: str) -> bool:
    return _normalize_artifact_source(source) in PUBLISHED_ARTIFACT_SOURCES.get(artifact, set())


def _normalize_artifact_source(source: str) -> str:
    chars = [ch.lower() if ch.isalnum() else "_" for ch in source.strip()]
    return "_".join(part for part in "".join(chars).split("_") if part)


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) and value != "" else ""


def _parse_key_values(values: Sequence[str] | None) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values or []:
        key, sep, entry_value = value.partition("=")
        if sep and key.strip() and entry_value.strip():
            parsed[key.strip()] = entry_value.strip()
    return parsed


def _cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="durable-workflow-replay-conformance",
        description="Emit the Python SDK deterministic replay conformance evidence shard.",
    )
    parser.add_argument(
        "--artifact-version",
        action="append",
        default=[],
        help="Repeatable actor=version option for the published artifact tuple.",
    )
    parser.add_argument(
        "--artifact-source",
        action="append",
        default=[],
        help="Repeatable actor=source option proving the published artifact install channel.",
    )
    parser.add_argument("--output", help="Write the JSON report to a file instead of stdout.")
    parser.add_argument("--json", action="store_true", help="Accepted for parity; output is always JSON.")
    args = parser.parse_args(argv)

    report = compose_report(
        artifact_versions=_parse_key_values(args.artifact_version),
        artifact_sources=_parse_key_values(args.artifact_source),
    )
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text + "\n", encoding="utf-8")
    else:
        print(text)

    return 1 if any(scenario.get("status") != "pass" for scenario in report["scenario_results"]) else 0


def main(argv: Sequence[str] | None = None) -> int:
    return _cli(argv)


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())


__all__ = [
    "RESULT_SCHEMA",
    "RESULT_VERSION",
    "COVERAGE_SCOPE",
    "ReplayConformanceWorkflow",
    "ReplayConformanceDivergentWorkflow",
    "compose_report",
    "main",
]
