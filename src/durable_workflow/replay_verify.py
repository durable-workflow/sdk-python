"""Offline replay verification helpers for the durable-workflow SDK.

This module gives operators, CI runners, and agents a single offline
surface for asking "does this workflow code still replay every history I
care about?" without spinning up a server. It backs the platform-level
replay verification contract published by the control plane: every
official runtime replays the same shared golden-history fixtures and
emits a verdict that promotion gates can act on.

The two entry points are:

- :func:`verify_golden_history` — load a directory of
  ``durable-workflow.golden-history.v1`` fixtures, replay each case
  against a registry of workflow classes, and emit a structured pass/
  drift/error report.
- :func:`verify_replay` — replay one history list against a single
  workflow class and emit the same report shape, suitable for ad-hoc
  CI gates that diff a stored history against a candidate build.

Both surfaces produce the report shape published by the control plane's
``replay_verification_contract.replay_diff`` block, so a Python verdict
is directly comparable to a workflow-php verdict — that is the cross-
runtime replay contract.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .errors import ChildWorkflowFailed, WorkflowFailed
from .workflow import (
    CompleteWorkflow,
    ReplayOutcome,
    Replayer,
    ScheduleActivity,
    StartChildWorkflow,
)

REPORT_SCHEMA = "durable-workflow.v2.replay-diff"
"""Schema name that callers use to interoperate with the control plane."""

REPORT_SCHEMA_VERSION = 1

FIXTURE_SCHEMA = "durable-workflow.golden-history.v1"

REQUIRED_FAMILIES = frozenset(
    {
        "activity",
        "saga-compensation",
        "signal-update",
        "version-marker",
        "wait-condition",
    }
)

STATUS_REPLAYED = "replayed"
STATUS_DRIFTED = "drifted"
STATUS_FAILED = "failed"

REASON_NONE = "none"
REASON_SHAPE_MISMATCH = "shape_mismatch"
REASON_REPLAY_ERROR = "replay_error"
REASON_BUNDLE_INVALID = "bundle_invalid"
REASON_EXPECTATION_MISMATCH = "expectation_mismatch"


@dataclass(frozen=True)
class CaseReport:
    """Per-case verdict from replaying one golden history fixture entry."""

    id: str
    status: str
    reason: str
    workflow_type: str | None
    family: str | None
    source: Mapping[str, Any] | None = None
    expected: Mapping[str, Any] | None = None
    observed: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "reason": self.reason,
            "workflow_type": self.workflow_type,
            "family": self.family,
            "source": dict(self.source) if self.source is not None else None,
            "expected": dict(self.expected) if self.expected is not None else None,
            "observed": dict(self.observed) if self.observed is not None else None,
            "error": dict(self.error) if self.error is not None else None,
        }


@dataclass
class GoldenHistoryReport:
    """Aggregate verdict for a golden-history fixture run."""

    schema: str = REPORT_SCHEMA
    schema_version: int = REPORT_SCHEMA_VERSION
    status: str = STATUS_REPLAYED
    fixture_schema: str = FIXTURE_SCHEMA
    cases: list[CaseReport] = field(default_factory=list)
    missing_families: list[str] = field(default_factory=list)
    summary: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "schema_version": self.schema_version,
            "status": self.status,
            "fixture_schema": self.fixture_schema,
            "summary": dict(self.summary),
            "missing_families": list(self.missing_families),
            "cases": [case.to_dict() for case in self.cases],
        }


def verify_replay(
    workflow_cls: type,
    history: Iterable[dict[str, Any]] | Mapping[str, Any],
    *,
    start_input: list[Any] | None = None,
    expected: Mapping[str, Any] | None = None,
    case_id: str = "verify_replay",
    workflow_type: str | None = None,
) -> CaseReport:
    """Replay one history against a workflow class and produce a CaseReport.

    Pass ``expected`` to assert that the recorded history is the one the
    code under test would produce — the same shape used by
    ``durable-workflow.golden-history.v1`` fixtures
    (``{"command_type": ..., "result": ...}``).
    """

    replayer = Replayer(workflows=[workflow_cls])
    return _replay_case(
        replayer,
        case={
            "id": case_id,
            "history": list(history) if not isinstance(history, Mapping) else history,
            "start_input": list(start_input or []),
            "workflow_type": workflow_type
            or getattr(workflow_cls, "__workflow_name__", workflow_cls.__name__),
            "expected": dict(expected) if expected is not None else None,
            "family": None,
            "source": None,
        },
    )


def verify_golden_history(
    fixture_dir: str | Path,
    workflows: Sequence[type],
    *,
    required_families: Iterable[str] = REQUIRED_FAMILIES,
) -> GoldenHistoryReport:
    """Replay every fixture under ``fixture_dir`` and emit a structured report.

    ``workflows`` is the closed registry of workflow classes used to
    resolve each case's ``workflow_type``. Missing families and replay
    drifts are surfaced in the returned report rather than raised — the
    caller decides whether to gate promotion on them.
    """

    fixtures = sorted(Path(fixture_dir).glob("*.json"))

    if not fixtures:
        report = GoldenHistoryReport(status=STATUS_FAILED)
        report.summary = {
            "fixtures": 0,
            "cases": 0,
            "replayed": 0,
            "drifted": 0,
            "failed": 0,
        }
        report.cases.append(
            CaseReport(
                id="<no-fixtures>",
                status=STATUS_FAILED,
                reason=REASON_BUNDLE_INVALID,
                workflow_type=None,
                family=None,
                error={
                    "class": "FileNotFoundError",
                    "message": f"no golden-history fixtures found in {fixture_dir}",
                },
            )
        )
        return report

    replayer = Replayer(workflows=list(workflows))
    cases: list[CaseReport] = []
    covered_families: set[str] = set()

    for path in fixtures:
        fixture = _load_fixture_or_record_error(path, cases)
        if fixture is None:
            continue

        for case in fixture.get("cases", []):
            if not isinstance(case, dict):
                continue
            family = case.get("family")
            if isinstance(family, str):
                covered_families.add(family)

            case_id = (
                f"{fixture['source']['runtime']}@{fixture['source']['version']}"
                f"::{case.get('name', '<unnamed>')}"
            )
            cases.append(
                _replay_case(
                    replayer,
                    {
                        "id": case_id,
                        "history": case.get("history", []),
                        "start_input": case.get("start_input", []),
                        "workflow_type": case.get("workflow_type"),
                        "expected": case.get("expected"),
                        "family": family,
                        "source": fixture["source"],
                    },
                )
            )

    missing = sorted(set(required_families) - covered_families)
    summary = _summarize(cases)
    summary["fixtures"] = len(fixtures)

    if any(case.status == STATUS_FAILED for case in cases) or missing:
        overall = STATUS_FAILED
    elif any(case.status == STATUS_DRIFTED for case in cases):
        overall = STATUS_DRIFTED
    else:
        overall = STATUS_REPLAYED

    return GoldenHistoryReport(
        status=overall,
        cases=cases,
        missing_families=missing,
        summary=summary,
    )


def _load_fixture_or_record_error(
    path: Path, cases: list[CaseReport]
) -> Mapping[str, Any] | None:
    try:
        with path.open() as handle:
            fixture = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        cases.append(
            CaseReport(
                id=f"<unparseable:{path.name}>",
                status=STATUS_FAILED,
                reason=REASON_BUNDLE_INVALID,
                workflow_type=None,
                family=None,
                error={"class": type(exc).__name__, "message": str(exc)},
            )
        )
        return None

    if not isinstance(fixture, dict) or fixture.get("fixture_schema") != FIXTURE_SCHEMA:
        cases.append(
            CaseReport(
                id=f"<schema-drift:{path.name}>",
                status=STATUS_FAILED,
                reason=REASON_BUNDLE_INVALID,
                workflow_type=None,
                family=None,
                error={
                    "class": "FixtureSchemaError",
                    "message": (
                        f"{path.name} must declare fixture_schema={FIXTURE_SCHEMA}; "
                        f"found {fixture.get('fixture_schema')!r}"
                    ),
                },
            )
        )
        return None

    source = fixture.get("source")
    if not isinstance(source, dict) or not source.get("runtime") or not source.get("version"):
        cases.append(
            CaseReport(
                id=f"<source-missing:{path.name}>",
                status=STATUS_FAILED,
                reason=REASON_BUNDLE_INVALID,
                workflow_type=None,
                family=None,
                error={
                    "class": "FixtureSchemaError",
                    "message": f"{path.name} is missing source.runtime or source.version",
                },
            )
        )
        return None

    return fixture


def _replay_case(replayer: Replayer, case: Mapping[str, Any]) -> CaseReport:
    case_id = str(case.get("id", "<unnamed>"))
    workflow_type = case.get("workflow_type")
    family = case.get("family") if isinstance(case.get("family"), str) else None
    source = case.get("source") if isinstance(case.get("source"), Mapping) else None

    try:
        outcome = replayer.replay(
            case["history"],
            case.get("start_input") or [],
            workflow_type=workflow_type,
        )
    except (TypeError, ValueError, ChildWorkflowFailed, WorkflowFailed) as exc:
        return CaseReport(
            id=case_id,
            status=STATUS_DRIFTED,
            reason=REASON_SHAPE_MISMATCH,
            workflow_type=workflow_type,
            family=family,
            source=source,
            expected=case.get("expected"),
            error={"class": type(exc).__name__, "message": str(exc)},
        )
    except Exception as exc:  # pragma: no cover - defensive
        return CaseReport(
            id=case_id,
            status=STATUS_FAILED,
            reason=REASON_REPLAY_ERROR,
            workflow_type=workflow_type,
            family=family,
            source=source,
            expected=case.get("expected"),
            error={
                "class": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            },
        )

    expected = case.get("expected")
    observed = _summarize_outcome(outcome)

    if expected is None:
        return CaseReport(
            id=case_id,
            status=STATUS_REPLAYED,
            reason=REASON_NONE,
            workflow_type=workflow_type,
            family=family,
            source=source,
            observed=observed,
        )

    mismatch = _check_expected(outcome, expected)
    if mismatch is None:
        return CaseReport(
            id=case_id,
            status=STATUS_REPLAYED,
            reason=REASON_NONE,
            workflow_type=workflow_type,
            family=family,
            source=source,
            expected=dict(expected),
            observed=observed,
        )

    return CaseReport(
        id=case_id,
        status=STATUS_DRIFTED,
        reason=REASON_EXPECTATION_MISMATCH,
        workflow_type=workflow_type,
        family=family,
        source=source,
        expected=dict(expected),
        observed=observed,
        error={"class": "ExpectationMismatch", "message": mismatch},
    )


def _check_expected(outcome: ReplayOutcome, expected: Mapping[str, Any]) -> str | None:
    expected_type = expected.get("command_type")

    if not outcome.commands:
        return f"expected {expected_type!r} but the workflow yielded no terminal command"

    command = outcome.commands[0]

    if expected_type == "CompleteWorkflow":
        if not isinstance(command, CompleteWorkflow):
            return (
                f"expected CompleteWorkflow but workflow yielded {type(command).__name__}"
            )
        if command.result != expected.get("result"):
            return (
                f"workflow result {command.result!r} does not match expected "
                f"{expected.get('result')!r}"
            )
        return None

    if expected_type == "ScheduleActivity":
        if not isinstance(command, ScheduleActivity):
            return (
                f"expected ScheduleActivity but workflow yielded {type(command).__name__}"
            )
        if command.activity_type != expected.get("activity_type"):
            return (
                f"activity_type {command.activity_type!r} does not match expected "
                f"{expected.get('activity_type')!r}"
            )
        if list(command.arguments) != list(expected.get("arguments", [])):
            return "activity arguments do not match expected"
        return None

    if expected_type == "StartChildWorkflow":
        if not isinstance(command, StartChildWorkflow):
            return (
                f"expected StartChildWorkflow but workflow yielded {type(command).__name__}"
            )
        if command.workflow_type != expected.get("workflow_type"):
            return (
                f"child workflow_type {command.workflow_type!r} does not match expected "
                f"{expected.get('workflow_type')!r}"
            )
        return None

    return f"unsupported expected command_type {expected_type!r}"


def _summarize_outcome(outcome: ReplayOutcome) -> dict[str, Any]:
    if not outcome.commands:
        return {"command_type": None}

    command = outcome.commands[0]
    summary: dict[str, Any] = {"command_type": type(command).__name__}

    if isinstance(command, CompleteWorkflow):
        summary["result"] = command.result
    elif isinstance(command, ScheduleActivity):
        summary["activity_type"] = command.activity_type
        summary["arguments"] = list(command.arguments)
    elif isinstance(command, StartChildWorkflow):
        summary["workflow_type"] = command.workflow_type

    return summary


def _summarize(cases: Sequence[CaseReport]) -> dict[str, int]:
    summary = {
        "cases": len(cases),
        "replayed": 0,
        "drifted": 0,
        "failed": 0,
    }
    for case in cases:
        if case.status == STATUS_REPLAYED:
            summary["replayed"] += 1
        elif case.status == STATUS_DRIFTED:
            summary["drifted"] += 1
        else:
            summary["failed"] += 1
    return summary


def _resolve_workflow_loader(spec: str) -> Sequence[type]:
    """Resolve ``module:callable`` to a list of workflow classes.

    The callable must take no arguments and return an iterable of
    workflow classes; this is the same convention used by the
    ``durable-workflow worker`` entry point.
    """

    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise ValueError("workflow loader must be 'module:callable'")
    import importlib

    module = importlib.import_module(module_name)
    factory: Callable[[], Iterable[type]] = getattr(module, attr)
    return list(factory())


def _cli(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m durable_workflow.replay_verify",
        description=(
            "Replay golden-history fixtures against a registry of workflow "
            "classes and emit a JSON verdict consumable by promotion gates."
        ),
    )
    parser.add_argument(
        "fixture_dir",
        help="Directory of durable-workflow.golden-history.v1 JSON fixtures.",
    )
    parser.add_argument(
        "--workflows",
        required=True,
        help=(
            "Workflow loader of the form 'module:callable'. The callable "
            "must return an iterable of workflow classes."
        ),
    )
    parser.add_argument(
        "--output",
        help="Write the JSON report to a file instead of stdout.",
    )
    parser.add_argument(
        "--strict-missing-families",
        action="store_true",
        help="Treat missing required families as a failure exit.",
    )
    args = parser.parse_args(argv)

    workflows = _resolve_workflow_loader(args.workflows)
    report = verify_golden_history(args.fixture_dir, workflows)

    payload = report.to_dict()
    text = json.dumps(payload, indent=2, sort_keys=True)

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)

    if report.status == STATUS_FAILED:
        return 1
    if report.status == STATUS_DRIFTED:
        return 1
    if args.strict_missing_families and report.missing_families:
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point used by ``python -m durable_workflow.replay_verify``."""

    return _cli(argv)


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())


__all__ = [
    "REPORT_SCHEMA",
    "REPORT_SCHEMA_VERSION",
    "FIXTURE_SCHEMA",
    "REQUIRED_FAMILIES",
    "STATUS_REPLAYED",
    "STATUS_DRIFTED",
    "STATUS_FAILED",
    "REASON_NONE",
    "REASON_SHAPE_MISMATCH",
    "REASON_REPLAY_ERROR",
    "REASON_BUNDLE_INVALID",
    "REASON_EXPECTATION_MISMATCH",
    "CaseReport",
    "GoldenHistoryReport",
    "verify_replay",
    "verify_golden_history",
    "main",
]
