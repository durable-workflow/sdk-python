from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import Replayer, workflow
from durable_workflow.errors import ChildWorkflowFailed
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, WorkflowContext

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "golden_history"
FIXTURE_SCHEMA = "durable-workflow.golden-history.v1"
REQUIRED_FAMILIES = {
    "activity",
    "saga-compensation",
    "signal-update",
    "version-marker",
    "wait-condition",
}


@workflow.defn(name="golden.single-activity")
class GoldenSingleActivityWorkflow:
    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        return (yield ctx.schedule_activity("golden.greet", [name]))


@workflow.defn(name="golden.signal-wait")
class GoldenSignalWaitWorkflow:
    def __init__(self) -> None:
        self.approved_by: str | None = None

    @workflow.signal("approve")
    def approve(self, approved_by: str) -> None:
        self.approved_by = approved_by

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        satisfied = yield ctx.wait_condition(
            lambda: self.approved_by is not None,
            key="approval",
            timeout=30,
        )
        if not satisfied:
            return "timed-out"

        activity_result = yield ctx.schedule_activity("golden.notify", [self.approved_by])
        return {
            "approved_by": self.approved_by,
            "activity_result": activity_result,
        }


@workflow.defn(name="golden.timeout-wait")
class GoldenTimeoutWaitWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        if not (yield ctx.wait_condition(lambda: False, key="approval", timeout=5)):
            return "timed-out"

        return "satisfied"


@workflow.defn(name="golden.version-marker")
class GoldenVersionMarkerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        version = yield ctx.get_version("golden-version", 1, 2)
        activity = "golden.version-new-path" if version >= 2 else "golden.version-old-path"
        return (yield ctx.schedule_activity(activity, []))


@workflow.defn(name="golden.saga-compensation")
class GoldenSagaCompensationWorkflow:
    def run(self, ctx: WorkflowContext, order_id: str):  # type: ignore[no-untyped-def]
        reservation_id = yield ctx.schedule_activity("golden.reserve-inventory", [order_id])
        try:
            charge_id = yield ctx.start_child_workflow("golden.charge-customer", [order_id, reservation_id])
        except ChildWorkflowFailed as exc:
            return (
                yield ctx.schedule_activity(
                    "golden.release-inventory",
                    [order_id, reservation_id, str(exc)],
                )
            )

        return {"reservation_id": reservation_id, "charge_id": charge_id}


def _fixture_files() -> list[Path]:
    paths = sorted(FIXTURE_DIR.glob("*.json"))
    assert paths, f"expected at least one golden-history fixture in {FIXTURE_DIR}"

    return paths


def _fixture_id(case: dict[str, Any]) -> str:
    return str(case["id"])


def _load_fixture(path: Path) -> dict[str, Any]:
    with path.open() as handle:
        fixture = json.load(handle)

    assert isinstance(fixture, dict), f"{path.name} must use a top-level fixture object"
    assert fixture.get("fixture_schema") == FIXTURE_SCHEMA, f"{path.name} must declare {FIXTURE_SCHEMA}"

    source = fixture.get("source")
    assert isinstance(source, dict), f"{path.name} must declare release source metadata"
    assert source.get("runtime") in {"sdk-python", "workflow-php"}, f"{path.name} must name a supported runtime"
    assert source.get("package"), f"{path.name} must name the package that emitted the history"
    assert source.get("version"), f"{path.name} must name the released package version that emitted the history"
    assert "dev" not in str(source["version"]).lower(), f"{path.name} must not use a dev version as replay baseline"
    assert source.get("worker_protocol_version"), f"{path.name} must declare the worker protocol version"

    cases = fixture.get("cases")
    assert isinstance(cases, list), f"{path.name} must contain a cases list"
    assert cases, f"{path.name} must include at least one replay case"

    return fixture


def _golden_cases() -> list[dict[str, Any]]:
    loaded_cases: list[dict[str, Any]] = []
    covered_families: set[str] = set()

    for path in _fixture_files():
        fixture = _load_fixture(path)
        source = fixture["source"]

        for case in fixture["cases"]:
            assert isinstance(case, dict), f"{path.name} contains a non-object replay case"
            assert case.get("name"), f"{path.name} contains a replay case without a name"
            assert case.get("family") in REQUIRED_FAMILIES, f"{path.name}:{case.get('name')} has an unknown family"
            assert case.get("workflow_type"), f"{path.name}:{case['name']} must name a workflow type"
            assert "history" in case, f"{path.name}:{case['name']} must contain history"
            assert "expected" in case, f"{path.name}:{case['name']} must contain an expected result"

            covered_families.add(str(case["family"]))
            loaded_cases.append(
                {
                    **case,
                    "id": f"{source['runtime']}@{source['version']}::{case['name']}",
                    "source": source,
                }
            )

    assert covered_families >= REQUIRED_FAMILIES, (
        "golden-history fixtures must cover every replay-critical family; "
        f"missing {sorted(REQUIRED_FAMILIES - covered_families)}"
    )

    return loaded_cases


@pytest.mark.parametrize("case", _golden_cases(), ids=_fixture_id)
def test_golden_history_replay_contract(case: dict[str, Any]) -> None:
    replayer = Replayer(
        workflows=[
            GoldenSingleActivityWorkflow,
            GoldenSignalWaitWorkflow,
            GoldenTimeoutWaitWorkflow,
            GoldenVersionMarkerWorkflow,
            GoldenSagaCompensationWorkflow,
        ],
    )

    outcome = replayer.replay(
        case["history"],
        case["start_input"],
        workflow_type=case["workflow_type"],
    )

    assert len(outcome.commands) == 1
    expected = case["expected"]
    command = outcome.commands[0]

    if expected["command_type"] == "CompleteWorkflow":
        assert isinstance(command, CompleteWorkflow)
        assert command.result == expected["result"]

        return

    if expected["command_type"] == "ScheduleActivity":
        assert isinstance(command, ScheduleActivity)
        assert command.activity_type == expected["activity_type"]
        assert command.arguments == expected["arguments"]

        return

    raise AssertionError(f"Unsupported golden expected command type: {expected['command_type']}")
