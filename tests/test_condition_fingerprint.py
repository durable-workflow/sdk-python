"""Stable v2 condition fingerprints and v1 compatibility (#258)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import serializer, workflow
from durable_workflow._condition_fingerprint import (
    V2_PREFIX,
    fingerprint,
    fingerprint_verdict,
    is_legacy,
    legacy_fingerprint,
)
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.workflow import CompleteWorkflow, WaitCondition, WorkflowContext, replay
from tests.condition_fingerprint_predicates import Approvals

ROOT = Path(__file__).resolve().parent.parent
GOLDEN = json.loads((ROOT / "tests/fixtures/condition_fingerprints_v2.json").read_text(encoding="utf-8"))
FOREIGN_V1 = "sha256:" + "1" * 64  # a v1 value recorded by another Python version or process
OTHER_V1 = "sha256:" + "2" * 64


def test_v2_fingerprints_match_the_values_recorded_on_python_3_12() -> None:
    # Every CI Python version (3.10 to 3.15) must reproduce the 3.12 values.
    computed = {name: fingerprint(predicate) for name, predicate in Approvals().predicates().items()}
    assert computed == GOLDEN["fingerprints"]
    assert all(value.startswith(V2_PREFIX) for value in computed.values())


def test_v2_fingerprints_are_the_same_in_fresh_processes() -> None:
    # v1 changed in every process for nested functions (genexpr, nested lambda).
    script = (
        "import json, sys; sys.path.insert(0, sys.argv[1]);"
        "from tests.condition_fingerprint_predicates import Approvals;"
        "from durable_workflow._condition_fingerprint import fingerprint;"
        "print(json.dumps({k: fingerprint(v) for k, v in Approvals().predicates().items()}))"
    )
    for _ in range(2):
        output = subprocess.run([sys.executable, "-c", script, str(ROOT)], check=True, capture_output=True,
                                text=True, cwd=ROOT).stdout
        assert json.loads(output) == GOLDEN["fingerprints"]


def test_v2_ignores_formatting_but_not_logic() -> None:
    reformatted = lambda: (  # noqa: E731
        Approvals().approved   # a comment
    )
    flipped = lambda: not Approvals().approved  # noqa: E731
    original = lambda: Approvals().approved  # noqa: E731
    # Same module and qualname: only the predicate's logic decides.
    assert fingerprint(reformatted) == fingerprint(original)
    assert fingerprint(original) != fingerprint(flipped)


def test_a_predicate_without_source_has_no_fingerprint() -> None:
    predicate = eval("lambda: False")  # noqa: S307 - a predicate with no source file
    assert fingerprint(predicate) is None
    assert WorkflowContext(run_id="x").wait_condition(predicate, key="k").condition_definition_fingerprint is None


@pytest.mark.parametrize(("recorded", "authored", "expected"), [
    (None, "dwpy2:sha256:" + "a" * 64, "match"),
    ("dwpy2:sha256:" + "a" * 64, "dwpy2:sha256:" + "a" * 64, "match"),
    ("dwpy2:sha256:" + "a" * 64, "dwpy2:sha256:" + "b" * 64, "mismatch"),
    ("dwpy2:sha256:" + "a" * 64, None, "unverifiable"),
    (FOREIGN_V1, "dwpy2:sha256:" + "a" * 64, "unverifiable"),
    (FOREIGN_V1, None, "unverifiable"),
    ("sha256:different", "dwpy2:sha256:" + "a" * 64, "mismatch"),
    (FOREIGN_V1, "changed", "mismatch"),  # an explicitly supplied non-v2 identity is compared exactly
    (FOREIGN_V1, FOREIGN_V1, "match"),
])
def test_fingerprint_verdicts(recorded: Any, authored: str | None, expected: str) -> None:
    assert fingerprint_verdict(recorded, authored, lambda: False) == expected


def test_a_v1_value_from_this_process_still_matches() -> None:
    def predicate() -> bool:
        return False

    assert is_legacy(legacy_fingerprint(predicate))
    assert fingerprint_verdict(legacy_fingerprint(predicate), fingerprint(predicate), predicate) == "match"


# Mixed chains across the upgrade (General's correction 1).
@workflow.defn(name="tests.fingerprint.keyless-approval")
class KeylessApproval:
    def __init__(self) -> None:
        self.approved = False

    @workflow.signal("approve")
    def approve(self) -> None:
        self.approved = True

    @workflow.signal("note")
    def note(self) -> None:
        return None

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.approved)
        return "approved"


AT = "2026-11-04T00:00:0{}+00:00"


def _opened(wait_id: str, sequence: int, value: str | None, second: int) -> dict[str, Any]:
    payload: dict[str, Any] = {"condition_wait_id": wait_id, "sequence": sequence}
    if value is not None:
        payload["condition_definition_fingerprint"] = value
    return {"event_type": "ConditionWaitOpened", "recorded_at": AT.format(second), "payload": payload}


def _signal(name: str, sequence: int, second: int) -> dict[str, Any]:
    return {"event_type": "SignalReceived", "recorded_at": AT.format(second), "payload": {
        "signal_name": name, "value": serializer.envelope([]), "payload_codec": "avro",
        "workflow_sequence": sequence}}


def _satisfied(wait_id: str, sequence: int, second: int) -> dict[str, Any]:
    return {"event_type": "ConditionWaitSatisfied", "recorded_at": AT.format(second),
            "payload": {"condition_wait_id": wait_id, "sequence": sequence}}


START = {"event_type": "WorkflowStarted", "recorded_at": AT.format(0), "payload": {}}


def test_a_reopen_of_a_v1_first_open_sends_the_recorded_v1_value() -> None:
    # Opened under 2.14.0 (v1), then a false wake under 2.14.1. The Server
    # records ConditionWaitSatisfied with this task's reopen, so the task sees
    # only the open and the signal.
    history = [START, _opened("w1", 1, FOREIGN_V1, 1), _signal("note", 1, 2)]
    (command,) = replay(KeylessApproval, history, []).commands
    assert isinstance(command, WaitCondition)
    assert command.condition_definition_fingerprint == FOREIGN_V1


def test_a_v1_chain_links_v1_to_v1_after_the_upgrade() -> None:
    history = [START, _opened("w1", 1, FOREIGN_V1, 1), _signal("note", 1, 2), _satisfied("w1", 1, 2),
               _opened("w2", 2, FOREIGN_V1, 3), _signal("approve", 2, 4), _satisfied("w2", 2, 4)]
    assert list(replay(KeylessApproval, history, []).commands) == [CompleteWorkflow("approved")]


def test_a_reopen_without_source_keeps_echoing_the_recorded_value() -> None:
    @workflow.defn(name="tests.fingerprint.no-source")
    class NoSource:
        def __init__(self) -> None:
            self.approved = False

        @workflow.signal("note")
        def note(self) -> None:
            return None

        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            yield ctx.wait_condition(eval("lambda self=self: self.approved"))  # noqa: S307
            return "approved"

    history = [START, _opened("w1", 1, FOREIGN_V1, 1), _signal("note", 1, 2)]
    (command,) = replay(NoSource, history, []).commands
    assert isinstance(command, WaitCondition) and command.condition_definition_fingerprint == FOREIGN_V1


# Keyless unverifiable values never bind a wait to another call (correction 2).
@workflow.defn(name="tests.fingerprint.two-keyless-waits")
class TwoKeylessWaits:
    def __init__(self) -> None:
        self.approvals = 0

    @workflow.signal("approve")
    def approve(self) -> None:
        self.approvals += 1

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.wait_condition(lambda: self.approvals >= 1)
        yield ctx.wait_condition(lambda: self.approvals >= 2)
        return self.approvals


def test_a_later_keyless_v1_open_binds_to_the_next_call_not_as_a_reopen() -> None:
    history = [START, _opened("w1", 1, FOREIGN_V1, 1), _signal("approve", 1, 2), _satisfied("w1", 1, 2),
               _opened("w2", 2, OTHER_V1, 3), _signal("approve", 2, 4), _satisfied("w2", 2, 4)]
    assert list(replay(TwoKeylessWaits, history, []).commands) == [CompleteWorkflow(2)]


def test_a_changed_v2_predicate_is_refused() -> None:
    (command,) = replay(KeylessApproval, [START], []).commands
    assert isinstance(command, WaitCondition) and command.condition_definition_fingerprint.startswith(V2_PREFIX)
    changed = "dwpy2:sha256:" + "0" * 64
    history = [START, _opened("w1", 1, changed, 1), _signal("approve", 1, 2), _satisfied("w1", 1, 2)]
    with pytest.raises(NonDeterministicReplayError, match="condition_fingerprint_changed"):
        replay(KeylessApproval, history, [])
