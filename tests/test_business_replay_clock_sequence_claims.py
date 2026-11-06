from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from durable_workflow.errors import NonDeterministicReplayError
from tests.test_business_replay_clock import history, result
from tests.test_business_replay_clock_child import execute
from tests.test_business_replay_clock_child import history as child_history


def malformed_claim(payload: dict[str, Any], shape: str) -> None:
    sequence = payload["sequence"]
    if shape == "digits":
        payload["sequence"] = str(sequence)
    elif shape == "leading-zero":
        payload["sequence"] = f"0{sequence}"
    elif shape == "unicode-digits":
        payload["sequence"] = "١" if sequence == 1 else "٢"
    elif shape == "alias-only":
        payload["workflow_sequence"] = payload.pop("sequence")
    elif shape == "alias-conflict":
        payload["workflow_sequence"] = sequence + 1
    elif shape == "alias-string":
        payload["workflow_sequence"] = str(sequence)
    elif shape == "alias-fallback":
        payload.update(sequence=0, workflow_sequence=sequence)
    else:
        raise AssertionError(shape)


CLAIMS = ["digits", "leading-zero", "unicode-digits", "alias-only", "alias-conflict", "alias-string", "alias-fallback"]


@pytest.mark.parametrize("family,position,observation", [("activity", 1, 1), ("timer", 3, 2)])
@pytest.mark.parametrize("shape", CLAIMS)
def test_legacy_claimed_duplicate_opening_is_visible_but_cannot_prove_time(
    family: str, position: int, observation: int, shape: str,
) -> None:
    events = history()
    expected = result(events)
    duplicate = deepcopy(events[position])
    malformed_claim(duplicate["payload"], shape)
    events.insert(position, duplicate)
    expected["clock"][observation] = None
    expected["elapsed_seconds"] = None
    original = deepcopy(events)
    assert result(events) == expected, family
    assert result(events) == expected
    assert events == original


@pytest.mark.parametrize("shape", CLAIMS)
@pytest.mark.parametrize("foreign_call", [False, True])
def test_child_duplicate_claim_cannot_hide_behind_an_unindexed_counter_or_another_call(
    shape: str, foreign_call: bool,
) -> None:
    events = child_history()
    expected = execute(events)
    duplicate = deepcopy(events[1])
    malformed_claim(duplicate["payload"], shape)
    if foreign_call:
        duplicate["payload"]["child_call_id"] = "foreign-call"
    events.insert(1, duplicate)
    expected["clock"][1] = None
    assert execute(events) == expected
    assert execute(events) == expected


@pytest.mark.parametrize("value", [True, "1", "01", "١"])
def test_boolean_and_string_claims_do_not_weaken_the_required_actual_integer(value: Any) -> None:
    events = history()
    duplicate = deepcopy(events[1])
    duplicate["payload"]["sequence"] = value
    events.insert(1, duplicate)
    assert result(events)["clock"] == [None, None, "2026-11-01T00:00:05.654321+00:00"]


@pytest.mark.parametrize("position", [2, 4])
def test_duplicate_terminal_preserves_the_original_replay_error(position: int) -> None:
    events = history()
    duplicate = deepcopy(events[position])
    duplicate["payload"]["sequence"] = str(duplicate["payload"]["sequence"])
    events.insert(position, duplicate)
    with pytest.raises(NonDeterministicReplayError, match="Keep yielded workflow steps stable"):
        result(events)


def test_nonparseable_digit_counter_preserves_the_original_parser_error() -> None:
    events = history()
    events[1]["payload"]["sequence"] = "²"
    with pytest.raises(ValueError, match="invalid literal for int"):
        result(events)


def test_unclaimed_foreign_opening_does_not_poison_a_proved_sequence() -> None:
    events = history()
    expected = result(events)
    duplicate = deepcopy(events[1])
    duplicate["payload"]["sequence"] = "not-a-counter"
    duplicate["payload"]["activity_execution_id"] = "unrelated-activity"
    events.insert(1, duplicate)
    assert result(events) == expected
