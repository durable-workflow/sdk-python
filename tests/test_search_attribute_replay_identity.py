"""Replay identity of SearchAttributesUpserted (Workflow protocol 1.20).

Values follow Core's UpsertSearchAttributesCall exactly and fail closed.
Types follow Core's SearchAttributeReplayIdentity where the SDK can know
them; see test_recorded_type_identity_is_limited_to_what_an_untyped_upsert_fixes.
"""

from __future__ import annotations

import math
from copy import deepcopy
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.errors import NonDeterministicReplayError
from durable_workflow.workflow import WorkflowContext, replay
from tests.test_replay_clock import (
    RedriveChangedStageWorkflow,
    RedriveVisibilityWorkflow,
    only_event,
    redriven,
)

PLAIN = "search-attribute-value-mismatch"
REUSED = "redrive-reused-search-attribute-value-mismatch"


def authoring(attributes: Any) -> type:
    @workflow.defn(name="tests.replay.search-attribute-authored")
    class Authored(RedriveVisibilityWorkflow):
        def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
            yield ctx.upsert_memo({"stage": "greeting", "customer": name})
            yield ctx.upsert_search_attributes(attributes)
            return (yield ctx.schedule_activity("tests.redrive.first", [name]))

    return Authored


def recorded(fixture: str, attributes: Any, attribute_types: Any = ...) -> list[dict[str, Any]]:
    history = deepcopy(redriven(fixture))
    payload = only_event(history, "SearchAttributesUpserted")["payload"]
    payload["attributes"] = attributes
    if attribute_types is ...:
        payload.pop("attribute_types", None)
    else:
        payload["attribute_types"] = attribute_types
    return history


def replays(workflow_cls: type, history: list[dict[str, Any]]) -> list[Any]:
    return list(replay(workflow_cls, history, ["Taylor"], run_id="search-attribute-run", payload_codec="avro").commands)


def refused(workflow_cls: type, history: list[dict[str, Any]], reason: str) -> None:
    with pytest.raises(NonDeterministicReplayError, match=reason) as error:
        replays(workflow_cls, history)
    assert error.value.workflow_sequence == 2


@pytest.fixture(params=[PLAIN, REUSED])
def fixture(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def test_recorded_values_and_types_replay(fixture: str) -> None:
    payload = only_event(redriven(fixture), "SearchAttributesUpserted")["payload"]
    assert payload["attributes"] == {"Stage": "greeting"}
    assert payload["attribute_types"] == {"Stage": "keyword"}
    assert [type(command).__name__ for command in replays(RedriveVisibilityWorkflow, redriven(fixture))] == [
        "ScheduleActivity",
    ]


def test_changed_value_is_refused(fixture: str) -> None:
    refused(RedriveChangedStageWorkflow, redriven(fixture), "search_attribute_value_mismatch")


@pytest.mark.parametrize("authored", [
    {"Stage": "approval"}, {"Stage": None}, {"Stage": ""}, {"Stage": ["greeting"]}, {"stage": "greeting"},
    {"Stage": "greeting", "Region": "eu"},
])
def test_value_identity_is_exact(fixture: str, authored: dict[str, Any]) -> None:
    refused(authoring(authored), redriven(fixture), "search_attribute_value_mismatch")


@pytest.mark.parametrize(("authored", "stored", "types"), [
    ({"Stage": " greeting\n"}, {"Stage": "greeting"}, {"Stage": "string"}),
    ({"Stage": "greeting", "Old": "  "}, {"Old": None, "Stage": "greeting"}, {"Old": "string", "Stage": "keyword"}),
    ({"Stage": "greeting", "Old": ""}, {"Old": None, "Stage": "greeting"}, {"Old": "keyword", "Stage": "keyword"}),
    ({"Tags": [" a ", "b"]}, {"Tags": ["a", "b"]}, {"Tags": "keyword_list"}),
    ({"Tags": []}, {"Tags": []}, {"Tags": "keyword_list"}),
    ({"Count": 3}, {"Count": 3}, {"Count": "int"}),
    ({"Score": 2.5}, {"Score": 2.5}, {"Score": "float"}),
    ({"Done": False, "Gone": None}, {"Done": False, "Gone": None}, {"Done": "bool"}),
])
def test_server_normalized_history_replays(
    fixture: str, authored: dict[str, Any], stored: dict[str, Any], types: dict[str, str],
) -> None:
    # The Server trims strings and list items, stores a blank string as null,
    # and types every non-null raw value, including a blank one.
    commands = replays(authoring(authored), recorded(fixture, stored, types))
    assert [type(command).__name__ for command in commands] == ["ScheduleActivity"]


@pytest.mark.parametrize(("authored", "stored"), [
    ({"Count": 1}, {"Count": 1.0}),
    ({"Count": 1}, {"Count": True}),
    ({"Count": 1}, {"Count": "1"}),
    ({"Score": 1.0}, {"Score": 1}),
    ({"Done": True}, {"Done": 1}),
])
def test_values_are_compared_with_their_types(fixture: str, authored: dict[str, Any], stored: dict[str, Any]) -> None:
    refused(authoring(authored), recorded(fixture, stored), "search_attribute_value_mismatch")


class Greeting:
    """An unsupported object whose repr equals the recorded string."""

    def __repr__(self) -> str:
        return "greeting"


@pytest.mark.parametrize("authored", [
    {"Stage": Greeting()},  # Was accepted through json default=repr.
    {"Stage": {"greeting"}},
    {"Stage": ("greeting", 1)},
    {"Stage": {"value": "greeting"}},
    {"Stage": ["greeting", 1]},
    {"Stage": b"greeting"},
    {"Stage": math.nan},
    {"Stage": math.inf},
    {"Stage": 2**63},
    {1: "greeting"},
    {"bad key": "greeting"},
    {"Stage": "x" * 2049},
    {"Stage": ["x" * 256]},
    {},
])
def test_unsupported_authored_values_fail_closed(fixture: str, authored: Any) -> None:
    refused(authoring(authored), redriven(fixture), "search_attribute_value_mismatch: the current search attribute "
            "update is invalid")


def test_unsupported_authored_value_with_a_colliding_recorded_form_fails_closed(fixture: str) -> None:
    # Each authored value below collapsed onto its recorded form before.
    for authored, stored in [
        ({"Stage": Greeting()}, {"Stage": "greeting"}),
        ({"Tags": ("a", 1)}, {"Tags": ["a", "1"]}),
        ({7: "greeting"}, {"7": "greeting"}),
    ]:
        refused(authoring(authored), recorded(fixture, stored), "invalid")


@pytest.mark.parametrize("stored", [
    ["Stage", "greeting"], {}, "greeting", {"Stage": {"value": "greeting"}}, {"Stage": ["greeting", 1]},
    {"Stage": math.nan}, {"bad key": "greeting"},
])
def test_malformed_recorded_values_are_refused(fixture: str, stored: Any) -> None:
    refused(RedriveVisibilityWorkflow, recorded(fixture, stored), "search_attribute_value_mismatch")


@pytest.mark.parametrize("types", [
    {"Stage": "int"}, {"Stage": "float"}, {"Stage": "bool"}, {"Stage": "keyword_list"},
    {"Stage": "text"}, {"Stage": None}, {}, [], {"Stage": "keyword", "Other": "keyword"}, {"stage": "keyword"},
    "keyword", ["keyword"],
])
def test_recorded_type_mismatch_is_refused(fixture: str, types: Any) -> None:
    history = recorded(fixture, {"Stage": "greeting"}, types)
    refused(RedriveVisibilityWorkflow, history, "search_attribute_type_mismatch")


@pytest.mark.parametrize(("authored", "types"), [
    ({"Count": 3}, {"Count": "keyword"}),
    ({"Count": 3}, {"Count": "bool"}),
    ({"Score": 2.5}, {"Score": "int"}),
    ({"Done": True}, {"Done": "int"}),
    ({"Tags": ["a"]}, {"Tags": "keyword"}),
    ({"Gone": None}, {"Gone": "keyword"}),
    ({"Old": ""}, {}),
    ({"Old": "  ", "Stage": "greeting"}, {"Stage": "keyword"}),
])
def test_recorded_type_must_hold_the_value(fixture: str, authored: dict[str, Any], types: dict[str, str]) -> None:
    refused(authoring(authored), recorded(fixture, authored, types), "search_attribute_type_mismatch")


@pytest.mark.parametrize(("authored", "types"), [
    ({"Stage": "greeting"}, {"Stage": "string"}),
    ({"At": "2026-11-03T00:00:00Z"}, {"At": "datetime"}),
    ({"At": "2026-11-03"}, {"At": "datetime"}),
    ({"At": "Tue, 03 Nov 2026 00:00:00 +0000"}, {"At": "datetime"}),
    ({"Note": "two words"}, {"Note": "keyword"}),
    ({"At": "2026-11-03T00:00:00"}, {"At": "datetime"}),
    # Conservative: any string may be a registered datetime. The Server
    # refuses one that does not parse, so such a history cannot be written.
    ({"At": "greeting"}, {"At": "datetime"}),
    ({"Old": ""}, {"Old": "datetime"}),
    ({"Count": 3}, {"Count": "float"}),
])
def test_recorded_type_identity_is_limited_to_what_an_untyped_upsert_fixes(
    fixture: str, authored: dict[str, Any], types: dict[str, str],
) -> None:
    # The SDK's upsert declares no types, but the Server records a registered
    # key's namespace type in place of the inferred one: PHP Server
    # SearchAttributeValueValidator.php:93 ($registeredType ?? $declaredType)
    # filled in at WorkerController.php:2734, and Rust Server
    # search_attribute_upserts.rs:254-264. Replay cannot see the registry, so
    # these recorded types are legitimate for the same value and must replay.
    history = recorded(fixture, authored, types)
    assert replays(authoring(authored), history) == replays(authoring(authored), recorded(fixture, authored))


def test_history_without_types_keeps_value_only_identity(fixture: str) -> None:
    # Events recorded before attribute_types existed, as in Core.
    history = recorded(fixture, {"Stage": "greeting"})
    assert replays(RedriveVisibilityWorkflow, history) == replays(RedriveVisibilityWorkflow, redriven(fixture))
    refused(RedriveChangedStageWorkflow, history, "search_attribute_value_mismatch")


@pytest.mark.parametrize("authored", [("a", " b "), ["a", "b"]])
def test_authored_string_tuple_replays_as_its_wire_list(fixture: str, authored: Any) -> None:
    # A tuple of strings is sent as the same JSON list the Server records.
    history = recorded(fixture, {"Tags": ["a", "b"]}, {"Tags": "keyword_list"})
    commands = replays(authoring({"Tags": authored}), history)
    assert [type(command).__name__ for command in commands] == ["ScheduleActivity"]


@pytest.mark.parametrize("authored", [("a", 1), ("a", None), ("a", ("b",)), ("a", "c")])
def test_authored_tuple_keeps_the_strict_list_rules(fixture: str, authored: Any) -> None:
    history = recorded(fixture, {"Tags": ["a", "b"]}, {"Tags": "keyword_list"})
    refused(authoring({"Tags": authored}), history, "search_attribute_value_mismatch")


def test_recorded_history_never_treats_a_non_list_as_a_list(fixture: str) -> None:
    # Only the authored side is normalized; recorded JSON has no tuples.
    from durable_workflow.workflow import _canonical_search_attributes

    with pytest.raises(ValueError):
        _canonical_search_attributes({"Tags": ("a", "b")})
    assert _canonical_search_attributes({"Tags": ("a", "b")}, authored=True) == {"Tags": ("list", ("a", "b"))}


@pytest.mark.parametrize("types", [
    {"Stage": []}, {"Stage": {}}, {"Stage": ["keyword"]}, {"Stage": {"type": "keyword"}}, {"Stage": 1},
    {"Stage": True}, {"Stage": None}, {"Stage": 1.5},
])
def test_malformed_recorded_type_values_are_refused_not_raised(fixture: str, types: Any) -> None:
    history = recorded(fixture, {"Stage": "greeting"}, types)
    refused(RedriveVisibilityWorkflow, history, "search_attribute_type_mismatch")


@pytest.mark.parametrize("workflow_cls", [RedriveVisibilityWorkflow, RedriveChangedStageWorkflow])
def test_missing_recorded_attributes_are_refused(fixture: str, workflow_cls: type) -> None:
    history = deepcopy(redriven(fixture))
    del only_event(history, "SearchAttributesUpserted")["payload"]["attributes"]
    refused(workflow_cls, history, "search_attribute_update_missing: SearchAttributesUpserted history is missing "
            "its attributes")


# Durable command sequences. A recorded upsert must carry its own sequence,
# a sequence records at most one upsert, and two durable command kinds never
# share a sequence (only a wait and its own timeout timer do).
def refused_at(workflow_cls: type, history: list[dict[str, Any]], reason: str, sequence: int) -> None:
    with pytest.raises(NonDeterministicReplayError, match=f"{reason}:") as error:
        replays(workflow_cls, history)
    assert error.value.workflow_sequence == sequence


def _with(fixture: str, change: Any) -> list[dict[str, Any]]:
    history = deepcopy(redriven(fixture))
    change(history)
    return history


def _append(history: list[dict[str, Any]], event_type: str, payload: dict[str, Any]) -> None:
    history.append({"sequence": len(history) + 1, "event_type": event_type,
                    "recorded_at": "2026-11-03T00:05:01+00:00", "payload": payload})


@pytest.mark.parametrize("missing", [None, 0, -1, "", "two", "-1", True, 1.0, [1]])
def test_search_attribute_update_without_a_sequence_is_refused(fixture: str, missing: Any) -> None:
    def change(history: list[dict[str, Any]]) -> None:
        payload = only_event(history, "SearchAttributesUpserted")["payload"]
        if missing is None:
            del payload["sequence"]
        else:
            payload["sequence"] = missing

    refused_at(RedriveVisibilityWorkflow, _with(fixture, change), "search_attribute_update_missing_sequence", 2)


def test_duplicate_search_attribute_update_is_refused(fixture: str) -> None:
    def change(history: list[dict[str, Any]]) -> None:
        duplicate = deepcopy(only_event(history, "SearchAttributesUpserted"))
        _append(history, "SearchAttributesUpserted", duplicate["payload"])

    refused_at(RedriveVisibilityWorkflow, _with(fixture, change), "duplicate_search_attribute_update", 2)


@pytest.mark.parametrize(("event_type", "payload", "sequence"), [
    ("ActivityScheduled", {"sequence": 2, "activity_type": "tests.redrive.first"}, 2),
    ("ActivityCompleted", {"sequence": 1, "activity_type": "tests.redrive.first"}, 1),
    ("TimerScheduled", {"sequence": 2, "delay_seconds": 5}, 2),
    ("SideEffectRecorded", {"sequence": 1}, 1),
    ("VersionMarkerRecorded", {"sequence": 2, "change_id": "change", "version": 1}, 2),
    ("ConditionWaitOpened", {"sequence": 2, "condition_wait_id": "wait"}, 2),
    ("SignalWaitOpened", {"sequence": 1, "signal_name": "approve", "signal_wait_id": "wait"}, 1),
    ("MemoUpserted", {"sequence": 2}, 2),
])
def test_two_durable_commands_at_one_sequence_are_refused(
    fixture: str, event_type: str, payload: dict[str, Any], sequence: int,
) -> None:
    history = _with(fixture, lambda history: _append(history, event_type, payload))
    refused_at(RedriveVisibilityWorkflow, history, "durable_command_sequence_collision", sequence)


def test_a_search_attribute_update_cannot_take_an_earlier_step(fixture: str) -> None:
    # The upsert moved onto the memo's step 1: a collision, not an overwrite.
    history = _with(fixture, lambda history: only_event(history, "SearchAttributesUpserted")["payload"].__setitem__(
        "sequence", 1))
    refused_at(RedriveVisibilityWorkflow, history, "durable_command_sequence_collision", 1)


def test_only_a_wait_and_its_timeout_timer_share_a_sequence() -> None:
    from durable_workflow.workflow import _DURABLE_COMMAND_KINDS, _durable_command_kinds_compatible

    kinds = set(_DURABLE_COMMAND_KINDS.values())
    shared = {
        frozenset({left, right}) for left in kinds for right in kinds
        if left != right and _durable_command_kinds_compatible(frozenset({left, right}))
    }
    assert shared == {frozenset({"condition", "timer"}), frozenset({"signal", "timer"})}
    assert not _durable_command_kinds_compatible(frozenset({"condition", "signal", "timer"}))
