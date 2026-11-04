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
    redrive_fixture,
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


def replays(workflow_cls: type, history: list[dict[str, Any]], **options: Any) -> list[Any]:
    return list(replay(
        workflow_cls, history, ["Taylor"], run_id="search-attribute-run", payload_codec="avro", **options,
    ).commands)


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
def refused_at(
    workflow_cls: type, history: list[dict[str, Any]], reason: str, sequence: int, **options: Any,
) -> None:
    with pytest.raises(NonDeterministicReplayError, match=f"{reason}:") as error:
        replays(workflow_cls, history, **options)
    assert error.value.workflow_sequence == sequence


def _with(fixture: str, change: Any) -> list[dict[str, Any]]:
    history = deepcopy(redriven(fixture))
    change(history)
    return history


def _append(history: list[dict[str, Any]], event_type: str, payload: dict[str, Any]) -> None:
    history.append({"sequence": len(history) + 1, "event_type": event_type,
                    "recorded_at": "2026-11-03T00:05:01+00:00", "payload": payload})


@pytest.mark.parametrize(("value", "reason"), [
    (..., "durable_command_sequence_missing"),
    (None, "durable_command_sequence_missing"),
    *[(value, "durable_command_sequence_invalid") for value in (0, -1, "", "two", "-1", "2", True, 1.0, [1])],
])
def test_search_attribute_update_without_a_usable_sequence_is_refused(fixture: str, value: Any, reason: str) -> None:
    def change(history: list[dict[str, Any]]) -> None:
        payload = only_event(history, "SearchAttributesUpserted")["payload"]
        if value is ...:
            del payload["sequence"]
        else:
            payload["sequence"] = value

    refused_at(RedriveVisibilityWorkflow, _with(fixture, change), reason, 2)


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


def test_collisions_are_refused_when_they_involve_an_upsert() -> None:
    from durable_workflow.workflow import _DURABLE_COMMAND_KINDS, _durable_command_kinds_compatible

    kinds = set(_DURABLE_COMMAND_KINDS.values())
    for upsert in ("memo", "search_attributes"):
        assert _durable_command_kinds_compatible(frozenset({upsert}))
        for other in kinds - {upsert}:
            assert not _durable_command_kinds_compatible(frozenset({upsert, other})), other
    # Lifecycle events of one command, and pairs without an upsert, are not
    # judged by this rule (sdk-php#246 has the same scope).
    for kind in kinds - {"memo", "search_attributes"}:
        assert _durable_command_kinds_compatible(frozenset({kind}))
    assert _durable_command_kinds_compatible(frozenset({"condition", "timer"}))
    lifecycle = {kind: sorted(event for event, owner in _DURABLE_COMMAND_KINDS.items() if owner == kind)
                 for kind in ("activity", "timer", "condition", "signal", "child", "service_call")}
    assert lifecycle["signal"] == [
        "SignalApplied", "SignalWaitCancelled", "SignalWaitOpened", "SignalWaitReceived", "SignalWaitTimedOut",
    ]
    assert {"ActivityHeartbeatRecorded", "ActivityRetryScheduled"} <= set(lifecycle["activity"])
    assert _DURABLE_COMMAND_KINDS["CancellationScopeOpened"] == "cancellation_scope"
    assert "SignalReceived" not in _DURABLE_COMMAND_KINDS
    assert _DURABLE_COMMAND_KINDS["CooperativeCancellationDelivered"] == "cancellation_delivery"
    assert "CooperativeCancellationRequested" not in _DURABLE_COMMAND_KINDS


# Every event that claims a workflow sequence, by command kind. Fixed here so
# the table cannot shrink unnoticed; each kind, with its ordinary opener
# absent, collides with an upsert whichever of the two is recorded first.
EXPECTED_CLAIMS = {
    **dict.fromkeys(("ActivityScheduled", "ActivityStarted", "ActivityHeartbeatRecorded", "ActivityRetryScheduled",
                     "ActivityCompleted", "ActivityFailed", "ActivityTimedOut", "ActivityCancelled",
                     "ActivityCancellationAcknowledged"), "activity"),
    **dict.fromkeys(("TimerScheduled", "TimerFired", "TimerCancelled"), "timer"),
    **dict.fromkeys(("ChildWorkflowScheduled", "ChildRunStarted", "ChildRunCompleted", "ChildRunFailed",
                     "ChildRunCancelled", "ChildRunTerminated", "ChildCancellationRequested",
                     "ChildCancellationResolved"), "child"),
    **dict.fromkeys(("ConditionWaitOpened", "ConditionWaitSatisfied", "ConditionWaitTimedOut",
                     "ConditionWaitCancelled"), "condition"),
    **dict.fromkeys(("SignalWaitOpened", "SignalApplied", "SignalWaitReceived", "SignalWaitTimedOut",
                     "SignalWaitCancelled"), "signal"),
    **dict.fromkeys(("ServiceOperationScheduled", "ServiceCallStarted", "ServiceCallCompleted", "ServiceCallFailed",
                     "ServiceCallCancelled"), "service_call"),
    "CancellationScopeOpened": "cancellation_scope",
    **dict.fromkeys(("CooperativeCancellationDelivered", "CancellationScopeDelivered"), "cancellation_delivery"),
    "WorkflowContinuedAsNew": "continue_as_new",
    "SideEffectRecorded": "side_effect",
    "VersionMarkerRecorded": "version_marker",
    "MemoUpserted": "memo",
    "SearchAttributesUpserted": "search_attributes",
}
NEWLY_CLAIMED = (
    "ActivityHeartbeatRecorded", "ActivityRetryScheduled", "ActivityCancellationAcknowledged",
    "ChildCancellationRequested", "ChildCancellationResolved", "ServiceOperationScheduled", "SignalWaitReceived",
    "CancellationScopeOpened", "WorkflowContinuedAsNew",
)


def test_claim_table_is_exact() -> None:
    from durable_workflow.workflow import _DURABLE_COMMAND_KINDS

    assert _DURABLE_COMMAND_KINDS == EXPECTED_CLAIMS


# Deliveries need a valid cancellation history to reach the classifier; they
# have their own tests below.
@pytest.mark.parametrize("event_type", sorted(set(EXPECTED_CLAIMS) - {
    "SearchAttributesUpserted", "CooperativeCancellationDelivered", "CancellationScopeDelivered",
}))
@pytest.mark.parametrize("order", ["after_upsert", "before_upsert"])
def test_every_claimed_kind_collides_with_an_upsert_without_its_opener(
    fixture: str, event_type: str, order: str,
) -> None:
    scope = event_type == "CancellationScopeOpened"

    def change(history: list[dict[str, Any]]) -> None:
        upsert = only_event(history, "SearchAttributesUpserted")
        payload: dict[str, Any] = {"sequence": 2}
        if scope:
            # A canonical authored opening, so scope history admits it and
            # replay reaches the sequence classification.
            payload.update(schema="durable-workflow.cancellation-scope/v1", workflow_run_id="search-attribute-run",
                           scope_id="scope-one", parent_scope_id="root", shield_parent=False)
        claim = {"event_type": event_type, "recorded_at": upsert["recorded_at"], "payload": payload}
        at = history.index(upsert) + (1 if order == "after_upsert" else 0)
        history.insert(at, claim)
        for number, event in enumerate(history, start=1):
            event["sequence"] = number
            if scope:
                event.update(id=f"event-{number}", namespace="tenant")

    options = {"allow_cancellation_scope_authoring": True} if scope else {}
    refused_at(RedriveVisibilityWorkflow, _with(fixture, change), "durable_command_sequence_collision", 2, **options)


def test_newly_claimed_kinds_are_claimed() -> None:
    from durable_workflow.workflow import _DURABLE_COMMAND_KINDS

    assert set(NEWLY_CLAIMED) <= set(_DURABLE_COMMAND_KINDS)


@pytest.mark.parametrize("event_type", [
    "SignalReceived", "UpdateApplied", "UpdateCompleted", "FailureHandled", "CooperativeCancellationRequested",
    "CancellationScopeRequested", "CancellationScopeDeliveryPrepared", "SelectionResolved",
])
def test_pointer_records_claim_no_sequence(event_type: str) -> None:
    from durable_workflow.workflow import _DURABLE_COMMAND_KINDS

    assert event_type not in _DURABLE_COMMAND_KINDS



# Cancellation deliveries are durable command positions (Core
# WorkflowStepHistory::nextSequenceFromEvents). A delivery claims its sequence
# and, with sequence_span, every sequence through sequence + span - 1.
DELIVERY_FIXTURE = "redrive-successor-reused-upserts-cancellation-clock"


def _cooperative(fixture: str, sequence: int, span: int, order: str, drop_memo: bool = False) -> list[dict[str, Any]]:
    """The fixture's history with the cancellation of the cleanup fixture,
    delivered at ``sequence`` for ``span`` calls, and no scheduled opener."""
    source = redriven(DELIVERY_FIXTURE)
    request, delivery = deepcopy(source[-2]), deepcopy(source[-1])
    assert request["event_type"] == "CooperativeCancellationRequested"
    delivery["payload"].update(sequence=sequence, call_kind="parallel" if span > 1 else "activity")
    if span > 1:
        delivery["payload"]["sequence_span"] = span
    history = deepcopy(redriven(fixture))
    if drop_memo:
        history.remove(only_event(history, "MemoUpserted"))
    upsert = only_event(history, "SearchAttributesUpserted")
    at = history.index(upsert) + (1 if order == "after_upsert" else 0)
    history[at:at] = [request, delivery]
    for number, event in enumerate(history, start=1):
        event["sequence"] = number
    return history


def refused_with_cancellation(history: list[dict[str, Any]], sequence: int) -> None:
    workflow = redrive_fixture(DELIVERY_FIXTURE)["workflow"]
    with pytest.raises(NonDeterministicReplayError, match="durable_command_sequence_collision:") as error:
        replay(RedriveVisibilityWorkflow, history, ["Taylor"], run_id=workflow["run_id"],
               workflow_id=workflow["workflow_id"], payload_codec="avro")
    assert error.value.workflow_sequence == sequence


@pytest.mark.parametrize("order", ["after_upsert", "before_upsert"])
def test_a_single_call_delivery_at_an_upsert_sequence_collides(fixture: str, order: str) -> None:
    refused_with_cancellation(_cooperative(fixture, 2, 1, order), 2)


@pytest.mark.parametrize("order", ["after_upsert", "before_upsert"])
@pytest.mark.parametrize(("base", "span"), [(1, 3), (1, 2), (2, 2)])
def test_a_delivery_span_claims_every_sequence_it_covers(fixture: str, order: str, base: int, span: int) -> None:
    # The upsert at 2 is interior to 1..3, the end of 1..2, or the base of 2..3.
    # The memo at 1 is removed so the upsert is the only one inside the span.
    refused_with_cancellation(_cooperative(fixture, base, span, order, drop_memo=True), 2)


def test_a_delivery_span_ending_before_the_upsert_does_not_collide(fixture: str) -> None:
    # Span 1..1 does not reach the upsert at 2; the classifier stays quiet and
    # replay is refused, if at all, for an unrelated reason.
    history = _cooperative(fixture, 1, 1, "after_upsert", drop_memo=True)
    workflow = redrive_fixture(DELIVERY_FIXTURE)["workflow"]
    try:
        replay(RedriveVisibilityWorkflow, history, ["Taylor"], run_id=workflow["run_id"],
               workflow_id=workflow["workflow_id"], payload_codec="avro")
    except NonDeterministicReplayError as error:
        assert "durable_command_sequence_collision" not in str(error)


@pytest.mark.parametrize(("span", "claimed"), [
    (..., [3]),  # Absent means 1.
    (None, [3]),  # Null means 1, as in Core (WorkflowStepHistory.php:270).
    (1, [3]),
    (3, [3, 4, 5]),
])
@pytest.mark.parametrize("event_type", ["CooperativeCancellationDelivered", "CancellationScopeDelivered"])
def test_a_valid_delivery_span_claims_its_inclusive_range(event_type: str, span: Any, claimed: list[int]) -> None:
    from durable_workflow.workflow import _durable_command_claimed_range

    payload: dict[str, Any] = {"sequence": 3}
    if span is not ...:
        payload["sequence_span"] = span
    assert list(_durable_command_claimed_range(event_type, payload, 3)) == claimed
    assert list(_durable_command_claimed_range("ActivityScheduled", payload, 3)) == [3]


SECRET_SPANS = ["sk_live_TESTSECRET123", {"token": "TESTSECRET456"}]
MALFORMED_SPANS = [0, -1, "2", "3", 2.0, 3.0, True, False, [3], {"span": 3}, *SECRET_SPANS]


@pytest.mark.parametrize("span", MALFORMED_SPANS)
@pytest.mark.parametrize("event_type", ["CooperativeCancellationDelivered", "CancellationScopeDelivered"])
def test_a_malformed_delivery_span_is_refused_not_narrowed(event_type: str, span: Any) -> None:
    from durable_workflow.workflow import _delivery_sequence_span, _durable_command_claimed_range

    payload = {"sequence": 3, "sequence_span": span}
    with pytest.raises(ValueError, match="sequence_span must be a positive integer"):
        _delivery_sequence_span(payload)
    with pytest.raises(ValueError):
        _durable_command_claimed_range(event_type, payload, 3)


@pytest.mark.parametrize("span", MALFORMED_SPANS)
@pytest.mark.parametrize("order", ["after_upsert", "before_upsert"])
def test_a_malformed_delivery_span_refuses_replay(fixture: str, order: str, span: Any) -> None:
    # The upsert at 2 would be interior to an intended 1..3 span; a malformed
    # span refuses replay rather than claiming only the base.
    history = _cooperative(fixture, 1, 3, order, drop_memo=True)
    next(event for event in history if event["event_type"] == "CooperativeCancellationDelivered")[
        "payload"]["sequence_span"] = span
    workflow = redrive_fixture(DELIVERY_FIXTURE)["workflow"]
    with pytest.raises(NonDeterministicReplayError, match="durable_command_sequence_invalid:"):
        replay(RedriveVisibilityWorkflow, history, ["Taylor"], run_id=workflow["run_id"],
               workflow_id=workflow["workflow_id"], payload_codec="avro")


@pytest.mark.parametrize("span", MALFORMED_SPANS)
def test_a_malformed_scope_delivery_span_refuses_replay(span: Any) -> None:
    history, task = _scope_delivery_with_upsert("after_delivery")
    next(event for event in history if event["event_type"] == "CancellationScopeDelivered")[
        "payload"]["sequence_span"] = span
    with pytest.raises(NonDeterministicReplayError, match="durable_command_sequence_invalid:"):
        replay(RedriveVisibilityWorkflow, history, [], workflow_id=task["workflow_id"], run_id=task["run_id"],
               payload_codec="avro", allow_cancellation_scope_authoring=True, allow_cancellation_scope_delivery=True)


@pytest.mark.parametrize("span", [..., None])
def test_an_absent_or_null_delivery_span_claims_one_sequence(fixture: str, span: Any) -> None:
    # Absent or null means 1: a delivery at 1 does not reach the upsert at 2,
    # and replay is not refused for the span.
    history = _cooperative(fixture, 1, 1, "after_upsert", drop_memo=True)
    delivered = next(event for event in history if event["event_type"] == "CooperativeCancellationDelivered")
    assert "sequence_span" not in delivered["payload"]
    if span is None:
        delivered["payload"]["sequence_span"] = None
    workflow = redrive_fixture(DELIVERY_FIXTURE)["workflow"]
    try:
        replay(RedriveVisibilityWorkflow, history, ["Taylor"], run_id=workflow["run_id"],
               workflow_id=workflow["workflow_id"], payload_codec="avro")
    except NonDeterministicReplayError as error:
        assert "durable_command_sequence" not in str(error)


def _scope_delivery_with_upsert(order: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from tests.test_committed_cancellation_scope_history import fixture as scope_fixture

    value = scope_fixture("committed-scope-delivery.json", "unshielded")
    history = value["history"]
    delivered = next(event for event in history if event["event_type"] == "CancellationScopeDelivered")
    upsert = {"id": "upsert-event", "namespace": history[0]["namespace"], "event_type": "SearchAttributesUpserted",
              "timestamp": delivered.get("timestamp"), "payload": {
                  "sequence": delivered["payload"]["sequence"], "attributes": {"Stage": "greeting"},
                  "attribute_types": {"Stage": "keyword"}, "merged": {"Stage": "greeting"}}}
    at = history.index(delivered) + (1 if order == "after_delivery" else 0)
    history.insert(at, upsert)
    for number, event in enumerate(history, start=1):
        event["sequence"] = number
    return history, value["task"]


@pytest.mark.parametrize("order", ["after_delivery", "before_delivery"])
def test_a_scope_delivery_at_an_upsert_sequence_collides(order: str) -> None:
    # A valid committed scope delivery with no scheduled timer opener, replayed
    # with scope delivery enabled; an upsert at its sequence collides.
    history, task = _scope_delivery_with_upsert(order)
    sequence = next(event for event in history if event["event_type"] == "CancellationScopeDelivered")[
        "payload"]["sequence"]
    with pytest.raises(NonDeterministicReplayError, match="durable_command_sequence_collision:") as error:
        replay(RedriveVisibilityWorkflow, history, [], workflow_id=task["workflow_id"], run_id=task["run_id"],
               payload_codec="avro", allow_cancellation_scope_authoring=True, allow_cancellation_scope_delivery=True)
    assert error.value.workflow_sequence == sequence



# Diagnostics name reasons, keys, types, sequences and event types, never a
# recorded or authored value.
SECRETS = ("sk_live_TESTSECRET123", "TESTSECRET456", "sk_live_TESTSECRET789", "TESTSECRET000", "KEYSECRET")


def _assert_no_secret(error: BaseException) -> None:
    rendered = [str(error), repr(error), *(repr(argument) for argument in error.args)]
    for secret in SECRETS:
        assert not any(secret in text for text in rendered), (secret, rendered)


@pytest.mark.parametrize("span", SECRET_SPANS)
def test_a_secret_shaped_span_never_reaches_the_diagnostic(fixture: str, span: Any) -> None:
    from durable_workflow.workflow import _delivery_sequence_span

    with pytest.raises(ValueError) as helper_error:
        _delivery_sequence_span({"sequence": 1, "sequence_span": span})
    _assert_no_secret(helper_error.value)
    assert ("got string" if isinstance(span, str) else "got object") in str(helper_error.value)
    history = _cooperative(fixture, 1, 3, "after_upsert", drop_memo=True)
    next(event for event in history if event["event_type"] == "CooperativeCancellationDelivered")[
        "payload"]["sequence_span"] = span
    workflow = redrive_fixture(DELIVERY_FIXTURE)["workflow"]
    with pytest.raises(NonDeterministicReplayError, match="durable_command_sequence_invalid:") as error:
        replay(RedriveVisibilityWorkflow, history, ["Taylor"], run_id=workflow["run_id"],
               workflow_id=workflow["workflow_id"], payload_codec="avro")
    _assert_no_secret(error.value)
    scope_history, task = _scope_delivery_with_upsert("after_delivery")
    next(event for event in scope_history if event["event_type"] == "CancellationScopeDelivered")[
        "payload"]["sequence_span"] = span
    with pytest.raises(NonDeterministicReplayError, match="durable_command_sequence_invalid:") as scope_error:
        replay(RedriveVisibilityWorkflow, scope_history, [], workflow_id=task["workflow_id"], run_id=task["run_id"],
               payload_codec="avro", allow_cancellation_scope_authoring=True, allow_cancellation_scope_delivery=True)
    _assert_no_secret(scope_error.value)


@pytest.mark.parametrize(("authored", "stored", "types", "reason"), [
    ({"Stage": "sk_live_TESTSECRET789"}, {"Stage": "greeting"}, ..., "search_attribute_value_mismatch"),
    ({"Stage": "greeting"}, {"Stage": "sk_live_TESTSECRET789"}, ..., "search_attribute_value_mismatch"),
    ({"Stage": ["TESTSECRET000", 1]}, {"Stage": "greeting"}, ..., "search_attribute_value_mismatch"),
    ({"Stage": "greeting"}, {"Stage": {"token": "TESTSECRET000"}}, ..., "search_attribute_value_mismatch"),
    ({"sk_live_KEYSECRET 1": "x"}, {"Stage": "greeting"}, ..., "search_attribute_value_mismatch"),
    ({"Stage": "greeting"}, {"sk_live_KEYSECRET 1": "x"}, ..., "search_attribute_value_mismatch"),
    ({"Stage": "greeting"}, {"Stage": "greeting"}, {"Stage": "sk_live_TESTSECRET789"},
     "search_attribute_type_mismatch"),
    ({"Stage": "greeting"}, {"Stage": "greeting"}, {"Stage": {"token": "TESTSECRET000"}},
     "search_attribute_type_mismatch"),
    ({"Stage": "greeting"}, {"Stage": "greeting"}, "sk_live_TESTSECRET789", "search_attribute_type_mismatch"),
])
def test_search_attribute_diagnostics_never_echo_values(
    fixture: str, authored: dict[str, Any], stored: Any, types: Any, reason: str,
) -> None:
    with pytest.raises(NonDeterministicReplayError, match=reason) as error:
        replays(authoring(authored), recorded(fixture, stored, types))
    _assert_no_secret(error.value)


@pytest.mark.parametrize("change", [
    lambda payload: payload.__setitem__("sequence", "sk_live_TESTSECRET123"),
    lambda payload: payload.__setitem__("sequence", {"token": "TESTSECRET456"}),
])
def test_sequence_diagnostics_never_echo_values(fixture: str, change: Any) -> None:
    history = deepcopy(redriven(fixture))
    change(only_event(history, "SearchAttributesUpserted")["payload"])
    with pytest.raises(NonDeterministicReplayError, match="durable_command_sequence_invalid") as error:
        replays(RedriveVisibilityWorkflow, history)
    _assert_no_secret(error.value)
