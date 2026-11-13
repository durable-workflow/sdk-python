from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from durable_workflow import serializer, workflow
from durable_workflow.errors import (
    ActivityFailed,
    ChildWorkflowFailed,
    QueryFailed,
    TypedValueError,
    UpdateValidationFailed,
)
from durable_workflow.workflow import (
    CompleteWorkflow,
    FailUpdate,
    LocalActivityExecutionAborted,
    WorkflowContext,
    _read_parallel_group_policies,
    _replay_state,
    apply_update,
    commands_to_server_commands,
    query_state,
    replay,
    validate_update,
)
from tests.business_clock_workflows import ParallelFailureBusinessClockWorkflow, clock_text
from tests.test_parallel_completion_order_replay import ParallelCompletionOrderMixed, _terminal_event

FIXTURE = Path(__file__).parent / "fixtures/replay_regressions/legacy-parallel-failure-remains-pending.json"


@workflow.defn(name="tests.replay.parallel-failure-policy")
class ParallelFailurePolicyWorkflow:
    def __init__(self) -> None:
        self.caught: str | None = None

    @workflow.query("caught")
    def caught_failure(self) -> str | None:
        return self.caught

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [ctx.schedule_activity("slow-first", []), ctx.schedule_activity("fast-second", [])]
        except ActivityFailed as error:
            self.caught = error.activity_type
        return self.caught


def test_absent_policy_keeps_the_failure_handler_suspended_until_all_members_settle() -> None:
    history = json.loads(FIXTURE.read_text())["history"]
    outcome = replay(ParallelFailurePolicyWorkflow, history, [])
    assert not any(isinstance(command, CompleteWorkflow) for command in outcome.commands)
    assert query_state(ParallelFailurePolicyWorkflow, history, [], "caught") is None


def test_absent_policy_preserves_positional_failure_after_a_later_sibling_settles() -> None:
    history = json.loads(FIXTURE.read_text())["history"]
    first = dict(history[1]["payload"])
    first["message"] = "lower position failed later"
    history.append({"event_type": "ActivityFailed", "recorded_at": "2026-01-01T00:00:03Z", "payload": first})
    outcome = replay(ParallelFailurePolicyWorkflow, history, [])
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == "slow-first"


def explicit_history(policy: str = "propagate") -> list[dict]:
    history = json.loads(FIXTURE.read_text())["history"]
    for event in history:
        payload = event["payload"]
        if "parallel_group_id" in payload:
            payload["parallel_group_policy"] = policy
            for frame in payload["parallel_group_path"]:
                frame["parallel_group_policy"] = policy
    return history


def draft_replay(history: list[dict], definition: type = ParallelFailurePolicyWorkflow):
    return _replay_state(
        definition, history, [], payload_codec="avro", parallel_group_policies=("propagate",),
    ).outcome


@pytest.mark.parametrize("policy", ["propagate", "cancel_remaining", "settle"])
def test_public_replay_does_not_silently_interpret_an_unsupported_recorded_policy(policy: str) -> None:
    history = explicit_history(policy)
    with pytest.raises(LocalActivityExecutionAborted, match="parallel_group_policy_not_supported"):
        replay(ParallelFailurePolicyWorkflow, history, [])


def test_explicit_propagate_raises_while_the_other_member_is_open() -> None:
    outcome = draft_replay(explicit_history())
    assert len(outcome.commands) == 1
    assert isinstance(outcome.commands[0], CompleteWorkflow)
    assert outcome.commands[0].result == "fast-second"


def test_enabling_draft_support_does_not_change_absent_policy_history() -> None:
    history = json.loads(FIXTURE.read_text())["history"]
    assert not any(isinstance(command, CompleteWorkflow) for command in draft_replay(history).commands)


def test_enabling_draft_support_keeps_the_legacy_failure_clock_gap() -> None:
    fixture = json.loads(FIXTURE.with_name("business-clock-parallel-failure-selection-gap.json").read_text())
    outcome = draft_replay(fixture["history"], ParallelFailureBusinessClockWorkflow)
    assert outcome.commands[0].result == fixture["expected"]["command_sequence"][0]["result"]


def test_new_draft_groups_record_the_policy_but_public_authoring_stays_legacy() -> None:
    ordinary = commands_to_server_commands(replay(ParallelFailurePolicyWorkflow, [], []).commands, "queue")
    explicit = commands_to_server_commands(draft_replay([]).commands, "queue")
    for encoded in ordinary:
        assert "parallel_group_policy" not in encoded
    for encoded in explicit:
        assert encoded["parallel_group_policy"] == "propagate"
        assert all(frame["parallel_group_policy"] == "propagate" for frame in encoded["parallel_group_path"])


def test_recorded_event_order_selects_the_failure_even_when_timestamps_tie() -> None:
    history = explicit_history()
    payload = deepcopy(history[1]["payload"])
    payload["message"] = "lower position failed second"
    history.append({"event_type": "ActivityFailed", "recorded_at": history[-1]["recorded_at"], "payload": payload})
    assert draft_replay(history).commands[0].result == "fast-second"


@pytest.mark.parametrize("value", [None, "", "unknown", 1, True, ["propagate"]])
def test_malformed_policy_fails_closed(value: object) -> None:
    history = explicit_history()
    history[1]["payload"]["parallel_group_policy"] = value
    history[1]["payload"]["parallel_group_path"][0]["parallel_group_policy"] = value
    with pytest.raises(ValueError, match="parallel_group_policy"):
        draft_replay(history)


def test_inconsistent_members_or_flattened_policy_fail_closed() -> None:
    history = explicit_history()
    del history[1]["payload"]["parallel_group_policy"]
    with pytest.raises(ValueError, match="canonical path"):
        draft_replay(history)
    del history[1]["payload"]["parallel_group_path"][0]["parallel_group_policy"]
    with pytest.raises(ValueError, match="inconsistent"):
        draft_replay(history)


def test_policy_cannot_change_flattened_group_identity() -> None:
    history = explicit_history()
    history[1]["payload"]["parallel_group_id"] = "another-group"
    with pytest.raises(ValueError, match="flattened group identity"):
        draft_replay(history)


def test_mixed_legacy_and_explicit_nested_policies_are_not_interpreted() -> None:
    history = explicit_history()
    for event in history[1:]:
        payload = event["payload"]
        outer = {**payload["parallel_group_path"][0], "parallel_group_id": "outer"}
        del outer["parallel_group_policy"]
        payload["parallel_group_path"].insert(0, outer)
    with pytest.raises(LocalActivityExecutionAborted, match="mixed nested composition"):
        draft_replay(history)


def test_select_frame_refuses_policy_but_an_enclosing_all_frame_can_have_it() -> None:
    root = deepcopy(explicit_history()[1]["payload"]["parallel_group_path"][0])
    selection = {**root, "parallel_group_id": "selection-group", "parallel_group_size": 1,
                 "parallel_group_mode": "select", "selection_member_key": "first"}
    del selection["parallel_group_policy"]
    payload = {**selection, "sequence": 1, "parallel_group_path": [root, selection]}
    history = [{"event_type": "ActivityScheduled", "payload": payload}]
    policies = _read_parallel_group_policies(history, ("propagate",))
    assert list(policies.values()) == ["propagate"]
    payload["parallel_group_policy"] = selection["parallel_group_policy"] = "propagate"
    with pytest.raises(ValueError, match="select frames"):
        _read_parallel_group_policies(history, ("propagate",))


@workflow.defn(name="tests.replay.parallel-policy-tail")
class ParallelPolicyTailWorkflow:
    def __init__(self) -> None:
        self.signals: list[str] = []

    @workflow.signal("mark")
    def mark(self, value: str) -> None:
        self.signals.append(value)

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [ctx.schedule_activity("slow-first", []), ctx.schedule_activity("fast-second", [])]
        except ActivityFailed:
            at_failure = {"clock": clock_text(ctx), "signals": list(self.signals)}
        tail = yield ctx.schedule_activity("after-group", [])
        return {"at_failure": at_failure, "tail": tail, "clock": clock_text(ctx), "signals": self.signals}


def tail_history() -> list[dict]:
    history = explicit_history()
    history.extend([
        {"event_type": "ActivityScheduled", "recorded_at": "2026-01-01T00:00:01Z", "payload": {
            "sequence": 3, "activity_type": "after-group", "activity_execution_id": "tail",
        }},
        {"event_type": "ActivityCompleted", "recorded_at": "2026-01-01T00:00:02Z", "payload": {
            "sequence": 3, "activity_type": "after-group", "activity_execution_id": "tail",
            "result": serializer.encode("tail-result", codec="avro"),
        }},
        {"event_type": "ActivityCompleted", "recorded_at": "2026-01-01T00:00:09Z", "payload": {
            **deepcopy(history[1]["payload"]), "result": serializer.encode("late-loser", codec="avro"),
        }},
    ])
    return history


def test_late_sibling_cannot_take_a_successor_result_or_advance_the_failure_clock() -> None:
    outcome = draft_replay(tail_history(), ParallelPolicyTailWorkflow)
    assert outcome.commands[0].result == {
        "at_failure": {"clock": "2026-01-01T00:00:01.000000+00:00", "signals": []},
        "tail": "tail-result", "clock": "2026-01-01T00:00:02.000000+00:00", "signals": [],
    }


def test_signals_stay_on_their_side_of_the_first_failure_boundary() -> None:
    history = tail_history()
    def signal(value: str) -> dict:
        return {"event_type": "SignalReceived", "payload": {
            "signal_name": "mark", "arguments": serializer.encode([value], codec="avro"), "payload_codec": "avro",
        }}
    history.insert(3, signal("before"))
    history.insert(5, signal("after"))
    result = draft_replay(history, ParallelPolicyTailWorkflow).commands[0].result
    assert result["at_failure"]["signals"] == ["before"]
    assert result["signals"] == ["before", "after"]
    assert result["tail"] == "tail-result"


def test_successful_explicit_groups_keep_member_order() -> None:
    @workflow.defn(name="tests.parallel-policy-success")
    class Success:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            return (yield [ctx.schedule_activity("slow-first", []), ctx.schedule_activity("fast-second", [])])
    history = explicit_history()[:3]
    for index in (1, 0):
        payload = deepcopy(history[index + 1]["payload"])
        payload["result"] = serializer.encode(f"position-{index}", codec="avro")
        history.append({"event_type": "ActivityCompleted", "recorded_at": "2026-01-01T00:00:02Z", "payload": payload})
    assert draft_replay(history, Success).commands[0].result == ["position-0", "position-1"]


def test_invalid_policy_is_refused_before_constructing_application_state() -> None:
    @workflow.defn(name="tests.parallel-policy-no-application-side-effect")
    class NoApplication(ParallelFailurePolicyWorkflow):
        calls = 0

        def __init__(self) -> None:
            type(self).calls += 1
            super().__init__()
    history = explicit_history("unknown")
    with pytest.raises(ValueError, match="parallel_group_policy"):
        draft_replay(history, NoApplication)
    assert NoApplication.calls == 0


@pytest.mark.parametrize("operation", ["replay", "query", "update", "validation"])
@pytest.mark.parametrize("as_iterator", [False, True])
def test_policy_on_run_close_settlement_refuses_before_application_construction(
    operation: str, as_iterator: bool,
) -> None:
    @workflow.defn(name="tests.parallel-policy-close-preflight")
    class Probe(ParallelFailurePolicyWorkflow):
        calls = 0

        def __init__(self) -> None:
            type(self).calls += 1
            super().__init__()

        @workflow.update("noop")
        def noop(self) -> None:
            pass

        @workflow.update_validator("noop")
        def validate_noop(self) -> None:
            pass

    history = json.loads(FIXTURE.with_name("run-close-policy-requires-consumer.json").read_text())["history"]
    history = iter(history) if as_iterator else history
    if operation == "update":
        result = apply_update(Probe, history, [], "update-id")
        assert isinstance(result, FailUpdate)
        assert result.exception_type == "LocalActivityExecutionAborted"
        assert "parallel_group_policy_not_supported" in result.message
    else:
        actions = {
            "replay": lambda: replay(Probe, history, []),
            "query": lambda: query_state(Probe, history, [], "caught"),
            "validation": lambda: validate_update(Probe, history, [], "noop", []),
        }
        errors = {"replay": LocalActivityExecutionAborted, "query": QueryFailed,
                  "validation": UpdateValidationFailed}
        action = actions[operation]
        with pytest.raises(errors[operation], match="parallel_group_policy_not_supported"):
            action()
    assert Probe.calls == 0


def test_draft_consumer_does_not_resolve_a_group_from_run_close_settlement() -> None:
    history = json.loads(FIXTURE.with_name("run-close-policy-requires-consumer.json").read_text())["history"]
    state = _replay_state(ParallelFailurePolicyWorkflow, iter(history), [], parallel_group_policies=("propagate",))
    assert state.instance.caught is None
    assert not any(isinstance(command, CompleteWorkflow) for command in state.outcome.commands)
    assert state.context._business_clock.snapshot() is None
    state.outcome.close()


def test_typed_success_decoding_errors_are_raised_at_the_group_yield() -> None:
    @workflow.defn(name="tests.parallel-policy-typed-failure")
    class Typed:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield [ctx.schedule_activity("slow-first", [], result_type=int),
                       ctx.schedule_activity("fast-second", [], result_type=int)]
            except TypedValueError:
                return "handled"
    history = explicit_history()[:3]
    for index in (1, 0):
        payload = deepcopy(history[index + 1]["payload"])
        payload["result"] = serializer.encode("not-an-int", codec="avro")
        history.append({"event_type": "ActivityCompleted", "payload": payload})
    assert draft_replay(history, Typed).commands[0].result == "handled"


def test_nested_groups_propagate_without_waiting_for_open_members() -> None:
    @workflow.defn(name="tests.parallel-policy-nested")
    class Nested:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield [ctx.schedule_activity("slow-first", []),
                       [ctx.schedule_activity("middle", []), ctx.schedule_activity("fast-third", [])]]
            except ActivityFailed as error:
                return error.activity_type
    history = [{"event_type": "WorkflowStarted", "payload": {}}]
    for index, name in enumerate(("slow-first", "middle", "fast-third")):
        root = {"parallel_group_id": "parallel-activities:1:3", "parallel_group_kind": "activity",
                "parallel_group_base_sequence": 1, "parallel_group_size": 3, "parallel_group_index": index,
                "parallel_group_policy": "propagate"}
        path = [root]
        if index:
            path.append({**root, "parallel_group_id": "parallel-activities:2:2", "parallel_group_base_sequence": 2,
                         "parallel_group_size": 2, "parallel_group_index": index - 1})
        history.append({"event_type": "ActivityScheduled", "payload": {
            "sequence": index + 1, "activity_type": name, **path[-1], "parallel_group_path": path,
        }})
    history.append({"event_type": "ActivityFailed", "payload": {**deepcopy(history[-1]["payload"]),
                                                                 "message": "failed first"}})
    assert draft_replay(history, Nested).commands[0].result == "fast-third"


def with_explicit_policy(history: list[dict]) -> list[dict]:
    for event in history:
        payload = event["payload"]
        payload["parallel_group_policy"] = "propagate"
        for frame in payload["parallel_group_path"]:
            frame["parallel_group_policy"] = "propagate"
    return history


def test_explicit_mixed_success_keeps_activity_child_and_timer_positions() -> None:
    history = with_explicit_policy([
        _terminal_event("TimerFired", "mixed", 3, 2),
        _terminal_event("ChildRunCompleted", "mixed", 3, 1, value="child-result",
                        child_workflow_type="mixed-child"),
        _terminal_event("ActivityCompleted", "mixed", 3, 0, value="activity-result",
                        activity_type="mixed-activity"),
    ])
    assert draft_replay(history, ParallelCompletionOrderMixed).commands[0].result == [
        "activity-result", "child-result", None,
    ]


def test_explicit_child_failure_does_not_wait_for_a_timer_or_activity() -> None:
    @workflow.defn(name="tests.parallel-policy-mixed-child-failure")
    class Mixed:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            try:
                yield [ctx.schedule_activity("open", []), ctx.start_child_workflow("failed-child", []),
                       ctx.start_timer(30)]
            except ChildWorkflowFailed as error:
                return error.child_workflow_type
    history = with_explicit_policy([
        _terminal_event("ActivityScheduled", "mixed", 3, 0, activity_type="open"),
        _terminal_event("ChildWorkflowScheduled", "mixed", 3, 1, workflow_type="failed-child"),
        _terminal_event("TimerScheduled", "mixed", 3, 2),
        _terminal_event("ChildRunFailed", "mixed", 3, 1, child_workflow_type="failed-child", message="failed"),
    ])
    assert draft_replay(history, Mixed).commands[0].result == "failed-child"
