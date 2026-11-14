from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from durable_workflow import ActivityRetryPolicy, serializer, workflow
from durable_workflow.workflow import CompleteWorkflow, RecordLocalActivity, ScheduleActivity, replay


def test_generated_default_cap_is_finite_without_changing_total_attempts():
    policy = ActivityRetryPolicy.exponential(max_attempts=9)
    assert policy.maximum_interval_seconds == 60
    assert policy.to_dict() == {
        "max_attempts": 9,
        "backoff_seconds": [1, 2, 4, 8, 16, 32, 60, 60],
        "non_retryable_error_types": [],
    }


@pytest.mark.parametrize("maximum,expected", [
    (4, [1, 2, 4, 4]), (2.5, [1, 2, 3, 3]), (0, [0, 0, 0, 0]),
])
def test_explicit_cap_retains_whole_second_command_resolution(maximum, expected):
    policy = ActivityRetryPolicy.exponential(max_attempts=5, maximum_interval_seconds=maximum)
    assert policy.to_dict()["backoff_seconds"] == expected


def test_generated_policy_preserves_the_total_execution_deadline_on_the_command():
    policy = ActivityRetryPolicy.exponential(max_attempts=9)
    command = ScheduleActivity(
        "tests.retry", [], retry_policy=policy, start_to_close_timeout=5, schedule_to_close_timeout=15,
    ).to_server_command("queue")
    assert command["retry_policy"] == policy.to_dict()
    assert command["start_to_close_timeout"] == 5
    assert command["schedule_to_close_timeout"] == 15


@pytest.mark.parametrize("field", ["initial_interval_seconds", "maximum_interval_seconds", "backoff_coefficient"])
@pytest.mark.parametrize("value", [
    None, float("nan"), float("inf"), float("-inf"), -1, True, "60", pytest.param(10 ** 1000, id="too-large"),
])
def test_generated_parameters_refuse_nonfinite_or_unsupported_numbers(field, value: Any):
    with pytest.raises(ValueError, match=field + " must be a finite number"):
        ActivityRetryPolicy.exponential(**{field: value})


def test_generated_coefficient_cannot_shrink_the_exponential_schedule():
    with pytest.raises(ValueError, match="backoff_coefficient must be a finite number >= 1"):
        ActivityRetryPolicy.exponential(backoff_coefficient=0.5)


@pytest.mark.parametrize("attempts", [None, True, 0, -1, 3.0, "3"])
def test_attempt_count_remains_a_positive_total(attempts: Any):
    with pytest.raises(ValueError, match="max_attempts must be an integer"):
        ActivityRetryPolicy.exponential(max_attempts=attempts)


def test_one_total_attempt_has_no_retry_and_error_types_are_copied():
    errors = ["ValidationError"]
    policy = ActivityRetryPolicy.exponential(max_attempts=1, non_retryable_error_types=errors)
    errors.append("LaterError")
    assert policy.to_dict() == {
        "max_attempts": 1, "backoff_seconds": [], "non_retryable_error_types": ["ValidationError"],
    }


def test_legacy_constructor_and_explicit_lists_keep_their_original_schedules():
    assert ActivityRetryPolicy(max_attempts=20).to_dict()["backoff_seconds"] == [2 ** power for power in range(19)]
    explicit_none = ActivityRetryPolicy(max_attempts=20, maximum_interval_seconds=None).to_dict()
    assert explicit_none["backoff_seconds"][-1] == 262144
    assert ActivityRetryPolicy(max_attempts=5, backoff_seconds=[1, 2]).to_dict() == {
        "max_attempts": 5, "backoff_seconds": [1, 2], "non_retryable_error_types": [],
    }


LEGACY = json.loads((Path(__file__).parent / "fixtures/activity_retry_legacy_2172.json").read_text())


def test_new_authoring_caps_the_published_long_schedule_without_rewriting_it():
    legacy = json.loads(LEGACY["samples"][0]["command_json"])["retry_policy"]["backoff_seconds"]
    current = ActivityRetryPolicy.exponential(max_attempts=20).to_dict()["backoff_seconds"]
    assert legacy[-1] == 262144
    assert current == [*legacy[:6], *([60] * 13)]


@workflow.defn(name="tests.retry.legacy-remote")
class LegacyRemote:
    def run(self, ctx, options):
        return (yield ctx.schedule_activity("tests.retry", ["input"], retry_policy=ActivityRetryPolicy(**options)))


@workflow.defn(name="tests.retry.legacy-local")
class LegacyLocal:
    def run(self, ctx, policy):
        return (yield ctx.local_activity("tests.retry", ["input"], retry_policy=policy))


@pytest.mark.parametrize("sample", LEGACY["samples"], ids=lambda sample: sample["id"])
def test_legacy_commands_match_published_sdk_bytes_and_local_history_replays_without_callback(sample):
    pending = replay(LegacyRemote, [], [sample["options"]])
    try:
        actual = pending.commands[0].to_server_command("queue", payload_codec="avro")
        assert json.dumps(actual, sort_keys=True, separators=(",", ":")) == sample["command_json"]
    finally:
        pending.close()
    recorded_policy = json.loads(sample["command_json"])["retry_policy"]
    backoff = recorded_policy["backoff_seconds"]
    attempts = [
        {"attempt_number": number, "outcome": "failed", "backoff_seconds": backoff[min(number - 1, len(backoff) - 1)]}
        for number in range(1, recorded_policy["max_attempts"])
    ]
    attempts.append({"attempt_number": recorded_policy["max_attempts"], "outcome": "completed"})
    history = [{"event_type": "ActivityCompleted", "payload": {
        "workflow_sequence": 1, "activity_type": "tests.retry", "execution_mode": "local",
        "retry_policy": recorded_policy, "attempts": attempts, "result": serializer.envelope("retained"),
    }}]
    retained = deepcopy(history)

    def refuse_callback(_: RecordLocalActivity):
        pytest.fail("committed legacy local result must not execute a callback")

    outcome = replay(LegacyLocal, history, [recorded_policy], local_activity_executor=refuse_callback)
    try:
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "retained"
        assert history == retained
    finally:
        outcome.close()
