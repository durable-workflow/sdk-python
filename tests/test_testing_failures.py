"""Mock outcomes use durable failure delivery and bounded scheduled retries."""

from __future__ import annotations

import asyncio
import gc
import time
import warnings
from collections.abc import Callable
from typing import Any

import pytest

from durable_workflow import workflow
from durable_workflow.errors import ActivityFailed, ChildWorkflowFailed, NonRetryableError, WorkflowFailed
from durable_workflow.testing import WorkflowEnvironment
from durable_workflow.workflow import ActivityRetryPolicy, ChildWorkflowRetryPolicy, WorkflowContext


@workflow.defn(name="testing.failure-boundary")
class FailureBoundary:
    def run(self, ctx: WorkflowContext, kind: str, policy: Any):  # type: ignore[no-untyped-def]
        try:
            if kind == "activity":
                return (yield ctx.schedule_activity("operation", ["input"], retry_policy=policy))
            return (yield ctx.start_child_workflow("operation", ["input"], retry_policy=policy))
        except ActivityFailed as error:
            return {"caught": "activity", "message": str(error), "type": error.exception_type,
                    "class": error.exception_class, "non_retryable": error.non_retryable}
        except ChildWorkflowFailed as error:
            return {"caught": "child", "message": str(error), "class": error.exception_class}


def _register(env: WorkflowEnvironment, kind: str, fn: Callable[..., Any]) -> None:
    if kind == "activity":
        env.register_activity("operation", fn)
    else:
        env.register_child_workflow("operation", fn)


def _failure(_value: str) -> None:
    raise ValueError("declined")


@pytest.mark.parametrize("kind", ["activity", "child_workflow"])
def test_mock_exception_is_delivered_at_the_authored_yield(kind: str) -> None:
    env = WorkflowEnvironment()
    _register(env, kind, _failure)
    result = env.execute_workflow(FailureBoundary, kind, None)
    assert result["caught"] == ("activity" if kind == "activity" else "child")
    assert result["message"] == "declined"
    assert result["class"] == "builtins.ValueError"
    if kind == "activity":
        assert result["type"] == "ValueError"
        assert result["non_retryable"] is False
    call = env.mock_calls[0]
    assert call.kind == kind
    assert call.name == "operation"
    assert call.attempt_count == 1
    assert call.status == "failed"


@pytest.mark.parametrize("kind", ["activity", "child_workflow"])
@pytest.mark.parametrize("policy_kind", ["mapping", "typed"])
def test_retry_each_attempt_then_deliver_success(kind: str, policy_kind: str) -> None:
    calls: list[str] = []

    def flaky(value: str) -> str:
        calls.append(value)
        if len(calls) < 3:
            raise ValueError("temporary")
        return "recovered"

    policy: Any = {"max_attempts": 3, "backoff_seconds": [1000, 2000]}
    if policy_kind == "typed":
        cls = ActivityRetryPolicy if kind == "activity" else ChildWorkflowRetryPolicy
        policy = cls(max_attempts=3, backoff_seconds=[1000, 2000])
    env = WorkflowEnvironment()
    _register(env, kind, flaky)
    assert env.execute_workflow(FailureBoundary, kind, policy) == "recovered"
    assert calls == ["input", "input", "input"]
    assert env.mock_calls[0].attempt_count == 3
    assert env.mock_calls[0].status == "completed"
    assert len(env.runs[0].history) == 1


@pytest.mark.parametrize("kind", ["activity", "child_workflow"])
def test_exhausted_policy_delivers_the_last_failure(kind: str) -> None:
    attempts = 0

    def failing(value: str) -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError(f"attempt {attempts}: {value}")

    env = WorkflowEnvironment()
    _register(env, kind, failing)
    result = env.execute_workflow(FailureBoundary, kind, {"max_attempts": 3})
    assert result["message"] == "attempt 3: input"
    assert attempts == 3
    assert env.mock_calls[0].attempt_count == 3
    expected_event = "ActivityFailed" if kind == "activity" else "ChildRunFailed"
    assert [event["event_type"] for event in env.runs[0].history] == [expected_event]
    if kind == "child_workflow":
        assert "non_retryable" not in env.runs[0].history[0]["payload"]


@pytest.mark.parametrize("kind", ["activity", "child_workflow"])
@pytest.mark.parametrize("failure_name", ["ValueError", "builtins.ValueError"])
def test_short_and_qualified_non_retryable_types_stop_retries(kind: str, failure_name: str) -> None:
    env = WorkflowEnvironment()
    _register(env, kind, _failure)
    result = env.execute_workflow(FailureBoundary, kind, {
        "max_attempts": 4, "non_retryable_error_types": [failure_name],
    })
    assert result["message"] == "declined"
    assert env.mock_calls[0].attempt_count == 1


def test_activity_non_retryable_marker_keeps_outer_type_and_cause_semantics() -> None:
    def fail(_value: str) -> None:
        raise NonRetryableError("permanent", cause=ValueError("inner cause"))

    env = WorkflowEnvironment()
    env.register_activity("operation", fail)
    result = env.execute_workflow(FailureBoundary, "activity", {"max_attempts": 5})
    assert result == {"caught": "activity", "message": "permanent", "type": "NonRetryableError",
                      "class": "durable_workflow.errors.NonRetryableError", "non_retryable": True}
    assert env.mock_calls[0].attempt_count == 1


def test_child_marker_uses_workflow_failure_mapping_not_activity_retry_semantics() -> None:
    def fail(_value: str) -> None:
        raise NonRetryableError("a workflow body raised the activity marker")

    env = WorkflowEnvironment()
    env.register_child_workflow("operation", fail)
    result = env.execute_workflow(FailureBoundary, "child_workflow", {"max_attempts": 2})
    assert result["class"] == "durable_workflow.errors.NonRetryableError"
    assert env.mock_calls[0].attempt_count == 2


@pytest.mark.parametrize("kind", ["activity", "child_workflow"])
def test_attempt_cap_does_not_misreport_policy_exhaustion(kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    expand = ActivityRetryPolicy._backoff_seconds

    def no_expansion(_self: ActivityRetryPolicy) -> list[int]:
        assert _self.max_attempts <= 3, "A huge retry budget must not expand its backoff list"
        return expand(_self)

    monkeypatch.setattr(ActivityRetryPolicy, "_backoff_seconds", no_expansion)
    env = WorkflowEnvironment(mock_attempt_limit=3)
    _register(env, kind, _failure)
    with pytest.raises(RuntimeError, match="mock attempt limit.*3") as exc:
        env.execute_workflow(FailureBoundary, kind, ActivityRetryPolicy(max_attempts=10**12))
    assert isinstance(exc.value.__cause__, ValueError)
    assert env.mock_calls[0].attempt_count == 3
    assert env.mock_calls[0].status == "attempt_limit_exceeded"
    assert env.runs[0].history == []


@pytest.mark.parametrize("limit", [True, False, 0, -1, 1.5, "3", None])
def test_attempt_cap_requires_a_positive_integer(limit: Any) -> None:
    with pytest.raises(ValueError, match="mock_attempt_limit"):
        WorkflowEnvironment(mock_attempt_limit=limit)


def test_backoff_does_not_sleep_in_wall_clock_time(monkeypatch: pytest.MonkeyPatch) -> None:
    def must_not_sleep(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Mock retries must not sleep")

    monkeypatch.setattr(time, "sleep", must_not_sleep)
    monkeypatch.setattr(asyncio, "sleep", must_not_sleep)
    env = WorkflowEnvironment()
    env.register_activity("operation", _failure)
    env.execute_workflow(FailureBoundary, "activity", {"max_attempts": 3, "backoff_seconds": [10**12]})
    assert env.mock_calls[0].attempt_count == 3


@workflow.defn(name="testing.failure-compensation")
class FailureCompensation:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("reserve", [])
        try:
            yield ctx.schedule_activity("charge", [], retry_policy={"max_attempts": 2})
        except ActivityFailed:
            yield ctx.schedule_activity("release", [])
            return "compensated"
        return "charged"


def test_compensation_and_later_replay_do_not_invoke_an_old_mock_again() -> None:
    calls: list[str] = []

    def decline() -> None:
        calls.append("charge")
        raise ValueError("declined")

    env = WorkflowEnvironment()
    env.register_activity_result("reserve", None)
    env.register_activity("charge", decline)
    env.register_activity_result("release", None)
    assert env.execute_workflow(FailureCompensation) == "compensated"
    assert calls == ["charge", "charge"]
    assert [(call.name, call.attempt_count) for call in env.mock_calls] == [
        ("reserve", 1), ("charge", 2), ("release", 1),
    ]


@pytest.mark.parametrize("kind", ["activity", "child_workflow"])
def test_missing_mock_remains_a_harness_error(kind: str) -> None:
    env = WorkflowEnvironment()
    with pytest.raises(KeyError, match="no mock registered"):
        env.execute_workflow(FailureBoundary, kind, {"max_attempts": 3})
    assert env.mock_calls == []


def test_async_mock_is_rejected_without_unawaited_coroutine_warning() -> None:
    async def async_mock(_value: str) -> str:
        return "unsupported async fixture"

    env = WorkflowEnvironment()
    env.register_activity("operation", async_mock)
    with warnings.catch_warnings(record=True) as captured:
        with pytest.raises(TypeError, match="synchronous"):
            env.execute_workflow(FailureBoundary, "activity", {"max_attempts": 3})
        gc.collect()
    assert not [warning for warning in captured if issubclass(warning.category, RuntimeWarning)]


def test_base_exception_is_not_retried_or_converted() -> None:
    def stop(_value: str) -> None:
        raise KeyboardInterrupt()

    env = WorkflowEnvironment()
    env.register_activity("operation", stop)
    with pytest.raises(KeyboardInterrupt):
        env.execute_workflow(FailureBoundary, "activity", {"max_attempts": 3})
    assert env.mock_calls[0].attempt_count == 1


def test_uncaught_mock_failure_becomes_workflow_failed() -> None:
    @workflow.defn(name="testing.uncaught-mock-failure")
    class Uncaught:
        def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
            return (yield ctx.schedule_activity("operation", ["input"]))

    env = WorkflowEnvironment()
    env.register_activity("operation", _failure)
    with pytest.raises(WorkflowFailed, match="declined"):
        env.execute_workflow(Uncaught)


def test_call_records_reset_between_executions() -> None:
    env = WorkflowEnvironment()
    env.register_activity_result("operation", "done")
    assert env.execute_workflow(FailureBoundary, "activity", None) == "done"
    assert env.execute_workflow(FailureBoundary, "activity", None) == "done"
    assert len(env.mock_calls) == 1
    assert env.mock_calls[0].run_number == 1


def test_later_child_registration_replaces_result_or_callable() -> None:
    env = WorkflowEnvironment()
    env.register_child_workflow("operation", _failure)
    env.register_child_workflow_result("operation", "canned")
    assert env.execute_workflow(FailureBoundary, "child_workflow", None) == "canned"
    env.register_child_workflow("operation", lambda value: "callable: " + value)
    assert env.execute_workflow(FailureBoundary, "child_workflow", None) == "callable: input"


@pytest.mark.parametrize("maximum", [True, 0, -1, 1.5, "3", None])
def test_invalid_retry_budget_refuses_before_invoking_the_mock(maximum: Any) -> None:
    calls: list[str] = []
    env = WorkflowEnvironment()
    env.register_activity("operation", lambda value: calls.append(value))
    with pytest.raises(ValueError, match="max_attempts"):
        env.execute_workflow(FailureBoundary, "activity", {"max_attempts": maximum})
    assert calls == []


def test_attempt_cap_and_records_are_per_call_across_continue_as_new() -> None:
    @workflow.defn(name="testing.retry-continued-runs")
    class Continued:
        def run(self, ctx: WorkflowContext, count: int):  # type: ignore[no-untyped-def]
            yield ctx.schedule_activity("step", [count], retry_policy={"max_attempts": 2})
            if count:
                return ctx.continue_as_new(count - 1)
            return "done"

    attempts: dict[int, int] = {}

    def once_per_run(count: int) -> None:
        attempts[count] = attempts.get(count, 0) + 1
        if attempts[count] == 1:
            raise ValueError("first attempt")

    env = WorkflowEnvironment(mock_attempt_limit=2)
    env.register_activity("step", once_per_run)
    assert env.execute_workflow(Continued, 1) == "done"
    assert [(call.run_number, call.attempt_count) for call in env.mock_calls] == [(1, 2), (2, 2)]
    assert attempts == {1: 2, 0: 2}
