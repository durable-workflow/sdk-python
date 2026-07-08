from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from durable_workflow import Replayer, ReplayOutcome, serializer, workflow
from durable_workflow.errors import (
    ActivityFailed,
    ChildWorkflowCancelled,
    ChildWorkflowFailed,
    ChildWorkflowTerminated,
    NexusOperationFailed,
    NonDeterministicReplayError,
)
from durable_workflow.nexus import NexusOperationResult
from durable_workflow.workflow import (
    ActivityRetryPolicy,
    ChildWorkflowRetryPolicy,
    CompleteWorkflow,
    ContinueAsNew,
    FailWorkflow,
    NexusServiceCall,
    RecordSideEffect,
    RecordVersionMarker,
    ScheduleActivity,
    StartChildWorkflow,
    StartTimer,
    UpsertSearchAttributes,
    WorkflowContext,
    replay,
)
from tests.integration.polyglot_fixtures import (
    PHP_HISTORY_EVENT_EXPECTED_KEYS,
    PHP_HISTORY_EVENT_FIXTURES,
    PHP_VERSION_MARKER_EXPECTED_KEYS,
    PHP_VERSION_MARKER_RECORDED_EVENT,
    PolyglotVersionMarkerWorkflow,
)


@workflow.defn(name="simple-return")
class SimpleReturn:
    def run(self, ctx: WorkflowContext) -> str:
        return "done"


@workflow.defn(name="one-activity")
class OneActivity:
    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        result = yield ctx.schedule_activity("greet", [name])
        return {"greeting": result}


@workflow.defn(name="activity-failed-saga")
class ActivityFailedSaga:
    def run(self, ctx: WorkflowContext, order_id: str):  # type: ignore[no-untyped-def]
        try:
            yield ctx.schedule_activity("charge-card", [order_id])
        except ActivityFailed as exc:
            released = yield ctx.schedule_activity(
                "release-inventory",
                [
                    order_id,
                    exc.activity_type,
                    exc.exception_type,
                    exc.failure_id,
                    exc.non_retryable,
                    str(exc),
                ],
            )
            return {
                "released": released,
                "failure_category": exc.failure_category,
                "exception_class": exc.exception_class,
            }
        return {"charged": True}


@workflow.defn(name="reverse-compensation-saga")
class ReverseCompensationSaga:
    def run(self, ctx: WorkflowContext, payload: dict):  # type: ignore[type-arg]
        completed: list[str] = []
        compensations: list[str] = []
        steps = [
            ("reserve_flight", "cancel_flight"),
            ("reserve_hotel", "cancel_hotel"),
            ("charge_card", "refund_card"),
            ("send_confirmation", ""),
        ]

        try:
            for action, compensation in steps:
                yield ctx.schedule_activity(action, [payload])
                completed.append(action)
                if compensation:
                    compensations.append(compensation)
        except ActivityFailed:
            if not compensations:
                raise
            for compensation in reversed(compensations):
                yield ctx.schedule_activity(compensation, [payload])
                completed.append(compensation)
            return {
                "status": "compensated",
                "activity_log": completed,
                "compensations": compensations,
            }

        return {
            "status": "completed",
            "activity_log": completed,
            "compensations": compensations,
        }


@workflow.defn(name="activity-failure-payload-inspector")
class ActivityFailurePayloadInspector:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.schedule_activity("polyglot.php.fail", [])
        except ActivityFailed as exc:
            return {
                "activity_type": exc.activity_type,
                "failure_category": exc.failure_category,
                "exception_type": exc.exception_type,
                "non_retryable": exc.non_retryable,
                "message": str(exc),
                "exception_payload": exc.exception_payload,
            }
        return {"ok": True}


@workflow.defn(name="two-activities")
class TwoActivities:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        a = yield ctx.schedule_activity("step1", [])
        b = yield ctx.schedule_activity("step2", [a])
        return [a, b]


@workflow.defn(name="timer-workflow")
class TimerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.start_timer(5)
        result = yield ctx.schedule_activity("greet", ["after-timer"])
        return result


@workflow.defn(name="failing-workflow")
class FailingWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("step1", [])
        raise ValueError("something went wrong")


@workflow.defn(name="typed-compensation-failure-workflow")
class TypedCompensationFailureWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.schedule_activity("charge_card", [])
        except ActivityFailed:
            try:
                yield ctx.schedule_activity("cancel_flight", [])
            except ActivityFailed as exc:
                failure_type = exc.exception_type or type(exc).__name__
                raise RuntimeError(f"compensation failed for cancel_flight: {failure_type}: {exc}") from exc
        return "completed"


@workflow.defn(name="nexus-caller-wf")
class NexusCallerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        result = yield ctx.call_nexus_service(
            "greeter",
            "shared",
            "greet",
            ["Ada"],
            service_sdk_language="workflow-php",
            artifact_tuple={"sdk-python": "0.4.95"},
            published_artifact_worker_execution=True,
        )
        return {
            "service_call_id": result.service_call_id,
            "greeting": result.result["greeting"],
            "caller_sdk_language": result.caller_sdk_language,
            "service_sdk_language": result.service_sdk_language,
        }


@workflow.defn(name="nexus-failure-catcher-wf")
class NexusFailureCatcherWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.call_nexus_service(
                "greeter",
                "shared",
                "fail",
                ["Ada"],
                service_sdk_language="workflow-php",
            )
        except NexusOperationFailed as exc:
            return {
                "service_call_id": exc.service_call_id,
                "service_error_type": exc.service_error_type,
                "caller_observed_error_type": exc.caller_observed_error_type,
                "typed_error_message": exc.typed_error_message,
            }
        return {"ok": True}


def _nexus_side_effect_event(payload: dict, *, sequence: int = 1) -> dict:
    return {
        "event_type": "SideEffectRecorded",
        "payload": {
            "sequence": sequence,
            "payload_codec": "json",
            "result": serializer.encode(payload, codec="json"),
        },
    }


def _activity_completed_event(sequence: int, activity_type: str) -> dict:
    return {
        "event_type": "ActivityCompleted",
        "payload": {
            "sequence": sequence,
            "activity_type": activity_type,
            "payload_codec": "json",
            "result": serializer.encode({"activity": activity_type}, codec="json"),
        },
    }


def _activity_failed_event(sequence: int, activity_type: str) -> dict:
    return {
        "event_type": "ActivityFailed",
        "payload": {
            "sequence": sequence,
            "activity_type": activity_type,
            "failure_id": f"failure-{sequence}",
            "failure_category": "activity",
            "exception_type": "RuntimeError",
            "exception_class": "builtins.RuntimeError",
            "message": f"{activity_type} planned saga failure before forward effect",
            "non_retryable": True,
        },
    }


class TestSimpleReturn:
    def test_non_generator_completes(self) -> None:
        outcome = replay(SimpleReturn, [], [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == "done"


class TestNexusServiceCalls:
    def test_fresh_replay_yields_public_nexus_command_with_stable_id(self) -> None:
        first = replay(NexusCallerWorkflow, [], [], workflow_id="wf-caller", run_id="run-caller")
        second = replay(NexusCallerWorkflow, [], [], workflow_id="wf-caller", run_id="run-caller")

        assert len(first.commands) == 1
        assert len(second.commands) == 1
        cmd = first.commands[0]
        assert isinstance(cmd, NexusServiceCall)
        assert isinstance(second.commands[0], NexusServiceCall)
        assert cmd.endpoint_name == "greeter"
        assert cmd.service_name == "shared"
        assert cmd.operation_name == "greet"
        assert cmd.arguments == ["Ada"]
        assert cmd.idempotency_key == second.commands[0].idempotency_key
        assert cmd.idempotency_key.startswith("dw-py-nexus-")
        assert cmd.service_sdk_language == "workflow-php"
        assert cmd.published_artifact_worker_execution is True

    def test_recorded_nexus_success_resumes_workflow_with_result(self) -> None:
        initial = replay(NexusCallerWorkflow, [], [], workflow_id="wf-caller", run_id="run-caller")
        cmd = initial.commands[0]
        assert isinstance(cmd, NexusServiceCall)
        result = NexusOperationResult(
            accepted=True,
            service_call_id="svc-call-1",
            endpoint_name="greeter",
            service_name="shared",
            operation_name="greet",
            caller_workflow_instance_id="wf-caller",
            caller_workflow_run_id="run-caller",
            service_sdk_language="workflow-php",
            request_payload={
                "arguments": ["Ada"],
                "idempotency_key": cmd.idempotency_key,
                "caller_workflow_instance_id": "wf-caller",
                "caller_workflow_run_id": "run-caller",
            },
            response_or_failure_surface={"status": "completed", "result": {"greeting": "hello Ada"}},
            artifact_tuple={"sdk-python": "0.4.95"},
            published_artifact_worker_execution=True,
            status="completed",
            result={"greeting": "hello Ada"},
        )
        outcome = replay(
            NexusCallerWorkflow,
            [_nexus_side_effect_event(result.to_recorded_payload())],
            [],
            workflow_id="wf-caller",
            run_id="run-caller",
            payload_codec="json",
        )

        assert len(outcome.commands) == 1
        complete = outcome.commands[0]
        assert isinstance(complete, CompleteWorkflow)
        assert complete.result == {
            "service_call_id": "svc-call-1",
            "greeting": "hello Ada",
            "caller_sdk_language": "sdk-python",
            "service_sdk_language": "workflow-php",
        }

    def test_recorded_nexus_failure_raises_typed_exception_into_workflow(self) -> None:
        initial = replay(NexusFailureCatcherWorkflow, [], [], workflow_id="wf-caller", run_id="run-caller")
        cmd = initial.commands[0]
        assert isinstance(cmd, NexusServiceCall)
        result = NexusOperationResult(
            accepted=False,
            service_call_id="svc-call-failed",
            endpoint_name="greeter",
            service_name="shared",
            operation_name="fail",
            caller_workflow_instance_id="wf-caller",
            caller_workflow_run_id="run-caller",
            service_sdk_language="workflow-php",
            request_payload={
                "arguments": ["Ada"],
                "idempotency_key": cmd.idempotency_key,
                "caller_workflow_instance_id": "wf-caller",
                "caller_workflow_run_id": "run-caller",
            },
            response_or_failure_surface={"status": "failed"},
            status="failed",
            outcome="handler_failed",
            service_error_type="SharedGreeterUnavailable",
            caller_observed_error_type="SharedGreeterUnavailable",
            typed_error_message="shared greeter is unavailable",
        )
        outcome = replay(
            NexusFailureCatcherWorkflow,
            [_nexus_side_effect_event(result.to_recorded_payload())],
            [],
            workflow_id="wf-caller",
            run_id="run-caller",
            payload_codec="json",
        )

        assert len(outcome.commands) == 1
        complete = outcome.commands[0]
        assert isinstance(complete, CompleteWorkflow)
        assert complete.result == {
            "service_call_id": "svc-call-failed",
            "service_error_type": "SharedGreeterUnavailable",
            "caller_observed_error_type": "SharedGreeterUnavailable",
            "typed_error_message": "shared greeter is unavailable",
        }


class TestPublicReplayer:
    def test_replays_registered_workflow_with_explicit_input(self) -> None:
        outcome = Replayer(workflows=[OneActivity]).replay([], ["world"])

        assert isinstance(outcome, ReplayOutcome)
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.arguments == ["world"]

    def test_infers_workflow_type_and_input_from_workflow_started_event(self) -> None:
        history = {
            "events": [
                {
                    "event_type": "WorkflowStarted",
                    "payload": {
                        "workflow_type": "one-activity",
                        "input": serializer.envelope(["Ada"], codec="json"),
                    },
                },
            ],
        }

        outcome = Replayer(workflows=[OneActivity]).replay(history)

        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.arguments == ["Ada"]

    def test_rejects_unknown_workflow_type(self) -> None:
        history = [
            {"event_type": "WorkflowStarted", "payload": {"workflow_type": "missing"}},
        ]

        with pytest.raises(ValueError, match="not registered"):
            Replayer(workflows=[OneActivity]).replay(history)


class TestOneActivity:
    def test_first_replay_schedules(self) -> None:
        outcome = replay(OneActivity, [], ["world"])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.activity_type == "greet"
        assert cmd.arguments == ["world"]

    def test_pending_timer_history_rejects_single_activity_drift(self) -> None:
        history = [
            {
                "event_type": "TimerScheduled",
                "payload": {"sequence": 1, "timer_kind": "durable_timer"},
            },
        ]

        with pytest.raises(NonDeterministicReplayError) as exc_info:
            replay(OneActivity, history, ["world"])

        assert exc_info.value.workflow_sequence == 1
        assert exc_info.value.expected_shape == "activity"
        assert exc_info.value.recorded_event_types == ["TimerScheduled"]

    def test_completed_activity_triggers_completion(self) -> None:
        history = [{"event_type": "ActivityCompleted", "payload": {"result": '"hello, world"'}}]
        outcome = replay(OneActivity, history, ["world"])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {"greeting": "hello, world"}

    def test_completed_activity_uses_event_payload_codec(self) -> None:
        history = [
            {
                "event_type": "ActivityCompleted",
                "payload": {
                    "result": serializer.encode("hello, avro", codec="avro"),
                    "payload_codec": "avro",
                },
            },
        ]
        outcome = replay(OneActivity, history, ["world"])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {"greeting": "hello, avro"}

    def test_failed_activity_is_thrown_into_workflow_for_compensation(self) -> None:
        history = [
            {
                "event_type": "ActivityFailed",
                "payload": {
                    "activity_name": "charge-card",
                    "activity_execution_id": "act-1",
                    "activity_attempt_id": "attempt-1",
                    "failure_id": "failure-1",
                    "failure_category": "activity",
                    "non_retryable": True,
                    "exception_type": "PaymentDeclined",
                    "exception_class": "payments.PaymentDeclined",
                    "message": "card declined",
                    "exception": {"type": "PaymentDeclined", "message": "card declined"},
                },
            }
        ]

        outcome = replay(ActivityFailedSaga, history, ["order-1"])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.activity_type == "release-inventory"
        assert cmd.arguments == [
            "order-1",
            "charge-card",
            "PaymentDeclined",
            "failure-1",
            True,
            "card declined",
        ]

    def test_failed_activity_compensation_can_complete(self) -> None:
        history = [
            {
                "event_type": "ActivityFailed",
                "payload": {
                    "activity_type": "charge-card",
                    "failure_id": "failure-1",
                    "failure_category": "activity",
                    "exception_type": "PaymentDeclined",
                    "exception_class": "payments.PaymentDeclined",
                    "message": "card declined",
                },
            },
            {"event_type": "ActivityCompleted", "payload": {"result": '"released"'}},
        ]

        outcome = replay(ActivityFailedSaga, history, ["order-1"])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {
            "released": "released",
            "failure_category": "activity",
            "exception_class": "payments.PaymentDeclined",
        }

    def test_php_activity_failure_payload_omits_runtime_diagnostics_by_default(self) -> None:
        history = [
            {
                "event_type": "ActivityFailed",
                "payload": {
                    "activity_type": "polyglot.php.fail",
                    "failure_category": "activity",
                    "exception_type": "PolyglotPhpPlannedFailure",
                    "exception_class": "RuntimeException",
                    "message": "php activity planned failure",
                    "non_retryable": True,
                    "exception": {
                        "class": "RuntimeException",
                        "type": "PolyglotPhpPlannedFailure",
                        "message": "php activity planned failure",
                        "file": "/app/vendor/durable-workflow/workflow/src/V2/Support/FailureFactory.php",
                        "line": 571,
                        "trace": [
                            {
                                "file": "/app/vendor/durable-workflow/workflow/src/V2/Support/FailureFactory.php"
                            }
                        ],
                        "properties": [],
                        "details": "encoded-details",
                        "details_payload_codec": "avro",
                        "non_retryable": True,
                    },
                },
            }
        ]

        outcome = replay(ActivityFailurePayloadInspector, history, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {
            "activity_type": "polyglot.php.fail",
            "failure_category": "activity",
            "exception_type": "PolyglotPhpPlannedFailure",
            "non_retryable": True,
            "message": "php activity planned failure",
            "exception_payload": {
                "type": "PolyglotPhpPlannedFailure",
                "message": "php activity planned failure",
                "details": "encoded-details",
                "details_payload_codec": "avro",
                "non_retryable": True,
            },
        }

    def test_activity_failure_payload_preserves_explicit_diagnostics_envelope(self) -> None:
        history = [
            {
                "event_type": "ActivityFailed",
                "payload": {
                    "activity_type": "polyglot.php.fail",
                    "failure_category": "activity",
                    "exception_type": "PolyglotPhpPlannedFailure",
                    "message": "php activity planned failure",
                    "exception": {
                        "type": "PolyglotPhpPlannedFailure",
                        "message": "php activity planned failure",
                        "runtime_diagnostics": {
                            "class": "RuntimeException",
                            "file": "/app/src/TimeoutActivity.php",
                        },
                    },
                },
            }
        ]

        outcome = replay(ActivityFailurePayloadInspector, history, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result["exception_payload"] == {
            "type": "PolyglotPhpPlannedFailure",
            "message": "php activity planned failure",
            "runtime_diagnostics": {
                "class": "RuntimeException",
                "file": "/app/src/TimeoutActivity.php",
            },
        }

    def test_server_command_shape(self) -> None:
        outcome = replay(OneActivity, [], ["world"])
        cmd = outcome.commands[0]
        server_cmd = cmd.to_server_command("default-queue")
        assert server_cmd["type"] == "schedule_activity"
        assert server_cmd["activity_type"] == "greet"
        assert server_cmd["queue"] == "default-queue"
        assert server_cmd["arguments"]["codec"] == "avro"

    def test_server_command_uses_payload_codec(self) -> None:
        outcome = replay(OneActivity, [], ["world"])
        cmd = outcome.commands[0]
        server_cmd = cmd.to_server_command("default-queue", payload_codec="json")
        assert server_cmd["arguments"]["codec"] == "json"
        assert serializer.decode(server_cmd["arguments"]["blob"], codec="json") == ["world"]

    def test_server_command_accepts_payload_warning_context(self, caplog) -> None:
        cmd = ScheduleActivity(activity_type="charge-card", arguments=["x" * 20], queue="payments")
        config = serializer.PayloadSizeWarningConfig(limit_bytes=10, threshold_percent=50)
        context = serializer.PayloadSizeWarningContext(
            kind="workflow_command",
            workflow_id="wf-1",
            run_id="run-1",
            task_queue="payments",
        )

        with caplog.at_level(logging.WARNING, logger="durable_workflow.serializer"):
            cmd.to_server_command(
                "default-queue",
                payload_codec="json",
                size_warning=config,
                warning_context=context,
            )

        payload = caplog.records[0].durable_workflow_payload
        assert payload["kind"] == "activity_input"
        assert payload["workflow_id"] == "wf-1"
        assert payload["run_id"] == "run-1"
        assert payload["activity_name"] == "charge-card"
        assert payload["task_queue"] == "payments"

    def test_schedule_activity_server_command_includes_retry_policy_and_timeouts(self) -> None:
        cmd = ScheduleActivity(
            activity_type="charge-card",
            arguments=[{"order_id": "o-1"}],
            queue="payments",
            retry_policy=ActivityRetryPolicy(
                max_attempts=4,
                initial_interval_seconds=1,
                backoff_coefficient=3,
                maximum_interval_seconds=10,
                non_retryable_error_types=["ValidationError"],
            ),
            start_to_close_timeout=120,
            schedule_to_start_timeout=10,
            schedule_to_close_timeout=300,
            heartbeat_timeout=15,
        )

        server_cmd = cmd.to_server_command("default-queue")

        assert server_cmd["type"] == "schedule_activity"
        assert server_cmd["queue"] == "payments"
        assert server_cmd["retry_policy"] == {
            "max_attempts": 4,
            "backoff_seconds": [1, 3, 9],
            "non_retryable_error_types": ["ValidationError"],
        }
        assert server_cmd["start_to_close_timeout"] == 120
        assert server_cmd["schedule_to_start_timeout"] == 10
        assert server_cmd["schedule_to_close_timeout"] == 300
        assert server_cmd["heartbeat_timeout"] == 15

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("start_to_close_timeout", 0, "start_to_close_timeout must be >= 1 second"),
            ("schedule_to_start_timeout", 0, "schedule_to_start_timeout must be >= 1 second"),
            ("schedule_to_close_timeout", 0, "schedule_to_close_timeout must be >= 1 second"),
            ("heartbeat_timeout", 0, "heartbeat_timeout must be >= 1 second"),
        ],
    )
    def test_schedule_activity_rejects_non_positive_timeout_budgets(
        self,
        field: str,
        value: int,
        message: str,
    ) -> None:
        cmd = ScheduleActivity(
            activity_type="charge-card",
            arguments=[],
            **{field: value},
        )

        with pytest.raises(ValueError, match=message):
            cmd.to_server_command("default-queue")

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            (
                {"start_to_close_timeout": 10, "heartbeat_timeout": 11},
                "heartbeat_timeout must be <= start_to_close_timeout",
            ),
            (
                {"start_to_close_timeout": 301, "schedule_to_close_timeout": 300},
                "start_to_close_timeout must be <= schedule_to_close_timeout",
            ),
            (
                {"schedule_to_start_timeout": 301, "schedule_to_close_timeout": 300},
                "schedule_to_start_timeout must be <= schedule_to_close_timeout",
            ),
        ],
    )
    def test_schedule_activity_rejects_incoherent_timeout_envelopes(
        self,
        kwargs: dict[str, int],
        message: str,
    ) -> None:
        cmd = ScheduleActivity(
            activity_type="charge-card",
            arguments=[],
            **kwargs,
        )

        with pytest.raises(ValueError, match=message):
            cmd.to_server_command("default-queue")


class TestReverseCompensationSaga:
    def test_resumes_reverse_compensation_after_first_refund(self) -> None:
        payload = {"scenario_id": "reverse-compensation"}
        history = [
            _activity_completed_event(1, "reserve_flight"),
            _activity_completed_event(2, "reserve_hotel"),
            _activity_completed_event(3, "charge_card"),
            _activity_failed_event(4, "send_confirmation"),
            _activity_completed_event(5, "refund_card"),
        ]

        outcome = replay(ReverseCompensationSaga, history, [payload], payload_codec="json")

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.activity_type == "cancel_hotel"
        assert cmd.arguments == [payload]

    def test_completes_reverse_compensation_without_duplicates(self) -> None:
        payload = {"scenario_id": "reverse-compensation"}
        history = [
            _activity_completed_event(1, "reserve_flight"),
            _activity_completed_event(2, "reserve_hotel"),
            _activity_completed_event(3, "charge_card"),
            _activity_failed_event(4, "send_confirmation"),
            _activity_completed_event(5, "refund_card"),
            _activity_completed_event(6, "cancel_hotel"),
            _activity_completed_event(7, "cancel_flight"),
        ]

        outcome = replay(ReverseCompensationSaga, history, [payload], payload_codec="json")

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {
            "status": "compensated",
            "activity_log": [
                "reserve_flight",
                "reserve_hotel",
                "charge_card",
                "refund_card",
                "cancel_hotel",
                "cancel_flight",
            ],
            "compensations": ["cancel_flight", "cancel_hotel", "refund_card"],
        }


class TestTwoActivities:
    def test_first_schedules(self) -> None:
        outcome = replay(TwoActivities, [], [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "step1"

    def test_one_completed_schedules_next(self) -> None:
        history = [{"event_type": "ActivityCompleted", "payload": {"result": '"val1"'}}]
        outcome = replay(TwoActivities, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "step2"
        assert outcome.commands[0].arguments == ["val1"]

    def test_both_completed(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "payload": {"result": '"val1"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"val2"'}},
        ]
        outcome = replay(TwoActivities, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == ["val1", "val2"]


class TestTimerWorkflow:
    def test_first_replay_starts_timer(self) -> None:
        outcome = replay(TimerWorkflow, [], [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, StartTimer)
        assert cmd.delay_seconds == 5

    def test_timer_fired_schedules_activity(self) -> None:
        history = [{"event_type": "TimerFired", "payload": {}}]
        outcome = replay(TimerWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "greet"

    def test_timer_and_activity_completed(self) -> None:
        history = [
            {"event_type": "TimerFired", "payload": {}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"hi"'}},
        ]
        outcome = replay(TimerWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "hi"

    def test_timer_server_command_shape(self) -> None:
        outcome = replay(TimerWorkflow, [], [])
        server_cmd = outcome.commands[0].to_server_command("q")
        assert server_cmd["type"] == "start_timer"
        assert server_cmd["delay_seconds"] == 5


class TestFailingWorkflow:
    def test_exception_produces_fail_command(self) -> None:
        history = [{"event_type": "ActivityCompleted", "payload": {"result": '"ok"'}}]
        outcome = replay(FailingWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, FailWorkflow)
        assert "something went wrong" in cmd.message
        assert cmd.exception_type == "ValueError"
        assert cmd.exception_class == "builtins.ValueError"

    def test_fail_server_command_shape(self) -> None:
        history = [{"event_type": "ActivityCompleted", "payload": {"result": '"ok"'}}]
        outcome = replay(FailingWorkflow, history, [])
        server_cmd = outcome.commands[0].to_server_command("q")
        assert server_cmd["type"] == "fail_workflow"
        assert "something went wrong" in server_cmd["message"]
        assert server_cmd["exception_class"] == "builtins.ValueError"

    def test_typed_compensation_activity_failure_reaches_terminal_command(self) -> None:
        history = [
            {
                "event_type": "ActivityFailed",
                "payload": {
                    "sequence": 1,
                    "activity_type": "charge_card",
                    "exception_type": "PlannedSagaFailure",
                    "exception_class": "sagas.PlannedSagaFailure",
                    "message": "planned saga failure",
                },
            },
            {
                "event_type": "ActivityFailed",
                "payload": {
                    "sequence": 2,
                    "activity_type": "cancel_flight",
                    "activity_attempt_id": "attempt-cancel-flight",
                    "exception_type": "TypedCancelFlightError",
                    "exception_class": "__main__.TypedCancelFlightError",
                    "message": "cancel_flight typed compensation failure",
                },
            },
        ]

        outcome = replay(TypedCompensationFailureWorkflow, history, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, FailWorkflow)
        assert cmd.exception_type == "TypedCancelFlightError"
        assert cmd.exception_class == "__main__.TypedCancelFlightError"
        assert cmd.exception is not None
        assert cmd.exception["type"] == "TypedCancelFlightError"
        assert cmd.exception["message"] == cmd.message
        assert cmd.exception["activity_type"] == "cancel_flight"
        assert cmd.exception["activity_attempt_id"] == "attempt-cancel-flight"
        assert "cancel_flight typed compensation failure" in cmd.message

        server_cmd = cmd.to_server_command("q")
        assert server_cmd["exception_type"] == "TypedCancelFlightError"
        assert server_cmd["exception_class"] == "__main__.TypedCancelFlightError"
        assert server_cmd["exception"]["message"] == cmd.message


class TestCompleteWorkflowCommand:
    def test_server_command(self) -> None:
        cmd = CompleteWorkflow(result={"key": "val"})
        server_cmd = cmd.to_server_command("q")
        assert server_cmd["type"] == "complete_workflow"
        assert server_cmd["result"]["codec"] == "avro"
        assert serializer.decode(server_cmd["result"]["blob"], codec="avro") == {"key": "val"}

    def test_server_command_uses_payload_codec(self) -> None:
        cmd = CompleteWorkflow(result={"key": "val"})
        server_cmd = cmd.to_server_command("q", payload_codec="json")
        assert server_cmd["result"]["codec"] == "json"
        assert serializer.decode(server_cmd["result"]["blob"], codec="json") == {"key": "val"}


@workflow.defn(name="continue-as-new-wf")
class ContinueAsNewWorkflow:
    def run(self, ctx: WorkflowContext, counter: int):  # type: ignore[no-untyped-def]
        if counter > 0:
            return ctx.continue_as_new(counter - 1)
        return "done"


@workflow.defn(name="continue-as-new-yield-wf")
class ContinueAsNewYieldWorkflow:
    def run(self, ctx: WorkflowContext, counter: int):  # type: ignore[no-untyped-def]
        yield ctx.schedule_activity("step", [])
        return ctx.continue_as_new(counter - 1)


@workflow.defn(name="side-effect-wf")
class SideEffectWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        val = yield ctx.side_effect(lambda: 42)
        result = yield ctx.schedule_activity("use-val", [val])
        return result


@workflow.defn(name="context-wf")
class ContextWorkflow:
    def run(self, ctx: WorkflowContext) -> dict:  # type: ignore[type-arg]
        t = ctx.now().isoformat()
        r = ctx.random().random()
        u = str(ctx.uuid4())
        return {"time": t, "rand": r, "uuid": u}


class TestContinueAsNew:
    def test_non_generator_continue(self) -> None:
        outcome = replay(ContinueAsNewWorkflow, [], [3])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ContinueAsNew)
        assert cmd.arguments == [2]

    def test_non_generator_complete(self) -> None:
        outcome = replay(ContinueAsNewWorkflow, [], [0])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "done"

    def test_generator_return_continue(self) -> None:
        history = [{"event_type": "ActivityCompleted", "payload": {"result": '"ok"'}}]
        outcome = replay(ContinueAsNewYieldWorkflow, history, [5])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ContinueAsNew)
        assert cmd.arguments == [4]

    def test_server_command_shape(self) -> None:
        cmd = ContinueAsNew(workflow_type="other", arguments=[1, 2], task_queue="q2")
        sc = cmd.to_server_command("default-q")
        assert sc["type"] == "continue_as_new"
        assert sc["workflow_type"] == "other"
        assert sc["queue"] == "q2"

    def test_server_command_defaults(self) -> None:
        cmd = ContinueAsNew(arguments=[1])
        sc = cmd.to_server_command("default-q")
        assert sc["queue"] == "default-q"
        assert "workflow_type" not in sc


class TestSideEffect:
    def test_first_replay_records_and_continues(self) -> None:
        outcome = replay(SideEffectWorkflow, [], [])
        assert len(outcome.commands) == 2
        assert isinstance(outcome.commands[0], RecordSideEffect)
        assert outcome.commands[0].result == 42
        assert isinstance(outcome.commands[1], ScheduleActivity)
        assert outcome.commands[1].activity_type == "use-val"
        assert outcome.commands[1].arguments == [42]

    def test_replayed_side_effect_skips_fn(self) -> None:
        history = [{"event_type": "SideEffectRecorded", "payload": {"result": "99"}}]
        outcome = replay(SideEffectWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.arguments == [99]

    def test_full_replay(self) -> None:
        history = [
            {"event_type": "SideEffectRecorded", "payload": {"result": "42"}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"final"'}},
        ]
        outcome = replay(SideEffectWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "final"

    def test_server_command_shape(self) -> None:
        cmd = RecordSideEffect(result={"key": "val"})
        sc = cmd.to_server_command("q")
        assert sc["type"] == "record_side_effect"
        assert serializer.decode(sc["result"], codec="avro") == {"key": "val"}

    def test_server_command_uses_payload_codec(self) -> None:
        cmd = RecordSideEffect(result={"key": "val"})
        sc = cmd.to_server_command("q", payload_codec="json")
        assert serializer.decode(sc["result"], codec="json") == {"key": "val"}


class TestWorkflowContext:
    def test_now_returns_deterministic_time(self) -> None:
        t = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ctx = WorkflowContext(run_id="r1", current_time=t)
        assert ctx.now() == t

    def test_random_seeded_by_run_id(self) -> None:
        ctx1 = WorkflowContext(run_id="same-run")
        ctx2 = WorkflowContext(run_id="same-run")
        assert ctx1.random().random() == ctx2.random().random()

    def test_random_differs_by_run_id(self) -> None:
        ctx1 = WorkflowContext(run_id="run-a")
        ctx2 = WorkflowContext(run_id="run-b")
        assert ctx1.random().random() != ctx2.random().random()

    def test_uuid4_deterministic(self) -> None:
        ctx1 = WorkflowContext(run_id="run-x")
        ctx2 = WorkflowContext(run_id="run-x")
        assert ctx1.uuid4() == ctx2.uuid4()

    def test_uuid4_is_version_4(self) -> None:
        ctx = WorkflowContext(run_id="run-y")
        u = ctx.uuid4()
        assert u.version == 4

    def test_uuid7_deterministic(self) -> None:
        t = datetime(2026, 1, 1, 12, 30, 15, 123000, tzinfo=timezone.utc)
        ctx1 = WorkflowContext(run_id="run-v7", current_time=t)
        ctx2 = WorkflowContext(run_id="run-v7", current_time=t)

        assert ctx1.uuid7() == ctx2.uuid7()
        assert ctx1.uuid7() == ctx2.uuid7()

    def test_uuid7_is_version_7_and_uses_context_time(self) -> None:
        t = datetime(2026, 1, 1, 12, 30, 15, 123000, tzinfo=timezone.utc)
        ctx = WorkflowContext(run_id="run-v7", current_time=t)

        u = ctx.uuid7()

        assert u.version == 7
        assert (u.int >> 80) == int(t.timestamp() * 1000)

    def test_uuid7_orders_same_tick_calls_by_sequence(self) -> None:
        t = datetime(2026, 1, 1, 12, 30, 15, 123000, tzinfo=timezone.utc)
        ctx = WorkflowContext(run_id="run-v7", current_time=t)

        first = ctx.uuid7()
        second = ctx.uuid7()

        assert first < second
        assert ((first.int >> 64) & 0xFFF) == 0
        assert ((second.int >> 64) & 0xFFF) == 1

    def test_logger_silent_during_replay(self) -> None:
        import logging
        import logging.handlers

        ctx = WorkflowContext(run_id="r1")
        ctx.logger._set_replaying(True)
        logger = logging.getLogger("durable_workflow.workflow.replay")
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.MemoryHandler(capacity=100)
        logger.addHandler(handler)
        try:
            ctx.logger.info("should not appear")
            assert len(handler.buffer) == 0
        finally:
            logger.removeHandler(handler)

    def test_logger_active_when_not_replaying(self) -> None:
        import logging
        import logging.handlers

        ctx = WorkflowContext(run_id="r1")
        ctx.logger._set_replaying(False)
        logger = logging.getLogger("durable_workflow.workflow.replay")
        logger.setLevel(logging.DEBUG)
        handler = logging.handlers.MemoryHandler(capacity=100)
        logger.addHandler(handler)
        try:
            ctx.logger.info("should appear")
            assert len(handler.buffer) == 1
        finally:
            logger.removeHandler(handler)

    def test_schedule_activity_accepts_retry_policy_and_timeouts(self) -> None:
        ctx = WorkflowContext(run_id="r1")
        policy = ActivityRetryPolicy(max_attempts=2, backoff_seconds=[7])

        cmd = ctx.schedule_activity(
            "charge-card",
            [{"order_id": "o-1"}],
            retry_policy=policy,
            start_to_close_timeout=120,
            schedule_to_start_timeout=10,
            schedule_to_close_timeout=300,
            heartbeat_timeout=15,
        )

        assert cmd.retry_policy is policy
        assert cmd.start_to_close_timeout == 120
        assert cmd.schedule_to_start_timeout == 10
        assert cmd.schedule_to_close_timeout == 300
        assert cmd.heartbeat_timeout == 15

    def test_start_child_workflow_accepts_retry_policy_and_timeouts(self) -> None:
        ctx = WorkflowContext(run_id="r1")
        policy = ChildWorkflowRetryPolicy(max_attempts=2, backoff_seconds=[7])

        cmd = ctx.start_child_workflow(
            "child",
            [{"order_id": "o-1"}],
            retry_policy=policy,
            execution_timeout_seconds=600,
            run_timeout_seconds=120,
        )

        assert cmd.retry_policy is policy
        assert cmd.execution_timeout_seconds == 600
        assert cmd.run_timeout_seconds == 120


class TestReplayWithRunId:
    def test_run_id_passed_to_context(self) -> None:
        outcome = replay(ContextWorkflow, [], [], run_id="test-run-123")
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result["uuid"]
        assert cmd.result["time"]

    def test_timestamp_from_history(self) -> None:
        history = [
            {"event_type": "WorkflowStarted", "payload": {"timestamp": "2026-06-01T12:00:00Z"}},
        ]
        outcome = replay(ContextWorkflow, history, [], run_id="r1")
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert "2026-06-01" in cmd.result["time"]


@workflow.defn(name="child-wf")
class ChildWorkflow:
    def run(self, ctx: WorkflowContext, name: str):  # type: ignore[no-untyped-def]
        result = yield ctx.start_child_workflow("sub-workflow", [name])
        return {"child_result": result}


@workflow.defn(name="child-wf-failed")
class ChildWorkflowFailedWf:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_child_workflow("sub-workflow", [])
        except ChildWorkflowFailed:
            return "handled"


@workflow.defn(name="child-wf-failure-details")
class ChildWorkflowFailureDetailsWf:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_child_workflow("sub-workflow", [])
        except ChildWorkflowFailed as exc:
            return {
                "message": str(exc),
                "exception_class": exc.exception_class,
                "failure_kind": exc.failure_kind,
                "child_workflow_run_id": exc.child_workflow_run_id,
                "child_workflow_type": exc.child_workflow_type,
            }


@workflow.defn(name="child-wf-cancelled")
class ChildWorkflowCancelledWf:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_child_workflow("sub-workflow", [])
        except ChildWorkflowCancelled as exc:
            return {
                "message": str(exc),
                "failure_kind": exc.failure_kind,
                "child_workflow_run_id": exc.child_workflow_run_id,
                "child_workflow_type": exc.child_workflow_type,
            }


@workflow.defn(name="child-wf-terminated")
class ChildWorkflowTerminatedWf:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_child_workflow("sub-workflow", [])
        except ChildWorkflowTerminated as exc:
            return {
                "message": str(exc),
                "failure_kind": exc.failure_kind,
                "child_workflow_run_id": exc.child_workflow_run_id,
            }


@workflow.defn(name="child-wf-failed-fallback")
class ChildWorkflowFailedFallbackWf:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield ctx.start_child_workflow("sub-workflow", [])
        except ChildWorkflowFailed:
            result = yield ctx.schedule_activity("fallback", [])
            return {"fallback": result}


@workflow.defn(name="version-wf")
class VersionWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        version = yield ctx.get_version("change-1", 1, 2)
        if version >= 2:
            result = yield ctx.schedule_activity("new-path", [])
        else:
            result = yield ctx.schedule_activity("old-path", [])
        return result


@workflow.defn(name="patched-wf")
class PatchedWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        if (yield ctx.patched("patch-1")):
            result = yield ctx.schedule_activity("patched-path", [])
        else:
            result = yield ctx.schedule_activity("legacy-path", [])
        return result


@workflow.defn(name="deprecated-patch-wf")
class DeprecatedPatchWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.deprecate_patch("patch-1")
        result = yield ctx.schedule_activity("patched-path", [])
        return result


@workflow.defn(name="search-attr-wf")
class SearchAttrWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield ctx.upsert_search_attributes({"status": "processing"})
        result = yield ctx.schedule_activity("work", [])
        yield ctx.upsert_search_attributes({"status": "done"})
        return result


@workflow.defn(name="tests.polyglot.history-contract")
class PolyglotHistoryContractWorkflow:
    def __init__(self) -> None:
        self.approved_by: str | None = None
        self.name = "original"

    @workflow.signal("approve")
    def approve(self, approved_by: str) -> None:
        self.approved_by = approved_by

    @workflow.update("rename")
    def rename(self, name: str) -> None:
        self.name = name

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        activity = yield ctx.schedule_activity("tests.polyglot.activity", [])
        yield ctx.start_timer(5)
        yield ctx.wait_condition(lambda: self.approved_by is not None, key="approval", timeout=30)
        side_effect = yield ctx.side_effect(lambda: {"side_effect": "unstable"})
        child = yield ctx.start_child_workflow("tests.polyglot.child", [])
        yield ctx.upsert_search_attributes({"status": "processing"})

        return {
            "activity": activity,
            "approved_by": self.approved_by,
            "name": self.name,
            "side_effect": side_effect,
            "child": child,
        }


class TestChildWorkflow:
    def test_first_replay_starts_child(self) -> None:
        outcome = replay(ChildWorkflow, [], ["alice"])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, StartChildWorkflow)
        assert cmd.workflow_type == "sub-workflow"
        assert cmd.arguments == ["alice"]

    @pytest.mark.parametrize(
        "payload",
        [
            {"output": '"sub-result"'},
            {"output": serializer.envelope("sub-result", codec="json")},
            {"result": '"sub-result"'},
        ],
    )
    def test_child_completed(self, payload: dict[str, object]) -> None:
        history = [{"event_type": "ChildRunCompleted", "payload": payload}]
        outcome = replay(ChildWorkflow, history, ["alice"])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {"child_result": "sub-result"}

    def test_child_failed_caught(self) -> None:
        history = [{"event_type": "ChildRunFailed", "payload": {"message": "child failed"}}]
        outcome = replay(ChildWorkflowFailedWf, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == "handled"

    def test_child_failed_preserves_typed_details(self) -> None:
        history = [
            {
                "event_type": "ChildRunFailed",
                "payload": {
                    "message": "child failed",
                    "exception_class": "ExampleChildError",
                    "failure_category": "child_workflow",
                    "child_workflow_run_id": "child-run-1",
                    "child_workflow_type": "sub-workflow",
                },
            },
        ]

        outcome = replay(ChildWorkflowFailureDetailsWf, history, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {
            "message": "child failed",
            "exception_class": "ExampleChildError",
            "failure_kind": "child_workflow",
            "child_workflow_run_id": "child-run-1",
            "child_workflow_type": "sub-workflow",
        }

    def test_child_failed_prefers_nested_exception_class_over_type(self) -> None:
        history = [
            {
                "event_type": "ChildRunFailed",
                "payload": {
                    "message": "child failed",
                    "exception_type": "NestedType",
                    "exception": {
                        "type": "NestedType",
                        "class": "pkg.NestedClass",
                        "message": "child failed",
                    },
                },
            },
        ]

        outcome = replay(ChildWorkflowFailureDetailsWf, history, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result["exception_class"] == "pkg.NestedClass"

    def test_child_cancelled_is_observed_by_parent_as_typed_cancellation(self) -> None:
        history = [
            {
                "event_type": "ChildRunCancelled",
                "payload": {
                    "message": "cancelled by operator",
                    "child_workflow_run_id": "child-run-1",
                    "child_workflow_type": "sub-workflow",
                },
            },
        ]

        outcome = replay(ChildWorkflowCancelledWf, history, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {
            "message": "cancelled by operator",
            "failure_kind": "cancelled",
            "child_workflow_run_id": "child-run-1",
            "child_workflow_type": "sub-workflow",
        }

    def test_child_terminated_is_observed_by_parent_as_typed_child_failure(self) -> None:
        history = [
            {
                "event_type": "ChildRunTerminated",
                "payload": {
                    "message": "terminated by operator",
                    "child_workflow_run_id": "child-run-1",
                },
            },
        ]

        outcome = replay(ChildWorkflowTerminatedWf, history, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {
            "message": "terminated by operator",
            "failure_kind": "terminated",
            "child_workflow_run_id": "child-run-1",
        }

    def test_child_failed_then_fallback_yields_command(self) -> None:
        history = [{"event_type": "ChildRunFailed", "payload": {"message": "child failed"}}]
        outcome = replay(ChildWorkflowFailedFallbackWf, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.activity_type == "fallback"

    def test_child_failed_then_fallback_completes(self) -> None:
        history = [
            {"event_type": "ChildRunFailed", "payload": {"message": "child failed"}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"ok"'}},
        ]
        outcome = replay(ChildWorkflowFailedFallbackWf, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {"fallback": "ok"}

    def test_server_command_shape(self) -> None:
        cmd = StartChildWorkflow(
            workflow_type="sub",
            arguments=[1],
            task_queue="q2",
            parent_close_policy="terminate",
            retry_policy=ChildWorkflowRetryPolicy(
                max_attempts=3,
                backoff_seconds=[2, 8],
                non_retryable_error_types=["ValidationError"],
            ),
            execution_timeout_seconds=600,
            run_timeout_seconds=120,
        )
        sc = cmd.to_server_command("default-q")
        assert sc["type"] == "start_child_workflow"
        assert sc["workflow_type"] == "sub"
        assert sc["queue"] == "q2"
        assert sc["parent_close_policy"] == "terminate"
        assert sc["retry_policy"] == {
            "max_attempts": 3,
            "backoff_seconds": [2, 8],
            "non_retryable_error_types": ["ValidationError"],
        }
        assert sc["execution_timeout_seconds"] == 600
        assert sc["run_timeout_seconds"] == 120

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("execution_timeout_seconds", 0, "execution_timeout_seconds must be >= 1 second"),
            ("run_timeout_seconds", 0, "run_timeout_seconds must be >= 1 second"),
        ],
    )
    def test_server_command_rejects_non_positive_child_timeout_budgets(
        self,
        field: str,
        value: int,
        message: str,
    ) -> None:
        cmd = StartChildWorkflow(
            workflow_type="sub",
            arguments=[],
            **{field: value},
        )

        with pytest.raises(ValueError, match=message):
            cmd.to_server_command("default-q")

    def test_server_command_rejects_child_run_timeout_larger_than_execution_timeout(self) -> None:
        cmd = StartChildWorkflow(
            workflow_type="sub",
            arguments=[],
            execution_timeout_seconds=120,
            run_timeout_seconds=121,
        )

        with pytest.raises(ValueError, match="run_timeout_seconds must be <= execution_timeout_seconds"):
            cmd.to_server_command("default-q")

    def test_server_command_defaults(self) -> None:
        cmd = StartChildWorkflow(workflow_type="sub", arguments=[])
        sc = cmd.to_server_command("default-q")
        assert sc["queue"] == "default-q"
        assert "parent_close_policy" not in sc
        assert "retry_policy" not in sc
        assert "execution_timeout_seconds" not in sc
        assert "run_timeout_seconds" not in sc


class TestVersionMarker:
    def test_replays_php_history_event_wire_fixtures(self) -> None:
        events_by_type = {event["event_type"]: event for event in PHP_HISTORY_EVENT_FIXTURES}

        for event_type, expected_keys in PHP_HISTORY_EVENT_EXPECTED_KEYS.items():
            if event_type == "VersionMarkerRecorded":
                payload = PHP_VERSION_MARKER_RECORDED_EVENT["payload"]
            else:
                payload = events_by_type[event_type]["payload"]

            assert sorted(payload) == sorted(expected_keys)

        replay_events = [
            events_by_type["ActivityCompleted"],
            events_by_type["TimerFired"],
            events_by_type["SignalReceived"],
            events_by_type["UpdateApplied"],
            events_by_type["ConditionWaitOpened"],
            events_by_type["ConditionWaitSatisfied"],
            events_by_type["SideEffectRecorded"],
            events_by_type["ChildRunCompleted"],
            events_by_type["SearchAttributesUpserted"],
        ]

        outcome = replay(PolyglotHistoryContractWorkflow, replay_events, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {
            "activity": {"activity": "ok"},
            "approved_by": "alice",
            "name": "new-name",
            "side_effect": {"side_effect": "stable"},
            "child": {"child": "ok"},
        }

    def test_replays_php_emitted_version_marker_wire_fixture(self) -> None:
        payload = PHP_VERSION_MARKER_RECORDED_EVENT["payload"]

        assert sorted(payload) == sorted(PHP_VERSION_MARKER_EXPECTED_KEYS)
        assert payload == {
            "sequence": 1,
            "change_id": "polyglot-version-marker",
            "version": 2,
            "min_supported": 1,
            "max_supported": 2,
        }

        outcome = replay(PolyglotVersionMarkerWorkflow, [PHP_VERSION_MARKER_RECORDED_EVENT], [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.activity_type == "tests.polyglot.version-marker-new-path"

    def test_first_replay_records_marker_and_continues(self) -> None:
        outcome = replay(VersionWorkflow, [], [])
        assert len(outcome.commands) == 2
        cmd = outcome.commands[0]
        assert isinstance(cmd, RecordVersionMarker)
        assert cmd.change_id == "change-1"
        assert cmd.version == 2
        assert cmd.min_supported == 1
        assert cmd.max_supported == 2
        assert isinstance(outcome.commands[1], ScheduleActivity)
        assert outcome.commands[1].activity_type == "new-path"

    def test_version_from_history(self) -> None:
        history = [
            {"event_type": "VersionMarkerRecorded", "payload": {"version": 2}},
        ]
        outcome = replay(VersionWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.activity_type == "new-path"

    def test_old_version_from_history(self) -> None:
        history = [
            {"event_type": "VersionMarkerRecorded", "payload": {"version": 1}},
        ]
        outcome = replay(VersionWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.activity_type == "old-path"

    def test_full_replay(self) -> None:
        history = [
            {"event_type": "VersionMarkerRecorded", "payload": {"version": 2}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"done"'}},
        ]
        outcome = replay(VersionWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "done"

    def test_server_command_shape(self) -> None:
        cmd = RecordVersionMarker(change_id="c1", version=3, min_supported=1, max_supported=3)
        sc = cmd.to_server_command("q")
        assert sc["type"] == "record_version_marker"
        assert sc["change_id"] == "c1"
        assert sc["version"] == 3

    def test_patched_records_marker_and_resolves_true_for_new_runs(self) -> None:
        outcome = replay(PatchedWorkflow, [], [])

        assert len(outcome.commands) == 2
        cmd = outcome.commands[0]
        assert isinstance(cmd, RecordVersionMarker)
        assert cmd.change_id == "patch-1"
        assert cmd.version == 1
        assert cmd.min_supported == -1
        assert cmd.max_supported == 1
        assert cmd.to_server_command("q") == {
            "type": "record_version_marker",
            "change_id": "patch-1",
            "version": 1,
            "min_supported": -1,
            "max_supported": 1,
        }
        assert isinstance(outcome.commands[1], ScheduleActivity)
        assert outcome.commands[1].activity_type == "patched-path"

    def test_patched_uses_recorded_marker_for_existing_patched_runs(self) -> None:
        history = [
            {"event_type": "VersionMarkerRecorded", "payload": {"version": 1}},
        ]

        outcome = replay(PatchedWorkflow, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "patched-path"

    def test_patched_uses_legacy_default_for_existing_unpatched_runs(self) -> None:
        history = [
            {"event_type": "VersionMarkerRecorded", "payload": {"version": -1}},
        ]

        outcome = replay(PatchedWorkflow, history, [])

        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "legacy-path"

    def test_deprecate_patch_consumes_or_records_marker_without_branching(self) -> None:
        first = replay(DeprecatedPatchWorkflow, [], [])
        assert len(first.commands) == 2
        marker = first.commands[0]
        assert isinstance(marker, RecordVersionMarker)
        assert marker.change_id == "patch-1"
        assert marker.version == 1
        assert marker.min_supported == -1
        assert marker.max_supported == 1
        assert isinstance(first.commands[1], ScheduleActivity)
        assert first.commands[1].activity_type == "patched-path"

        replayed = replay(
            DeprecatedPatchWorkflow,
            [{"event_type": "VersionMarkerRecorded", "payload": {"version": 1}}],
            [],
        )
        assert len(replayed.commands) == 1
        assert isinstance(replayed.commands[0], ScheduleActivity)
        assert replayed.commands[0].activity_type == "patched-path"


class TestSearchAttributeUpsert:
    def test_first_replay_upserts_then_schedules(self) -> None:
        outcome = replay(SearchAttrWorkflow, [], [])
        assert len(outcome.commands) == 2
        assert isinstance(outcome.commands[0], UpsertSearchAttributes)
        assert outcome.commands[0].attributes == {"status": "processing"}
        assert isinstance(outcome.commands[1], ScheduleActivity)

    def test_with_upsert_in_history(self) -> None:
        history = [
            {"event_type": "SearchAttributesUpserted", "payload": {}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"result"'}},
        ]
        outcome = replay(SearchAttrWorkflow, history, [])
        assert len(outcome.commands) == 2
        assert isinstance(outcome.commands[0], UpsertSearchAttributes)
        assert outcome.commands[0].attributes == {"status": "done"}
        assert isinstance(outcome.commands[1], CompleteWorkflow)
        assert outcome.commands[1].result == "result"

    def test_server_command_shape(self) -> None:
        cmd = UpsertSearchAttributes(attributes={"key": "val"})
        sc = cmd.to_server_command("q")
        assert sc["type"] == "upsert_search_attributes"
        assert sc["attributes"] == {"key": "val"}


@workflow.defn(name="fan-out-wf")
class FanOutWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        results = yield [
            ctx.schedule_activity("fetch-a", []),
            ctx.schedule_activity("fetch-b", []),
        ]
        return {"a": results[0], "b": results[1]}


@workflow.defn(name="fan-out-timers-wf")
class FanOutTimersWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield [ctx.start_timer(5), ctx.start_timer(10)]
        result = yield ctx.schedule_activity("after-timers", [])
        return result


@workflow.defn(name="fan-out-activity-timer-wf")
class FanOutActivityTimerWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        yield [ctx.schedule_activity("fetch", []), ctx.start_timer(5)]
        return "done"


@workflow.defn(name="fan-out-then-sequential-wf")
class FanOutThenSequentialWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        results = yield [
            ctx.schedule_activity("a", []),
            ctx.schedule_activity("b", []),
        ]
        final = yield ctx.schedule_activity("combine", results)
        return final


@workflow.defn(name="fan-out-activity-fail-wf")
class FanOutActivityFailWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            results = yield [
                ctx.schedule_activity("ok-activity", []),
                ctx.schedule_activity("bad-activity", []),
            ]
            return {"results": results}
        except ActivityFailed as exc:
            return {"caught": exc.activity_type, "exception_type": exc.exception_type}


class TestFanOut:
    def test_no_history_emits_batch(self) -> None:
        outcome = replay(FanOutWorkflow, [], [])
        assert len(outcome.commands) == 2
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "fetch-a"
        assert isinstance(outcome.commands[1], ScheduleActivity)
        assert outcome.commands[1].activity_type == "fetch-b"

    def test_all_completed(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "payload": {"result": '"val-a"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"val-b"'}},
        ]
        outcome = replay(FanOutWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == {"a": "val-a", "b": "val-b"}

    def test_timers_no_history(self) -> None:
        outcome = replay(FanOutTimersWorkflow, [], [])
        assert len(outcome.commands) == 2
        assert isinstance(outcome.commands[0], StartTimer)
        assert isinstance(outcome.commands[1], StartTimer)

    def test_timers_fired_then_activity(self) -> None:
        history = [
            {"event_type": "TimerFired", "payload": {}},
            {"event_type": "TimerFired", "payload": {}},
        ]
        outcome = replay(FanOutTimersWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "after-timers"

    def test_pending_batch_history_matches_by_workflow_sequence(self) -> None:
        history = [
            {
                "event_type": "TimerScheduled",
                "payload": {"sequence": 2, "timer_kind": "durable_timer"},
            },
            {
                "event_type": "ActivityScheduled",
                "payload": {"sequence": 1, "activity_type": "fetch"},
            },
        ]

        outcome = replay(FanOutActivityTimerWorkflow, history, [])

        assert len(outcome.commands) == 2
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert isinstance(outcome.commands[1], StartTimer)

    def test_fan_out_then_sequential(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "payload": {"result": '"r1"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"r2"'}},
        ]
        outcome = replay(FanOutThenSequentialWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "combine"
        assert outcome.commands[0].arguments == ["r1", "r2"]

    def test_fan_out_then_sequential_full(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "payload": {"result": '"r1"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"r2"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"combined"'}},
        ]
        outcome = replay(FanOutThenSequentialWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], CompleteWorkflow)
        assert outcome.commands[0].result == "combined"

    def test_fan_out_activity_failure_throws(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "payload": {"result": '"ok"'}},
            {
                "event_type": "ActivityFailed",
                "payload": {
                    "activity_type": "bad-activity",
                    "exception_type": "DownstreamRejected",
                    "message": "downstream rejected",
                },
            },
        ]

        outcome = replay(FanOutActivityFailWorkflow, history, [])

        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {
            "caught": "bad-activity",
            "exception_type": "DownstreamRejected",
        }


@workflow.defn(name="fan-out-child-fail-wf")
class FanOutChildFailWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            results = yield [
                ctx.start_child_workflow("ok-child", []),
                ctx.start_child_workflow("bad-child", []),
            ]
            return {"results": results}
        except ChildWorkflowFailed:
            return "caught-batch-failure"


@workflow.defn(name="fan-out-child-fail-fallback-wf")
class FanOutChildFailFallbackWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        try:
            yield [
                ctx.start_child_workflow("a", []),
                ctx.start_child_workflow("b", []),
            ]
        except ChildWorkflowFailed:
            result = yield ctx.schedule_activity("fallback", [])
            return {"fallback": result}


@workflow.defn(name="fan-out-child-fail-unhandled-wf")
class FanOutChildFailUnhandledWorkflow:
    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        results = yield [
            ctx.start_child_workflow("ok-child", []),
            ctx.start_child_workflow("bad-child", []),
        ]
        return results


class TestFanOutChildFailure:
    def test_batch_child_failure_throws(self) -> None:
        history = [
            {"event_type": "ChildRunCompleted", "payload": {"result": '"ok"'}},
            {"event_type": "ChildRunFailed", "payload": {"message": "child crashed"}},
        ]
        outcome = replay(FanOutChildFailWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == "caught-batch-failure"

    def test_batch_child_failure_first_position(self) -> None:
        history = [
            {"event_type": "ChildRunFailed", "payload": {"message": "first failed"}},
            {"event_type": "ChildRunCompleted", "payload": {"result": '"ok"'}},
        ]
        outcome = replay(FanOutChildFailWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == "caught-batch-failure"

    def test_batch_child_failure_fallback_yields(self) -> None:
        history = [
            {"event_type": "ChildRunCompleted", "payload": {"result": '"ok"'}},
            {"event_type": "ChildRunFailed", "payload": {"message": "oops"}},
        ]
        outcome = replay(FanOutChildFailFallbackWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)
        assert outcome.commands[0].activity_type == "fallback"

    def test_batch_child_failure_fallback_completes(self) -> None:
        history = [
            {"event_type": "ChildRunCompleted", "payload": {"result": '"ok"'}},
            {"event_type": "ChildRunFailed", "payload": {"message": "oops"}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"recovered"'}},
        ]
        outcome = replay(FanOutChildFailFallbackWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {"fallback": "recovered"}

    def test_batch_child_failure_unhandled_produces_fail(self) -> None:
        history = [
            {"event_type": "ChildRunCompleted", "payload": {"result": '"ok"'}},
            {"event_type": "ChildRunFailed", "payload": {"message": "child crashed"}},
        ]
        outcome = replay(FanOutChildFailUnhandledWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, FailWorkflow)
        assert "child crashed" in cmd.message

    def test_batch_all_succeed_no_throw(self) -> None:
        history = [
            {"event_type": "ChildRunCompleted", "payload": {"result": '"r1"'}},
            {"event_type": "ChildRunCompleted", "payload": {"result": '"r2"'}},
        ]
        outcome = replay(FanOutChildFailWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {"results": ["r1", "r2"]}


@workflow.defn(name="nontrivial-wf")
class NontrivialWorkflow:
    """Fan-out activities, timer, child workflow, and sequential combine."""

    def run(self, ctx: WorkflowContext):  # type: ignore[no-untyped-def]
        results = yield [
            ctx.schedule_activity("fetch-a", []),
            ctx.schedule_activity("fetch-b", []),
        ]
        yield ctx.start_timer(30)
        child_result = yield ctx.start_child_workflow("sub-process", results)
        final = yield ctx.schedule_activity("finalize", [child_result])
        return {"fan_out": results, "child": child_result, "final": final}


class TestNontrivialWorkflow:
    def test_first_replay_fans_out(self) -> None:
        outcome = replay(NontrivialWorkflow, [], [])
        assert len(outcome.commands) == 2
        assert all(isinstance(c, ScheduleActivity) for c in outcome.commands)

    def test_after_fan_out_starts_timer(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "payload": {"result": '"a"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"b"'}},
        ]
        outcome = replay(NontrivialWorkflow, history, [])
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], StartTimer)

    def test_after_timer_starts_child(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "payload": {"result": '"a"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"b"'}},
            {"event_type": "TimerFired", "payload": {}},
        ]
        outcome = replay(NontrivialWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, StartChildWorkflow)
        assert cmd.workflow_type == "sub-process"
        assert cmd.arguments == ["a", "b"]

    def test_after_child_schedules_finalize(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "payload": {"result": '"a"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"b"'}},
            {"event_type": "TimerFired", "payload": {}},
            {"event_type": "ChildRunCompleted", "payload": {"result": '"processed"'}},
        ]
        outcome = replay(NontrivialWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, ScheduleActivity)
        assert cmd.activity_type == "finalize"
        assert cmd.arguments == ["processed"]

    def test_full_replay_completes(self) -> None:
        history = [
            {"event_type": "ActivityCompleted", "payload": {"result": '"a"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"b"'}},
            {"event_type": "TimerFired", "payload": {}},
            {"event_type": "ChildRunCompleted", "payload": {"result": '"processed"'}},
            {"event_type": "ActivityCompleted", "payload": {"result": '"done"'}},
        ]
        outcome = replay(NontrivialWorkflow, history, [])
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        assert isinstance(cmd, CompleteWorkflow)
        assert cmd.result == {
            "fan_out": ["a", "b"],
            "child": "processed",
            "final": "done",
        }


class TestWorkflowRegistry:
    def test_registered(self) -> None:
        reg = workflow.registry()
        assert "simple-return" in reg
        assert "one-activity" in reg
        assert "timer-workflow" in reg
        assert "continue-as-new-wf" in reg
        assert "side-effect-wf" in reg
        assert "child-wf" in reg
        assert "version-wf" in reg
        assert "search-attr-wf" in reg
        assert "fan-out-wf" in reg


class TestCanonicalEventTypeOnly:
    # Regression guard for #432. The server's HistoryEventType enum stores
    # PascalCase values (`case ActivityCompleted = 'ActivityCompleted'`) and
    # emits `$event->event_type->value` directly on the wire. The SDK used to
    # accept a snake_case fallback alongside; collapsing to the single
    # canonical form means a future reintroduction of the fallback fails
    # these tests loudly instead of passing by tolerance.

    def test_snake_case_activity_completed_is_ignored(self) -> None:
        history = [{"event_type": "activity_completed", "payload": {"result": '"hello"'}}]
        outcome = replay(OneActivity, history, ["world"])
        # Snake_case is not recognized, so the replayer behaves as if the
        # activity has not yet completed and re-schedules it.
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], ScheduleActivity)

    def test_snake_case_timer_fired_is_ignored(self) -> None:
        history = [{"event_type": "timer_fired", "payload": {}}]
        outcome = replay(TimerWorkflow, history, [])
        # Without recognition of TimerFired, the replayer stays at the
        # StartTimer command and does not advance to the activity.
        assert len(outcome.commands) == 1
        assert isinstance(outcome.commands[0], StartTimer)
