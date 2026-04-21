"""
Polyglot interop test fixtures for Python SDK.

These fixtures test bidirectional PHP↔Python worker interop:
- Python activities called from PHP workflows
- PHP activities called from Python workflows
- Avro payload round-trip across runtimes
"""

from __future__ import annotations

from durable_workflow import activity, serializer, workflow

PHP_HISTORY_EVENT_EXPECTED_KEYS = {
    "WorkflowStarted": [
        "workflow_class",
        "workflow_type",
        "workflow_instance_id",
        "workflow_run_id",
        "workflow_command_id",
        "business_key",
        "visibility_labels",
        "memo",
        "search_attributes",
        "execution_timeout_seconds",
        "run_timeout_seconds",
        "execution_deadline_at",
        "run_deadline_at",
        "workflow_definition_fingerprint",
        "declared_queries",
        "declared_query_contracts",
        "declared_signals",
        "declared_signal_contracts",
        "declared_updates",
        "declared_update_contracts",
        "declared_entry_method",
        "declared_entry_mode",
        "declared_entry_declaring_class",
    ],
    "ActivityCompleted": [
        "activity_execution_id",
        "activity_attempt_id",
        "activity_class",
        "activity_type",
        "sequence",
        "attempt_number",
        "result",
        "payload_codec",
        "activity",
        "parallel_group_path",
    ],
    "TimerFired": [
        "timer_id",
        "sequence",
        "delay_seconds",
        "fired_at",
        "timer_kind",
        "condition_wait_id",
        "condition_key",
        "condition_definition_fingerprint",
        "signal_wait_id",
        "signal_name",
    ],
    "SignalReceived": [
        "workflow_command_id",
        "signal_id",
        "workflow_instance_id",
        "workflow_run_id",
        "signal_name",
        "signal_wait_id",
        "arguments",
        "payload_codec",
    ],
    "UpdateApplied": [
        "workflow_command_id",
        "update_id",
        "workflow_instance_id",
        "workflow_run_id",
        "update_name",
        "arguments",
        "sequence",
        "payload_codec",
    ],
    "ConditionWaitOpened": [
        "condition_wait_id",
        "condition_key",
        "condition_definition_fingerprint",
        "sequence",
        "timeout_seconds",
    ],
    "ConditionWaitSatisfied": [
        "condition_wait_id",
    ],
    "SideEffectRecorded": [
        "sequence",
        "result",
        "payload_codec",
    ],
    "VersionMarkerRecorded": [
        "sequence",
        "change_id",
        "version",
        "min_supported",
        "max_supported",
    ],
    "ChildWorkflowScheduled": [
        "sequence",
        "workflow_link_id",
        "child_call_id",
        "child_workflow_instance_id",
        "child_workflow_run_id",
        "child_workflow_class",
        "child_workflow_type",
        "parent_close_policy",
        "retry_policy",
        "timeout_policy",
    ],
    "ChildRunCompleted": [
        "sequence",
        "workflow_link_id",
        "child_call_id",
        "child_workflow_instance_id",
        "child_workflow_run_id",
        "child_workflow_class",
        "child_workflow_type",
        "child_run_number",
        "child_status",
        "closed_reason",
        "closed_at",
        "output",
        "parallel_group_path",
        "result",
        "payload_codec",
    ],
    "SearchAttributesUpserted": [
        "sequence",
        "attributes",
    ],
}

PHP_HISTORY_EVENT_FIXTURES = [
    {
        "event_type": "WorkflowStarted",
        "payload": {
            "workflow_class": "Tests\\Fixtures\\V2\\PolyglotWorkflow",
            "workflow_type": "tests.polyglot.history-contract",
            "workflow_instance_id": "wf-polyglot-1",
            "workflow_run_id": "run-polyglot-1",
            "workflow_command_id": "cmd-start-1",
            "business_key": "order-1001",
            "visibility_labels": {"tenant": "acme"},
            "memo": {"source": "php"},
            "search_attributes": {"CustomerId": "customer-1"},
            "execution_timeout_seconds": 3600,
            "run_timeout_seconds": 600,
            "execution_deadline_at": "2026-04-21T21:00:00Z",
            "run_deadline_at": "2026-04-21T20:50:00Z",
            "workflow_definition_fingerprint": "sha256:workflow-definition",
            "declared_queries": ["status"],
            "declared_query_contracts": {"status": {"returns": "object"}},
            "declared_signals": ["approve"],
            "declared_signal_contracts": {"approve": {"arguments": ["string"]}},
            "declared_updates": ["rename"],
            "declared_update_contracts": {"rename": {"arguments": ["string"]}},
            "declared_entry_method": "run",
            "declared_entry_mode": "generator",
            "declared_entry_declaring_class": "Tests\\Fixtures\\V2\\PolyglotWorkflow",
        },
    },
    {
        "event_type": "ActivityCompleted",
        "payload": {
            "activity_execution_id": "act-exec-1",
            "activity_attempt_id": "act-attempt-1",
            "activity_class": "Tests\\Fixtures\\V2\\PolyglotActivity",
            "activity_type": "tests.polyglot.activity",
            "sequence": 1,
            "attempt_number": 1,
            "result": serializer.encode({"activity": "ok"}, codec="json"),
            "payload_codec": "json",
            "activity": "tests.polyglot.activity",
            "parallel_group_path": "0",
        },
    },
    {
        "event_type": "TimerFired",
        "payload": {
            "timer_id": "timer-1",
            "sequence": 2,
            "delay_seconds": 5,
            "fired_at": "2026-04-21T20:45:00Z",
            "timer_kind": "sleep",
            "condition_wait_id": None,
            "condition_key": None,
            "condition_definition_fingerprint": None,
            "signal_wait_id": None,
            "signal_name": None,
        },
    },
    {
        "event_type": "SignalReceived",
        "payload": {
            "workflow_command_id": "cmd-signal-1",
            "signal_id": "sig-1",
            "workflow_instance_id": "wf-polyglot-1",
            "workflow_run_id": "run-polyglot-1",
            "signal_name": "approve",
            "signal_wait_id": "signal-wait-1",
            "arguments": serializer.encode(["alice"], codec="json"),
            "payload_codec": "json",
        },
    },
    {
        "event_type": "UpdateApplied",
        "payload": {
            "workflow_command_id": "cmd-update-1",
            "update_id": "upd-1",
            "workflow_instance_id": "wf-polyglot-1",
            "workflow_run_id": "run-polyglot-1",
            "update_name": "rename",
            "arguments": serializer.encode(["new-name"], codec="json"),
            "sequence": 3,
            "payload_codec": "json",
        },
    },
    {
        "event_type": "ConditionWaitOpened",
        "payload": {
            "condition_wait_id": "wait-1",
            "condition_key": "approval",
            "condition_definition_fingerprint": None,
            "sequence": 4,
            "timeout_seconds": 30,
        },
    },
    {
        "event_type": "ConditionWaitSatisfied",
        "payload": {
            "condition_wait_id": "wait-1",
        },
    },
    {
        "event_type": "SideEffectRecorded",
        "payload": {
            "sequence": 5,
            "result": serializer.encode({"side_effect": "stable"}, codec="json"),
            "payload_codec": "json",
        },
    },
    {
        "event_type": "ChildWorkflowScheduled",
        "payload": {
            "sequence": 7,
            "workflow_link_id": "link-1",
            "child_call_id": "child-call-1",
            "child_workflow_instance_id": "child-wf-1",
            "child_workflow_run_id": "child-run-1",
            "child_workflow_class": "Tests\\Fixtures\\V2\\ChildWorkflow",
            "child_workflow_type": "tests.polyglot.child",
            "parent_close_policy": "terminate",
            "retry_policy": {"max_attempts": 3, "backoff_seconds": [1, 2]},
            "timeout_policy": {"execution_timeout_seconds": 120},
        },
    },
    {
        "event_type": "ChildRunCompleted",
        "payload": {
            "sequence": 8,
            "workflow_link_id": "link-1",
            "child_call_id": "child-call-1",
            "child_workflow_instance_id": "child-wf-1",
            "child_workflow_run_id": "child-run-1",
            "child_workflow_class": "Tests\\Fixtures\\V2\\ChildWorkflow",
            "child_workflow_type": "tests.polyglot.child",
            "child_run_number": 1,
            "child_status": "completed",
            "closed_reason": "completed",
            "closed_at": "2026-04-21T20:46:00Z",
            "output": serializer.encode({"child": "ok"}, codec="json"),
            "parallel_group_path": None,
            "result": serializer.encode({"child": "ok"}, codec="json"),
            "payload_codec": "json",
        },
    },
    {
        "event_type": "SearchAttributesUpserted",
        "payload": {
            "sequence": 9,
            "attributes": {"status": "done"},
        },
    },
]

PHP_VERSION_MARKER_RECORDED_EVENT = {
    "event_type": "VersionMarkerRecorded",
    "payload": {
        "sequence": 1,
        "change_id": "polyglot-version-marker",
        "version": 2,
        "min_supported": 1,
        "max_supported": 2,
    },
}

PHP_VERSION_MARKER_EXPECTED_KEYS = [
    "sequence",
    "change_id",
    "version",
    "min_supported",
    "max_supported",
]


@activity.defn(name="tests.polyglot.python-activity")
async def polyglot_python_activity(input_data: dict) -> dict:
    """
    Python activity fixture for polyglot interop testing.

    Accepts structured input from a PHP workflow and returns
    enriched output to validate codec-aware round-trip behavior.
    """
    return {
        "runtime": "python",
        "received_input": input_data,
        "type_checks": {
            "has_string": "name" in input_data and isinstance(input_data["name"], str),
            "has_int": "count" in input_data and isinstance(input_data["count"], int),
            "has_float": "price" in input_data and isinstance(input_data["price"], float),
            "has_bool": "active" in input_data and isinstance(input_data["active"], bool),
            "has_array": "tags" in input_data and isinstance(input_data["tags"], list),
            "has_nested": "metadata" in input_data and isinstance(input_data["metadata"], dict),
        },
        "computed": {
            "name_length": len(input_data.get("name", "")),
            "count_doubled": input_data.get("count", 0) * 2,
            "tags_count": len(input_data.get("tags", [])),
        },
    }


@workflow.defn(name="tests.polyglot.python-workflow")
class PolyglotPythonWorkflow:
    """
    Python workflow fixture for polyglot interop testing.

    Schedules a PHP activity to validate that:
    1. Python workflows can call PHP activities
    2. Avro payloads round-trip correctly across runtimes
    3. Activity results from PHP are decoded properly in Python
    """

    def run(self, ctx, data: dict):  # type: ignore[no-untyped-def]
        # Schedule PHP activity with structured input
        php_result = yield ctx.schedule_activity("tests.polyglot.php-activity", [data])

        return {
            "workflow_runtime": "python",
            "php_activity_result": php_result,
            "validation": {
                "called_php_activity": True,
                "result_is_dict": isinstance(php_result, dict),
                "result_has_runtime": php_result.get("runtime") == "php" if isinstance(php_result, dict) else False,
            },
        }


@workflow.defn(name="tests.polyglot.version-marker-python-workflow")
class PolyglotVersionMarkerWorkflow:
    """Python replay fixture for PHP-emitted VersionMarkerRecorded history."""

    def run(self, ctx):  # type: ignore[no-untyped-def]
        version = yield ctx.get_version("polyglot-version-marker", 1, 2)
        if version >= 2:
            return (yield ctx.schedule_activity("tests.polyglot.version-marker-new-path", []))

        return (yield ctx.schedule_activity("tests.polyglot.version-marker-old-path", []))
