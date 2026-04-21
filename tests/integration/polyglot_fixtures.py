"""
Polyglot interop test fixtures for Python SDK.

These fixtures test bidirectional PHP↔Python worker interop:
- Python activities called from PHP workflows
- PHP activities called from Python workflows
- Avro payload round-trip across runtimes
"""
from __future__ import annotations

from durable_workflow import activity, workflow

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
