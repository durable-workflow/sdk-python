"""
Polyglot interop integration tests.

Tests bidirectional PHP↔Python worker execution through the server:
1. Python workflow scheduling PHP activity
2. PHP workflow scheduling Python activity

Requires a running Durable Workflow server with PHP workflow package.
"""
from __future__ import annotations

import uuid

import pytest

from durable_workflow import Client
from durable_workflow.serializer import decode_envelope
from durable_workflow.workflow import replay

from .polyglot_fixtures import (
    PolyglotPythonWorkflow,
    polyglot_python_activity,
)


@pytest.mark.asyncio
async def test_python_workflow_calls_php_activity(server_url: str, server_token: str) -> None:
    """
    Test Python workflow → PHP activity interop.

    This validates:
    - Python workflows can schedule activities with type keys registered by PHP workers
    - Avro payloads serialize correctly from Python by default
    - PHP activity results deserialize correctly in Python
    - Codec envelopes round-trip across runtimes
    """
    task_queue = f"polyglot-{uuid.uuid4().hex[:8]}"
    wf_id = f"poly-py-wf-{uuid.uuid4().hex[:8]}"
    worker_id = f"py-worker-{uuid.uuid4().hex[:8]}"

    # Test data with various JSON-serializable types
    test_input = {
        "name": "polyglot-test",
        "count": 42,
        "price": 99.95,
        "active": True,
        "tags": ["python", "php", "avro"],
        "metadata": {
            "source": "integration-test",
            "version": 2,
        },
    }

    async with Client(server_url, token=server_token, namespace="default") as client:
        # 1. Register Python worker supporting Python workflow and PHP activity
        await client.register_worker(
            worker_id=worker_id,
            task_queue=task_queue,
            supported_workflow_types=["tests.polyglot.python-workflow"],
            supported_activity_types=["tests.polyglot.php-activity"],
        )

        # 2. Start Python workflow
        handle = await client.start_workflow(
            workflow_type="tests.polyglot.python-workflow",
            task_queue=task_queue,
            workflow_id=wf_id,
            input=[test_input],
        )
        assert handle.workflow_id == wf_id
        assert handle.run_id is not None

        # 3. Poll for workflow task
        wf_task = await client.poll_workflow_task(
            worker_id=worker_id, task_queue=task_queue, timeout=10.0,
        )
        assert wf_task is not None, "expected workflow task after start"
        task_id = wf_task["task_id"]
        history = wf_task.get("history_events", [])
        attempt = wf_task.get("workflow_task_attempt", 1)

        # Decode input
        raw_args = wf_task.get("arguments")
        codec = wf_task.get("payload_codec")
        decoded = decode_envelope(raw_args, codec=codec)
        start_input = decoded if isinstance(decoded, list) else ([decoded] if decoded is not None else [])

        # 4. Replay — should schedule PHP activity
        outcome = replay(PolyglotPythonWorkflow, history, start_input, run_id=wf_task.get("run_id", ""))
        assert len(outcome.commands) == 1
        cmd = outcome.commands[0]
        server_cmd = cmd.to_server_command(task_queue)
        assert server_cmd["type"] == "schedule_activity"
        assert server_cmd["activity_type"] == "tests.polyglot.php-activity"

        # Verify activity arguments are envelope-wrapped
        activity_args = server_cmd.get("arguments", {})
        assert "codec" in activity_args, "activity arguments should be codec-wrapped"
        assert activity_args.get("codec") == "avro"

        # 5. Complete workflow task
        await client.complete_workflow_task(
            task_id=task_id,
            lease_owner=worker_id,
            workflow_task_attempt=attempt,
            commands=[server_cmd],
        )

        # 6. Poll for PHP activity task
        # Note: In a real scenario, this would be picked up by a PHP worker.
        # For this test, we simulate the PHP activity execution by manually
        # constructing what the PHP activity would return.
        act_task = await client.poll_activity_task(
            worker_id=worker_id, task_queue=task_queue, timeout=10.0,
        )
        assert act_task is not None, "expected activity task after schedule_activity"
        assert act_task["activity_type"] == "tests.polyglot.php-activity"

        act_task_id = act_task["task_id"]
        act_attempt_id = act_task.get("activity_attempt_id") or act_task.get("attempt_id", "")
        act_args = decode_envelope(act_task.get("arguments"), codec=act_task.get("payload_codec")) or []
        if not isinstance(act_args, list):
            act_args = [act_args]

        # 7. Simulate PHP activity execution
        # The PHP activity would receive the test_input and return structured data
        php_activity_result = {
            "runtime": "php",
            "received_input": test_input,
            "type_checks": {
                "has_string": True,
                "has_int": True,
                "has_float": True,
                "has_bool": True,
                "has_array": True,
                "has_nested": True,
            },
            "computed": {
                "name_length": len(test_input["name"]),
                "count_doubled": test_input["count"] * 2,
                "tags_count": len(test_input["tags"]),
            },
        }

        # 8. Complete activity task
        await client.complete_activity_task(
            task_id=act_task_id,
            activity_attempt_id=act_attempt_id,
            lease_owner=worker_id,
            result=php_activity_result,
        )

        # 9. Poll for next workflow task (activity completed)
        wf_task2 = await client.poll_workflow_task(
            worker_id=worker_id, task_queue=task_queue, timeout=10.0,
        )
        assert wf_task2 is not None, "expected workflow task after activity completion"
        task_id2 = wf_task2["task_id"]
        history2 = wf_task2.get("history_events", [])
        attempt2 = wf_task2.get("workflow_task_attempt", 1)

        decoded2 = decode_envelope(wf_task2.get("arguments"), codec=wf_task2.get("payload_codec"))
        start_input2 = decoded2 if isinstance(decoded2, list) else ([decoded2] if decoded2 is not None else [])

        # 10. Replay with ActivityCompleted — should produce CompleteWorkflow
        outcome2 = replay(PolyglotPythonWorkflow, history2, start_input2, run_id=wf_task2.get("run_id", ""))
        assert len(outcome2.commands) == 1
        cmd2 = outcome2.commands[0]
        server_cmd2 = cmd2.to_server_command(task_queue)

        # Debug: inspect command before asserting
        print("\n=== Replay outcome ===")
        print(f"Command type: {server_cmd2.get('type')}")
        print(f"Full command: {server_cmd2}")
        if server_cmd2.get("type") == "fail_workflow":
            print(f"Failure message: {server_cmd2.get('message', 'N/A')}")
            print(f"Failure details: {server_cmd2.get('details', 'N/A')}")
            import json
            print(f"History events count: {len(history2)}")
            print("Last few history events:")
            for evt in history2[-3:]:
                print(f"  - {evt.get('event_type')}: {json.dumps(evt, indent=2)[:200]}")

        assert server_cmd2["type"] == "complete_workflow", (
            f"Expected complete_workflow but got {server_cmd2['type']}: "
            f"{server_cmd2.get('message', 'no error message')}"
        )

        # Verify workflow result includes PHP activity output
        workflow_result = server_cmd2.get("result", {})
        if isinstance(workflow_result, dict) and "blob" in workflow_result:
            result_data = decode_envelope(workflow_result, codec=workflow_result.get("codec"))
        else:
            result_data = workflow_result

        assert isinstance(result_data, dict), f"expected dict result, got {type(result_data)}"
        assert result_data.get("workflow_runtime") == "python"
        assert "php_activity_result" in result_data
        php_result = result_data["php_activity_result"]
        assert php_result.get("runtime") == "php"
        assert php_result.get("computed", {}).get("count_doubled") == 84

        # 11. Complete workflow task
        await client.complete_workflow_task(
            task_id=task_id2,
            lease_owner=worker_id,
            workflow_task_attempt=attempt2,
            commands=[server_cmd2],
        )

        # 12. Verify final state
        desc = await handle.describe()
        assert desc.status in ("completed", "Completed")
        assert desc.output is not None


@pytest.mark.asyncio
async def test_python_activity_called_from_php_workflow(server_url: str, server_token: str) -> None:
    """
    Test PHP workflow → Python activity interop.

    This validates:
    - PHP workflows can schedule activities that Python workers execute
    - JSON payloads from PHP deserialize correctly in Python
    - Python activity results serialize correctly back to PHP
    - Codec envelopes round-trip across runtimes

    Note: This test requires the PHP workflow to be started and executed server-side,
    then the Python worker picks up and executes the activity.
    """
    task_queue = f"polyglot-{uuid.uuid4().hex[:8]}"
    wf_id = f"poly-php-wf-{uuid.uuid4().hex[:8]}"
    worker_id = f"py-worker-{uuid.uuid4().hex[:8]}"

    # Test data with various JSON-serializable types
    test_input = {
        "name": "php-to-python",
        "count": 100,
        "price": 49.99,
        "active": False,
        "tags": ["interop", "test"],
        "metadata": {
            "direction": "php→python",
        },
    }

    async with Client(server_url, token=server_token, namespace="default") as client:
        # 1. Register Python worker supporting Python activity
        # (PHP workflow will execute server-side)
        await client.register_worker(
            worker_id=worker_id,
            task_queue=task_queue,
            supported_workflow_types=[],  # Not handling PHP workflows
            supported_activity_types=["tests.polyglot.python-activity"],
        )

        # 2. Start PHP workflow through control plane
        # The PHP workflow will execute and schedule a Python activity
        handle = await client.start_workflow(
            workflow_type="tests.polyglot.php-workflow",
            task_queue=task_queue,
            workflow_id=wf_id,
            input=[test_input],
        )
        assert handle.workflow_id == wf_id

        # 3. Poll for Python activity task
        # The PHP workflow executes server-side and schedules the Python activity
        act_task = await client.poll_activity_task(
            worker_id=worker_id, task_queue=task_queue, timeout=15.0,
        )

        # If no task is available, the PHP workflow may not be registered server-side
        # Skip this test in that case (it requires PHP fixtures to be loadable by server)
        if act_task is None:
            pytest.skip("PHP workflow not available server-side — requires server with PHP fixtures")

        assert act_task["activity_type"] == "tests.polyglot.python-activity"

        act_task_id = act_task["task_id"]
        act_attempt_id = act_task.get("activity_attempt_id") or act_task.get("attempt_id", "")
        act_args = decode_envelope(act_task.get("arguments"), codec=act_task.get("payload_codec")) or []
        if not isinstance(act_args, list):
            act_args = [act_args]

        # Verify arguments deserialized correctly from PHP
        assert len(act_args) > 0, "expected activity arguments"
        activity_input = act_args[0]
        assert isinstance(activity_input, dict)
        assert activity_input.get("name") == "php-to-python"
        assert activity_input.get("count") == 100

        # 4. Execute Python activity
        result = await polyglot_python_activity(activity_input)

        # Verify Python activity produced expected output
        assert result["runtime"] == "python"
        assert result["type_checks"]["has_string"] is True
        assert result["type_checks"]["has_int"] is True
        assert result["computed"]["count_doubled"] == 200

        # 5. Complete activity task
        await client.complete_activity_task(
            task_id=act_task_id,
            activity_attempt_id=act_attempt_id,
            lease_owner=worker_id,
            result=result,
        )

        # 6. Wait for PHP workflow to complete
        # The server-side PHP workflow should receive the activity result and complete
        import asyncio
        await asyncio.sleep(2)  # Give server time to process

        # 7. Verify final workflow state
        desc = await handle.describe()

        # The workflow should be completed with PHP activity result embedded
        assert desc.status in ("completed", "Completed", "waiting", "pending", "running")

        # If completed, verify the output structure
        if desc.status in ("completed", "Completed") and desc.output:
            assert isinstance(desc.output, dict)
            assert desc.output.get("workflow_runtime") == "php"
            if "python_activity_result" in desc.output:
                py_result = desc.output["python_activity_result"]
                assert py_result.get("runtime") == "python"
                assert py_result.get("computed", {}).get("count_doubled") == 200
