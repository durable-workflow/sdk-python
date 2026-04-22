from __future__ import annotations

import re
from pathlib import Path
from types import FunctionType

from durable_workflow import serializer
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, replay


def _readme_quickstart_code() -> str:
    readme = Path(__file__).resolve().parents[1] / "README.md"
    text = readme.read_text(encoding="utf-8")
    match = re.search(r"## Quickstart\s+```python\n(?P<code>.*?)\n```", text, flags=re.DOTALL)
    assert match is not None, "README Quickstart must contain a Python code block"

    return match.group("code")


def _completed(result: object) -> dict[str, object]:
    return {
        "event_type": "ActivityCompleted",
        "payload": {
            "result": serializer.encode(result, codec="json"),
            "payload_codec": "json",
        },
    }


def test_readme_quickstart_is_a_runnable_script() -> None:
    namespace = {"__name__": "readme_quickstart_test"}
    exec(_readme_quickstart_code(), namespace)

    assert isinstance(namespace["main"], FunctionType)
    assert namespace["asyncio"].__name__ == "asyncio"


def test_readme_quickstart_replays_documented_greeter() -> None:
    namespace = {"__name__": "readme_quickstart_test"}
    exec(_readme_quickstart_code(), namespace)

    workflow_class = namespace["GreeterWorkflow"]
    greet = namespace["greet"]

    first = replay(workflow_class, [], ["world"]).commands[0]
    assert isinstance(first, ScheduleActivity)
    assert first.activity_type == "greet"
    assert first.arguments == ["world"]

    final = replay(workflow_class, [_completed(greet("world"))], ["world"]).commands[0]
    assert isinstance(final, CompleteWorkflow)
    assert final.result == "hello, world"
