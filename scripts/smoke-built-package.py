#!/usr/bin/env python3
"""Smoke test the built wheel as an installed distribution.

The README quickstart is the first path many users copy. In-tree tests can
accidentally import from ``src/``; this script installs the built wheel into a
temporary virtualenv, runs from outside the checkout, and verifies the README
workflow snippet against that installed package.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path


def find_wheel(dist: Path) -> Path:
    wheels = sorted(dist.glob("*.whl"))
    if len(wheels) != 1:
        raise SystemExit(f"Expected exactly one wheel in {dist}, found {len(wheels)}.")
    return wheels[0]


def run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--dist",
        type=Path,
        default=Path("dist"),
        help="Directory containing the built wheel. Defaults to ./dist.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="Repository root. Defaults to the parent of this script directory.",
    )
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    readme = repo_root / "README.md"
    wheel = find_wheel((repo_root / args.dist).resolve() if not args.dist.is_absolute() else args.dist.resolve())

    smoke_code = r'''
import importlib
import importlib.metadata
import os
import re
from pathlib import Path
from types import FunctionType

import durable_workflow
from durable_workflow import serializer
from durable_workflow.workflow import CompleteWorkflow, ScheduleActivity, replay


def completed(result):
    return {
        "event_type": "ActivityCompleted",
        "payload": {
            "result": serializer.encode(result, codec="json"),
            "payload_codec": "json",
        },
    }


repo_root = Path(os.environ["DW_REPO_ROOT"]).resolve()
package_file = Path(durable_workflow.__file__).resolve()
if package_file.is_relative_to(repo_root):
    raise AssertionError(f"durable_workflow imported from source checkout: {package_file}")

metadata_version = importlib.metadata.version("durable-workflow")
if durable_workflow.__version__ != metadata_version:
    raise AssertionError(
        f"durable_workflow.__version__={durable_workflow.__version__!r} "
        f"does not match installed metadata {metadata_version!r}"
    )

typed_marker = package_file.parent / "py.typed"
if not typed_marker.is_file():
    raise AssertionError(f"installed package is missing PEP 561 marker: {typed_marker}")

missing_exports = [name for name in durable_workflow.__all__ if not hasattr(durable_workflow, name)]
if missing_exports:
    raise AssertionError(f"durable_workflow.__all__ names missing exports: {missing_exports}")

reference_modules = [
    "durable_workflow.activity",
    "durable_workflow.auth_composition",
    "durable_workflow.client",
    "durable_workflow.errors",
    "durable_workflow.external_storage",
    "durable_workflow.external_task_input",
    "durable_workflow.external_task_result",
    "durable_workflow.invocable",
    "durable_workflow.metrics",
    "durable_workflow.retry_policy",
    "durable_workflow.serializer",
    "durable_workflow.sync",
    "durable_workflow.testing",
    "durable_workflow.worker",
    "durable_workflow.workflow",
]
for module_name in reference_modules:
    module = importlib.import_module(module_name)
    module_file = Path(module.__file__).resolve()
    if module_file.is_relative_to(repo_root):
        raise AssertionError(f"{module_name} imported from source checkout: {module_file}")

readme = Path(os.environ["DW_README"])
text = readme.read_text(encoding="utf-8")
match = re.search(r"## Quickstart\s+```python\n(?P<code>.*?)\n```", text, flags=re.DOTALL)
if match is None:
    raise AssertionError("README Quickstart must contain a Python code block")

namespace = {"__name__": "readme_quickstart_package_smoke"}
exec(match.group("code"), namespace)

assert isinstance(namespace["main"], FunctionType)
workflow_class = namespace["GreeterWorkflow"]
greet = namespace["greet"]

first = replay(workflow_class, [], ["world"]).commands[0]
assert isinstance(first, ScheduleActivity)
assert first.activity_type == "greet"
assert first.arguments == ["world"]

final = replay(workflow_class, [completed(greet("world"))], ["world"]).commands[0]
assert isinstance(final, CompleteWorkflow)
assert final.result == "hello, world"
print(f"README quickstart smoke passed using {package_file}")
    '''

    with tempfile.TemporaryDirectory(prefix="dw-sdk-wheel-smoke-") as tmp:
        tmp_path = Path(tmp)
        venv = tmp_path / ".venv"
        run([sys.executable, "-m", "venv", str(venv)])
        python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        run([str(python), "-m", "pip", "install", "--upgrade", "pip"], cwd=tmp_path)
        run([str(python), "-m", "pip", "install", str(wheel)], cwd=tmp_path)
        env = {
            **os.environ,
            "DW_REPO_ROOT": str(repo_root),
            "DW_README": str(readme),
            "PYTHONPATH": "",
        }
        run([str(python), "-c", smoke_code], cwd=tmp_path, env=env)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
