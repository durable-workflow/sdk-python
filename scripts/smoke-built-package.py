#!/usr/bin/env python3
"""Smoke test built distributions as installed packages.

The README quickstart is the first path many users copy. In-tree tests can
accidentally import from ``src/``; this script installs each built artifact into
a temporary virtualenv, runs from outside the checkout, and verifies the README
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


def find_sdist(dist: Path) -> Path:
    sdists = sorted(dist.glob("*.tar.gz"))
    if len(sdists) != 1:
        raise SystemExit(f"Expected exactly one source distribution in {dist}, found {len(sdists)}.")
    return sdists[0]


def run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, env=env, check=True)


def smoke_distribution(artifact: Path, *, repo_root: Path, readme: Path) -> None:
    smoke_code = r'''
import importlib
import importlib.metadata
import json
import os
import re
import subprocess
import sys
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
    "durable_workflow.python_conformance",
    "durable_workflow.retry_policy",
    "durable_workflow.serializer",
    "durable_workflow.sync",
    "durable_workflow.testing",
    "durable_workflow.worker",
    "durable_workflow.workflow",
    "durable_workflow.workflow_updates_conformance",
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

evidence_path = Path.cwd() / "python-sdk-workflow-updates-evidence.json"
workflow_updates_script = Path(sys.executable).with_name("durable-workflow-workflow-updates-conformance")
if not workflow_updates_script.is_file():
    workflow_updates_script = workflow_updates_script.with_suffix(".exe")
if not workflow_updates_script.is_file():
    raise AssertionError("installed package is missing durable-workflow-workflow-updates-conformance script")
subprocess.run(
    [
        str(workflow_updates_script),
        "--expected-version",
        metadata_version,
        "--output",
        str(evidence_path),
    ],
    check=True,
)
evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
scenario = evidence["scenario_results"]["python_client_worker_update_surface"]
outputs = scenario["observed_outputs"]
if scenario["status"] != "pass":
    raise AssertionError(f"workflow update conformance shard did not pass: {scenario!r}")
if scenario["local_product_source_checkouts_used"]:
    raise AssertionError("workflow update conformance shard reported local source checkout usage")
if not scenario["published_artifact_cell_executed"]:
    raise AssertionError("workflow update conformance shard did not report published artifact execution")
if outputs["sdk_python_artifact_version"] != metadata_version:
    raise AssertionError("workflow update shard did not record the installed package version")
required_cells = {
    "accepted_update_control_plane_and_history",
    "completed_update_result_round_trip",
    "failed_update_outcome",
    "unknown_update_refusal",
    "invalid_input_refusal",
    "duplicate_request_idempotency",
    "terminal_workflow_update_behavior",
    "payload_envelope_round_trip",
}
if not required_cells.issubset(set(outputs["covered_cells"])):
    raise AssertionError(f"workflow update shard missing covered cells: {outputs['covered_cells']!r}")
if not outputs["python_worker_update_handler"]["observations"]:
    raise AssertionError("workflow update shard did not record worker handler observations")
if not outputs["python_client_update_request"]["requests"]:
    raise AssertionError("workflow update shard did not record client update requests")
print(f"README quickstart smoke passed using {package_file}")
    '''

    with tempfile.TemporaryDirectory(prefix="dw-sdk-package-smoke-") as tmp:
        tmp_path = Path(tmp)
        venv = tmp_path / ".venv"
        run([sys.executable, "-m", "venv", str(venv)])
        python = venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        run([str(python), "-m", "pip", "install", "--upgrade", "pip"], cwd=tmp_path)
        run([str(python), "-m", "pip", "install", str(artifact)], cwd=tmp_path)
        env = {
            **os.environ,
            "DW_REPO_ROOT": str(repo_root),
            "DW_README": str(readme),
            "PYTHONPATH": "",
        }
        run([str(python), "-c", smoke_code], cwd=tmp_path, env=env)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--dist",
        type=Path,
        default=Path("dist"),
        help="Directory containing the built wheel and source distribution. Defaults to ./dist.",
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
    dist = (repo_root / args.dist).resolve() if not args.dist.is_absolute() else args.dist.resolve()
    for artifact in (find_wheel(dist), find_sdist(dist)):
        smoke_distribution(artifact, repo_root=repo_root, readme=readme)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
