"""Execute the portal's release selector without GitHub or deployment access."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.skipif(sys.version_info < (3, 11), reason="The portal selector uses Python 3.12 tomllib")
WORKFLOW = Path(__file__).parents[1] / ".github" / "workflows" / "docs.yml"


def test_reviewed_python_trust_is_ready_before_publication_authority_reads() -> None:
    steps = yaml.safe_load(WORKFLOW.read_text())["jobs"]["build"]["steps"]
    setup = next(i for i, step in enumerate(steps) if step.get("uses", "").startswith("actions/setup-python@"))
    trust = next(i for i, step in enumerate(steps) if step.get("uses", "").startswith(
        "durable-workflow/.github/.github/actions/bootstrap-patched-pip@"))
    selector = next(i for i, step in enumerate(steps) if step.get("id") == "release-channel")
    assert setup < trust < selector
    assert steps[trust]["with"]["clients"] == "pip,python"


def select_channel(
    root: Path, *, version: str = "2.14.1", tag: str = "", event: str = "workflow_dispatch",
    ref: str = "refs/heads/main", published: bool = True, lookup_exit: int = 0,
    matching_source: bool = True, registry: dict[str, Any] | None = None,
) -> tuple[subprocess.CompletedProcess[str], dict[str, str], str]:
    steps = yaml.safe_load(WORKFLOW.read_text())["jobs"]["build"]["steps"]
    validation = next(step["run"] for step in steps if step.get("name") == "Validate the selected stable tag")
    selector = next(step["run"] for step in steps if step.get("id") == "release-channel")
    tools = root / "tools"
    tools.mkdir()
    (tools / "python").symlink_to(sys.executable)
    github = tools / "gh"
    github.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> "$DOCS_TEST_CALLS"\n'
        'printf "%s\\n" "$DOCS_TEST_PUBLISHED"\nexit "$DOCS_TEST_EXIT"\n'
    )
    github.chmod(0o700)
    if registry is None:
        registry = {"info": {"name": "durable-workflow", "version": "2.14.1"}, "urls": [
            {"filename": "durable_workflow-2.14.1-py3-none-any.whl", "packagetype": "bdist_wheel",
             "yanked": False, "digests": {"sha256": "1" * 64}},
            {"filename": "durable_workflow-2.14.1.tar.gz", "packagetype": "sdist",
             "yanked": False, "digests": {"sha256": "2" * 64}},
        ]}
    (tools / "sitecustomize.py").write_text(
        'import io, os, urllib.request\n'
        'def offline_metadata(url, **kwargs):\n'
        '    if url != "https://pypi.org/pypi/durable-workflow/2.14.1/json":\n'
        '        raise RuntimeError("unexpected metadata request")\n'
        '    return io.BytesIO(os.environ["DOCS_TEST_REGISTRY"].encode())\n'
        'urllib.request.urlopen = offline_metadata\n'
    )
    output, calls = root / "output", root / "calls"
    environment = {
        "PATH": str(tools) + os.pathsep + os.defpath,
        "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
        "STABLE_TAG": tag, "GITHUB_EVENT_NAME": event, "GITHUB_REF": ref,
        "GITHUB_REPOSITORY": "durable-workflow/sdk-python", "GITHUB_OUTPUT": str(output),
        "DOCS_TEST_CALLS": str(calls), "DOCS_TEST_PUBLISHED": str(published).lower(),
        "DOCS_TEST_EXIT": str(lookup_exit),
        "DOCS_TEST_REGISTRY": json.dumps(registry), "PYTHONPATH": str(tools),
    }

    def git(*arguments: str) -> None:
        subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", "-c", "user.name=Portal test",
             "-c", "user.email=portal@example.test", *arguments],
            cwd=root, env=environment, check=True, capture_output=True, text=True,
        )

    (root / "pyproject.toml").write_text(f'[project]\nversion = "{version}"\n')
    git("init")
    git("add", "pyproject.toml")
    git("commit", "-m", "Fixture source")
    git("tag", "2.14.1")
    if not matching_source:
        git("commit", "--allow-empty", "-m", "Unpublished successor")
    script = (validation + "\n" if tag else "") + selector
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        cwd=root, env=environment, capture_output=True, text=True,
    )
    values = dict(line.split("=", 1) for line in output.read_text().splitlines()) if output.exists() else {}
    return result, values, calls.read_text() if calls.exists() else ""


@pytest.mark.parametrize("version", ["2.15.0", "2.14.1", "2.16.0.dev1"])
def test_main_source_is_preview_even_with_a_numeric_version(tmp_path: Path, version: str) -> None:
    result, values, calls = select_channel(tmp_path, version=version, event="push")
    assert result.returncode == 0, result.stderr
    assert values == {"stable": "false", "version": version}
    assert calls == ""


def test_explicit_published_tag_with_exact_source_can_select_stable(tmp_path: Path) -> None:
    result, values, calls = select_channel(tmp_path, tag="2.14.1")
    assert result.returncode == 0, result.stderr
    assert values == {"stable": "true", "version": "2.14.1"}
    assert "release view 2.14.1 --repo durable-workflow/sdk-python" in calls


@pytest.mark.parametrize("change", [
    {"published": False}, {"lookup_exit": 1}, {"version": "2.15.0"}, {"matching_source": False},
    {"tag": "2.15.0"}, {"tag": "2.14.1-rc.1"}, {"tag": "2.14.1;invalid"},
    {"event": "push"}, {"ref": "refs/heads/feature"},
])
def test_unqualified_selection_cannot_emit_stable(tmp_path: Path, change: dict[str, object]) -> None:
    result, values, _ = select_channel(tmp_path, **{"tag": "2.14.1", **change})  # type: ignore[arg-type]
    assert result.returncode != 0
    assert values.get("stable") != "true"


@pytest.mark.parametrize("registry", [
    {}, {"info": {"name": "durable-workflow", "version": "2.15.0"}},
    {"info": {"name": "other", "version": "2.14.1"}},
    {"info": {"name": "durable-workflow", "version": "2.14.1"}, "urls": []},
    {"info": {"name": "durable-workflow", "version": "2.14.1"}, "urls": [
        {"filename": "durable_workflow-2.14.1.tar.gz", "packagetype": "sdist", "yanked": True}]},
    {"info": {"name": "durable-workflow", "version": "2.14.1"}, "urls": [
        {"filename": "durable_workflow-2.14.1.tar.gz", "packagetype": "sdist", "yanked": False}]},
    {"info": {"name": "durable-workflow", "version": "2.14.1"}, "urls": [
        {"filename": "other.tar.gz", "packagetype": "sdist", "yanked": False,
         "digests": {"sha256": "2" * 64}}]},
])
def test_registry_identity_or_missing_artifacts_refuse_stable(tmp_path: Path, registry: dict[str, Any]) -> None:
    result, values, _ = select_channel(tmp_path, tag="2.14.1", registry=registry)
    assert result.returncode != 0
    assert values.get("stable") != "true"
