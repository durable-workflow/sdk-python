from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.ci.classify_pr_qualification import (
    COMPLETE,
    DOCS_ONLY_FILES,
    DOCS_ONLY_PREFIXES,
    FOCUSED_DOCUMENTATION,
    changed_files_between,
    classify_changed_files,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"
DOCS_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "docs-pr.yml"
DOCS_VISUAL_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "docs-visual.yml"
PUBLIC_BOUNDARY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "public-boundary.yml"
CLASSIFIER = REPO_ROOT / "scripts" / "ci" / "classify_pr_qualification.py"

RUNTIME_JOBS = {
    "regression-corpus",
    "lint",
    "test",
    "package",
    "cli-parity",
    "integration",
}


def load_workflow(path: Path) -> dict[str, Any]:
    document = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(document, dict)
    return document


@pytest.mark.parametrize(
    "path",
    [
        "docs/index.md",
        "docs/assets/diagram.svg",
        "docs/javascripts/navigation-accessibility.js",
        "docs/javascripts/search-accessibility.js",
        "docs/stylesheets/layout.css",
        "overrides/main.html",
        "overrides/partials/source.html",
        "mkdocs.yml",
        "scripts/ci/classify_docs_visual_changes.py",
        "scripts/ci/test-classify-docs-visual-changes.py",
        "scripts/check_api_reference_install.py",
        "scripts/check-docs-analytics.py",
        "scripts/check-docs-layout.py",
        "scripts/mkdocs_hooks.py",
    ],
)
def test_documentation_surfaces_receive_focused_qualification(path: str) -> None:
    result = classify_changed_files([path])

    assert result.classification == FOCUSED_DOCUMENTATION


@pytest.mark.parametrize(
    "path",
    [
        "src/durable_workflow/client.py",
        "pyproject.toml",
        "uv.lock",
        "scripts/api_reference_release.py",
        "scripts/ci/classify_pr_qualification.py",
        "scripts/ci/check-docs-release-audit.sh",
        "schema/history.avsc",
        ".github/workflows/docs-pr.yml",
        ".github/workflows/ci.yml",
        "tests/test_client.py",
        "README.md",
        "unclassified/new-surface.txt",
    ],
)
def test_runtime_sensitive_policy_and_unclassified_paths_receive_complete_qualification(path: str) -> None:
    result = classify_changed_files([path])

    assert result.classification == COMPLETE


def test_mixed_changes_receive_complete_qualification() -> None:
    result = classify_changed_files(["docs/index.md", "src/durable_workflow/client.py"])

    assert result.classification == COMPLETE


@pytest.mark.parametrize("paths", [[], ["../docs/index.md"], ["docs//index.md"], ["docs/index.md\npyproject.toml"]])
def test_missing_or_malformed_path_identity_fails_closed(paths: Sequence[str]) -> None:
    result = classify_changed_files(paths)

    assert result.classification == COMPLETE
    assert result.reason == "changed-path-identity-unavailable"


def test_unavailable_git_identity_fails_closed_with_a_successful_classifier_step(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(CLASSIFIER),
            "--root",
            str(tmp_path),
            "--event-name",
            "pull_request",
            "--base-ref",
            "0" * 40,
            "--head-ref",
            "1" * 40,
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["classification"] == COMPLETE
    assert payload["reason"] == "changed-path-identity-unavailable"


def test_rename_from_runtime_into_docs_remains_complete(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Qualification Test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=tmp_path, check=True)
    source = tmp_path / "src" / "runtime.py"
    source.parent.mkdir()
    source.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "Add runtime"], cwd=tmp_path, check=True)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, check=True, capture_output=True, text=True
    ).stdout.strip()
    destination = tmp_path / "docs" / "runtime.md"
    destination.parent.mkdir()
    subprocess.run(["git", "mv", str(source), str(destination)], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-q", "-m", "Move runtime"], cwd=tmp_path, check=True)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, check=True, capture_output=True, text=True
    ).stdout.strip()

    changed_files = changed_files_between(tmp_path, base, head)

    assert set(changed_files) == {"src/runtime.py", "docs/runtime.md"}
    assert classify_changed_files(changed_files).classification == COMPLETE


def test_ci_schedules_runtime_matrix_only_for_complete_qualification() -> None:
    workflow = load_workflow(CI_WORKFLOW)
    jobs = workflow["jobs"]
    classifier = jobs["qualification-class"]
    checkout = classifier["steps"][0]
    assert checkout["with"] == {"fetch-depth": "0", "persist-credentials": "false"}
    assert classifier["outputs"] == {
        "classification": "${{ steps.classify.outputs.classification }}",
        "reason": "${{ steps.classify.outputs.reason }}",
        "changed_count": "${{ steps.classify.outputs.changed_count }}",
    }
    classifier_commands = "\n".join(step.get("run", "") for step in classifier["steps"])
    assert "classify_pr_qualification.py" in classifier_commands
    assert '--base-ref "$SOURCE_BASE_SHA"' in classifier_commands
    assert '--head-ref "$SOURCE_HEAD_SHA"' in classifier_commands

    for job_name in RUNTIME_JOBS:
        job = jobs[job_name]
        assert job["if"] == "${{ needs.qualification-class.outputs.classification == 'complete' }}"
        needs = job["needs"]
        if isinstance(needs, str):
            needs = [needs]
        assert "qualification-class" in needs

    report = jobs["qualification-class-report"]
    assert report["name"] == "Qualification class — ${{ needs.qualification-class.outputs.classification }}"
    assert report["needs"] == "qualification-class"


def test_terminal_status_accepts_only_the_selected_qualification_shape() -> None:
    workflow = load_workflow(CI_WORKFLOW)
    qualification = workflow["jobs"]["target-branch-qualification"]
    assert qualification["name"] == "Target branch qualification"
    assert set(qualification["needs"]) == RUNTIME_JOBS | {
        "qualification-class",
        "qualification-class-report",
    }
    commands = "\n".join(step.get("run", "") for step in qualification["steps"])
    assert 'if [ "$QUALIFICATION_CLASS" = focused-documentation ]' in commands
    assert commands.count("= skipped") == len(RUNTIME_JOBS)
    assert 'elif [ "$QUALIFICATION_CLASS" = complete ]' in commands
    assert commands.count("= success") == len(RUNTIME_JOBS) + 2
    assert "else\n  exit 1\nfi" in commands


def test_focused_path_retains_docs_layout_visual_and_public_boundary_contracts() -> None:
    docs = load_workflow(DOCS_WORKFLOW)
    docs_paths = set(docs["on"]["pull_request"]["paths"])
    assert docs_paths >= DOCS_ONLY_FILES
    assert {f"{prefix}**" for prefix in DOCS_ONLY_PREFIXES} <= docs_paths

    validate = docs["jobs"]["validate"]
    validate_commands = "\n".join(step.get("run", "") for step in validate["steps"])
    assert "mkdocs build --strict" in validate_commands
    assert "python scripts/check-docs-layout.py site" in validate_commands
    assert validate["name"] == "Strict documentation build and rendered layout"

    visual = load_workflow(DOCS_VISUAL_WORKFLOW)
    visual_commands = "\n".join(step.get("run", "") for step in visual["jobs"]["visual-evidence"]["steps"])
    assert "1440x900 768x1024 390x844 640x360" in visual_commands
    assert "capture navigation-open" in visual_commands
    assert "capture search-open" in visual_commands
    assert "capture search-populated" in visual_commands

    boundary = load_workflow(PUBLIC_BOUNDARY_WORKFLOW)
    assert "pull_request" in boundary["on"]
    assert boundary["permissions"] == {"contents": "read"}
    boundary_commands = "\n".join(step.get("run", "") for step in boundary["jobs"]["scan"]["steps"])
    assert "scripts/check-public-boundary.sh" in boundary_commands
