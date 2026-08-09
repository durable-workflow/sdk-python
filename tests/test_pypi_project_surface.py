from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.check_pypi_project_surface import (
    ProjectSurfaceError,
    main,
    supported_prerelease_requirement,
    verify_exact_version_json,
    verify_pip_report,
    verify_project_json,
)
from scripts.check_release_metadata import SourceMetadata

REPO_ROOT = Path(__file__).resolve().parents[1]


def source_metadata() -> SourceMetadata:
    return SourceMetadata(
        commit="a" * 40,
        name="durable-workflow",
        version="2.0.0-rc.24",
        registry_version="2.0.0rc24",
        summary="Release candidate Python SDK for the Durable Workflow 2.0 train",
        classifiers=("Programming Language :: Python :: 3",),
        readme=(
            "# Durable Workflow\n\n"
            "Current release-candidate metadata and the pre-accept update-validator boundary.\n\n"
            "## Install\n\n"
            "```bash\n"
            "pip install 'durable-workflow~=2.0.0rc0'\n"
            "```\n"
        ),
    )


def release_file(version: str, *, yanked: bool) -> dict[str, object]:
    return {
        "filename": f"durable_workflow-{version}-py3-none-any.whl",
        "url": f"https://files.pythonhosted.org/{version}.whl",
        "yanked": yanked,
        "yanked_reason": "The supported line is the 2.0 release-candidate train" if yanked else None,
    }


def project_payload() -> dict[str, Any]:
    source = source_metadata()
    current = release_file(source.registry_version, yanked=False)
    return {
        "info": {
            "name": source.name,
            "version": source.registry_version,
            "summary": source.summary,
            "classifiers": list(source.classifiers),
            "description": source.readme,
            "description_content_type": "text/markdown",
        },
        "releases": {
            "0.4.105": [release_file("0.4.105", yanked=True)],
            "0.4.106": [release_file("0.4.106", yanked=True)],
            source.registry_version: [current],
        },
        "urls": [current],
    }


def exact_version_payload() -> dict[str, Any]:
    payload = project_payload()
    return {"info": payload["info"], "urls": payload["urls"]}


def pip_report(version: str) -> dict[str, object]:
    return {"install": [{"metadata": {"name": "durable_workflow", "version": version}}]}


def test_project_root_matches_current_prerelease_and_preserves_yanked_history() -> None:
    exact_urls = verify_exact_version_json(exact_version_payload(), source_metadata())

    assert verify_project_json(project_payload(), source_metadata(), exact_urls) == ("0.4.105", "0.4.106")


def test_exact_version_json_must_match_current_source_metadata() -> None:
    payload = exact_version_payload()
    payload["info"]["summary"] = "Obsolete beta metadata"

    with pytest.raises(ProjectSurfaceError, match="exact-version JSON field summary differs"):
        verify_exact_version_json(payload, source_metadata())


def test_project_root_files_must_match_exact_version_json() -> None:
    exact = exact_version_payload()
    exact["urls"] = [release_file("2.0.0rc24-repacked", yanked=False)]

    with pytest.raises(ProjectSurfaceError, match="differ from the exact-version JSON"):
        verify_project_json(
            project_payload(),
            source_metadata(),
            verify_exact_version_json(exact, source_metadata()),
        )


def test_obsolete_default_project_metadata_is_rejected() -> None:
    payload = project_payload()
    payload["info"]["version"] = "0.4.106"

    with pytest.raises(ProjectSurfaceError, match="field version differs"):
        verify_project_json(payload, source_metadata())


def test_current_version_with_obsolete_description_is_rejected() -> None:
    payload = project_payload()
    payload["info"]["description"] = "Beta SDK headed toward 1.0; Python query routing is unfinished."

    with pytest.raises(ProjectSurfaceError, match="field description differs"):
        verify_project_json(payload, source_metadata())


def test_root_selected_files_must_belong_to_the_current_prerelease() -> None:
    payload = project_payload()
    payload["urls"] = [release_file("0.4.106", yanked=False)]

    with pytest.raises(ProjectSurfaceError, match="selected-release files differ"):
        verify_project_json(payload, source_metadata())


def test_non_yanked_legacy_release_cannot_override_the_prerelease_surface() -> None:
    payload = project_payload()
    payload["releases"]["0.4.106"][0]["yanked"] = False

    with pytest.raises(ProjectSurfaceError, match="yank these historical stable releases.*0.4.106"):
        verify_project_json(payload, source_metadata())


def test_legacy_release_retirement_requires_a_public_reason() -> None:
    payload = project_payload()
    payload["releases"]["0.4.106"][0]["yanked_reason"] = None

    with pytest.raises(ProjectSurfaceError, match="add a PyPI yank reason for: 0.4.106"):
        verify_project_json(payload, source_metadata())


def test_retirement_must_retain_legacy_release_files() -> None:
    payload = project_payload()
    payload["releases"]["0.4.106"] = []

    with pytest.raises(ProjectSurfaceError, match="does not retain files for 0.4.106"):
        verify_project_json(payload, source_metadata())


def test_default_and_explicit_supported_installs_resolve_exact_current_rc() -> None:
    source = source_metadata()

    assert supported_prerelease_requirement(source) == "durable-workflow~=2.0.0rc0"
    verify_pip_report(pip_report(source.registry_version), source, source.registry_version)

    with pytest.raises(ProjectSurfaceError, match="pip selected 0.4.106"):
        verify_pip_report(pip_report("0.4.106"), source, source.registry_version)


def test_live_audit_uses_authoritative_json_without_requesting_project_html(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    requested_urls: list[str] = []
    requirements: list[tuple[str, bool]] = []

    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)

    def request_json(url: str) -> object:
        requested_urls.append(url)
        if url.endswith(f"/{source.registry_version}/json"):
            return exact_version_payload()
        return project_payload()

    def resolve(requirement: str, *, prerelease: bool) -> object:
        requirements.append((requirement, prerelease))
        version = "0.4.106" if requirement.endswith("==0.4.106") else source.registry_version
        return pip_report(version)

    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", request_json)
    monkeypatch.setattr("scripts.check_pypi_project_surface._pip_report", resolve)

    assert main(["--source-ref", "release-source"]) == 0
    assert requested_urls == [
        "https://pypi.org/pypi/durable-workflow/json",
        "https://pypi.org/pypi/durable-workflow/2.0.0rc24/json",
    ]
    assert requirements == [
        ("durable-workflow", False),
        ("durable-workflow~=2.0.0rc0", False),
        ("durable-workflow==0.4.106", False),
    ]


def test_successful_audit_writes_machine_readable_public_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr(
        "scripts.check_pypi_project_surface._request_json",
        lambda url: exact_version_payload() if url.endswith(f"/{source.registry_version}/json") else project_payload(),
    )
    monkeypatch.setattr(
        "scripts.check_pypi_project_surface._pip_report",
        lambda requirement, *, prerelease: pip_report(
            "0.4.106" if requirement.endswith("==0.4.106") else source.registry_version
        ),
    )
    evidence_path = tmp_path / "evidence.json"

    assert main(["--source-ref", "release-source", "--evidence", str(evidence_path)]) == 0

    evidence = yaml.safe_load(evidence_path.read_text(encoding="utf-8"))
    assert evidence == {
        "schema": "durable-workflow.python-pypi-project-surface.v1",
        "source_commit": source.commit,
        "package": source.name,
        "public_urls": {
            "project_json": "https://pypi.org/pypi/durable-workflow/json",
            "exact_version_json": "https://pypi.org/pypi/durable-workflow/2.0.0rc24/json",
        },
        "selected_version": source.registry_version,
        "documented_requirement": "durable-workflow~=2.0.0rc0",
        "documented_install_version": source.registry_version,
        "yanked_versions": ["0.4.105", "0.4.106"],
        "historical_exact_probe": "0.4.106",
    }


def test_source_only_qualification_does_not_require_a_published_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)

    def unexpected_public_request(*_: object, **__: object) -> object:
        raise AssertionError("source-only qualification must not query public package state")

    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", unexpected_public_request)
    monkeypatch.setattr("scripts.check_pypi_project_surface._pip_report", unexpected_public_request)

    assert main(["--source-ref", "release-source", "--source-only"]) == 0


def test_public_project_surface_qualification_is_recurring_and_release_blocking() -> None:
    workflow = yaml.load(
        (REPO_ROOT / ".github" / "workflows" / "pypi-project-surface.yml").read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )
    assert workflow["permissions"] == {"contents": "read"}
    assert set(workflow["on"]) == {"push", "schedule", "workflow_dispatch"}
    steps = workflow["jobs"]["audit"]["steps"]
    commands = "\n".join(step.get("run", "") for step in steps if isinstance(step, dict))
    push_commands = "\n".join(
        step.get("run", "")
        for step in steps
        if isinstance(step, dict) and step.get("if") == "github.event_name == 'push'"
    )
    public_commands = "\n".join(
        step.get("run", "")
        for step in steps
        if isinstance(step, dict) and step.get("if") == "github.event_name != 'push'"
    )
    assert "pytest tests/test_pypi_project_surface.py tests/test_release_metadata.py -q" in push_commands
    assert "python scripts/check_pypi_project_surface.py --source-ref HEAD --source-only" in push_commands
    assert "--source-only" not in public_commands
    assert "git tag -l '2.0.0-rc.*' --sort=-version:refname" in commands
    assert "python scripts/check_pypi_project_surface.py" in public_commands
    assert "--evidence pypi-project-surface-evidence.json" in public_commands
    assert any(
        step.get("uses") == "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
        and step.get("with", {}).get("path") == "pypi-project-surface-evidence.json"
        for step in steps
        if isinstance(step, dict)
    )

    publish = (REPO_ROOT / ".github" / "workflows" / "publish.yml").read_text(encoding="utf-8")
    exact_audit = publish.index("python scripts/check_release_metadata.py", publish.index("  publish:"))
    root_audit = publish.index("python scripts/check_pypi_project_surface.py", exact_audit)
    github_release = publish.index('gh release create "$RELEASE_TAG"', root_audit)
    assert exact_audit < root_audit < github_release
