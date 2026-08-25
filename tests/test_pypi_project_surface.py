from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.check_pypi_project_surface import (
    ProjectSurfaceError,
    main,
    release_channel,
    supported_prerelease_requirement,
    verify_exact_version_json,
    verify_pip_report,
    verify_stable_project_json,
)
from scripts.check_release_metadata import SourceMetadata

REPO_ROOT = Path(__file__).resolve().parents[1]


def source_metadata(*, stable: bool = False) -> SourceMetadata:
    version = "2.0.0" if stable else "2.0.0-rc.36"
    registry_version = "2.0.0" if stable else "2.0.0rc36"
    return SourceMetadata(
        commit="a" * 40,
        name="durable-workflow",
        version=version,
        registry_version=registry_version,
        summary="Python SDK for the Durable Workflow 2.0 train",
        classifiers=("Programming Language :: Python :: 3",),
        readme=(
            "# Durable Workflow\n\n"
            "Build replay-safe Python workflows.\n\n"
            "## Install\n\n"
            "```bash\n"
            "pip install 'durable-workflow~=2.0.0rc0'\n"
            "```\n"
        ),
    )


def release_file(version: str, *, yanked: bool = False) -> dict[str, object]:
    return {
        "filename": f"durable_workflow-{version}-py3-none-any.whl",
        "url": f"https://files.pythonhosted.org/{version}.whl",
        "yanked": yanked,
        "yanked_reason": "Withdrawn for an independent release defect" if yanked else None,
    }


def source_info(source: SourceMetadata) -> dict[str, object]:
    return {
        "name": source.name,
        "version": source.registry_version,
        "summary": source.summary,
        "classifiers": list(source.classifiers),
        "description": source.readme,
        "description_content_type": "text/markdown",
    }


def exact_version_payload(source: SourceMetadata | None = None) -> dict[str, Any]:
    source = source or source_metadata()
    return {
        "info": source_info(source),
        "urls": [release_file(source.registry_version)],
    }


def stable_project_payload() -> dict[str, Any]:
    source = source_metadata(stable=True)
    current = release_file(source.registry_version)
    return {
        "info": source_info(source),
        "releases": {
            "0.4.105": [release_file("0.4.105")],
            "0.4.106": [release_file("0.4.106")],
            source.registry_version: [current],
        },
        "urls": [current],
    }


def pip_report(version: str) -> dict[str, object]:
    return {"install": [{"metadata": {"name": "durable_workflow", "version": version}}]}


def test_release_channel_distinguishes_prerelease_and_stable() -> None:
    assert release_channel(source_metadata()) == "prerelease"
    assert release_channel(source_metadata(stable=True)) == "stable"


def test_prerelease_with_existing_final_skips_project_root_and_bare_install(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    requested_urls: list[str] = []
    requirements: list[tuple[str, bool]] = []
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)

    def request_json(url: str) -> object:
        requested_urls.append(url)
        assert url.endswith(f"/{source.registry_version}/json")
        return exact_version_payload(source)

    def resolve(requirement: str, *, prerelease: bool) -> object:
        requirements.append((requirement, prerelease))
        if requirement == source.name:
            raise AssertionError("a prerelease audit must not perform a bare install")
        return pip_report(source.registry_version)

    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", request_json)
    monkeypatch.setattr("scripts.check_pypi_project_surface._pip_report", resolve)

    assert main(["--source-ref", "release-source", "--attempts", "30"]) == 0
    assert requested_urls == [f"https://pypi.org/pypi/{source.name}/{source.registry_version}/json"]
    assert requirements == [
        (f"{source.name}=={source.registry_version}", False),
        ("durable-workflow~=2.0.0rc0", False),
    ]


def test_exact_version_metadata_mismatch_fails_without_retrying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    payload = exact_version_payload(source)
    payload["info"]["summary"] = "metadata from a different build"
    requested_urls: list[str] = []
    sleeps: list[float] = []
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr(
        "scripts.check_pypi_project_surface._request_json",
        lambda url: requested_urls.append(url) or payload,
    )
    monkeypatch.setattr("scripts.check_pypi_project_surface.time.sleep", sleeps.append)

    with pytest.raises(ProjectSurfaceError, match="exact-version JSON field summary differs"):
        main(["--source-ref", "release-source", "--attempts", "30", "--interval-seconds", "10"])

    assert len(requested_urls) == 1
    assert sleeps == []


def test_documented_prerelease_range_must_select_exact_current_rc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    requirements: list[str] = []
    sleeps: list[float] = []
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", lambda _: exact_version_payload(source))

    def resolve(requirement: str, *, prerelease: bool) -> object:
        del prerelease
        requirements.append(requirement)
        version = source.registry_version if "==" in requirement else "2.0.0rc23"
        return pip_report(version)

    monkeypatch.setattr("scripts.check_pypi_project_surface._pip_report", resolve)
    monkeypatch.setattr("scripts.check_pypi_project_surface.time.sleep", sleeps.append)

    with pytest.raises(ProjectSurfaceError, match="pip selected 2.0.0rc23; expected 2.0.0rc36"):
        main(["--source-ref", "release-source", "--attempts", "30", "--interval-seconds", "10"])

    assert requirements == [
        f"{source.name}=={source.registry_version}",
        "durable-workflow~=2.0.0rc0",
    ]
    assert sleeps == []


def test_prerelease_pip_probes_retry_simple_api_lag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    pip_attempts: dict[str, int] = {}
    sleeps: list[float] = []
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", lambda _: exact_version_payload(source))
    monkeypatch.setattr("scripts.check_pypi_project_surface.time.sleep", sleeps.append)

    def run_pip(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        assert "--no-cache-dir" in command
        requirement = command[-1]
        pip_attempts[requirement] = pip_attempts.get(requirement, 0) + 1
        if "==" in requirement and pip_attempts[requirement] == 1:
            detail = (
                f"ERROR: Could not find a version that satisfies the requirement {requirement} "
                "(from versions: 0.4.106, 2.0.0rc35)\n"
                f"ERROR: No matching distribution found for {requirement}\n"
            )
            return subprocess.CompletedProcess(command, 1, stdout="", stderr=detail)

        version = source.registry_version
        if "~=" in requirement and pip_attempts[requirement] == 1:
            version = "2.0.0rc35"
        report_path = Path(command[command.index("--report") + 1])
        report_path.write_text(json.dumps(pip_report(version)), encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr("scripts.check_pypi_project_surface.subprocess.run", run_pip)
    evidence_path = tmp_path / "evidence.json"

    assert (
        main(
            [
                "--source-ref",
                "release-source",
                "--attempts",
                "3",
                "--interval-seconds",
                "4",
                "--evidence",
                str(evidence_path),
            ]
        )
        == 0
    )
    assert pip_attempts == {
        f"{source.name}=={source.registry_version}": 2,
        "durable-workflow~=2.0.0rc0": 2,
    }
    assert sleeps == [4.0, 4.0]
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["exact_install_version"] == source.registry_version
    assert evidence["documented_install_version"] == source.registry_version


def test_malformed_pip_report_fails_without_retrying(monkeypatch: pytest.MonkeyPatch) -> None:
    source = source_metadata()
    requirements: list[str] = []
    sleeps: list[float] = []
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", lambda _: exact_version_payload(source))
    monkeypatch.setattr(
        "scripts.check_pypi_project_surface._pip_report",
        lambda requirement, *, prerelease: requirements.append(requirement) or {"install": "invalid"},
    )
    monkeypatch.setattr("scripts.check_pypi_project_surface.time.sleep", sleeps.append)

    with pytest.raises(ProjectSurfaceError, match="pip resolution report is invalid"):
        main(["--source-ref", "release-source", "--attempts", "30", "--interval-seconds", "10"])

    assert requirements == [f"{source.name}=={source.registry_version}"]
    assert sleeps == []


def test_exact_pip_semantic_mismatch_fails_without_retrying(monkeypatch: pytest.MonkeyPatch) -> None:
    source = source_metadata()
    requirements: list[str] = []
    sleeps: list[float] = []
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", lambda _: exact_version_payload(source))
    monkeypatch.setattr(
        "scripts.check_pypi_project_surface._pip_report",
        lambda requirement, *, prerelease: requirements.append(requirement) or pip_report("2.0.0rc24"),
    )
    monkeypatch.setattr("scripts.check_pypi_project_surface.time.sleep", sleeps.append)

    with pytest.raises(ProjectSurfaceError, match="pip selected 2.0.0rc24; expected 2.0.0rc36"):
        main(["--source-ref", "release-source", "--attempts", "30", "--interval-seconds", "10"])

    assert requirements == [f"{source.name}=={source.registry_version}"]
    assert sleeps == []


def test_exact_pip_visibility_lag_exhausts_configured_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    source = source_metadata()
    pip_attempts = 0
    sleeps: list[float] = []
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", lambda _: exact_version_payload(source))
    monkeypatch.setattr("scripts.check_pypi_project_surface.time.sleep", sleeps.append)

    def run_pip(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        nonlocal pip_attempts
        pip_attempts += 1
        requirement = command[-1]
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr=(
                f"ERROR: Could not find a version that satisfies the requirement {requirement} "
                "(from versions: 0.4.106, 2.0.0rc24)\n"
                f"ERROR: No matching distribution found for {requirement}\n"
            ),
        )

    monkeypatch.setattr("scripts.check_pypi_project_surface.subprocess.run", run_pip)

    with pytest.raises(ProjectSurfaceError, match="Simple API did not converge.*after 3 attempt"):
        main(["--source-ref", "release-source", "--attempts", "3", "--interval-seconds", "2"])

    assert pip_attempts == 3
    assert sleeps == [2.0, 2.0]


def test_pip_package_metadata_error_fails_without_retrying(monkeypatch: pytest.MonkeyPatch) -> None:
    source = source_metadata()
    pip_attempts = 0
    sleeps: list[float] = []
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", lambda _: exact_version_payload(source))
    monkeypatch.setattr("scripts.check_pypi_project_surface.time.sleep", sleeps.append)

    def run_pip(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        nonlocal pip_attempts
        pip_attempts += 1
        requirement = command[-1]
        detail = (
            "WARNING: Requested durable-workflow has inconsistent name: expected 'durable-workflow', "
            "but metadata has 'another-package'\n"
            f"ERROR: Could not find a version that satisfies the requirement {requirement} "
            "(from versions: 0.4.106, 2.0.0rc24)\n"
            f"ERROR: No matching distribution found for {requirement}\n"
        )
        return subprocess.CompletedProcess(command, 1, stdout="", stderr=detail)

    monkeypatch.setattr("scripts.check_pypi_project_surface.subprocess.run", run_pip)

    with pytest.raises(ProjectSurfaceError, match="has inconsistent name"):
        main(["--source-ref", "release-source", "--attempts", "30", "--interval-seconds", "10"])

    assert pip_attempts == 1
    assert sleeps == []


def test_stable_project_root_mismatch_remains_release_blocking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata(stable=True)
    root = stable_project_payload()
    root["info"]["version"] = "0.4.106"
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr(
        "scripts.check_pypi_project_surface._request_json",
        lambda url: exact_version_payload(source) if url.endswith(f"/{source.registry_version}/json") else root,
    )
    monkeypatch.setattr("scripts.check_pypi_project_surface._pip_report", lambda *_args, **_kwargs: pip_report("2.0.0"))

    with pytest.raises(ProjectSurfaceError, match="stable project-root metadata did not converge.*field version"):
        main(["--source-ref", "release-source"])


def test_stable_audit_requires_root_and_bare_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata(stable=True)
    requested_urls: list[str] = []
    requirements: list[str] = []
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)

    def request_json(url: str) -> object:
        requested_urls.append(url)
        if url.endswith(f"/{source.registry_version}/json"):
            return exact_version_payload(source)
        return stable_project_payload()

    def resolve(requirement: str, *, prerelease: bool) -> object:
        assert prerelease is False
        requirements.append(requirement)
        return pip_report(source.registry_version)

    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", request_json)
    monkeypatch.setattr("scripts.check_pypi_project_surface._pip_report", resolve)
    evidence_path = tmp_path / "evidence.json"

    assert main(["--source-ref", "release-source", "--evidence", str(evidence_path)]) == 0
    assert requested_urls == [
        f"https://pypi.org/pypi/{source.name}/{source.registry_version}/json",
        f"https://pypi.org/pypi/{source.name}/json",
    ]
    assert requirements == [f"{source.name}=={source.registry_version}", source.name]
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["release_channel"] == "stable"
    assert evidence["default_install_version"] == source.registry_version
    assert evidence["historical_versions"] == ["0.4.105", "0.4.106"]


def test_stable_root_preserves_non_yanked_historical_files() -> None:
    source = source_metadata(stable=True)
    exact_urls = verify_exact_version_json(exact_version_payload(source), source)

    assert verify_stable_project_json(stable_project_payload(), source, exact_urls) == ("0.4.105", "0.4.106")

    payload = stable_project_payload()
    payload["releases"]["0.4.106"][0]["yanked"] = True
    with pytest.raises(ProjectSurfaceError, match="historical release 0.4.106 must remain retained and non-yanked"):
        verify_stable_project_json(payload, source, exact_urls)


def test_prerelease_evidence_records_deferred_default_surface(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = source_metadata()
    monkeypatch.setattr("scripts.check_pypi_project_surface.load_source_metadata", lambda _: source)
    monkeypatch.setattr("scripts.check_pypi_project_surface._request_json", lambda _: exact_version_payload(source))
    monkeypatch.setattr(
        "scripts.check_pypi_project_surface._pip_report",
        lambda _requirement, *, prerelease: pip_report(source.registry_version),
    )
    evidence_path = tmp_path / "evidence.json"

    assert main(["--source-ref", "release-source", "--evidence", str(evidence_path)]) == 0
    assert json.loads(evidence_path.read_text(encoding="utf-8")) == {
        "schema": "durable-workflow.python-pypi-project-surface.v2",
        "source_commit": source.commit,
        "package": source.name,
        "public_urls": {
            "project_json": None,
            "exact_version_json": f"https://pypi.org/pypi/{source.name}/{source.registry_version}/json",
        },
        "release_channel": "prerelease",
        "exact_install_version": source.registry_version,
        "default_install_version": None,
        "documented_requirement": "durable-workflow~=2.0.0rc0",
        "documented_install_version": source.registry_version,
        "historical_versions": [],
    }


def test_documented_requirement_is_machine_checked() -> None:
    source = source_metadata()
    assert supported_prerelease_requirement(source) == "durable-workflow~=2.0.0rc0"

    changed = SourceMetadata(**{**source.__dict__, "readme": source.readme.replace("rc0", "rc23")})
    with pytest.raises(ProjectSurfaceError, match="supported 2.0 prerelease line"):
        supported_prerelease_requirement(changed)


def test_pip_report_must_select_expected_version() -> None:
    source = source_metadata()
    verify_pip_report(pip_report(source.registry_version), source, source.registry_version)

    with pytest.raises(ProjectSurfaceError, match="pip selected 0.4.106"):
        verify_pip_report(pip_report("0.4.106"), source, source.registry_version)


def test_source_only_qualification_does_not_require_public_package_state(
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

    publish = (REPO_ROOT / ".github" / "workflows" / "publish.yml").read_text(encoding="utf-8")
    exact_audit = publish.index("python scripts/check_release_metadata.py", publish.index("  publish:"))
    surface_audit = publish.index("python scripts/check_pypi_project_surface.py", exact_audit)
    github_release = publish.index('gh release create "$RELEASE_TAG"', surface_audit)
    assert exact_audit < surface_audit < github_release
