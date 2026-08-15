from __future__ import annotations

import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.api_reference_release import (
    INSTALL_REQUIREMENT_TOKEN,
    RELEASE_EVIDENCE_FILENAME,
    load_release_identity,
    release_evidence,
    write_release_evidence,
)
from scripts.check_api_reference_install import (
    PUBLIC_PYPI_INDEX,
    PublicRequirementUnavailable,
    install_public_requirement,
    rendered_install_command,
    run_clean_install,
    validate_command,
    validate_rendered_site,
    validate_source_templates,
    verify_public_deployment,
)
from scripts.mkdocs_hooks import on_page_markdown

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_release_identity_aligns_install_paths_and_server_pairing() -> None:
    identity = load_release_identity(REPO_ROOT)

    validate_source_templates(REPO_ROOT, identity)


def test_landing_install_requirement_is_owned_by_the_release_helper() -> None:
    identity = load_release_identity(REPO_ROOT)
    source = (REPO_ROOT / "docs" / "index.md").read_text(encoding="utf-8")

    assert INSTALL_REQUIREMENT_TOKEN in source
    assert identity.requirement not in source


def test_rendered_first_install_command_is_discovered(tmp_path: Path) -> None:
    identity = load_release_identity(REPO_ROOT)
    (tmp_path / "index.html").write_text(
        '<main><h2 id="install">Install</h2><div class="highlight"><pre><code>'
        "pip<span> </span>install<span> </span>"
        "'durable-workflow<span>~=</span>2.0.0rc0'"
        '</code></pre></div><h2 id="versioning">Versioning</h2>'
        f"<p>SDK {identity.version}; durableworkflow/server:{identity.server_version}</p></main>",
        encoding="utf-8",
    )

    assert rendered_install_command(tmp_path) == identity.install_command
    assert validate_rendered_site(tmp_path, identity) == identity.requirement


def test_manifest_change_alone_changes_rendered_release_tuple(tmp_path: Path) -> None:
    current = load_release_identity(REPO_ROOT)
    sdk_version = "2.0.0-rc.999"
    registry_version = "2.0.0rc999"
    server_version = "2.0.0-rc.998"
    manifest = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    manifest = manifest.replace(f'version = "{current.version}"', f'version = "{sdk_version}"', 1)
    manifest = manifest.replace(
        f'product-train = "{current.version}"',
        f'product-train = "{sdk_version}"',
        1,
    )
    manifest = manifest.replace(
        f'registry-version = "{current.registry_version}"',
        f'registry-version = "{registry_version}"',
        1,
    )
    manifest = manifest.replace(
        f'supported-server-versions = "{current.server_version}"',
        f'supported-server-versions = "{server_version}"',
        1,
    )
    (tmp_path / "pyproject.toml").write_text(manifest, encoding="utf-8")

    source = (REPO_ROOT / "docs" / "index.md").read_text(encoding="utf-8")
    page = SimpleNamespace(file=SimpleNamespace(src_uri="index.md"))
    config = SimpleNamespace(config_file_path=str(tmp_path / "mkdocs.yml"))
    rendered = on_page_markdown(source, page, config, files=None)

    assert "pip install 'durable-workflow~=2.0.0rc0'" in rendered
    assert f"durableworkflow/server:{server_version}" in rendered
    assert current.version not in rendered


def test_unversioned_first_install_command_is_rejected() -> None:
    identity = load_release_identity(REPO_ROOT)

    with pytest.raises(ValueError, match="First install command must be"):
        validate_command("pip install durable-workflow", identity)


def test_delayed_public_pypi_release_is_retried_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requirement = load_release_identity(REPO_ROOT).requirement
    return_codes = iter((1, 1, 0))
    calls: list[tuple[list[str], dict[str, str]]] = []
    sleeps: list[float] = []

    def fake_run(
        command: list[str],
        *,
        cwd: Path,
        env: dict[str, str],
        check: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, check, text
        calls.append((command, env))
        return subprocess.CompletedProcess(command, next(return_codes))

    monkeypatch.setattr("scripts.check_api_reference_install.subprocess.run", fake_run)
    monkeypatch.setattr("scripts.check_api_reference_install.time.sleep", sleeps.append)

    install_public_requirement(
        Path("/clean/bin/python"),
        requirement,
        cwd=tmp_path,
        attempts=3,
        retry_sleep=0.25,
    )

    assert len(calls) == 3
    assert sleeps == [0.25, 0.25]
    for command, env in calls:
        assert command[-3:] == ["--index-url", PUBLIC_PYPI_INDEX, requirement]
        assert "--pre" not in command
        assert env["PIP_INDEX_URL"] == PUBLIC_PYPI_INDEX
        assert env["PIP_CONFIG_FILE"]
        assert "PIP_EXTRA_INDEX_URL" not in env


def test_unavailable_public_pypi_release_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requirement = load_release_identity(REPO_ROOT).requirement

    def unavailable(
        command: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(command, 1)

    monkeypatch.setattr("scripts.check_api_reference_install.subprocess.run", unavailable)
    monkeypatch.setattr("scripts.check_api_reference_install.time.sleep", lambda _: None)

    with pytest.raises(PublicRequirementUnavailable):
        install_public_requirement(
            Path("/clean/bin/python"),
            requirement,
            cwd=tmp_path,
            attempts=2,
            retry_sleep=0,
        )


def test_clean_install_probes_the_exact_release_before_the_documented_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = load_release_identity(REPO_ROOT)
    calls: list[tuple[str, str | None]] = []

    def run_requirement(
        requirement: str,
        *,
        expected_version: str | None,
        install_attempts: int,
        install_retry_sleep: float,
    ) -> None:
        assert install_attempts == 1
        assert install_retry_sleep == 0
        calls.append((requirement, expected_version))

    monkeypatch.setattr("scripts.check_api_reference_install._run_clean_requirement", run_requirement)

    run_clean_install(identity.requirement, identity, install_attempts=1, install_retry_sleep=0)

    assert calls == [
        (f"durable-workflow=={identity.registry_version}", identity.registry_version),
        ("durable-workflow~=2.0.0rc0", identity.registry_version),
    ]


def test_public_rc24_does_not_mask_an_unpublished_exact_rc31(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = load_release_identity(REPO_ROOT)
    calls: list[str] = []

    def run_requirement(
        requirement: str,
        *,
        expected_version: str | None,
        install_attempts: int,
        install_retry_sleep: float,
    ) -> None:
        del expected_version, install_attempts, install_retry_sleep
        calls.append(requirement)
        if requirement == identity.exact_requirement:
            raise PublicRequirementUnavailable(1, ["pip", "install", requirement])
        raise subprocess.CalledProcessError(
            1,
            ["python", "-c", "installed durable-workflow 2.0.0rc24, expected 2.0.0rc31"],
        )

    monkeypatch.setattr("scripts.check_api_reference_install._run_clean_requirement", run_requirement)

    with pytest.raises(PublicRequirementUnavailable):
        run_clean_install(identity.requirement, identity, install_attempts=1, install_retry_sleep=0)

    assert identity.registry_version == "2.0.0rc31"
    assert calls == [identity.exact_requirement]


def test_stale_documented_range_is_a_hard_failure_after_exact_rc31_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = load_release_identity(REPO_ROOT)
    calls: list[str] = []
    stale_range = subprocess.CalledProcessError(
        1,
        ["python", "-c", "installed durable-workflow 2.0.0rc24, expected 2.0.0rc31"],
    )

    def run_requirement(
        requirement: str,
        *,
        expected_version: str | None,
        install_attempts: int,
        install_retry_sleep: float,
    ) -> None:
        assert expected_version == identity.registry_version
        assert install_attempts == 1
        assert install_retry_sleep == 0
        calls.append(requirement)
        if requirement == identity.requirement:
            raise stale_range

    monkeypatch.setattr("scripts.check_api_reference_install._run_clean_requirement", run_requirement)

    with pytest.raises(subprocess.CalledProcessError) as failure:
        run_clean_install(identity.requirement, identity, install_attempts=1, install_retry_sleep=0)

    assert failure.value is stale_range
    assert calls == [identity.exact_requirement, identity.requirement]


def test_public_release_evidence_derives_from_manifest(tmp_path: Path) -> None:
    identity = load_release_identity(REPO_ROOT)
    source_revision = "a" * 40

    evidence_path = write_release_evidence(tmp_path, identity, source_revision)

    assert evidence_path == tmp_path / RELEASE_EVIDENCE_FILENAME
    assert json.loads(evidence_path.read_text(encoding="utf-8")) == {
        "schema": "durable-workflow.python-api-reference.release",
        "source_revision": source_revision,
        "pypi_version": identity.registry_version,
        "install_command": identity.install_command,
        "artifact_versions": {
            "sdk-python": identity.version,
            "server": identity.server_version,
        },
    }


def test_public_deployment_waits_for_the_exact_source_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = load_release_identity(REPO_ROOT)
    source_revision = "b" * 40
    stale = release_evidence(identity, "a" * 40)
    current = release_evidence(identity, source_revision)
    responses = iter((stale, current))
    sleeps: list[float] = []

    def fake_urlopen(request: object, *, timeout: int) -> io.BytesIO:
        del request
        assert timeout == 30
        return io.BytesIO(json.dumps(next(responses)).encode())

    monkeypatch.setattr("scripts.check_api_reference_install.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("scripts.check_api_reference_install.time.sleep", sleeps.append)

    evidence = verify_public_deployment(
        "https://python.durable-workflow.com/release-audit.json",
        identity,
        source_revision,
        attempts=2,
        retry_sleep=0.25,
    )

    assert evidence == current
    assert sleeps == [0.25]


def test_public_deployment_rejects_non_https_evidence_url() -> None:
    identity = load_release_identity(REPO_ROOT)

    with pytest.raises(ValueError, match="must use HTTPS"):
        verify_public_deployment(
            "http://python.durable-workflow.com/release-audit.json",
            identity,
            "a" * 40,
            attempts=1,
            retry_sleep=0,
        )
