from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.api_reference_release import load_release_identity
from scripts.check_api_reference_install import (
    PUBLIC_PYPI_INDEX,
    install_public_requirement,
    rendered_install_command,
    validate_command,
    validate_rendered_site,
    validate_source_templates,
)
from scripts.mkdocs_hooks import on_page_markdown

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_release_identity_aligns_install_paths_and_server_pairing() -> None:
    identity = load_release_identity(REPO_ROOT)

    validate_source_templates(REPO_ROOT, identity)


def test_rendered_first_install_command_is_discovered(tmp_path: Path) -> None:
    identity = load_release_identity(REPO_ROOT)
    (tmp_path / "index.html").write_text(
        '<main><h2 id="install">Install</h2><div class="highlight"><pre><code>'
        f"pip<span> </span>install<span> </span>durable-workflow<span>==</span>{identity.version}"
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

    assert f"pip install durable-workflow=={sdk_version}" in rendered
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

    with pytest.raises(subprocess.CalledProcessError):
        install_public_requirement(
            Path("/clean/bin/python"),
            requirement,
            cwd=tmp_path,
            attempts=2,
            retry_sleep=0,
        )
