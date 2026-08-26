from __future__ import annotations

import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.api_reference_release import (
    QUICKSTART_CONTRACT_SCHEMA,
    QUICKSTART_CONTRACT_URL,
    RELEASE_EVIDENCE_FILENAME,
    SUPPORTED_PRERELEASE_INSTALL_COMMAND,
    SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND,
    QualifiedOnboarding,
    load_qualified_onboarding,
    load_release_identity,
    release_evidence,
    write_release_evidence,
)
from scripts.check_api_reference_install import (
    PUBLIC_PYPI_INDEX,
    OnboardingCommands,
    PublicRequirementUnavailable,
    install_public_requirement,
    rendered_install_command,
    rendered_server_image_command,
    run_clean_install,
    validate_command,
    validate_rendered_site,
    validate_server_image_command,
    validate_source_templates,
    verify_public_deployment,
)
from scripts.mkdocs_hooks import on_page_markdown

REPO_ROOT = Path(__file__).resolve().parents[1]


def onboarding_commands() -> OnboardingCommands:
    return OnboardingCommands(
        install=SUPPORTED_PRERELEASE_INSTALL_COMMAND,
        server_image=SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND,
    )


def quickstart_contract(sdk_version: str, server_version: str) -> dict[str, object]:
    return {
        "schema": QUICKSTART_CONTRACT_SCHEMA,
        "artifacts": {
            "sdk-python": {"version": sdk_version},
            "server": {
                "version": server_version,
                "image": "durableworkflow/server",
                "reference": f"durableworkflow/server:{server_version}",
            },
        },
    }


def test_release_identity_and_onboarding_sources_are_valid() -> None:
    load_release_identity(REPO_ROOT)
    validate_source_templates(REPO_ROOT)


def test_rendered_first_install_command_is_discovered(tmp_path: Path) -> None:
    (tmp_path / "index.html").write_text(
        '<main><h2 id="install">Install</h2><div class="highlight"><pre><code>'
        f"{SUPPORTED_PRERELEASE_INSTALL_COMMAND}"
        '</code></pre></div><section data-docs-journey="local-self-hosted"><pre><code>'
        f"{SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND}"
        '</code></pre></section><h2 id="versioning">Versioning</h2>'
        f'<span data-release-authority-url="{QUICKSTART_CONTRACT_URL}"></span></main>',
        encoding="utf-8",
    )

    assert rendered_install_command(tmp_path) == SUPPORTED_PRERELEASE_INSTALL_COMMAND
    assert rendered_server_image_command(tmp_path) == SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND
    assert validate_rendered_site(tmp_path) == onboarding_commands()


def test_manifest_change_does_not_change_public_onboarding_resolvers(tmp_path: Path) -> None:
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
    load_release_identity(tmp_path)

    source = (REPO_ROOT / "docs" / "index.md").read_text(encoding="utf-8")
    page = SimpleNamespace(file=SimpleNamespace(src_uri="index.md"))
    config = SimpleNamespace(config_file_path=str(tmp_path / "mkdocs.yml"))
    rendered = on_page_markdown(source, page, config, files=None)

    assert SUPPORTED_PRERELEASE_INSTALL_COMMAND in rendered
    assert SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND in rendered
    assert sdk_version not in rendered
    assert server_version not in rendered
    assert current.version not in rendered


def test_contract_change_alone_changes_the_qualified_sdk_server_pair() -> None:
    current = load_qualified_onboarding(quickstart_contract("2.0.0-rc.101", "2.0.0-rc.201"))
    changed = load_qualified_onboarding(quickstart_contract("2.0.0-rc.102", "2.0.0-rc.202"))

    assert current == QualifiedOnboarding(
        sdk_version="2.0.0-rc.101",
        sdk_registry_version="2.0.0rc101",
        server_version="2.0.0-rc.201",
        server_reference="durableworkflow/server:2.0.0-rc.201",
    )
    assert changed.sdk_version != current.sdk_version
    assert changed.server_reference != current.server_reference


def test_contract_rejects_a_mismatched_server_reference() -> None:
    contract = quickstart_contract("2.0.0-rc.101", "2.0.0-rc.201")
    artifacts = contract["artifacts"]
    assert isinstance(artifacts, dict)
    server = artifacts["server"]
    assert isinstance(server, dict)
    server["reference"] = "durableworkflow/server:unqualified"

    with pytest.raises(ValueError, match="does not match"):
        load_qualified_onboarding(contract)


def test_server_resolver_rejects_a_local_manifest_command() -> None:
    identity = load_release_identity(REPO_ROOT)

    with pytest.raises(ValueError, match="public quickstart contract resolver"):
        validate_server_image_command(
            f"export DW_SERVER_IMAGE='durableworkflow/server:{identity.server_version}'"
        )


def test_direct_pip_install_is_rejected() -> None:
    with pytest.raises(ValueError, match="supported prerelease resolver"):
        validate_command("pip install durable-workflow")


def test_delayed_public_pypi_release_is_retried_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requirement = load_release_identity(REPO_ROOT).exact_requirement
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
    requirement = load_release_identity(REPO_ROOT).exact_requirement

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


def test_clean_install_probes_the_exact_release_before_the_documented_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = load_release_identity(REPO_ROOT)
    commands = onboarding_commands()
    qualified = load_qualified_onboarding(quickstart_contract("2.0.0-rc.101", "2.0.0-rc.201"))
    requirements: list[tuple[str, str | None]] = []
    resolver_pairs: list[tuple[OnboardingCommands, QualifiedOnboarding]] = []

    def run_requirement(
        requirement: str,
        *,
        expected_version: str | None,
        install_attempts: int,
        install_retry_sleep: float,
    ) -> None:
        assert install_attempts == 1
        assert install_retry_sleep == 0
        requirements.append((requirement, expected_version))

    monkeypatch.setattr("scripts.check_api_reference_install._run_clean_requirement", run_requirement)
    monkeypatch.setattr("scripts.check_api_reference_install.load_public_qualified_onboarding", lambda: qualified)
    monkeypatch.setattr(
        "scripts.check_api_reference_install._run_clean_onboarding",
        lambda selected_commands, selected_pair: resolver_pairs.append((selected_commands, selected_pair)),
    )

    run_clean_install(commands, identity, install_attempts=1, install_retry_sleep=0)

    assert requirements == [(identity.exact_requirement, identity.registry_version)]
    assert resolver_pairs == [(commands, qualified)]


def test_resolver_result_does_not_mask_an_unpublished_exact_release(
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
        pytest.fail(f"unexpected requirement probe: {requirement}")

    monkeypatch.setattr("scripts.check_api_reference_install._run_clean_requirement", run_requirement)
    monkeypatch.setattr(
        "scripts.check_api_reference_install.load_public_qualified_onboarding",
        lambda: pytest.fail("public resolver must not run before the exact API-reference release is public"),
    )

    with pytest.raises(PublicRequirementUnavailable):
        run_clean_install(onboarding_commands(), identity, install_attempts=1, install_retry_sleep=0)

    assert calls == [identity.exact_requirement]


def test_resolver_failure_is_a_hard_failure_after_exact_release_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = load_release_identity(REPO_ROOT)
    commands = onboarding_commands()
    qualified = load_qualified_onboarding(quickstart_contract("2.0.0-rc.101", "2.0.0-rc.201"))
    requirements: list[str] = []
    resolver_pairs: list[tuple[OnboardingCommands, QualifiedOnboarding]] = []
    resolver_failure = subprocess.CalledProcessError(
        1,
        ["sh", "-c", commands.install],
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
        requirements.append(requirement)

    def run_resolver(
        selected_commands: OnboardingCommands,
        selected_pair: QualifiedOnboarding,
    ) -> None:
        resolver_pairs.append((selected_commands, selected_pair))
        raise resolver_failure

    monkeypatch.setattr("scripts.check_api_reference_install._run_clean_requirement", run_requirement)
    monkeypatch.setattr("scripts.check_api_reference_install.load_public_qualified_onboarding", lambda: qualified)
    monkeypatch.setattr("scripts.check_api_reference_install._run_clean_onboarding", run_resolver)

    with pytest.raises(subprocess.CalledProcessError) as failure:
        run_clean_install(commands, identity, install_attempts=1, install_retry_sleep=0)

    assert failure.value is resolver_failure
    assert requirements == [identity.exact_requirement]
    assert resolver_pairs == [(commands, qualified)]


def test_public_release_evidence_derives_from_manifest(tmp_path: Path) -> None:
    identity = load_release_identity(REPO_ROOT)
    source_revision = "a" * 40

    evidence_path = write_release_evidence(tmp_path, identity, source_revision)

    assert evidence_path == tmp_path / RELEASE_EVIDENCE_FILENAME
    assert json.loads(evidence_path.read_text(encoding="utf-8")) == {
        "schema": "durable-workflow.python-api-reference.release",
        "source_revision": source_revision,
        "pypi_version": identity.registry_version,
        "install_command": SUPPORTED_PRERELEASE_INSTALL_COMMAND,
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
