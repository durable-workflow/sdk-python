from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from scripts.api_reference_release import (
    QUICKSTART_CONTRACT_URL,
    SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
QUALIFIER_PATH = REPO_ROOT / "scripts" / "qualify-docs-promotion.py"
SOURCE_REVISION = "a" * 40


def test_workflow_direct_file_entrypoint_resolves_repository_modules() -> None:
    result = subprocess.run(
        [sys.executable, "scripts/qualify-docs-promotion.py", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--source-revision" in result.stdout


@pytest.fixture
def qualifier() -> ModuleType:
    spec = importlib.util.spec_from_file_location("qualify_docs_promotion", QUALIFIER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def deployed_landing_with_commands(commands: list[str]) -> SimpleNamespace:
    def locator(selector: str) -> SimpleNamespace:
        assert selector == '[data-docs-journey="local-self-hosted"] code'
        return SimpleNamespace(all_text_contents=lambda: commands)

    return SimpleNamespace(locator=locator)


def test_deployed_landing_accepts_rendered_source_free_server_resolver(qualifier: ModuleType) -> None:
    command = SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND

    assert QUICKSTART_CONTRACT_URL in command
    assert "durableworkflow/server:" not in command
    assert (
        qualifier.validate_deployed_server_image_command(
            deployed_landing_with_commands(["python worker.py", command, 'docker run --rm "$DW_SERVER_IMAGE"']),
            "desktop",
        )
        == command
    )


def test_deployed_landing_rejects_unresolved_server_resolver_placeholder(qualifier: ModuleType) -> None:
    unresolved = SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND.replace(
        QUICKSTART_CONTRACT_URL,
        "{{ quickstart_contract_url }}",
    )

    with pytest.raises(ValueError, match="unresolved documentation template placeholder"):
        qualifier.validate_deployed_server_image_command(deployed_landing_with_commands([unresolved]), "mobile")


def test_live_revision_is_verified_before_any_browser_request(
    qualifier: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[object] = []
    identity = object()

    class Browser:
        def close(self) -> None:
            calls.append("browser-close")

    class Chromium:
        def launch(self, **options: object) -> Browser:
            calls.append(("browser-launch", options))
            return Browser()

    class Playwright:
        chromium = Chromium()

    class PlaywrightContext:
        def __enter__(self) -> Playwright:
            calls.append("playwright-enter")
            return Playwright()

        def __exit__(self, *errors: object) -> None:
            calls.append("playwright-exit")

    def verify_public_deployment(
        url: str,
        observed_identity: object,
        source_revision: str,
        *,
        attempts: int,
        retry_sleep: float,
    ) -> None:
        calls.append(("release-audit", url, observed_identity, source_revision, attempts, retry_sleep))

    monkeypatch.setattr(qualifier, "load_release_identity", lambda _: identity)
    monkeypatch.setattr(qualifier, "verify_public_deployment", verify_public_deployment)
    monkeypatch.setattr(qualifier, "sync_playwright", PlaywrightContext)
    monkeypatch.setattr(
        qualifier,
        "qualify_viewport",
        lambda _browser, name, width, height, _evidence: calls.append(("viewport", name, width, height)),
    )

    assert (
        qualifier.main(
            [
                "--source-revision",
                SOURCE_REVISION,
                "--release-audit-attempts",
                "3",
                "--release-audit-retry-sleep",
                "0.25",
                "--evidence-directory",
                str(tmp_path),
            ]
        )
        == 0
    )

    assert calls[0] == (
        "release-audit",
        "https://python.durable-workflow.com/release-audit.json",
        identity,
        SOURCE_REVISION,
        3,
        0.25,
    )
    assert calls[1] == "playwright-enter"
    assert [call for call in calls if isinstance(call, tuple) and call[0] == "viewport"] == [
        ("viewport", "desktop", 1440, 900),
        ("viewport", "intermediate", 768, 1024),
        ("viewport", "mobile", 390, 844),
        ("viewport", "short-height", 640, 360),
    ]


def test_live_viewport_retries_transient_transport_timeout(
    qualifier: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[tuple[object, str, int, int]] = []
    browser = object()

    def qualify_once(
        observed_browser: object,
        name: str,
        width: int,
        height: int,
        _evidence_directory: Path | None,
    ) -> None:
        attempts.append((observed_browser, name, width, height))
        if len(attempts) == 1:
            raise qualifier.PlaywrightTimeoutError("receiver response was temporarily unavailable")

    monkeypatch.setattr(qualifier, "qualify_viewport_once", qualify_once)

    qualifier.qualify_viewport(browser, "mobile", 390, 844)

    assert attempts == [
        (browser, "mobile", 390, 844),
        (browser, "mobile", 390, 844),
    ]


def test_live_viewport_fails_after_bounded_transport_timeouts(
    qualifier: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[int] = []

    def time_out(*_args: object) -> None:
        attempts.append(len(attempts) + 1)
        raise qualifier.PlaywrightTimeoutError("receiver response was unavailable")

    monkeypatch.setattr(qualifier, "qualify_viewport_once", time_out)

    with pytest.raises(AssertionError, match="timed out at desktop after 3 attempts"):
        qualifier.qualify_viewport(object(), "desktop", 1440, 900)

    assert attempts == [1, 2, 3]


def test_live_receiver_contract_accepts_only_bounded_qualification_payload(qualifier: ModuleType) -> None:
    request = SimpleNamespace(
        method="POST",
        post_data='{"source":"sdk-python-reference","event":"qualification"}',
        all_headers=lambda: {
            "content-type": "text/plain;charset=UTF-8",
            "origin": "https://python.durable-workflow.com",
            "referer": "https://python.durable-workflow.com/",
        },
    )
    response = SimpleNamespace(
        request=request,
        status=204,
        headers={
            "access-control-allow-origin": "https://python.durable-workflow.com",
            "cache-control": "no-store",
        },
    )

    qualifier.assert_event_response(response, qualifier.QUALIFICATION_EVENT)

    request.post_data = '{"source":"sdk-python-reference","event":"qualification","visitor":"stable-id"}'
    with pytest.raises(AssertionError, match="was not bounded"):
        qualifier.assert_event_response(response, qualifier.QUALIFICATION_EVENT)
