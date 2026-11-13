from __future__ import annotations

import importlib.util
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

PATH = Path(__file__).parents[1] / "scripts/ci/published-activity-acceptance.py"
SPEC = importlib.util.spec_from_file_location("published_activity_acceptance", PATH)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


@pytest.mark.parametrize("mapping", [
    "0.0.0.0:8080", "[::]:8080", "127.0.0.1:123456", "127.0.0.1:0",
    "127.0.0.1:65536", "127.0.0.1:８０８０", "127.0.0.1:8080\n0.0.0.0:8080",
])
def test_runtime_url_refuses_nonloopback_or_invalid_port_before_any_request(mapping, monkeypatch):
    monkeypatch.setattr(runner, "call", lambda *_args: SimpleNamespace(stdout=mapping))
    monkeypatch.setattr(runner.urllib.request, "build_opener", lambda *_args: pytest.fail("unexpected network request"))
    with pytest.raises(ValueError):
        runner.runtime_url("a" * 64)


def test_runtime_url_probes_only_the_validated_loopback_endpoint(monkeypatch):
    observed = []

    def open_url(url, *, timeout):
        parsed = urlsplit(url)
        observed.append((parsed.hostname, parsed.port, parsed.path, timeout))
        return nullcontext(SimpleNamespace(status=200))

    monkeypatch.setattr(runner, "call", lambda *_args: SimpleNamespace(stdout="127.0.0.1:54321\n"))
    monkeypatch.setattr(runner.urllib.request, "build_opener", lambda *_args: SimpleNamespace(open=open_url))
    result = urlsplit(runner.runtime_url("a" * 64))
    assert result.hostname == "127.0.0.1"
    assert result.port == 54321
    assert observed == [("127.0.0.1", 54321, "/api/health", 2)]


@pytest.mark.parametrize("change", ["id", "owner", "run"])
def test_cleanup_refuses_foreign_container_identity_or_ownership(change, monkeypatch):
    record = {
        "Id": "a" * 64,
        "Config": {"Labels": {
            "dev.durable-workflow.fixture-owner": "sdk-python", "dev.durable-workflow.fixture-run": "ours",
        }},
    }
    if change == "id":
        record["Id"] = "b" * 64
    else:
        record["Config"]["Labels"]["dev.durable-workflow.fixture-" + change] = "foreign"

    def call(*arguments):
        assert arguments == ("docker", "inspect", "a" * 64)
        return SimpleNamespace(stdout=json.dumps([record]))

    monkeypatch.setattr(runner, "call", call)
    with pytest.raises(ValueError, match="cleanup ownership differs"):
        runner.remove_owned_runtime("a" * 64, "ours", {})
