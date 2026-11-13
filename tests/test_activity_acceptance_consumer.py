from __future__ import annotations

import importlib.util
import ipaddress
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).parents[1] / "scripts/ci"


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + ".py"))
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


consumer = load("activity_acceptance_consumer")
transport = load("activity_acceptance_session")


@pytest.mark.parametrize(
    "url",
    [
        "http://172.29.0.2:8080",
        "foreign.example",
        None,
        True,
        -1,
        2**32,
        int(ipaddress.IPv4Address("127.0.0.1")),
        int(ipaddress.IPv4Address("8.8.8.8")),
        int(ipaddress.IPv4Address("0.0.0.0")),
        int(ipaddress.IPv4Address("224.0.0.1")),
        int(ipaddress.IPv4Address("169.254.1.1")),
    ],
)
def test_consumer_refuses_unselected_endpoint_before_any_request(url, monkeypatch):
    monkeypatch.setattr(consumer.urllib.request, "build_opener", lambda *_args: pytest.fail("unexpected request"))
    with pytest.raises(ValueError):
        consumer.await_runtime(url)


def test_consumer_uses_empty_proxies_for_its_numeric_internal_readiness_request(monkeypatch):
    seen = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    def handler(proxies):
        seen.append(proxies)
        return "empty proxy handler"

    def open_url(url, timeout):
        seen.append((url, timeout))
        return Response()

    monkeypatch.setattr(consumer.urllib.request, "ProxyHandler", handler)
    monkeypatch.setattr(consumer.urllib.request, "build_opener", lambda _handler: SimpleNamespace(open=open_url))
    consumer.await_runtime(int(ipaddress.IPv4Address("172.29.0.2")))
    assert seen == [{}, ("http://172.29.0.2:8080/api/health", 2)]


def test_installer_has_no_ambient_environment_or_network_fallback(monkeypatch):
    calls = []
    monkeypatch.setenv("PYTHONPATH", "/foreign/src")
    monkeypatch.setenv("HTTPS_PROXY", "http://foreign")
    monkeypatch.setenv("PIP_EXTRA_INDEX_URL", "https://foreign")
    monkeypatch.setattr(consumer, "run", lambda arguments, environment: calls.append((arguments, environment)))
    environment, _python = consumer.prepare_environment()
    assert "PYTHONPATH" not in environment
    assert "HTTPS_PROXY" not in environment
    assert "PIP_EXTRA_INDEX_URL" not in environment
    command = calls[-1][0]
    assert "--no-index" in command
    assert "--require-hashes" in command
    assert "--no-compile" in command
    assert command[1:3] == ["-I", "-B"]


def test_stdio_protocol_stays_open_for_authority_recheck_and_explicit_release(monkeypatch):
    original = subprocess.Popen
    # Only inert JSON messages run here; there is no SDK, package or API input.
    source = """
import json,sys
assert json.loads(sys.stdin.readline()) == {'setup': 'captured'}
print('READY=' + json.dumps({'complete': True}), flush=True)
assert json.loads(sys.stdin.readline()) == {'admit': True}
print('RESULT=' + json.dumps({'complete': True}), flush=True)
assert json.loads(sys.stdin.readline()) == {'release': True}
"""

    def launch(_arguments, **kwargs):
        return original([sys.executable, "-I", "-B", "-c", source], **kwargs)

    monkeypatch.setattr(transport.subprocess, "Popen", launch)
    session = transport.Session("a" * 64, {"PATH": os.defpath})
    try:
        session.send({"setup": "captured"})
        assert session.receive("READY=") == {"complete": True}
        session.send({"admit": True})
        assert session.receive("RESULT=") == {"complete": True}
        assert session.process.poll() is None
        session.send({"release": True})
        assert session.finish() == 0
    finally:
        session.close()


def test_input_digest_substitution_refuses_before_writing(tmp_path):
    with pytest.raises(ValueError, match="digest differs"):
        consumer.write(tmp_path, "input.py", {"base64": "cHJpbnQoJ3JlcGxhY2VtZW50Jyk=", "sha256": "0" * 64})
    assert list(tmp_path.iterdir()) == []


def test_closed_consumer_refuses_sdk_wheel_omission_or_substitution():
    packet = {
        "files": dict.fromkeys(
            ("requirements.txt", "verify_activity_install.py", "test_activity_identity.py", "conftest.py"), {}
        ),
        "wheels": {"sdk.whl": {"sha256": "a" * 64}},
        "wheel_name": "sdk.whl",
        "wheel_sha256": "b" * 64,
        "sdk_version": "test",
    }
    with pytest.raises(ValueError, match="differs from approval"):
        consumer.validate_setup(packet)
    packet["wheel_name"] = "absent.whl"
    with pytest.raises(ValueError, match="wheel is missing"):
        consumer.validate_setup(packet)
