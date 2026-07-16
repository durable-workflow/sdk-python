from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT_SCRIPT = ROOT / "scripts" / "ci" / "configure-integration-endpoint.py"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def load_endpoint_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("ci_integration_endpoint", ENDPOINT_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load the integration endpoint script")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/api/health":
            self.send_error(404)
            return

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, format: str, *args: object) -> None:
        pass


@pytest.fixture
def health_server() -> Iterator[int]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), HealthHandler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.mark.parametrize(
    ("runner_server_url", "docker_host", "expected_host"),
    [
        ("https://github.com", None, "localhost"),
        ("https://forgejo.example.test", "127.0.0.1", "127.0.0.1"),
    ],
    ids=["github-localhost", "containerized-forgejo-docker-host"],
)
def test_endpoint_selection_probes_the_runner_reachable_server(
    tmp_path: Path,
    health_server: int,
    runner_server_url: str,
    docker_host: str | None,
    expected_host: str,
) -> None:
    github_environment = tmp_path / "github-environment"
    environment = os.environ.copy()
    environment.update(
        {
            "GITHUB_ENV": str(github_environment),
            "GITHUB_SERVER_URL": runner_server_url,
            "SERVER_PORT": str(health_server),
        }
    )
    environment.pop("DOCKER_HOST", None)
    environment.pop("DURABLE_WORKFLOW_DOCKER_HOST", None)
    if docker_host:
        environment["DURABLE_WORKFLOW_DOCKER_HOST"] = docker_host

    result = subprocess.run(
        [
            sys.executable,
            str(ENDPOINT_SCRIPT),
            "--attempts",
            "1",
            "--retry-delay",
            "0",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )

    expected_endpoint = f"http://{expected_host}:{health_server}"
    assert github_environment.read_text(encoding="utf-8") == (f"DURABLE_WORKFLOW_SERVER_URL={expected_endpoint}\n")
    assert expected_endpoint in result.stdout


def test_containerized_forgejo_discovers_the_linux_docker_host_gateway(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint_module = load_endpoint_module()
    route_file = tmp_path / "route"
    route_file.write_text(
        "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\n"
        "eth0 00000000 010012AC 0003 0 0 0 00000000 0 0 0\n",
        encoding="ascii",
    )
    gateway = endpoint_module._default_route_gateway(route_file)
    assert gateway == "172.18.0.1"

    monkeypatch.setattr(endpoint_module, "_host_docker_internal", lambda: None)
    monkeypatch.setattr(endpoint_module, "_default_route_gateway", lambda: gateway)
    candidates = endpoint_module.endpoint_candidates({"GITHUB_SERVER_URL": "https://forgejo.example.test"})

    assert candidates == ["http://172.18.0.1:8080", "http://localhost:8080"]


def test_ci_probes_the_endpoint_before_mandatory_integration_tests() -> None:
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    integration_job = workflow.split("  integration:\n", maxsplit=1)[1].split(
        "  target-branch-qualification:\n", maxsplit=1
    )[0]
    qualification_job = workflow.split("  target-branch-qualification:\n", maxsplit=1)[1]

    compose_offset = integration_job.index("docker compose -f docker-compose.test.yml up")
    probe_offset = integration_job.index("configure-integration-endpoint.py")
    pytest_offset = integration_job.index("pytest tests/integration/ -v")

    assert compose_offset < probe_offset < pytest_offset
    assert "DURABLE_WORKFLOW_SERVER_URL: http://localhost:8080" not in integration_job
    assert "needs: [lint, test, package, cli-parity, integration]" in qualification_job
    assert 'test "$INTEGRATION_RESULT" = success' in qualification_job
