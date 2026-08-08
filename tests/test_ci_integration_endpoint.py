from __future__ import annotations

import importlib.util
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT_SCRIPT = ROOT / "scripts" / "ci" / "configure-integration-endpoint.py"
PROJECT_SCRIPT = ROOT / "scripts" / "ci" / "configure-compose-project.py"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
COMPOSE_FILE = ROOT / "docker-compose.test.yml"


def load_script_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {path}")

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
        ("https://ci.example.test", "127.0.0.1", "127.0.0.1"),
    ],
    ids=["github-localhost", "containerized-alternate-runner-docker-host"],
)
def test_endpoint_selection_probes_the_runner_reachable_server(
    tmp_path: Path,
    health_server: int,
    runner_server_url: str,
    docker_host: str | None,
    expected_host: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint_module = load_script_module("ci_integration_endpoint", ENDPOINT_SCRIPT)
    github_environment = tmp_path / "github-environment"
    monkeypatch.setenv("GITHUB_ENV", str(github_environment))
    monkeypatch.setenv("GITHUB_SERVER_URL", runner_server_url)
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "sdk-python-123-1")
    monkeypatch.delenv("DOCKER_HOST", raising=False)
    monkeypatch.delenv("DURABLE_WORKFLOW_DOCKER_HOST", raising=False)
    if docker_host:
        monkeypatch.setenv("DURABLE_WORKFLOW_DOCKER_HOST", docker_host)
    monkeypatch.setattr(
        endpoint_module,
        "discover_server_port",
        lambda project: health_server,
    )

    assert endpoint_module.main(["--attempts", "1", "--retry-delay", "0"]) == 0

    expected_endpoint = f"http://{expected_host}:{health_server}"
    assert github_environment.read_text(encoding="utf-8").splitlines() == [
        f"SERVER_PORT={health_server}",
        f"DURABLE_WORKFLOW_SERVER_URL={expected_endpoint}",
    ]


def test_compose_port_discovery_addresses_the_isolated_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint_module = load_script_module("ci_integration_endpoint_port", ENDPOINT_SCRIPT)
    calls: list[list[str]] = []

    def fake_run(arguments: list[str], **options: object) -> object:
        calls.append(arguments)
        assert options == {"check": True, "capture_output": True, "text": True}
        return type("Result", (), {"stdout": "0.0.0.0:49152\n[::]:49152\n"})()

    monkeypatch.setattr(endpoint_module.subprocess, "run", fake_run)

    assert endpoint_module.discover_server_port("sdk-python-123-2") == 49152
    assert calls == [
        [
            "docker",
            "compose",
            "--project-name",
            "sdk-python-123-2",
            "-f",
            "docker-compose.test.yml",
            "port",
            "server",
            "8080",
        ]
    ]


def test_compose_project_identity_isolates_runs_and_retries() -> None:
    project_module = load_script_module("ci_compose_project", PROJECT_SCRIPT)

    projects = {
        project_module.compose_project_name({"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1"}),
        project_module.compose_project_name({"GITHUB_RUN_ID": "124", "GITHUB_RUN_ATTEMPT": "1"}),
        project_module.compose_project_name({"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "2"}),
    }

    assert projects == {"sdk-python-123-1", "sdk-python-124-1", "sdk-python-123-2"}


def test_compose_project_configuration_exports_the_run_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_module = load_script_module("ci_compose_project_export", PROJECT_SCRIPT)
    github_environment = tmp_path / "github-environment"
    monkeypatch.setenv("GITHUB_ENV", str(github_environment))
    monkeypatch.setenv("GITHUB_RUN_ID", "987654")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "3")

    assert project_module.main() == 0
    assert github_environment.read_text(encoding="utf-8") == "COMPOSE_PROJECT_NAME=sdk-python-987654-3\n"


def test_containerized_alternate_runner_discovers_the_linux_docker_host_gateway(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint_module = load_script_module("ci_integration_endpoint_gateway", ENDPOINT_SCRIPT)
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
    candidates = endpoint_module.endpoint_candidates(
        {"GITHUB_SERVER_URL": "https://ci.example.test", "SERVER_PORT": "49152"}
    )

    assert candidates == ["http://172.18.0.1:49152", "http://localhost:49152"]


def test_ci_isolates_the_stack_and_emits_diagnostics_before_teardown() -> None:
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    integration_job = workflow.split("  integration:\n", maxsplit=1)[1].split(
        "  target-branch-qualification:\n", maxsplit=1
    )[0]
    qualification_job = workflow.split("  target-branch-qualification:\n", maxsplit=1)[1]

    project_offset = integration_job.index("configure-compose-project.py")
    compose_offset = integration_job.index('docker compose --project-name "$COMPOSE_PROJECT_NAME"')
    probe_offset = integration_job.index("configure-integration-endpoint.py")
    pytest_offset = integration_job.index("pytest tests/integration/ -v")
    diagnostics_offset = integration_job.index("Emit integration diagnostics")
    teardown_offset = integration_job.index("down -v --rmi local")

    assert project_offset < compose_offset < probe_offset < pytest_offset < diagnostics_offset < teardown_offset
    assert "up -d --build --wait --timeout 300" in integration_job
    assert integration_job.count('--project-name "$COMPOSE_PROJECT_NAME"') == 3
    assert "if: failure()" in integration_job
    assert "for service in bootstrap server worker" in integration_job
    assert 'logs --no-color --tail 200 "$service"' in integration_job
    assert diagnostics_offset < teardown_offset
    assert "DURABLE_WORKFLOW_SERVER_URL: http://localhost:8080" not in integration_job
    assert "      - qualification-class\n" in qualification_job
    assert "      - integration\n" in qualification_job
    assert 'test "$INTEGRATION_RESULT" = success' in qualification_job


def test_compose_lets_docker_allocate_the_server_host_port() -> None:
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    server = compose.split("  server:\n", maxsplit=1)[1].split("  worker:\n", maxsplit=1)[0]

    assert '      - "8080"' in server
    assert "SERVER_PORT" not in server
