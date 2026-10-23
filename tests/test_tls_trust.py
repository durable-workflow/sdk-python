from __future__ import annotations

import shutil
import ssl
import subprocess
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import certifi
import httpx
import pytest

from durable_workflow import serializer
from durable_workflow.client import Client
from durable_workflow.retry_policy import TransportRetryPolicy
from durable_workflow.sync import Client as SyncClient
from durable_workflow.worker import Worker
from tests.test_runtime_external_payload_transport import FakeRuntimePayloadServer


@dataclass
class TLSEndpoint:
    url: str
    mismatch_url: str
    ca_file: Path
    runtime: FakeRuntimePayloadServer

    def context(self) -> ssl.SSLContext:
        context = ssl.create_default_context(cafile=certifi.where())
        context.load_verify_locations(cafile=str(self.ca_file))
        return context


@pytest.fixture(scope="module")
def tls_endpoint(tmp_path_factory: pytest.TempPathFactory) -> Iterator[TLSEndpoint]:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("TLS controls require the OpenSSL CLI (required in supported-version CI).")
    directory = tmp_path_factory.mktemp("private-ca")
    commands = [
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=SDK Test CA",
         "-addext", "basicConstraints=critical,CA:TRUE", "-keyout", "ca.key", "-out", "ca.pem"],
        ["req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
         "-keyout", "server.key", "-out", "server.csr"],
        ["x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
         "-days", "1", "-extfile", "server.ext", "-out", "server.pem"],
    ]
    (directory / "server.ext").write_text("subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n")
    for arguments in commands:
        subprocess.run([openssl, *arguments], cwd=directory, check=True, capture_output=True, timeout=15)
    runtime = FakeRuntimePayloadServer()
    runtime.responses[("GET", "/api/health")] = {"status": "serving"}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.reply()

        def do_POST(self) -> None:
            self.reply()

        def reply(self) -> None:
            request = httpx.Request(
                self.command, "https://localhost" + self.path,
                headers=dict(self.headers.items()), content=self.rfile.read(int(self.headers.get("Content-Length", 0))),
            )
            response = runtime.handler(request)
            self.send_response(response.status_code)
            for name, value in response.headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(response.content)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(str(directory / "server.pem"), str(directory / "server.key"))
    server.socket = server_context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    port = server.server_address[1]
    try:
        yield TLSEndpoint(f"https://localhost:{port}", f"https://127.0.0.1:{port}", directory / "ca.pem", runtime)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


def options(endpoint: TLSEndpoint) -> dict[str, Any]:
    return {"token": "runtime-role-token", "namespace": "billing", "timeout": 2,
            "retry_policy": TransportRetryPolicy(max_attempts=1)}


async def test_default_client_refuses_an_untrusted_private_ca(
    tls_endpoint: TLSEndpoint, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    monkeypatch.delenv("SSL_CERT_DIR", raising=False)
    async with Client(tls_endpoint.url, **options(tls_endpoint)) as client:
        polling = client.health()
        with pytest.raises(httpx.ConnectError, match="CERTIFICATE_VERIFY_FAILED"):
            await polling


async def test_explicit_private_ca_context_preserves_public_roots(tls_endpoint: TLSEndpoint) -> None:
    context = ssl.create_default_context(cafile=certifi.where())
    public_roots = set(context.get_ca_certs(binary_form=True))
    context.load_verify_locations(cafile=str(tls_endpoint.ca_file))
    assert public_roots < set(context.get_ca_certs(binary_form=True))
    async with Client(tls_endpoint.url, tls_context=context, **options(tls_endpoint)) as client:
        assert await client.health() == {"status": "serving"}
        assert client.tls_context is context


async def test_explicit_context_still_refuses_a_hostname_mismatch(tls_endpoint: TLSEndpoint) -> None:
    async with Client(tls_endpoint.mismatch_url, tls_context=tls_endpoint.context(), **options(tls_endpoint)) as client:
        polling = client.health()
        with pytest.raises(httpx.ConnectError, match="CERTIFICATE_VERIFY_FAILED"):
            await polling


def test_synchronous_client_uses_the_supplied_context(tls_endpoint: TLSEndpoint) -> None:
    with SyncClient(tls_endpoint.url, tls_context=tls_endpoint.context(), **options(tls_endpoint)) as client:
        assert client.health() == {"status": "serving"}


async def test_query_clone_and_nexus_view_preserve_the_same_context(tls_endpoint: TLSEndpoint) -> None:
    context = tls_endpoint.context()
    async with Client(tls_endpoint.url, tls_context=context, **options(tls_endpoint)) as client:
        worker = Worker(client, task_queue="orders")
        async with worker._clone_client_for_query_tasks() as clone:
            assert clone.tls_context is context
            assert await clone.health() == {"status": "serving"}
        scoped = client._nexus_target_client("another-namespace")
        assert scoped.tls_context is context
        assert scoped._http is client._http
        assert await scoped.health() == {"status": "serving"}
        assert client.namespace == "billing"


async def test_worker_poll_and_payload_upload_fetch_use_verified_https(tls_endpoint: TLSEndpoint) -> None:
    runtime = tls_endpoint.runtime
    runtime.responses[("POST", "/api/worker/query-tasks/poll")] = {"task": None}
    async with Client(tls_endpoint.url, tls_context=tls_endpoint.context(), **options(tls_endpoint)) as client:
        assert await client.poll_query_task(worker_id="test-worker", task_queue="orders", timeout=0.05) is None
        expected = {"customer": "synthetic", "value": "x" * 200}
        wire = await client._externalize_runtime_payloads(
            serializer.envelope(expected), worker=False,
            transport=await client._runtime_external_payload_transport(), uploaded={}, slot=("arguments",),
        )
        assert "external_payload" in wire
        decoded = await client._resolve_runtime_payloads(
            wire, worker=False, transport=await client._runtime_external_payload_transport(), resolved={},
        )
        assert serializer.decode_envelope(decoded) == expected
    assert runtime.upload_count == runtime.fetch_count == 1


async def test_default_environment_ca_selection_remains_supported(
    tls_endpoint: TLSEndpoint, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_endpoint.ca_file))
    async with Client(tls_endpoint.url, **options(tls_endpoint)) as client:
        assert await client.health() == {"status": "serving"}


@pytest.mark.parametrize("disable_hostname,disable_certificates", [(True, False), (True, True)])
@pytest.mark.parametrize("client_class", [Client, SyncClient])
def test_contexts_with_disabled_verification_are_rejected(
    disable_hostname: bool, disable_certificates: bool, client_class: type,
) -> None:
    context = ssl.create_default_context(cafile=certifi.where())
    if disable_hostname:
        context.check_hostname = False
    if disable_certificates:
        context.verify_mode = ssl.CERT_NONE
    with pytest.raises(ValueError, match="certificate and hostname verification"):
        client_class("https://runtime.example", tls_context=context)


@pytest.mark.parametrize("value", [False, True, "private-ca.pem"])
def test_context_option_refuses_non_context_values(value: object) -> None:
    with pytest.raises(TypeError, match="SSLContext"):
        Client("https://runtime.example", tls_context=value)  # type: ignore[arg-type]
