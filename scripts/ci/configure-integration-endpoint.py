#!/usr/bin/env python3
"""Select and probe the integration server endpoint for the current CI runner."""

from __future__ import annotations

import argparse
import os
import socket
import struct
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path
from urllib.parse import urlsplit

HEALTH_PATH = "/api/health"
PUBLIC_GITHUB_HOST = "github.com"


def _docker_host_name(value: str) -> str | None:
    if not value or value.startswith("unix:"):
        return None

    parsed = urlsplit(value if "://" in value else f"//{value}")
    return parsed.hostname


def _default_route_gateway(route_file: Path = Path("/proc/net/route")) -> str | None:
    try:
        routes = route_file.read_text(encoding="ascii").splitlines()[1:]
    except OSError:
        return None

    for route in routes:
        fields = route.split()
        if len(fields) < 4 or fields[1] != "00000000":
            continue

        try:
            flags = int(fields[3], 16)
            gateway = socket.inet_ntoa(struct.pack("<L", int(fields[2], 16)))
        except (OSError, ValueError, struct.error):
            continue

        if flags & 0x2:
            return gateway

    return None


def _host_docker_internal() -> str | None:
    try:
        socket.getaddrinfo("host.docker.internal", None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return None
    return "host.docker.internal"


def _endpoint(host: str, port: int) -> str:
    formatted_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    return f"http://{formatted_host}:{port}"


def endpoint_candidates(environment: Mapping[str, str]) -> list[str]:
    """Return runner-appropriate endpoints in reachability preference order."""
    runner_host = urlsplit(environment.get("GITHUB_SERVER_URL", "https://github.com")).hostname
    port = int(environment.get("SERVER_PORT", "8080"))
    if not 1 <= port <= 65535:
        raise ValueError("SERVER_PORT must be between 1 and 65535")

    if runner_host == PUBLIC_GITHUB_HOST:
        hosts = ["localhost"]
    else:
        hosts = [
            _docker_host_name(environment.get("DURABLE_WORKFLOW_DOCKER_HOST", "")),
            _docker_host_name(environment.get("DOCKER_HOST", "")),
            _host_docker_internal(),
            _default_route_gateway(),
            "localhost",
        ]

    unique_hosts = list(dict.fromkeys(host for host in hosts if host))
    return [_endpoint(host, port) for host in unique_hosts]


def probe_endpoint(
    candidates: Sequence[str],
    *,
    attempts: int,
    retry_delay: float,
    timeout: float,
) -> str:
    """Return the first endpoint with a healthy HTTP response."""
    if attempts < 1:
        raise ValueError("attempts must be at least 1")

    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    failures: dict[str, str] = {}
    for attempt in range(1, attempts + 1):
        for endpoint in candidates:
            health_url = f"{endpoint}{HEALTH_PATH}"
            try:
                request = urllib.request.Request(health_url, method="GET")
                with opener.open(request, timeout=timeout) as response:
                    if 200 <= response.status < 400:
                        return endpoint
                    failures[endpoint] = f"HTTP {response.status}"
            except (OSError, urllib.error.URLError) as error:
                failures[endpoint] = str(error)

        if attempt < attempts:
            time.sleep(retry_delay)

    details = "; ".join(f"{endpoint}: {failures.get(endpoint, 'unreachable')}" for endpoint in candidates)
    raise RuntimeError(f"integration server health probe failed after {attempts} attempts ({details})")


def export_endpoint(endpoint: str, github_environment: Path) -> None:
    with github_environment.open("a", encoding="utf-8") as environment_file:
        environment_file.write(f"DURABLE_WORKFLOW_SERVER_URL={endpoint}\n")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attempts", type=int, default=30)
    parser.add_argument("--retry-delay", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=2.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    github_environment = os.environ.get("GITHUB_ENV")
    if not github_environment:
        raise RuntimeError("GITHUB_ENV must name the CI environment export file")

    candidates = endpoint_candidates(os.environ)
    endpoint = probe_endpoint(
        candidates,
        attempts=args.attempts,
        retry_delay=args.retry_delay,
        timeout=args.timeout,
    )
    export_endpoint(endpoint, Path(github_environment))
    print(f"Integration server is reachable at {endpoint}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
