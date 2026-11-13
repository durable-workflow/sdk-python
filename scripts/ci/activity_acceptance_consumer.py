"""Install a closed wheel set, then await admission to the ordinary retry case."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

DATA = Path("/run/acceptance-data")
WHEELS = Path("/run/acceptance-wheels")
ENVIRONMENT = Path("/run/acceptance-env")
LIMIT = 64 * 1024 * 1024


def receive(maximum):
    data = sys.stdin.buffer.readline(maximum + 1)
    if not data.endswith(b"\n") or len(data) > maximum:
        raise ValueError("consumer input exceeds its bound")
    return json.loads(data)


def write(directory, name, item):
    if not name or "/" in name or name in {".", ".."}:
        raise ValueError("consumer input filename differs")
    data = base64.b64decode(item["base64"], validate=True)
    if hashlib.sha256(data).hexdigest() != item["sha256"]:
        raise ValueError("consumer input digest differs")
    descriptor = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(data)
    finally:
        os.close(descriptor)


def run(arguments, environment):
    return subprocess.run(arguments, env=environment, capture_output=True, text=True, timeout=90, check=True).stdout


def validate_setup(packet):
    if set(packet) != {"files", "wheels", "wheel_name", "wheel_sha256", "sdk_version"}:
        raise ValueError("consumer setup shape differs")
    expected_files = {"requirements.txt", "verify_activity_install.py", "test_activity_identity.py", "conftest.py"}
    if set(packet["files"]) != expected_files or not 1 <= len(packet["wheels"]) <= 30:
        raise ValueError("consumer setup membership differs")
    if packet["wheel_name"] not in packet["wheels"]:
        raise ValueError("approved SDK wheel is missing")
    if packet["wheels"][packet["wheel_name"]]["sha256"] != packet["wheel_sha256"]:
        raise ValueError("SDK wheel packet differs from approval")


def await_runtime(url):
    parsed = urlsplit(url)
    if parsed.scheme != "http" or parsed.port != 8080 or parsed.path or parsed.query or parsed.fragment:
        raise ValueError("consumer endpoint shape differs")
    # The controller binds this numeric endpoint to the two inspected IDs.
    import ipaddress

    address = ipaddress.ip_address(parsed.hostname)
    if address.version != 4 or not address.is_private or address.is_loopback:
        raise ValueError("consumer endpoint must be a private numeric IPv4 address")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with opener.open(url + "/api/health", timeout=2) as response:
                if response.status == 200:
                    return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError("published runtime readiness")


def inventory(python, wheel, packet, environment):
    output = run(
        [
            str(python),
            "-I",
            "-B",
            str(DATA / "verify_activity_install.py"),
            "--wheel",
            str(wheel),
            "--sha256",
            packet["wheel_sha256"],
            "--version",
            packet["sdk_version"],
        ],
        environment,
    )
    return json.loads(output)


def main():
    packet = prepare_inputs()
    environment, python = prepare_environment()
    wheel = WHEELS / packet["wheel_name"]
    before = inventory(python, wheel, packet, environment)
    ready = {
        "sdk_inventory": before,
        "python": sys.version,
        "environment": str(ENVIRONMENT),
        "distributions": run(
            [
                str(python),
                "-I",
                "-B",
                "-c",
                "import importlib.metadata,json; print(json.dumps(sorted((d.metadata['Name'],"
                "d.version) for d in importlib.metadata.distributions())))",
            ],
            environment,
        ),
    }
    print("ACTIVITY_CONSUMER_READY=" + json.dumps(ready, sort_keys=True), flush=True)
    admission = receive(8192)
    if set(admission) != {"server_url", "token"}:
        raise ValueError("consumer admission shape differs")
    await_runtime(admission["server_url"])
    output = execute_case(python, wheel, packet, before, environment, admission)
    print("ACTIVITY_CONSUMER_RESULT=" + json.dumps(output, sort_keys=True), flush=True)
    # Remain attached while the controller rechecks the same two IDs and IP.
    if receive(8192) != {"release": True}:
        raise ValueError("consumer release shape differs")
    if not output["complete"]:
        raise RuntimeError("ordinary activity case failed")


def prepare_inputs():
    if os.getuid() != 1000:
        raise ValueError("consumer must be unprivileged")
    for directory in (DATA, WHEELS, ENVIRONMENT):
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 1000 or info.st_mode & 0o077 or list(directory.iterdir()):
            raise ValueError("consumer scratch is not empty and private")
    packet = receive(LIMIT)
    validate_setup(packet)
    for name, item in packet["files"].items():
        write(DATA, name, item)
    for name, item in packet["wheels"].items():
        if not name.endswith(".whl"):
            raise ValueError("consumer accepts wheels only")
        write(WHEELS, name, item)
    return packet


def prepare_environment():
    environment = {
        "PATH": os.defpath,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONNOUSERSITE": "1",
        "PIP_CONFIG_FILE": os.devnull,
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "TMPDIR": str(DATA),
    }
    python = ENVIRONMENT / "bin/python"
    run([sys.executable, "-I", "-B", "-m", "venv", "--without-pip", "--copies", str(ENVIRONMENT)], environment)
    run(
        [
            sys.executable,
            "-I",
            "-B",
            "-m",
            "pip",
            "--python",
            str(python),
            "install",
            "--no-index",
            "--find-links",
            str(WHEELS),
            "--only-binary=:all:",
            "--require-hashes",
            "--no-compile",
            "-r",
            str(DATA / "requirements.txt"),
        ],
        environment,
    )
    return environment, python


def execute_case(python, wheel, packet, before, environment, admission):
    environment.update(
        {
            "DURABLE_WORKFLOW_SERVER_URL": admission["server_url"],
            "DURABLE_WORKFLOW_AUTH_TOKEN": admission["token"],
            "DURABLE_WORKFLOW_EXPECT_INSTALLED_VERSION": packet["sdk_version"],
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        }
    )
    result = subprocess.run(
        [
            str(python),
            "-I",
            "-B",
            "-m",
            "pytest",
            "-c",
            "/dev/null",
            "-p",
            "pytest_asyncio.plugin",
            "-p",
            "no:cacheprovider",
            "--asyncio-mode=auto",
            "--import-mode=importlib",
            "-v",
            "--capture=tee-sys",
            str(DATA / "test_activity_identity.py"),
            "--junitxml=" + str(DATA / "identity.xml"),
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=90,
    )
    after = inventory(python, wheel, packet, environment)
    if after != before:
        raise ValueError("installed SDK inventory changed")
    return {
        "complete": result.returncode == 0,
        "sdk_inventory": after,
        "case_output": result.stdout + result.stderr,
        "junit": (DATA / "identity.xml").read_text() if (DATA / "identity.xml").is_file() else "",
    }


if __name__ == "__main__":
    main()
