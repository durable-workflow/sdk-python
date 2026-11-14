"""Install a closed wheel set, then await admission to one fixed reviewed case."""

from __future__ import annotations

import base64
import hashlib
import importlib.machinery
import importlib.metadata
import ipaddress
import json
import os
import stat
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlunsplit

DATA = Path("/run/acceptance-data")
WHEELS = Path("/run/acceptance-wheels")
ENVIRONMENT = Path("/run/acceptance-env")
LIMIT = 64 * 1024 * 1024
JUNIT = DATA / "identity.xml"
CAPTURED_VERIFIER = ""
CAPTURED_ENTRY = ""
CASE_FILES = {
    "activity-identity": {"test_activity_identity.py"},
    "timer-redrive": {"test_native_timer_redrive.py", "redrive_timer_clock_workflow.py"},
    "retry-cap": {"test_activity_retry_cap.py"},
}


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


def run(arguments, environment, input_data=None):
    return subprocess.run(
        arguments, env=environment, input=input_data, capture_output=True, text=True, timeout=90, check=True
    ).stdout


def validate_setup(packet):
    if set(packet) != {"files", "wheels", "wheel_name", "wheel_sha256", "sdk_version", "scenario"}:
        raise ValueError("consumer setup shape differs")
    if packet["scenario"] not in CASE_FILES:
        raise ValueError("consumer scenario differs")
    expected_files = {"requirements.txt", "verify_activity_install.py", "conftest.py"} | CASE_FILES[packet["scenario"]]
    if set(packet["files"]) != expected_files or not 1 <= len(packet["wheels"]) <= 30:
        raise ValueError("consumer setup membership differs")
    if packet["wheel_name"] not in packet["wheels"]:
        raise ValueError("approved SDK wheel is missing")
    if packet["wheels"][packet["wheel_name"]]["sha256"] != packet["wheel_sha256"]:
        raise ValueError("SDK wheel packet differs from approval")


def await_runtime(number):
    if type(number) is not int or not 0 <= number <= 0xFFFFFFFF:
        raise ValueError("consumer endpoint must be an unsigned IPv4 integer")
    # The controller binds this numeric endpoint to the two inspected IDs.
    address = ipaddress.IPv4Address(number)
    if (
        not address.is_private
        or address.is_loopback
        or address.is_unspecified
        or address.is_multicast
        or address.is_reserved
        or address.is_link_local
    ):
        raise ValueError("consumer endpoint must be a private numeric IPv4 address")
    octets = [(number >> shift) & 255 for shift in (24, 16, 8, 0)]
    numeric_host = ".".join(str(octet) for octet in octets)
    url = urlunsplit(("http", numeric_host + ":8080", "", "", ""))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with opener.open(url + "/api/health", timeout=2) as response:
                if response.status == 200:
                    return url
        except OSError:
            time.sleep(0.2)
    raise TimeoutError("published runtime readiness")


def retained_verifier():
    if not CAPTURED_VERIFIER:
        raise ValueError("controller did not bind the Git-captured verifier")
    namespace = {"__name__": "captured_sdk_inventory"}
    exec(compile(CAPTURED_VERIFIER, "<captured SDK inventory>", "exec"), namespace)
    return namespace["verify_sdk"]


def inventory(verifier, python, packet):
    # The base interpreter, captured verifier and installer template remain in
    # bootstrap memory. SDK execution cannot replace the terminal checker file.
    from pip._vendor.distlib.scripts import SCRIPT_TEMPLATE

    site = (
        ENVIRONMENT
        / "lib"
        / ("python" + str(sys.version_info.major) + "." + str(sys.version_info.minor))
        / "site-packages"
    )
    spec = importlib.machinery.PathFinder.find_spec("durable_workflow", [str(site)])
    if spec is None or spec.origin is None:
        raise ValueError("installed SDK origin is unavailable")
    wheel = base64.b64decode(packet["wheels"][packet["wheel_name"]]["base64"], validate=True)
    result = verifier(site, spec.origin, wheel, packet["wheel_sha256"], python=python, script_template=SCRIPT_TEMPLATE)
    versions = [
        item.version
        for item in importlib.metadata.distributions(path=[str(site)])
        if item.metadata["Name"].lower().replace("_", "-") == "durable-workflow"
    ]
    if versions != [packet["sdk_version"]]:
        raise ValueError("installed SDK version differs")
    return result


def main():
    packet = prepare_inputs()
    verifier = retained_verifier()
    environment, python = prepare_environment()
    before = inventory(verifier, python, packet)
    entry = captured_entry()
    installed = entry["snapshot"](ENVIRONMENT)
    site = ENVIRONMENT / "lib" / ("python" + str(sys.version_info.major) + "." + str(sys.version_info.minor))
    distributions = sorted(
        (item.metadata["Name"], item.version)
        for item in importlib.metadata.distributions(path=[str(site / "site-packages")])
    )
    ready = {
        "sdk_inventory": before,
        "python": sys.version,
        "environment": str(ENVIRONMENT),
        "distributions": json.dumps(distributions),
        "environment_inventory": installed,
        "environment_inventory_sha256": entry["digest"](installed),
    }
    print("ACTIVITY_CONSUMER_READY=" + json.dumps(ready, sort_keys=True), flush=True)
    admission = receive(8192)
    if set(admission) != {"runtime_ipv4", "token"}:
        raise ValueError("consumer admission shape differs")
    admission["server_url"] = await_runtime(admission["runtime_ipv4"])
    output = execute_case(verifier, python, packet, before, environment, admission, installed)
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
        "PIP_NO_CACHE_DIR": "1",
        "PIP_NO_INPUT": "1",
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


def captured_entry():
    if not CAPTURED_ENTRY:
        raise ValueError("controller did not bind the Git-captured entry")
    namespace = {"__name__": "captured_entry"}
    exec(compile(CAPTURED_ENTRY, "<captured entry>", "exec"), namespace)
    return namespace


def entry_source(mode, case_name, case_sha, installed, worker_entry=""):
    source = CAPTURED_ENTRY
    values = {
        "MODE": mode, "INVENTORY_SHA256": captured_entry()["digest"](installed),
        "CASE_SHA256": case_sha, "CASE_NAME": case_name, "WORKER_ENTRY": worker_entry,
    }
    for name, value in values.items():
        placeholder = name + ' = ""'
        if source.count(placeholder) != 1:
            raise ValueError("captured entry binding placeholder differs")
        source = source.replace(placeholder, name + " = " + repr(value), 1)
    return source


def execute_case(verifier, python, packet, before, environment, admission, installed):
    redrive = packet["scenario"] == "timer-redrive"
    test_name = {
        "timer-redrive": "test_native_timer_redrive.py", "retry-cap": "test_activity_retry_cap.py",
        "activity-identity": "test_activity_identity.py",
    }[packet["scenario"]]
    environment.update(
        {
            "DURABLE_WORKFLOW_SERVER_URL": admission["server_url"],
            "DURABLE_WORKFLOW_AUTH_TOKEN": admission["token"],
            "DURABLE_WORKFLOW_EXPECT_INSTALLED_VERSION": packet["sdk_version"],
            "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        }
    )
    if redrive:
        environment["DURABLE_WORKFLOW_REDRIVE_CELL"] = "1"
    if packet["scenario"] == "retry-cap":
        environment["DURABLE_WORKFLOW_RETRY_CAP_CELL"] = "1"
    worker_entry = entry_source("worker", test_name, packet["files"][test_name]["sha256"], installed)
    parent_entry = entry_source("pytest", test_name, packet["files"][test_name]["sha256"], installed, worker_entry)
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-B",
            "-c",
            parent_entry,
        ],
        env=environment,
        input=json.dumps({"inventory": installed}),
        capture_output=True,
        text=True,
        timeout=90 if packet["scenario"] == "activity-identity" else 180,
    )
    after = inventory(verifier, python, packet)
    if after != before:
        raise ValueError("installed SDK inventory changed")
    if captured_entry()["snapshot"](ENVIRONMENT) != installed:
        raise ValueError("private install changed during the case")
    return {
        "complete": result.returncode == 0,
        "sdk_inventory": after,
        "case_output": result.stdout + result.stderr,
        "junit": JUNIT.read_text() if JUNIT.is_file() else "",
    }


if __name__ == "__main__":
    main()
