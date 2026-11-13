"""Run the activity retry identity case with one exact published SDK/Server pair."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlunsplit
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "published-activity-results"


def call(*arguments, check=True, **kwargs):
    return subprocess.run(arguments, check=check, capture_output=True, text=True, timeout=180, **kwargs)


def install_reviewed_sdk(selected):
    approval = json.loads((ROOT / ".github/release-candidates" / (selected["sdk_version"] + ".json")).read_text())
    wheel_name = next(name for name in approval["files"] if name.endswith(".whl"))
    call(
        sys.executable,
        "-m",
        "pip",
        "download",
        "--no-deps",
        "--only-binary=:all:",
        "--dest",
        str(OUTPUT),
        "durable-workflow==" + selected["sdk_version"],
    )
    wheel = OUTPUT / wheel_name
    if hashlib.sha256(wheel.read_bytes()).hexdigest() != approval["files"][wheel_name]:
        raise ValueError("registry wheel differs from the reviewed release")
    call(
        sys.executable,
        "-m",
        "pip",
        "install",
        "--only-binary=:all:",
        str(wheel),
        "pytest==9.2.0",
        "pytest-asyncio==1.5.0",
    )
    if importlib.metadata.version("durable-workflow") != selected["sdk_version"]:
        raise ValueError("installed SDK version differs")
    (OUTPUT / "installed-packages.txt").write_text(call(sys.executable, "-m", "pip", "freeze").stdout)
    return approval, wheel_name


def verify_published_image(selected):
    image = selected["server_image"]
    call("docker", "pull", "--platform", "linux/amd64", image)
    index = json.loads(call("docker", "buildx", "imagetools", "inspect", "--raw", image).stdout)
    platforms = [
        item["digest"]
        for item in index["manifests"]
        if item.get("platform") == {"architecture": "amd64", "os": "linux"}
    ]
    observed_image = json.loads(call("docker", "image", "inspect", image).stdout)[0]
    if (
        platforms != [selected["server_amd64_manifest"]]
        or observed_image["Architecture"] != "amd64"
        or observed_image["Os"] != "linux"
        or observed_image["Config"]["Labels"]["org.opencontainers.image.revision"] != selected["server_source"]
        or image not in observed_image["RepoDigests"]
    ):
        raise ValueError("runtime image differs from the qualified tuple")
    return observed_image


def runtime_url(identity):
    port = call("docker", "port", identity, "8080/tcp").stdout.strip()
    if re.fullmatch(r"127\.0\.0\.1:\d{1,5}", port, flags=re.ASCII) is None:
        raise ValueError("published runtime did not bind numeric loopback")
    if not 1 <= int(port.rsplit(":", 1)[1]) <= 65535:
        raise ValueError("published runtime port is outside its range")
    url = urlunsplit(("http", port, "", "", ""))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        try:
            with opener.open(url + "/api/health", timeout=2) as response:
                if response.status == 200:
                    break
        except OSError:
            time.sleep(0.2)
    else:
        raise TimeoutError("published runtime readiness")
    return url


def remove_owned_runtime(identity, nonce, report):
    record = json.loads(call("docker", "inspect", identity).stdout)[0]
    labels = record["Config"]["Labels"]
    if (
        record["Id"] != identity
        or labels.get("dev.durable-workflow.fixture-owner") != "sdk-python"
        or labels.get("dev.durable-workflow.fixture-run") != nonce
    ):
        raise ValueError("container cleanup ownership differs")
    report["oom_killed"] = record["State"]["OOMKilled"]
    logs = call("docker", "logs", identity, check=False)
    (OUTPUT / "server.log").write_text(logs.stdout + logs.stderr)
    volumes = [mount["Name"] for mount in record["Mounts"] if mount["Type"] == "volume"]
    call("docker", "rm", "-f", "-v", identity)
    absent = call("docker", "ps", "-aq", "--no-trunc", "--filter", "id=" + identity).stdout.strip() == ""
    report["container_absent"] = absent
    report["volumes_absent"] = all(
        call("docker", "volume", "ls", "-q", "--filter", "name=^" + volume + "$").stdout.strip() == ""
        for volume in volumes
    )
    report["complete"] = report["complete"] and absent and report["volumes_absent"] and not report["oom_killed"]


def main():
    OUTPUT.mkdir()
    selected = json.loads((ROOT / "tests/fixtures/published-activity-acceptance.json").read_text())
    approval, wheel_name = install_reviewed_sdk(selected)
    image = selected["server_image"]
    observed_image = verify_published_image(selected)
    nonce = uuid4().hex
    identity = None
    report = {
        "tuple": selected,
        "wheel_sha256": approval["files"][wheel_name],
        "image_id": observed_image["Id"],
        "complete": False,
    }
    try:
        identity = call(
            "docker",
            "create",
            "--name",
            "sdk-python-published-activity-" + nonce[:12],
            "--label",
            "dev.durable-workflow.fixture-owner=sdk-python",
            "--label",
            "dev.durable-workflow.fixture-run=" + nonce,
            "--init",
            "--read-only",
            "--cpus",
            "1",
            "--memory",
            "1g",
            "--pids-limit",
            "256",
            "--tmpfs",
            "/tmp:mode=1777,size=128m",
            "--tmpfs",
            "/app/database:uid=1000,gid=1000,mode=0700,size=128m",
            "--tmpfs",
            "/app/storage:uid=1000,gid=1000,mode=0700,size=64m",
            "-e",
            "DB_CONNECTION=sqlite",
            "-e",
            "DB_DATABASE=/app/database/activity-identity.sqlite",
            "-e",
            "DW_AUTH_TOKEN=published-activity-test-token",
            "-e",
            "DW_RUNTIME_CREDENTIALS_ENABLED=false",
            "-p",
            "127.0.0.1::8080",
            image,
        ).stdout.strip()
        if re.fullmatch(r"[0-9a-f]{64}", identity) is None:
            raise ValueError("Docker did not return one full container ID")
        report["container_id"] = identity
        call("docker", "start", identity)
        url = runtime_url(identity)
        environment = {
            **os.environ,
            "DURABLE_WORKFLOW_SERVER_URL": url,
            "DURABLE_WORKFLOW_AUTH_TOKEN": "published-activity-test-token",
            "DURABLE_WORKFLOW_EXPECT_INSTALLED_VERSION": selected["sdk_version"],
        }
        # Ignore project pytest pythonpath settings so the checkout's src tree
        # cannot replace the installed SDK during this published-package cell.
        result = call(
            sys.executable,
            "-m",
            "pytest",
            "-c",
            "/dev/null",
            "--asyncio-mode=auto",
            "-v",
            "--capture=tee-sys",
            str(ROOT / "tests/integration/test_activity_identity.py"),
            "--junitxml=" + str(OUTPUT / "identity.xml"),
            env=environment,
            check=False,
        )
        (OUTPUT / "identity.log").write_text(result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError("published activity identity case failed")
        report["complete"] = True
    finally:
        if identity and re.fullmatch(r"[0-9a-f]{64}", identity):
            remove_owned_runtime(identity, nonce, report)
        (OUTPUT / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    if not report["complete"]:
        raise RuntimeError("published activity result or cleanup is incomplete")


if __name__ == "__main__":
    main()
