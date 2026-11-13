"""Run one disposable installed-Python response-loss cohort, with exact cleanup."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

EXPECTED_SERVER = {
    "version": "3.9.2",
    "source": "5614274d2629d03f92932a07dfee0fb10349ddd9",
    "index": "sha256:8ffcdb2a36af3129a1987849aafe8715fe0d3bf9a9648467f0b5cb8eeef43ba9",
    "linux_amd64": "sha256:813c9337c44be7d497efd29e1b463546519e488aa796b5ecf9dfc6f4f10d2128",
    "qualification": "https://github.com/durable-workflow/server/pull/1209#issuecomment-6858140177",
}


def image_identity(document):
    image = document[0]
    if (
        image["Architecture"] != "amd64"
        or image["Os"] != "linux"
        or image["Config"]["Labels"]["org.opencontainers.image.revision"] != EXPECTED_SERVER["source"]
        or not any(value.endswith("@" + EXPECTED_SERVER["index"]) for value in image["RepoDigests"])
    ):
        raise ValueError("local runtime image differs from the qualified source/index/platform")
    return {
        "id": image["Id"],
        "architecture": image["Architecture"],
        "os": image["Os"],
        "revision": image["Config"]["Labels"]["org.opencontainers.image.revision"],
        "repo_digests": image["RepoDigests"],
    }


def run(args):
    root = args.output.resolve()
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    config = root / "docker-config"
    config.mkdir(mode=0o700)
    (config / "config.json").write_text('{"auths":{}}\n')
    scratch = root / "tmp"
    scratch.mkdir(mode=0o700)
    environment = {"PATH": os.defpath, "TMPDIR": str(scratch), "PYTHONUNBUFFERED": "1"}
    docker = ["docker", "--config", str(config)]

    def call(*arguments, check=True, timeout=45):
        return subprocess.run(
            docker + list(arguments), check=check, capture_output=True, text=True, timeout=timeout, env=environment
        )

    image = "ghcr.io/durable-workflow/server@" + EXPECTED_SERVER["index"]
    observed = image_identity(json.loads(call("image", "inspect", image).stdout))
    name = "sdk-python-216-response-loss-" + uuid4().hex[:12]
    credential = root / "fixture.env"
    descriptor = os.open(credential, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(
            "DW_AUTH_TOKEN=" + secrets.token_hex(24) + "\nDW_RUNTIME_CREDENTIALS_ENABLED=false\nDB_CONNECTION=sqlite\n"
        )
    source = Path(__file__).with_name("response_loss.py")
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "expected_server": EXPECTED_SERVER,
        "observed_image": observed,
        "controller_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "adapter_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "complete": False,
    }
    volumes = []
    created = False
    try:
        call(
            "run",
            "-d",
            "--rm",
            "--init",
            "--read-only",
            "--name",
            name,
            "--cpus",
            "1",
            "--memory",
            "1g",
            "--pids-limit",
            "256",
            "-p",
            "127.0.0.1::8080",
            "--env-file",
            str(credential),
            image,
        )
        created = True
        inspection = json.loads(call("inspect", name).stdout)[0]
        volumes = [mount["Name"] for mount in inspection["Mounts"] if mount["Type"] == "volume"]
        port = call("port", name, "8080/tcp").stdout.strip().removeprefix("127.0.0.1:")
        if not port.isascii() or not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("runtime did not bind an explicit loopback port")
        upstream = "http://127.0.0.1:" + port
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(upstream + "/api/health", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                time.sleep(0.2)
        else:
            raise TimeoutError("synthetic runtime readiness")
        with (root / "consumer.log").open("x") as log:
            result = subprocess.run(
                [
                    str(args.python.absolute()),
                    "-I",
                    str(source),
                    "--upstream",
                    upstream,
                    "--credential-file",
                    str(credential),
                    "--proxy-source",
                    str(args.proxy.resolve()),
                    "--wheel",
                    str(args.wheel.resolve()),
                    "--server-version",
                    EXPECTED_SERVER["version"],
                    "--output",
                    str(root / "consumer.json"),
                ],
                cwd=root,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=600,
            )
        report["consumer_exit_code"] = result.returncode
        if result.returncode != 0:
            raise RuntimeError("installed consumer failed; preserve private log and incomplete evidence")
        consumer = json.loads((root / "consumer.json").read_text())
        if not consumer["complete"] or consumer["adapter_sha256"] != report["adapter_sha256"]:
            raise ValueError("consumer proof is incomplete or source changed")
    except Exception as error:
        report["failure_type"] = type(error).__name__
        raise
    finally:
        final = call("inspect", name, check=False)
        if final.returncode == 0:
            inspected = json.loads(final.stdout)[0]
            volumes = [mount["Name"] for mount in inspected["Mounts"] if mount["Type"] == "volume"]
            report["oom_killed"] = inspected["State"]["OOMKilled"]
            call("rm", "-f", "-v", name)
        elif created:
            report["runtime_disappeared_before_cleanup"] = True
        credential.unlink(missing_ok=True)
        checked = call("inspect", name, check=False)
        absent = checked.returncode == 1 and f"No such object: {name}" in checked.stderr
        remaining = []
        for volume in volumes:
            checked = call("volume", "inspect", volume, check=False)
            if not (checked.returncode == 1 and f"no such volume: {volume}" in checked.stderr):
                remaining.append(volume)
        report["cleanup"] = {
            "container_absent": absent,
            "volumes_removed": len(volumes) - len(remaining),
            "volumes_remaining": len(remaining),
            "credential_removed": not credential.exists(),
        }
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["complete"] = (
            report.get("consumer_exit_code") == 0
            and absent
            and not remaining
            and report.get("oom_killed") is False
            and "failure_type" not in report
        )
        (root / "run.json").write_text(json.dumps(report, indent=2) + "\n")
        if not absent or remaining:
            raise RuntimeError("owned runtime cleanup incomplete")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True, help="registry-only virtualenv Python")
    parser.add_argument("--wheel", type=Path, required=True, help="downloaded bound PyPI wheel")
    parser.add_argument("--proxy", type=Path, required=True, help="exact accepted shared proxy")
    parser.add_argument("--output", type=Path, required=True, help="new private disk-backed directory")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
