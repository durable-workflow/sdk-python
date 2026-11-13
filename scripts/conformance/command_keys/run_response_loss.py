"""Run the published SDK in a bound interpreter with captured executable inputs."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import subprocess
import time
import urllib.request
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from uuid import uuid4

EXPECTED_SERVER = {
    "version": "3.9.2",
    "source": "5614274d2629d03f92932a07dfee0fb10349ddd9",
    "index": "sha256:8ffcdb2a36af3129a1987849aafe8715fe0d3bf9a9648467f0b5cb8eeef43ba9",
    "linux_amd64": "sha256:813c9337c44be7d497efd29e1b463546519e488aa796b5ecf9dfc6f4f10d2128",
    "qualification": "https://github.com/durable-workflow/server/pull/1209#issuecomment-6858140177",
}
PYTHON_INDEX = "sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df"
PYTHON_AMD64 = "sha256:0f086103b026048cc50cb5da25ec435c023c5241a83fc44daea593377abc61c4"
PYTHON_VERSION = "3.12.16"
WHEEL_SHA256 = "7dc77d3882fa5cbddbeaee6a4cc63b05285e280523c8ea40dc31e03cc12a40bc"
PROXY_SHA256 = "bd1e8ccbbf76f63fc2eafa278a023eb181d405b68d9c30016392e395a1eff725"
RESULT_PREFIX = "SDK_PYTHON_CONSUMER_RESULT="


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


def interpreter_identity(document):
    image = document[0]
    versions = [value for value in image["Config"]["Env"] if value.startswith("PYTHON_VERSION=")]
    if (
        image["Architecture"] != "amd64"
        or image["Os"] != "linux"
        or not any(value.endswith("@" + PYTHON_INDEX) for value in image["RepoDigests"])
        or versions != ["PYTHON_VERSION=" + PYTHON_VERSION]
    ):
        raise ValueError("require the fixed published Python interpreter image")
    return {
        "id": image["Id"],
        "index": PYTHON_INDEX,
        "expected_amd64": PYTHON_AMD64,
        "version": PYTHON_VERSION,
        "architecture": "amd64",
    }


def missing_object(result, name, *, volume=False):
    expected = f"Error response from daemon: get {name}: no such volume" if volume else f"error: no such object: {name}"
    return result.returncode == 1 and result.stderr.strip() == expected


def git_bytes(root, commit, relative):
    return subprocess.check_output(
        ["git", "-C", str(root), "show", commit + ":" + relative], env={"PATH": os.defpath}, timeout=10
    )


def captured_inputs(args, stack):
    if re.fullmatch(r"[0-9a-f]{40}", args.runner_commit) is None:
        raise ValueError("runner commit must be one exact full Git SHA")
    root = Path(__file__).absolute().parents[3]
    head = subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True, env={"PATH": os.defpath}, timeout=10
    ).strip()
    if head != args.runner_commit:
        raise ValueError("checkout differs from the declared reviewed runner commit")
    prefix = "scripts/conformance/command_keys/"
    guard_bytes = git_bytes(root, head, prefix + "input_guards.py")
    guards = ModuleType("reviewed_input_guards")
    # Load the immutable Git object, never a second lookup of a mutable file.
    exec(compile(guard_bytes, "reviewed-input-guards.py", "exec"), guards.__dict__)
    inputs = {}
    for role, filename in {
        "controller": "run_response_loss.py",
        "guard": "input_guards.py",
        "adapter": "response_loss.py",
        "bootstrap": "consumer_bootstrap.py",
        "requirements": "consumer-requirements.txt",
    }.items():
        expected = hashlib.sha256(git_bytes(root, head, prefix + filename)).hexdigest()
        inputs[role] = stack.enter_context(
            guards.CapturedInput(root / prefix / filename, role, maximum=256 * 1024, expected=expected)
        )
    inputs["proxy"] = stack.enter_context(
        guards.CapturedInput(args.proxy, "proxy", maximum=64 * 1024, expected=PROXY_SHA256)
    )
    inputs["wheel"] = stack.enter_context(
        guards.CapturedInput(args.wheel, "wheel", maximum=4 * 1024 * 1024, expected=WHEEL_SHA256)
    )
    return inputs


def consumer_result(stdout, inputs):
    lines = [line[len(RESULT_PREFIX) :] for line in stdout.splitlines() if line.startswith(RESULT_PREFIX)]
    if len(lines) != 1:
        raise ValueError("consumer did not emit exactly one bound result")
    result = json.loads(lines[0])
    if (
        result.get("complete") is not True
        or result.get("adapter_sha256") != inputs["adapter"].sha256
        or result.get("proxy_sha256") != PROXY_SHA256
        or result.get("sdk", {}).get("wheel_sha256") != WHEEL_SHA256
        or result.get("sdk", {}).get("python") != PYTHON_VERSION
        or result.get("toolchain", {}).get("post_inventory_equal") is not True
        or result["toolchain"].get("requirements_sha256") != inputs["requirements"].sha256
    ):
        raise ValueError("consumer result does not bind the captured inputs/toolchain")
    return result


def run_bound(args, inputs):
    root = args.output.absolute()
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    config = root / "docker-config"
    config.mkdir(mode=0o700)
    (config / "config.json").write_text('{"auths":{}}\n')
    environment = {"PATH": os.defpath, "TMPDIR": str(root)}
    docker = ["docker", "--config", str(config)]

    def call(*arguments, check=True, timeout=45):
        return subprocess.run(
            docker + list(arguments), check=check, capture_output=True, text=True, timeout=timeout, env=environment
        )

    image = "ghcr.io/durable-workflow/server@" + EXPECTED_SERVER["index"]
    python_image = "docker.io/library/python@" + PYTHON_INDEX
    observed = image_identity(json.loads(call("image", "inspect", image).stdout))
    interpreter = interpreter_identity(json.loads(call("image", "inspect", python_image).stdout))
    suffix = uuid4().hex[:12]
    names = ["sdk-python-216-response-loss-" + suffix, "sdk-python-216-consumer-" + suffix]
    token = secrets.token_hex(24)
    credential = root / "fixture.env"
    descriptor = os.open(credential, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write("DW_AUTH_TOKEN=" + token + "\nDW_RUNTIME_CREDENTIALS_ENABLED=false\nDB_CONNECTION=sqlite\n")
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "runner_commit": args.runner_commit,
        "expected_server": EXPECTED_SERVER,
        "observed_image": observed,
        "interpreter_image": interpreter,
        "inputs": {role: {"sha256": value.sha256, "bytes": len(value.data)} for role, value in inputs.items()},
        "complete": False,
    }
    try:
        for value in inputs.values():
            value.recheck()
        call(
            "run",
            "-d",
            "--init",
            "--read-only",
            "--name",
            names[0],
            "--cpus",
            "1",
            "--memory",
            "1g",
            "--pids-limit",
            "256",
            "--tmpfs",
            "/tmp:mode=1777,size=128m",
            "--tmpfs",
            "/app/storage:uid=1000,gid=1000,mode=0700,size=64m",
            "--tmpfs",
            "/app/database:uid=1000,gid=1000,mode=0700,size=64m",
            "-p",
            "127.0.0.1::8080",
            "--env-file",
            str(credential),
            "-e",
            "DB_DATABASE=/app/database/response-loss.sqlite",
            image,
        )
        address = call("port", names[0], "8080/tcp").stdout.strip()
        host, port = address.split(":")
        if host != "127.0.0.1" or not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("runtime did not bind numeric loopback")
        upstream = "http://" + address
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                with opener.open(upstream + "/api/health", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                time.sleep(0.2)
        else:
            raise TimeoutError("synthetic runtime readiness")
        bundle = {
            "sources": {
                role: {"sha256": value.sha256, "base64": base64.b64encode(value.data).decode()}
                for role, value in inputs.items()
                if role in {"adapter", "bootstrap", "requirements", "proxy", "wheel"}
            },
            "upstream": upstream,
            "token": token,
            "server_version": EXPECTED_SERVER["version"],
        }
        for value in inputs.values():
            value.recheck()
        # Host networking is needed only for the fixture's numeric loopback.
        # It has no host mounts or inherited host environment. Only the synthetic
        # runtime token is delivered through the captured stdin bundle.
        result = subprocess.run(
            docker
            + [
                "run",
                "--init",
                "--read-only",
                "--name",
                names[1],
                "--network",
                "host",
                "--cpus",
                "1",
                "--memory",
                "1g",
                "--pids-limit",
                "256",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "--tmpfs",
                "/tmp:mode=1777,size=512m",
                "-i",
                "--entrypoint",
                "/usr/local/bin/python3.12",
                python_image,
                "-I",
                "-B",
                "-c",
                inputs["bootstrap"].data.decode(),
            ],
            input=json.dumps(bundle),
            capture_output=True,
            text=True,
            env=environment,
            timeout=600,
        )
        (root / "consumer.log").write_text(result.stdout + result.stderr)
        report["consumer_exit_code"] = result.returncode
        for value in inputs.values():
            value.recheck()
        report["input_identities_rechecked"] = True
        if interpreter_identity(json.loads(call("image", "inspect", python_image).stdout)) != interpreter:
            raise ValueError("interpreter image identity changed")
        report["interpreter_identity_rechecked"] = True
        if result.returncode != 0:
            raise RuntimeError("installed consumer failed; retain its private incomplete evidence")
        consumer = consumer_result(result.stdout, inputs)
        (root / "consumer.json").write_text(json.dumps(consumer, indent=2) + "\n")
        report["consumer_complete"] = True
    except Exception as error:
        report["failure_type"] = type(error).__name__
        raise
    finally:
        clean = []
        oom = []
        try:
            for name in names:
                inspection = call("inspect", name, check=False)
                volumes = []
                if inspection.returncode == 0:
                    state = json.loads(inspection.stdout)[0]
                    volumes = [mount["Name"] for mount in state["Mounts"] if mount["Type"] == "volume"]
                    oom.append(state["State"]["OOMKilled"])
                    call("rm", "-f", "-v", name)
                absent = missing_object(call("inspect", name, check=False), name)
                absent_volumes = all(
                    missing_object(call("volume", "inspect", volume, check=False), volume, volume=True)
                    for volume in volumes
                )
                clean.append(absent and absent_volumes)
        except Exception as error:
            report["cleanup_failure_type"] = type(error).__name__
        finally:
            credential.unlink(missing_ok=True)
            report["cleanup"] = {
                "containers_and_volumes_absent": len(clean) == 2 and all(clean),
                "credential_removed": not credential.exists(),
                "oom_killed": any(oom),
            }
            report["finished_at"] = datetime.now(timezone.utc).isoformat()
            report["complete"] = (
                report.get("consumer_complete") is True
                and len(clean) == 2
                and all(clean)
                and len(oom) == 2
                and not any(oom)
                and "failure_type" not in report
                and "cleanup_failure_type" not in report
            )
            (root / "run.json").write_text(json.dumps(report, indent=2) + "\n")
        if not report["cleanup"]["containers_and_volumes_absent"]:
            raise RuntimeError("owned runtime cleanup incomplete")
        if not report["complete"] and "failure_type" not in report:
            raise RuntimeError("installed cohort evidence is incomplete")


def run(args):
    with ExitStack() as stack:
        inputs = captured_inputs(args, stack)
        run_bound(args, inputs)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-commit", required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--proxy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
