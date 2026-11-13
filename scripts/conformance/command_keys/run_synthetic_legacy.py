"""Run one synthetic SQLite legacy cell using the accepted unchanged helper."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import sqlite3
import subprocess
import time
import urllib.request
from contextlib import ExitStack, closing
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from uuid import uuid4

HELPER_SHA256 = "6fc6054ce087c136a86d7b6b222daeefaa87c58fa0b613b155a5df39e6a26fad"
MANIFEST_SHA256 = "da1990000c611d7377690c7526487d6415fa4f9cb42d299f59585a7c2eebf769"
SCENARIOS = {"readback", "two-rows", "legacy-token", "modern-raw", "case-only"}


def load(data, name, filename):
    module = ModuleType(name)
    module.__file__ = filename
    exec(compile(data, filename, "exec"), module.__dict__)
    return module


def controls_from_git(args):
    if re.fullmatch(r"[0-9a-f]{40}", args.runner_commit) is None:
        raise ValueError("require an exact runner commit")
    root = Path(__file__).absolute().parents[3]
    relative = "scripts/conformance/command_keys/run_response_loss.py"
    data = subprocess.check_output(
        ["git", "-C", str(root), "show", args.runner_commit + ":" + relative], env={"PATH": os.defpath}, timeout=10
    )
    return load(data, "reviewed_synthetic_controls", str(root / relative))


def accepted_helper(inputs):
    helper = inputs["snapshot_helper"].data
    manifest = inputs["manifest"].data
    if hashlib.sha256(helper).hexdigest() != HELPER_SHA256 or hashlib.sha256(manifest).hexdigest() != MANIFEST_SHA256:
        raise ValueError("accepted helper/manifest differs")
    module = load(helper, "accepted_synthetic_helper", "captured-sqlite-legacy.py")
    # The unchanged helper's manifest() opens its sibling file. Bind that single
    # data lookup to the captured accepted bytes, avoiding any pathname reopen.
    module.manifest = lambda: json.loads(manifest)
    return module


def validate_cell(state, scenario):
    if state not in {"open", "completed", "continued"} or scenario not in SCENARIOS:
        raise ValueError("unknown synthetic cell")
    if state != "open" and scenario != "readback":
        raise ValueError("ambiguity/case scenarios require their own open cell")


def run_bound(args, inputs, controls, workspace):
    validate_cell(args.state, args.scenario)
    legacy = accepted_helper(inputs)
    delta = load(inputs["delta"].data, "captured_case_delta", "captured-case-delta.py")
    database_fd = workspace.directory("database")
    database = Path(workspace.proc(database_fd))
    canonical_database = Path(workspace.mount_path(database_fd))
    held_databases = {}
    config = workspace.directory("docker-config")
    workspace.write("config.json", '{"auths":{}}\n', parent=config)
    environment = {"PATH": os.defpath, "TMPDIR": workspace.proc(workspace.root)}
    docker = ["docker", "--config", workspace.proc(config)]

    def call(*arguments, check=True, timeout=45):
        workspace.check()
        result = subprocess.run(
            docker + list(arguments), env=environment, check=check, capture_output=True, text=True, timeout=timeout
        )
        if arguments[0] != "create":
            workspace.check()
        return result

    def cleanup_call(*arguments, check=True, timeout=45):
        return subprocess.run(
            ["docker", "--config", "/proc/self/fd", *arguments],
            env=environment,
            check=check,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    image = "ghcr.io/durable-workflow/server@" + controls.EXPECTED_SERVER["index"]
    python_image = "docker.io/library/python@" + controls.PYTHON_INDEX
    observed = controls.image_identity(json.loads(call("image", "inspect", image).stdout))
    interpreter = controls.interpreter_identity(json.loads(call("image", "inspect", python_image).stdout))
    nonce = uuid4().hex
    token = secrets.token_hex(24)
    credential = workspace.write(
        "fixture.env",
        "DW_AUTH_TOKEN="
        + token
        + "\nDW_RUNTIME_CREDENTIALS_ENABLED=false\nDW_RUST_MAINTENANCE_LOOP=0\nDB_CONNECTION=sqlite\n",
    )
    report = {
        "runner_commit": args.runner_commit,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "scope": "synthetic legacy " + args.scenario + " " + args.state,
        "expected_server": controls.EXPECTED_SERVER,
        "observed_image": observed,
        "interpreter_image": interpreter,
        "helper_source": "4add871b175517f2031f71c868ce278bbfbba486",
        "manifest_source": "d66e33bfb5c76f94c5b202f8c9f7a9e8cd75cf1c",
        "ownership_nonce": nonce,
        "inputs": {role: {"sha256": item.sha256, "bytes": len(item.data)} for role, item in inputs.items()},
        "owned_containers": {},
        "cleanup_by_id": {},
        "complete": False,
    }

    def bind_id(role, identity):
        if identity in [entry["id"] for entry in report["owned_containers"].values()]:
            raise ValueError("duplicate container identity")
        report["owned_containers"][role] = {"id": identity}
        if controls.inspect_owned(call, identity, nonce) is None:
            raise ValueError("created object vanished")
        return identity

    def start(role, filename):
        identity = bind_id(
            role,
            controls.created_id(
                call(
                    "create",
                    "--name",
                    "sdk-python-216-synthetic-" + role + "-" + nonce[:12],
                    "--label",
                    controls.OWNER_LABEL + "=sdk-python",
                    "--label",
                    controls.RUN_LABEL + "=" + nonce,
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
                    "/app/storage:uid=1000,gid=1000,mode=0700,size=64m",
                    "--tmpfs",
                    "/app/database:uid=1000,gid=1000,mode=0700,size=16m",
                    "-v",
                    str(canonical_database) + ":/cohort:rw",
                    "--env-file",
                    workspace.proc(credential["fd"]),
                    "-e",
                    "DB_DATABASE=/cohort/" + filename,
                    "-p",
                    "127.0.0.1::8080",
                    "--entrypoint",
                    "/bin/sh",
                    image,
                    *workspace.native_arguments(database_fd, held_databases[filename]),
                ).stdout
            ),
        )
        call("start", identity)
        address = call("port", identity, "8080/tcp").stdout.strip()
        host, port = address.split(":")
        if host != "127.0.0.1" or not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("runtime is not numeric loopback")
        upstream = "http://" + address
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                with opener.open(upstream + "/api/health", timeout=2) as response:
                    if response.status == 200:
                        return identity, upstream
            except OSError:
                time.sleep(0.2)
        raise TimeoutError("synthetic runtime readiness")

    def stop(role):
        identity = report["owned_containers"][role]["id"]
        if controls.inspect_owned(call, identity, nonce) is None:
            raise ValueError("owned runtime vanished before stopped-copy attestation")
        call("stop", "--time", "10", identity)
        state = controls.inspect_owned(call, identity, nonce)
        if state is None or state["State"]["Running"] or state["State"]["ExitCode"] != 0:
            raise ValueError("owned runtime did not stop cleanly")
        report["cleanup_by_id"][role] = controls.cleanup_owned(cleanup_call, identity, nonce)

    def snapshot(path):
        workspace.check()
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as connection:
            connection.execute("BEGIN")
            result = legacy.snapshot(connection)
        workspace.check()
        return result

    def consume(phase, upstream, seed=None):
        for value in inputs.values():
            value.recheck()
        payload = {
            "schema": "sdk-python.synthetic-legacy-input.v1",
            "manifest_sha256": MANIFEST_SHA256,
            "phase": phase,
            "state": args.state,
            "scenario": args.scenario,
        }
        if seed is not None:
            payload["seed"] = seed
        data = json.dumps(payload, separators=(",", ":")).encode()
        captured = {
            role: value
            for role, value in inputs.items()
            if role in {"adapter", "bootstrap", "verifier", "wheel", "requirements"}
        }
        captured["seed"] = SimpleNamespace(data=data, sha256=hashlib.sha256(data).hexdigest())
        bundle = {
            "scenario": "synthetic_legacy",
            "upstream": upstream,
            "token": token,
            "server_version": controls.EXPECTED_SERVER["version"],
            "sources": {
                role: {"sha256": value.sha256, "base64": base64.b64encode(value.data).decode()}
                for role, value in captured.items()
            },
        }
        identity = bind_id(
            phase, controls.create_consumer(call, "sdk-python-216-synthetic-" + phase + "-" + nonce[:12], nonce, inputs)
        )
        result = subprocess.run(
            docker + ["start", "-a", "-i", identity],
            input=json.dumps(bundle),
            env=environment,
            capture_output=True,
            text=True,
            timeout=600,
        )
        workspace.write(phase + ".log", result.stdout + result.stderr)
        state = controls.inspect_owned(call, identity, nonce)
        if state is None or state["State"]["Running"] or state["State"]["ExitCode"] != 0 or result.returncode != 0:
            raise RuntimeError("installed synthetic consumer failed")
        proof = controls.consumer_result(result.stdout, captured, scenario="synthetic_legacy")
        workspace.write(phase + ".json", json.dumps(proof, indent=2) + "\n")
        for value in inputs.values():
            value.recheck()
        report["cleanup_by_id"][phase] = controls.cleanup_owned(cleanup_call, identity, nonce)
        return proof

    try:
        held_databases["source.sqlite"] = workspace.write("source.sqlite", b"", parent=database_fd, immutable=False)
        _, upstream = start("source", "source.sqlite")
        seed = consume("prepare", upstream)["seed"]
        stop("source")
        source, copied = database / "source.sqlite", database / "copied.sqlite"
        if args.scenario == "modern-raw":
            # No transform is appropriate for this ambiguity cell. SQLite backup
            # still creates a separately owned complete copy of the stopped DB.
            held_databases["copied.sqlite"] = workspace.write("copied.sqlite", b"", parent=database_fd, immutable=False)
            workspace.check()
            with (
                closing(sqlite3.connect(source.as_uri() + "?mode=ro&immutable=1", uri=True)) as original,
                closing(sqlite3.connect(copied)) as destination,
            ):
                original.backup(destination)
            if snapshot(source) != snapshot(copied):
                raise ValueError("unmodified modern copy differs")
            report["transform"] = {"transformed": 0, "manifest_sha256": MANIFEST_SHA256}
        else:
            workspace.check()
            report["transform"] = legacy.transform_copy(
                canonical_database / source.name, canonical_database / copied.name, seed["selections"], stopped=True
            )
            workspace.check()
            held_databases["copied.sqlite"] = workspace.capture(database_fd, "copied.sqlite")
        stopped_copy = snapshot(copied)
        stopped_fingerprint = legacy.fingerprint(stopped_copy)
        if args.scenario != "modern-raw" and report["transform"]["after_sha256"] != stopped_fingerprint:
            raise ValueError("stopped copy no longer matches the accepted transform")
        report["stopped_copy_sha256"] = stopped_fingerprint
        workspace.write("stopped-copy.json", json.dumps(stopped_copy, sort_keys=True) + "\n")
        _, upstream = start("readback", copied.name)
        before = snapshot(copied)
        delta.validate_restart(stopped_copy, before)
        report["restarted_copy_sha256"] = legacy.fingerprint(before)
        workspace.write("restarted-copy.json", json.dumps(before, sort_keys=True) + "\n")
        proof = consume("read", upstream, seed)
        after = snapshot(copied)
        if before != after:
            raise ValueError("legacy readback/ambiguity window changed complete logical rows/schema")
        report["zero_write_sha256"] = legacy.fingerprint(before)
        report["cells"] = proof["cells"]
        if args.scenario == "case-only":
            before = snapshot(copied)
            distinct_proof = consume("case_distinct", upstream, seed)
            report["distinct_cells"] = distinct_proof["cells"]
            after = snapshot(copied)
            workspace.write("case-before.json", json.dumps(before, sort_keys=True) + "\n")
            workspace.write("case-after.json", json.dumps(after, sort_keys=True) + "\n")
            report["case_distinct_delta"] = delta.validate(before, after, seed, distinct_proof)
            report["case_distinct_writes"] = {
                "before_sha256": legacy.fingerprint(before),
                "after_sha256": legacy.fingerprint(after),
            }
        report["consumer_complete"] = True
        workspace.check()
        report["workspace_identities_rechecked"] = True
    except Exception as error:
        report["failure_type"] = type(error).__name__
        raise
    finally:
        for role, entry in report["owned_containers"].items():
            if role not in report["cleanup_by_id"]:
                try:
                    report["cleanup_by_id"][role] = controls.cleanup_owned(cleanup_call, entry["id"], nonce)
                except Exception as error:
                    report["cleanup_by_id"][role] = {"id": entry["id"], "failure_type": type(error).__name__}
        clean = bool(report["cleanup_by_id"]) and all(
            value.get("absent") and value.get("volumes_absent") for value in report["cleanup_by_id"].values()
        )
        workspace.remove(credential)
        if clean:
            workspace.remove_database(database_fd)
        try:
            for value in inputs.values():
                value.recheck()
            report["input_identities_rechecked"] = True
            if controls.interpreter_identity(json.loads(call("image", "inspect", python_image).stdout)) != interpreter:
                raise ValueError("interpreter image identity changed")
            report["interpreter_identity_rechecked"] = True
        except Exception as error:
            report["post_input_failure_type"] = type(error).__name__
        report["cleanup"] = {
            "containers_and_volumes_absent": clean,
            "database_removed": clean,
            "credential_removed": True,
        }
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["complete"] = (
            clean
            and report.get("consumer_complete") is True
            and report.get("workspace_identities_rechecked") is True
            and "failure_type" not in report
            and "post_input_failure_type" not in report
            and all(value.get("oom_killed") is False for value in report["cleanup_by_id"].values())
        )
        workspace.write("run.json", json.dumps(report, indent=2) + "\n")
        workspace.check()
        if not clean:
            raise RuntimeError("synthetic owned cleanup incomplete")
        if not report["complete"] and "failure_type" not in report:
            raise RuntimeError("synthetic evidence incomplete")


def run(args):
    if not __debug__:
        raise ValueError("run acceptance without optimization")
    validate_cell(args.state, args.scenario)
    controls = controls_from_git(args)
    with ExitStack() as stack:
        inputs = controls.captured_inputs(
            args,
            stack,
            source_files={
                "controller": "run_synthetic_legacy.py",
                "support": "run_response_loss.py",
                "adapter": "synthetic_legacy.py",
                "verifier": "response_loss.py",
                "delta": "case_delta.py",
            },
            data_files={
                "wheel": (args.wheel, 4 * 1024 * 1024, controls.WHEEL_SHA256),
                "snapshot_helper": (args.snapshot_helper, 128 * 1024, HELPER_SHA256),
                "manifest": (args.manifest, 128 * 1024, MANIFEST_SHA256),
            },
        )
        workspace = stack.enter_context(controls.workspace_for(inputs, args.output))
        run_bound(args, inputs, controls, workspace)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-commit", required=True)
    parser.add_argument("--state", choices=["open", "completed", "continued"], default="open")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="readback")
    for name in ("wheel", "snapshot-helper", "manifest", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
