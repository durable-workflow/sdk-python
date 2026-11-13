"""Qualify one installed SDK consumer on a private copy of the producer seed."""

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
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from uuid import uuid4

HELPER_SHA256 = "6fc6054ce087c136a86d7b6b222daeefaa87c58fa0b613b155a5df39e6a26fad"
BASELINE_SHA256 = "7d1826f7681342bb3a5974509648e2e8d75e7a21d262f65f3ec1f58727ccbc0c"
SEED_SHA256 = "8c728ce827df3a11621dfb0e82cdd404bbab48784182a5e92c8d632d49047fc5"
STEP_SHA256 = "fc3ad1730682c09704027dcbabc17cd6ded9c642a3ff20d7988d5f4b4a5726fd"
JOURNAL_SHA384 = "53b752529a1f7addaefe62ab61160dd42b3990f805cfdc4ecd1b48927a6c2f7ce42526fc04522ac887a37208f343fc73"
JOURNAL_DESCRIPTION = (
    "full frozen PHP schema, native worker incarnations, session holders, stream reclaim deadlines, "
    "service call deadlines, service admission counters and committed external payload deletions and "
    "task ready origins and poll expiry indexes and bound run command contracts and task queue activity "
    "dispatch pauses and keyed command intents"
)


def load_bytes(data, name, expected, *, filename=None):
    if hashlib.sha256(data).hexdigest() != expected:
        raise ValueError("required executable source identity differs")
    module = ModuleType(name)
    module.__file__ = filename or "captured-" + name + ".py"
    exec(compile(data, module.__file__, "exec"), module.__dict__)
    module.source_sha256 = expected
    return module


def native_marker(connection, expected):
    if connection.execute("SELECT engine,version FROM dw_server_schema").fetchall() != [("rust-development", expected)]:
        raise ValueError("native schema ownership/version marker differs")


def validate_journal(table, began, finished):
    columns = table["columns"]
    if (
        set(columns) != {"version", "description", "installed_on", "success", "checksum", "execution_time"}
        or len(table["rows"]) != 1
    ):
        raise ValueError("upgraded migration journal shape differs")
    row = dict(zip(columns, table["rows"][0], strict=True))
    expected = {
        "version": ["integer", 13],
        "description": ["text", JOURNAL_DESCRIPTION],
        "success": ["integer", 1],
        "checksum": ["blob", JOURNAL_SHA384],
        "execution_time": ["integer", 0],
    }
    if any(row[key] != value for key, value in expected.items()):
        raise ValueError("upgraded migration journal field differs from tagged source")
    installed = row["installed_on"]
    if installed[0] != "text":
        raise ValueError("migration timestamp type differs")
    timestamp = datetime.fromisoformat(installed[1]).replace(tzinfo=timezone.utc).timestamp()
    if not int(began) <= timestamp <= finished:
        raise ValueError("migration timestamp is outside the actual upgrade window")


def validate_upgrade(before, after, added_schema, began, finished):
    # Preserve every business row and catalog entry. Only the exact source-defined
    # marker/journal changes and empty new intent table may differ.
    if sorted(before["schema"] + added_schema) != after["schema"]:
        raise ValueError("upgrade changed unexpected schema objects")
    validate_journal(after["tables"]["_sqlx_migrations"], began, finished)
    expected = deepcopy(before)
    expected["schema"] = after["schema"]
    marker = expected["tables"]["dw_server_schema"]
    version = marker["columns"].index("version")
    if len(marker["rows"]) != 1 or marker["rows"][0][version] != ["integer", 12]:
        raise ValueError("baseline marker differs")
    marker["rows"][0][version] = ["integer", 13]
    expected["tables"]["_sqlx_migrations"] = after["tables"]["_sqlx_migrations"]
    expected["tables"]["dw_command_intents"] = {"columns": ["command_id", "namespace", "intent_digest"], "rows": []}
    if expected != after:
        raise ValueError("upgrade changed business rows or unexpected metadata")


def controls_from_git(args):
    if re.fullmatch(r"[0-9a-f]{40}", args.runner_commit) is None:
        raise ValueError("require one exact runner commit")
    root = Path(__file__).absolute().parents[3]
    relative = "scripts/conformance/command_keys/run_response_loss.py"
    data = subprocess.check_output(
        ["git", "-C", str(root), "show", args.runner_commit + ":" + relative], env={"PATH": os.defpath}, timeout=10
    )
    return load_bytes(
        data, "reviewed_runtime_controls", hashlib.sha256(data).hexdigest(), filename=str(root / relative)
    )


def run_bound(args, inputs, controls, workspace):
    legacy = load_bytes(inputs["snapshot_helper"].data, "accepted_snapshot", HELPER_SHA256)
    with closing(sqlite3.connect(":memory:")) as catalog:
        catalog.execute("CREATE TABLE dw_server_schema(engine TEXT, version INTEGER)")
        catalog.executescript(inputs["step_sql"].data.decode())
        added_schema = [row for row in legacy.snapshot(catalog)["schema"] if row[2] != "dw_server_schema"]
    database = workspace.directory("database")
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

    def snapshot():
        return workspace.read_database(held_database, legacy.snapshot)

    image = "ghcr.io/durable-workflow/server@" + controls.EXPECTED_SERVER["index"]
    python_image = "docker.io/library/python@" + controls.PYTHON_INDEX
    observed = controls.image_identity(json.loads(call("image", "inspect", image).stdout))
    interpreter = controls.interpreter_identity(json.loads(call("image", "inspect", python_image).stdout))
    nonce = uuid4().hex
    token = secrets.token_hex(24)
    credential = workspace.write(
        "fixture.env",
        "DW_AUTH_TOKEN=" + token + "\nDW_RUNTIME_CREDENTIALS_ENABLED=false\n"
        "DW_RUST_MAINTENANCE_LOOP=0\nDB_CONNECTION=sqlite\nDB_DATABASE=/cohort/upgraded.sqlite\n",
    )
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "runner_commit": args.runner_commit,
        "accepted_contract_source": "a09f156399416216d2f097c257a2164f83c7eda7",
        "actual_seed_source": "09a4ccda3da11853606af031a8970bbde4b2d6ec",
        "actual_seed_driver_sha256": "01c0fa1251ac3c809723e58d71817881fe2c5c9498579bc4c2e6097324a3618b",
        "expected_server": controls.EXPECTED_SERVER,
        "observed_image": observed,
        "interpreter_image": interpreter,
        "ownership_nonce": nonce,
        "owned_containers": {},
        "inputs": {role: {"sha256": value.sha256, "bytes": len(value.data)} for role, value in inputs.items()},
        "cleanup_by_id": {},
        "complete": False,
    }
    common = [
        "--init",
        "--read-only",
        "--cpus",
        "1",
        "--memory",
        "1g",
        "--pids-limit",
        "256",
        "--label",
        controls.OWNER_LABEL + "=sdk-python",
        "--label",
        controls.RUN_LABEL + "=" + nonce,
        "--tmpfs",
        "/tmp:mode=1777,size=128m",
        "--tmpfs",
        "/app/storage:uid=1000,gid=1000,mode=0700,size=64m",
        "--tmpfs",
        "/app/database:uid=1000,gid=1000,mode=0700,size=16m",
        "-v",
        workspace.mount_path(database) + ":/cohort:rw",
        "--env-file",
        workspace.proc(credential["fd"]),
    ]

    def create(role, *arguments):
        name = "sdk-python-216-physical-" + role + "-" + nonce[:12]
        identity = controls.created_id(call("create", "--name", name, *common, *arguments).stdout)
        if identity in [entry["id"] for entry in report["owned_containers"].values()]:
            raise ValueError("duplicate creation ID")
        report["owned_containers"][role] = {"id": identity, "requested_name": name}
        if controls.inspect_owned(call, identity, nonce) is None:
            raise ValueError("created object vanished")
        return identity

    try:
        for value in inputs.values():
            value.recheck()
        held_database = workspace.write("upgraded.sqlite", inputs["baseline"].data, parent=database, immutable=False)
        workspace.read_database(held_database, lambda connection: native_marker(connection, 12))
        baseline = snapshot()
        began = time.time()
        upgrade_id = create(
            "upgrade",
            "--network",
            "none",
            "--entrypoint",
            "/bin/sh",
            image,
            *workspace.native_arguments(database, held_database, "schema-upgrade"),
        )
        call("start", upgrade_id)
        if call("wait", upgrade_id).stdout.strip() != "0":
            raise RuntimeError("schema upgrade failed")
        upgrade = json.loads(call("logs", upgrade_id).stdout)
        if upgrade != {"backend": "sqlite", "from_schema_version": 12, "schema_version": 13, "status": "upgraded"}:
            raise ValueError("unexpected upgrade result")
        finished = time.time()
        report["upgrade"] = upgrade
        report["cleanup_by_id"]["upgrade"] = controls.cleanup_owned(cleanup_call, upgrade_id, nonce)
        workspace.read_database(held_database, lambda connection: native_marker(connection, 13))
        validate_upgrade(baseline, snapshot(), added_schema, began, finished)
        report["upgrade_row_preservation"] = True
        report["journal_sha384"] = JOURNAL_SHA384
        runtime_id = create(
            "runtime",
            "-p",
            "127.0.0.1::8080",
            "--entrypoint",
            "/bin/sh",
            image,
            *workspace.native_arguments(database, held_database),
        )
        call("start", runtime_id)
        address = call("port", runtime_id, "8080/tcp").stdout.strip()
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
            raise TimeoutError("upgraded runtime readiness")
        bundle = {
            "scenario": "physical_readback",
            "sources": {
                role: {"sha256": value.sha256, "base64": base64.b64encode(value.data).decode()}
                for role, value in inputs.items()
                if role in {"adapter", "bootstrap", "requirements", "verifier", "seed", "wheel"}
            },
            "upstream": upstream,
            "token": token,
            "server_version": controls.EXPECTED_SERVER["version"],
        }
        for value in inputs.values():
            value.recheck()
        consumer_id = controls.create_consumer(call, "sdk-python-216-physical-consumer-" + nonce[:12], nonce, inputs)
        if consumer_id in [entry["id"] for entry in report["owned_containers"].values()]:
            raise ValueError("duplicate consumer ID")
        report["owned_containers"]["consumer"] = {"id": consumer_id}
        if controls.inspect_owned(call, consumer_id, nonce) is None:
            raise ValueError("consumer vanished")
        before = snapshot()
        result = subprocess.run(
            docker + ["start", "-a", "-i", consumer_id],
            input=json.dumps(bundle),
            env=environment,
            capture_output=True,
            text=True,
            timeout=600,
        )
        after = snapshot()
        workspace.write("consumer.log", result.stdout + result.stderr)
        report["request_window_before_sha256"] = legacy.fingerprint(before)
        report["request_window_after_sha256"] = legacy.fingerprint(after)
        if before != after:
            raise ValueError("consumer window changed complete logical rows/schema")
        state = controls.inspect_owned(call, consumer_id, nonce)
        if state is None or state["State"]["Running"]:
            raise ValueError("consumer final state unverified")
        report["consumer_exit_code"] = state["State"]["ExitCode"]
        if result.returncode != 0 or report["consumer_exit_code"] != 0:
            raise RuntimeError("installed consumer failed")
        for value in inputs.values():
            value.recheck()
        report["input_identities_rechecked"] = True
        if controls.interpreter_identity(json.loads(call("image", "inspect", python_image).stdout)) != interpreter:
            raise ValueError("interpreter image changed")
        report["interpreter_identity_rechecked"] = True
        proof = controls.consumer_result(result.stdout, inputs, scenario="physical_readback")
        workspace.write("consumer.json", json.dumps(proof, indent=2) + "\n")
        workspace.check()
        report["workspace_identities_rechecked"] = True
        report["consumer_complete"] = True
    except Exception as error:
        report["failure_type"] = type(error).__name__
        raise
    finally:
        for role, owned in report["owned_containers"].items():
            if role in report["cleanup_by_id"]:
                continue
            try:
                report["cleanup_by_id"][role] = controls.cleanup_owned(cleanup_call, owned["id"], nonce)
            except Exception as error:
                report["cleanup_by_id"][role] = {"id": owned["id"], "failure_type": type(error).__name__}
        workspace.remove(credential)
        clean = all(value.get("absent") and value.get("volumes_absent") for value in report["cleanup_by_id"].values())
        if clean:
            workspace.remove_database(database)
        try:
            for value in inputs.values():
                value.recheck()
            report["original_inputs_unchanged"] = True
        except Exception as error:
            report["post_input_failure_type"] = type(error).__name__
        report["cleanup"] = {
            "containers_and_volumes_absent": clean,
            "private_database_removed": clean,
            "credential_removed": True,
        }
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["complete"] = (
            len(report["cleanup_by_id"]) == 3
            and clean
            and report.get("consumer_complete") is True
            and all(value.get("oom_killed") is False for value in report["cleanup_by_id"].values())
            and "failure_type" not in report
            and "post_input_failure_type" not in report
        )
        workspace.write("run.json", json.dumps(report, indent=2) + "\n")
        workspace.check()
        if not clean:
            raise RuntimeError("owned physical cleanup incomplete")
        if not report["complete"] and "failure_type" not in report:
            raise RuntimeError("physical evidence incomplete")


def run(args):
    if not __debug__:
        raise ValueError("acceptance requires assertions enabled")
    controls = controls_from_git(args)
    with ExitStack() as stack:
        inputs = controls.captured_inputs(
            args,
            stack,
            source_files={
                "controller": "run_physical_readback.py",
                "support": "run_response_loss.py",
                "adapter": "physical_readback.py",
                "verifier": "response_loss.py",
            },
            data_files={
                "wheel": (args.wheel, 4 * 1024 * 1024, controls.WHEEL_SHA256),
                "baseline": (args.baseline, 64 * 1024 * 1024, BASELINE_SHA256),
                "seed": (args.seed, 256 * 1024, SEED_SHA256),
                "snapshot_helper": (args.snapshot_helper, 128 * 1024, HELPER_SHA256),
                "step_sql": (args.step_sql, 16 * 1024, STEP_SHA256),
            },
        )
        workspace = stack.enter_context(controls.workspace_for(inputs, args.output))
        run_bound(args, inputs, controls, workspace)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-commit", required=True)
    for name in ("wheel", "baseline", "seed", "snapshot-helper", "step-sql", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
