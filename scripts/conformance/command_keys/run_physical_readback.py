"""Qualify one installed SDK consumer on a private copy of the producer seed."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import stat
import subprocess
import time
import urllib.request
from contextlib import closing
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


def load(path, name, expected=None):
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if expected is not None and digest != expected:
        raise ValueError("required executable source identity differs")
    module = ModuleType(name)
    module.__file__ = str(path)
    # Execute precisely the bytes that were hashed, rather than reopening the
    # external helper through an import loader after its identity check.
    exec(compile(data, str(path), "exec"), module.__dict__)
    module.source_sha256 = digest
    return module


def verify_file(path, expected):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        raise ValueError("required producer/helper input identity differs")


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


def run(args):
    if not __debug__:
        raise ValueError("acceptance assertions require Python without optimization")
    # No Docker action or backup copy precedes all input identity checks.
    verify_file(args.baseline, BASELINE_SHA256)
    verify_file(args.seed, SEED_SHA256)
    verify_file(args.snapshot_helper, HELPER_SHA256)
    verify_file(args.step_sql, STEP_SHA256)
    legacy = load(args.snapshot_helper, "accepted_sqlite_snapshot", HELPER_SHA256)
    with closing(sqlite3.connect(":memory:")) as catalog:
        catalog.execute("CREATE TABLE dw_server_schema(engine TEXT, version INTEGER)")
        catalog.executescript(args.step_sql.read_text())
        added_schema = [row for row in legacy.snapshot(catalog)["schema"] if row[2] != "dw_server_schema"]
    controls = load(Path(__file__).with_name("run_response_loss.py"), "native_runtime_identity")
    root = args.output.absolute()
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    database = root / "database"
    database.mkdir(mode=0o700)
    copied = database / "upgraded.sqlite"
    config = root / "docker-config"
    config.mkdir(mode=0o700)
    (config / "config.json").write_text('{"auths":{}}\n')
    environment = {"PATH": os.defpath, "TMPDIR": str(root), "PYTHONUNBUFFERED": "1"}
    docker = ["docker", "--config", str(config)]

    def call(*arguments, check=True, timeout=45):
        return subprocess.run(
            docker + list(arguments), env=environment, check=check, capture_output=True, text=True, timeout=timeout
        )

    def snapshot():
        with closing(sqlite3.connect(copied.as_uri() + "?mode=ro", uri=True, timeout=2)) as connection:
            connection.execute("BEGIN")
            return legacy.snapshot(connection)

    image = "ghcr.io/durable-workflow/server@" + controls.EXPECTED_SERVER["index"]
    observed = controls.image_identity(json.loads(call("image", "inspect", image).stdout))
    suffix = uuid4().hex[:12]
    names = ["sdk-python-216-physical-upgrade-" + suffix, "sdk-python-216-physical-runtime-" + suffix]
    credential = root / "fixture.env"
    descriptor = os.open(credential, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(
            "DW_AUTH_TOKEN="
            + secrets.token_hex(24)
            + "\nDW_RUNTIME_CREDENTIALS_ENABLED=false\nDW_RUST_MAINTENANCE_LOOP=0"
            + "\nDB_CONNECTION=sqlite\nDB_DATABASE=/cohort/upgraded.sqlite\n"
        )
    consumer = Path(__file__).with_name("physical_readback.py")
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "baseline_sha256": BASELINE_SHA256,
        "seed_sha256": SEED_SHA256,
        "snapshot_helper_sha256": HELPER_SHA256,
        "runtime_verifier_sha256": controls.source_sha256,
        "tagged_step_sha256": STEP_SHA256,
        "expected_server": controls.EXPECTED_SERVER,
        "observed_image": observed,
        "controller_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "consumer_sha256": hashlib.sha256(consumer.read_bytes()).hexdigest(),
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
        "--tmpfs",
        "/tmp:mode=1777,size=128m",
        "--tmpfs",
        "/app/storage:uid=1000,gid=1000,mode=0700,size=64m",
        "--tmpfs",
        "/app/database:uid=1000,gid=1000,mode=0700,size=16m",
        "-v",
        str(database) + ":/cohort:rw",
        "--env-file",
        str(credential),
    ]
    clean = []
    try:
        shutil.copyfile(args.baseline, copied)
        copied.chmod(0o600)
        verify_file(copied, BASELINE_SHA256)
        with closing(sqlite3.connect(copied)) as connection:
            native_marker(connection, 12)
        baseline_snapshot = snapshot()
        upgrade_began = time.time()
        call("create", "--name", names[0], "--network", "none", *common, image, "schema-upgrade")
        call("start", names[0])
        assert call("wait", names[0]).stdout.strip() == "0"
        upgrade = json.loads(call("logs", names[0]).stdout)
        assert upgrade == {"backend": "sqlite", "from_schema_version": 12, "schema_version": 13, "status": "upgraded"}
        report["upgrade"] = upgrade
        upgrade_finished = time.time()
        upgrade_state = json.loads(call("inspect", names[0]).stdout)[0]
        assert upgrade_state["State"]["OOMKilled"] is False
        call("rm", "-v", names[0])
        with closing(sqlite3.connect(copied)) as connection:
            native_marker(connection, 13)
            assert connection.execute("SELECT count(*) FROM dw_command_intents").fetchone()[0] == 0
        upgraded_snapshot = snapshot()
        validate_upgrade(baseline_snapshot, upgraded_snapshot, added_schema, upgrade_began, upgrade_finished)
        report["upgrade_row_preservation"] = True
        report["journal_sha384"] = JOURNAL_SHA384
        call("create", "--name", names[1], "-p", "127.0.0.1::8080", *common, image)
        call("start", names[1])
        address = call("port", names[1], "8080/tcp").stdout.strip()
        host, port = address.split(":")
        assert host == "127.0.0.1" and port.isdigit() and 1 <= int(port) <= 65535
        upstream = "http://" + address
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(upstream + "/api/health", timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                time.sleep(0.2)
        else:
            raise TimeoutError("upgraded synthetic runtime readiness")
        before = snapshot()
        with (root / "consumer.log").open("x") as log:
            result = subprocess.run(
                [
                    str(args.python.absolute()),
                    "-I",
                    str(consumer),
                    "--upstream",
                    upstream,
                    "--server-version",
                    controls.EXPECTED_SERVER["version"],
                    "--credential-file",
                    str(credential),
                    "--seed",
                    str(args.seed.absolute()),
                    "--seed-sha256",
                    SEED_SHA256,
                    "--wheel",
                    str(args.wheel.absolute()),
                    "--output",
                    str(root / "consumer.json"),
                ],
                cwd=root,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=120,
            )
        after = snapshot()
        report["consumer_exit_code"] = result.returncode
        report["request_window_before_sha256"] = legacy.fingerprint(before)
        report["request_window_after_sha256"] = legacy.fingerprint(after)
        assert before == after, "consumer request window changed complete logical rows/schema"
        assert result.returncode == 0, "installed physical consumer failed"
        proof = json.loads((root / "consumer.json").read_text())
        assert proof["complete"] and proof["consumer_sha256"] == report["consumer_sha256"]
        report["consumer_complete"] = True
    except Exception as error:
        report["failure_type"] = type(error).__name__
        raise
    finally:
        for name in names:
            inspection = call("inspect", name, check=False)
            volumes = []
            if inspection.returncode == 0:
                state = json.loads(inspection.stdout)[0]
                volumes = [mount["Name"] for mount in state["Mounts"] if mount["Type"] == "volume"]
                report.setdefault("oom_by_container", {})[name] = state["State"]["OOMKilled"]
                call("rm", "-f", "-v", name)
            absent = controls.missing_object(call("inspect", name, check=False), name)
            volumes_absent = all(
                controls.missing_object(call("volume", "inspect", volume, check=False), volume, volume=True)
                for volume in volumes
            )
            clean.append(absent and volumes_absent)
        credential.unlink(missing_ok=True)
        if all(clean):
            for path in database.iterdir():
                path.unlink()
            database.rmdir()
        verify_file(args.baseline, BASELINE_SHA256)
        report["cleanup"] = {
            "containers_and_volumes_absent": all(clean),
            "private_database_removed": not database.exists(),
            "credential_removed": not credential.exists(),
            "original_backup_unchanged": True,
        }
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["complete"] = (
            all(clean)
            and report.get("consumer_complete") is True
            and not any(report.get("oom_by_container", {}).values())
            and "failure_type" not in report
        )
        (root / "run.json").write_text(json.dumps(report, indent=2) + "\n")
        if not all(clean):
            raise RuntimeError("owned physical runtime cleanup incomplete")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("python", "wheel", "baseline", "seed", "snapshot-helper", "step-sql", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
