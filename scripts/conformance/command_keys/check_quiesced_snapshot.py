"""Isolated synthetic controls for paused held-FD capture and private recovery.

This command runs no SDK, Server, historical seed or installed conformance cell.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import subprocess
import time
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from uuid import uuid4

HELPER_SHA256 = "6fc6054ce087c136a86d7b6b222daeefaa87c58fa0b613b155a5df39e6a26fad"
WRITER = """import sqlite3,time
path='/fixture/sample.sqlite'
first=sqlite3.connect(path)
first.execute('CREATE TABLE sample(value INTEGER)')
first.execute('INSERT INTO sample VALUES (0)')
first.commit()
first.execute('PRAGMA journal_mode=WAL')
first.execute('PRAGMA wal_autocheckpoint=0')
second=sqlite3.connect(path)
second.execute('PRAGMA wal_autocheckpoint=0')
for n in range(1,601):
    connection=first if n%2 else second
    connection.execute('INSERT INTO sample VALUES (?)',(n,))
    connection.commit()
    print(n,flush=True)
    time.sleep(0.1)
"""


def module(data, name, filename):
    result = ModuleType(name)
    result.__file__ = filename
    exec(compile(data, filename, "exec"), result.__dict__)
    return result


def main(args):
    root = Path(__file__).absolute().parents[3]
    relative = "scripts/conformance/command_keys/run_response_loss.py"
    source = subprocess.check_output(
        ["/usr/bin/git", "-C", str(root), "show", args.runner_commit + ":" + relative],
        env={"PATH": os.defpath},
        timeout=10,
    )
    controls = module(source, "captured_controls", str(root / relative))
    with ExitStack() as stack:
        inputs = controls.captured_inputs(
            args,
            stack,
            source_files={
                "controller": "check_quiesced_snapshot.py",
                "capture": "quiesced_capture.py",
                "snapshot_bootstrap": "snapshot_bootstrap.py",
            },
            data_files={"helper": (args.helper, 128 * 1024, HELPER_SHA256)},
        )
        workspace = stack.enter_context(controls.workspace_for(inputs, args.output))
        capture = module(inputs["capture"].data, "captured_capture", "captured-quiesced-capture.py")
        database = workspace.directory("database")
        held = workspace.write("sample.sqlite", b"", parent=database, immutable=False)
        nonce = uuid4().hex
        environment = {"PATH": os.defpath, "TMPDIR": workspace.proc(workspace.root)}

        def cleanup_call(*arguments, check=True, timeout=45):
            return subprocess.run(
                ["docker", "--config", "/proc/self/fd", *arguments],
                env=environment,
                capture_output=True,
                text=True,
                check=check,
                timeout=timeout,
            )

        def call(*arguments, check=True, timeout=45):
            workspace.check()
            result = cleanup_call(*arguments, check=check, timeout=timeout)
            workspace.check()
            return result

        image = "docker.io/library/python@" + controls.PYTHON_INDEX
        interpreter = controls.interpreter_identity(json.loads(call("image", "inspect", image).stdout))
        report = {
            "runner_commit": args.runner_commit,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "scope": "isolated synthetic snapshot primitive only",
            "interpreter": interpreter,
            "inputs": {role: value.sha256 for role, value in inputs.items()},
            "owned": {},
            "cleanup": {},
            "complete": False,
        }
        common = [
            "--init",
            "--read-only",
            "--network",
            "none",
            "--cpus",
            "1",
            "--label",
            controls.OWNER_LABEL + "=sdk-python",
            "--label",
            controls.RUN_LABEL + "=" + nonce,
            "--security-opt",
            "no-new-privileges",
            "--cap-drop",
            "ALL",
        ]

        def create(role, *arguments):
            identity = controls.created_id(
                call("create", "--name", "sdk-python-snapshot-" + role + "-" + nonce[:12], *common, *arguments).stdout
            )
            report["owned"][role] = identity
            if controls.inspect_owned(call, identity, nonce) is None:
                raise ValueError("created snapshot control disappeared")
            return identity

        def cleanup(role):
            report["cleanup"][role] = controls.cleanup_owned(cleanup_call, report["owned"][role], nonce)

        def recover(role, files):
            identity = create(
                role,
                "--memory",
                "1g",
                "--pids-limit",
                "128",
                "--user",
                "0:0",
                "--cap-add",
                "SETUID",
                "--cap-add",
                "SETGID",
                "--tmpfs",
                "/snapshot:mode=0700,size=192m,noexec,nosuid,nodev",
                "-i",
                "--entrypoint",
                "/usr/local/bin/python3",
                image,
                "-I",
                "-B",
                "-c",
                inputs["snapshot_bootstrap"].data.decode(),
            )
            record = controls.inspect_owned(call, identity, nonce)
            if record["Mounts"] or record["HostConfig"]["NetworkMode"] != "none":
                raise ValueError("private recovery has a host mount or network")
            packet = {"schema": "sdk-python.quiesced-snapshot.v1"}
            for name, data in {**files, "helper": inputs["helper"].data}.items():
                packet[name] = {"base64": base64.b64encode(data).decode(), "sha256": hashlib.sha256(data).hexdigest()}
            result = subprocess.run(
                ["docker", "--config", "/proc/self/fd", "start", "-a", "-i", identity],
                input=json.dumps(packet),
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
            )
            workspace.write(role + ".log", result.stdout + result.stderr)
            record = controls.inspect_owned(call, identity, nonce)
            if result.returncode or record["State"]["Running"] or record["State"]["ExitCode"]:
                raise RuntimeError("private recovery control failed")
            output = json.loads(result.stdout)
            if output["uid"] != 65534 or any(output[name + "_sha256"] != packet[name]["sha256"] for name in files):
                raise ValueError("private recovery input/provenance differs")
            cleanup(role)
            return output

        try:
            writer = create(
                "writer",
                "--memory",
                "512m",
                "--pids-limit",
                "64",
                "--user",
                str(os.getuid()) + ":" + str(os.getgid()),
                "--mount",
                "type=bind,src=" + workspace.mount_path(database) + ",dst=/fixture",
                "--entrypoint",
                "/usr/local/bin/python3",
                image,
                "-I",
                "-B",
                "-c",
                WRITER,
            )
            call("start", writer)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                lines = call("logs", writer).stdout.splitlines()
                if len(lines) >= 3:
                    break
                time.sleep(0.1)
            else:
                raise TimeoutError("synthetic multi-connection WAL writer readiness")
            call("pause", writer)

            def attest():
                state = controls.inspect_owned(call, writer, nonce)
                if state is None or not state["State"]["Running"] or not state["State"]["Paused"]:
                    raise ValueError("synthetic writer is not paused")

            try:
                files, report["capture"] = capture.capture(workspace, held, attest)
                if not files["wal"]:
                    raise ValueError("positive WAL control has no WAL")
                report["paused_writer_verified"] = True
            finally:
                # Unpause/cleanup use captured IDs even if the held workspace
                # check refuses. Never recover ownership through a name.
                cleanup_call("unpause", writer)
            cleanup("writer")
            with_wal = recover("with-wal", files)
            main_only = recover("main-only", {"main": files["main"], "wal": b""})
            values = with_wal["snapshot"]["tables"]["sample"]["rows"]
            integers = sorted(row[0][1] for row in values)
            if integers != list(range(len(integers))) or len(integers) < 4:
                raise ValueError("private recovery lost committed WAL rows")
            if main_only["snapshot"]["tables"]["sample"]["rows"] != [[["integer", 0]]]:
                raise ValueError("WAL positive did not distinguish the main file")
            report["wal_committed_rows"] = len(integers) - 1
            report["main_only_rows"] = 1
            report["private_recovery"] = {key: value for key, value in with_wal.items() if key != "snapshot"}
            for value in inputs.values():
                value.recheck()
            report["complete"] = True
        finally:
            for role in report["owned"]:
                if role not in report["cleanup"]:
                    cleanup(role)
            report["finished_at"] = datetime.now(timezone.utc).isoformat()
            report["complete"] = report["complete"] and all(
                value["absent"] and value["volumes_absent"] and value["oom_killed"] is False
                for value in report["cleanup"].values()
            )
            workspace.remove_database(database)
            workspace.write("primitive.json", json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runner-commit", required=True)
    parser.add_argument("--helper", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    main(parser.parse_args())
