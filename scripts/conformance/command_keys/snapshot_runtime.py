"""Quiesce owned writers and recover held bytes outside the host namespace."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
from types import ModuleType


class SnapshotReader:
    def __init__(self, workspace, inputs, controls, call, cleanup_call, report, nonce, writers):
        self.workspace = workspace
        self.inputs = inputs
        self.controls = controls
        self.call = call
        self.cleanup_call = cleanup_call
        self.report = report
        self.nonce = nonce
        self.writers = writers
        self.capture = ModuleType("captured_quiesced_capture")
        exec(compile(inputs["capture"].data, "captured-quiesced-capture.py", "exec"), self.capture.__dict__)

    def capture_bytes(self, database):
        paused = []
        original_states = {}
        proof = {"writers": {}}
        try:
            for identity in self.writers():
                record = self.controls.inspect_owned(self.call, identity, self.nonce)
                if record is None or record["State"]["Paused"]:
                    raise ValueError("snapshot writer is missing or already paused")
                original_states[identity] = bool(record["State"]["Running"])
                proof["writers"][identity] = "paused" if record["State"]["Running"] else "stopped"
                if record["State"]["Running"]:
                    paused.append(identity)
                    self.call("pause", identity)

            def attest():
                if set(self.writers()) != set(original_states):
                    raise ValueError("snapshot writer authority changed")
                for identity, running in original_states.items():
                    record = self.controls.inspect_owned(self.call, identity, self.nonce)
                    if (
                        record is None
                        or bool(record["State"]["Running"]) != running
                        or bool(record["State"]["Paused"]) != running
                    ):
                        raise ValueError("snapshot writer is not quiescent")

            files, proof["files"] = self.capture.capture(self.workspace, database, attest)
            return files, proof
        finally:
            # The immutable ID remains cleanup authority when a workspace path
            # refuses. Resume every writer this invocation paused, even if one
            # unpause fails; never inspect/recover ownership by reusable name.
            failures = []
            for identity in reversed(paused):
                try:
                    record = self.controls.inspect_owned(self.cleanup_call, identity, self.nonce)
                    if record is not None and record["State"]["Paused"]:
                        self.cleanup_call("unpause", identity)
                        record = self.controls.inspect_owned(self.cleanup_call, identity, self.nonce)
                    if record is None or record["State"]["Paused"] or not record["State"]["Running"]:
                        raise ValueError("snapshot writer resume is unverified")
                except Exception as error:
                    failures.append(type(error).__name__)
            if failures:
                raise RuntimeError("snapshot writer resume failed: " + ",".join(failures))

    def snapshot(self, database):
        for value in self.inputs.values():
            value.recheck()
        files, proof = self.capture_bytes(database)
        return self.recover(files, proof)["snapshot"]

    def recover(self, files, proof):
        role = "snapshot-" + str(len(self.report.setdefault("snapshots", [])))
        proof["role"] = role
        self.report["snapshots"].append(proof)
        image = "docker.io/library/python@" + self.controls.PYTHON_INDEX
        bootstrap = self.inputs["snapshot_bootstrap"].data
        identity = self.controls.created_id(
            self.call(
                "create",
                "--name",
                "sdk-python-216-" + role + "-" + self.nonce[:12],
                "--label",
                self.controls.OWNER_LABEL + "=sdk-python",
                "--label",
                self.controls.RUN_LABEL + "=" + self.nonce,
                "--init",
                "--read-only",
                "--network",
                "none",
                "--cpus",
                "1",
                "--memory",
                "1g",
                "--pids-limit",
                "128",
                "--user",
                "0:0",
                "--security-opt",
                "no-new-privileges",
                "--cap-drop",
                "ALL",
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
                bootstrap.decode(),
            ).stdout
        )
        if identity in [entry["id"] for entry in self.report["owned_containers"].values()]:
            raise ValueError("duplicate snapshot container identity")
        self.report["owned_containers"][role] = {"id": identity}
        record = self.controls.inspect_owned(self.call, identity, self.nonce)
        if record is None or record["Mounts"] or record["HostConfig"]["NetworkMode"] != "none":
            raise ValueError("private snapshot has a host mount or network")
        packet = {"schema": "sdk-python.quiesced-snapshot.v1"}
        for name, data in {
            "main": files["main"],
            "wal": files["wal"],
            "helper": self.inputs["snapshot_helper"].data,
        }.items():
            packet[name] = {"base64": base64.b64encode(data).decode(), "sha256": hashlib.sha256(data).hexdigest()}
        result = subprocess.run(
            ["docker", "--config", "/proc/self/fd", "start", "-a", "-i", identity],
            input=json.dumps(packet),
            env={"PATH": os.defpath, "TMPDIR": self.workspace.proc(self.workspace.root)},
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.workspace.write(role + ".log", result.stdout + result.stderr)
        record = self.controls.inspect_owned(self.call, identity, self.nonce)
        if record is None or result.returncode or record["State"]["Running"] or record["State"]["ExitCode"]:
            raise RuntimeError("private snapshot recovery failed")
        output = json.loads(result.stdout)
        if (
            output["schema"] != "sdk-python.quiesced-snapshot-result.v1"
            or output["uid"] != 65534
            or any(output[name + "_sha256"] != packet[name]["sha256"] for name in ("main", "wal", "helper"))
        ):
            raise ValueError("private snapshot provenance differs")
        proof["recovery"] = {key: value for key, value in output.items() if key != "snapshot"}
        self.report["cleanup_by_id"][role] = self.controls.cleanup_owned(self.cleanup_call, identity, self.nonce)
        for value in self.inputs.values():
            value.recheck()
        return output
