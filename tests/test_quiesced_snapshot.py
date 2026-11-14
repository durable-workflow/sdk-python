from __future__ import annotations

import importlib.util
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).parents[1] / "scripts/conformance/command_keys"


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + ".py"))
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


capture_module = load("quiesced_capture")
workspace_module = load("workspace_guards")


def initialize(path):
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE sample(value INTEGER)")
        connection.execute("INSERT INTO sample VALUES (1)")
        connection.commit()


def test_capture_never_opens_sqlite_path_and_keeps_committed_wal(tmp_path, monkeypatch):
    with workspace_module.Workspace(tmp_path / "work") as workspace:
        directory = workspace.directory("database")
        held = workspace.write("sample.sqlite", b"", parent=directory, immutable=False)
        path = tmp_path / "work/database/sample.sqlite"
        initialize(path)
        with closing(sqlite3.connect(path)) as writer:
            writer.execute("PRAGMA journal_mode=WAL")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("INSERT INTO sample VALUES (2)")
            writer.commit()
            with closing(sqlite3.connect(path)) as other:
                other.execute("INSERT INTO sample VALUES (3)")
                other.commit()
                monkeypatch.setattr(sqlite3, "connect", lambda *_a, **_kw: pytest.fail("capture reopened SQLite"))
                calls = []
                data, proof = capture_module.capture(workspace, held, lambda: calls.append(True))
                assert len(calls) == 3
                assert data["main"] == os.pread(held["fd"], capture_module.MAX_FILE, 0)
                assert data["wal"] == Path(str(path) + "-wal").read_bytes()
                assert proof["wal"]["bytes"] > 0


def test_quiescent_attestation_failure_refuses_before_capture(tmp_path, monkeypatch):
    with workspace_module.Workspace(tmp_path / "work") as workspace:
        directory = workspace.directory("database")
        held = workspace.write("sample.sqlite", b"", parent=directory, immutable=False)
        initialize(tmp_path / "work/database/sample.sqlite")

        def refuse():
            raise ValueError("writer is running")

        monkeypatch.setattr(capture_module.os, "pread", lambda *_a: pytest.fail("read before writer attestation"))
        with pytest.raises(ValueError, match="writer is running"):
            capture_module.capture(workspace, held, refuse)


def test_same_size_main_mutation_between_captures_refuses(tmp_path):
    with workspace_module.Workspace(tmp_path / "work") as workspace:
        directory = workspace.directory("database")
        held = workspace.write("sample.sqlite", b"", parent=directory, immutable=False)
        initialize(tmp_path / "work/database/sample.sqlite")
        calls = 0

        def invalid_attestation():
            nonlocal calls
            calls += 1
            if calls == 2:
                original = os.pread(held["fd"], 1, 100)
                os.pwrite(held["fd"], bytes([original[0] ^ 1]), 100)

        with pytest.raises(ValueError, match="captures differ"):
            capture_module.capture(workspace, held, invalid_attestation)


def test_wal_created_after_initial_absence_refuses(tmp_path):
    with workspace_module.Workspace(tmp_path / "work") as workspace:
        directory = workspace.directory("database")
        held = workspace.write("sample.sqlite", b"", parent=directory, immutable=False)
        path = tmp_path / "work/database/sample.sqlite"
        initialize(path)
        calls = 0

        def invalid_attestation():
            nonlocal calls
            calls += 1
            if calls == 2:
                Path(str(path) + "-wal").write_bytes(b"substituted")

        with pytest.raises(ValueError, match="sidecar identity changed"):
            capture_module.capture(workspace, held, invalid_attestation)


def test_sidecar_symlink_refuses(tmp_path):
    with workspace_module.Workspace(tmp_path / "work") as workspace:
        directory = workspace.directory("database")
        held = workspace.write("sample.sqlite", b"", parent=directory, immutable=False)
        path = tmp_path / "work/database/sample.sqlite"
        initialize(path)
        Path(str(path) + "-wal").symlink_to(path)
        with pytest.raises(OSError):
            capture_module.capture(workspace, held, lambda: None)


@pytest.mark.parametrize("failure", ["after_pause", "during_capture", "second_writer"])
def test_id_bound_writer_resumes_after_workspace_or_capture_refusal(failure):
    runtime = load("snapshot_runtime")
    first, second = "a" * 64, "b" * 64
    states = {first: {"State": {"Running": True, "Paused": False}}}
    calls = []

    def call(action, identity):
        calls.append((action, identity))
        if action == "pause":
            states[identity]["State"]["Paused"] = True
            if failure == "after_pause":
                raise ValueError("workspace replaced after successful Docker pause")
        elif action == "unpause":
            states[identity]["State"]["Paused"] = False

    controls = SimpleNamespace(inspect_owned=lambda _call, identity, _nonce: states.get(identity))
    inputs = {"capture": SimpleNamespace(data=(ROOT / "quiesced_capture.py").read_bytes())}
    reader = runtime.SnapshotReader(
        None,
        inputs,
        controls,
        call,
        call,
        {},
        "nonce",
        lambda: [first, second] if failure == "second_writer" else [first],
    )

    def refuse(*_args):
        raise ValueError("workspace replaced during capture")

    reader.capture.capture = refuse
    with pytest.raises(ValueError):
        reader.capture_bytes(None)
    assert states[first]["State"] == {"Running": True, "Paused": False}
    assert calls[-1] == ("unpause", first)
