from __future__ import annotations

import importlib.util
import os
import sqlite3
from pathlib import Path

import pytest

PATH = Path(__file__).parents[1] / "scripts/conformance/command_keys/workspace_guards.py"
SPEC = importlib.util.spec_from_file_location("command_workspace", PATH)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


def test_root_replacement_is_seen_and_descriptor_bind_keeps_original_database(tmp_path):
    root = tmp_path / "work"
    with module.Workspace(root) as workspace:
        directory = workspace.directory("database")
        workspace.write("copied.sqlite", b"original", parent=directory, immutable=False)
        binding = Path(workspace.proc(directory)) / "copied.sqlite"
        root.rename(tmp_path / "retained-original")
        (root / "database").mkdir(parents=True)
        (root / "database/copied.sqlite").write_bytes(b"substituted")
        # The predecessor mount/snapshot path follows the replacement.
        assert (root / "database/copied.sqlite").read_bytes() == b"substituted"
        assert binding.read_bytes() == b"original"
        with pytest.raises(ValueError, match="directory was replaced"):
            workspace.check()
        with pytest.raises(ValueError):
            workspace.write("run.json", '{"complete":true}')
        assert not (root / "run.json").exists()


@pytest.mark.parametrize("entry", ["database", "docker-config"])
def test_subdirectory_rename_replacement_refuses_use(tmp_path, entry):
    root = tmp_path / "work"
    with module.Workspace(root) as workspace:
        workspace.directory(entry)
        (root / entry).rename(root / (entry + "-original"))
        (root / entry).mkdir()
        with pytest.raises(ValueError):
            workspace.check()


@pytest.mark.parametrize("name", ["fixture.env", "consumer.log", "consumer.json", "run.json"])
@pytest.mark.parametrize("replacement", ["rename", "symlink", "hardlink", "rewrite"])
def test_workspace_output_and_credential_substitution_refuses(tmp_path, name, replacement):
    root = tmp_path / "work"
    with module.Workspace(root) as workspace:
        item = workspace.write(name, b"original")
        path = root / name
        if replacement == "rewrite":
            path.write_bytes(b"changed!")
        else:
            path.rename(root / (name + "-original"))
            outside = tmp_path / "outside"
            outside.write_bytes(b"foreign!")
            if replacement == "rename":
                path.write_bytes(b"foreign!")
                path.chmod(0o600)
            elif replacement == "symlink":
                path.symlink_to(outside)
            else:
                os.link(outside, path)
        # A pathname-only reader accepts the changed bytes; the held identity refuses.
        assert path.read_bytes() != b"original"
        with pytest.raises(ValueError):
            workspace.check()
        with pytest.raises(ValueError):
            workspace.remove(item)
        assert path.exists()


def test_evidence_writer_refuses_existing_symlink_without_touching_marker(tmp_path):
    root = tmp_path / "work"
    marker = tmp_path / "marker"
    marker.write_bytes(b"preserve")
    with module.Workspace(root) as workspace:
        (root / "run.json").symlink_to(marker)
        with pytest.raises(FileExistsError):
            workspace.write("run.json", '{"complete":true}')
        assert marker.read_bytes() == b"preserve"


def test_created_file_descriptor_is_held_without_reopen(tmp_path, monkeypatch):
    root = tmp_path / "work"
    with module.Workspace(root) as workspace:
        original = workspace.remember

        def replace_before_remember(parent, name, descriptor, **kwargs):
            path = root / name
            path.rename(root / "original")
            path.write_bytes(b"false-output")
            path.chmod(0o600)
            return original(parent, name, descriptor, **kwargs)

        monkeypatch.setattr(workspace, "remember", replace_before_remember)
        with pytest.raises(ValueError):
            workspace.write("consumer.json", b"true-output")


def test_mutable_database_keeps_inode_across_legitimate_sqlite_updates(tmp_path):
    root = tmp_path / "work"
    with module.Workspace(root) as workspace:
        directory = workspace.directory("database")
        workspace.write("copied.sqlite", b"", parent=directory, immutable=False)
        path = Path(workspace.proc(directory, "copied.sqlite"))
        with sqlite3.connect(path) as database:
            database.execute("CREATE TABLE values_table(value INTEGER)")
            database.execute("INSERT INTO values_table VALUES (1)")
        workspace.check()
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as database:
            assert database.execute("SELECT value FROM values_table").fetchall() == [(1,)]
        workspace.check()
        original = root / "database/copied.sqlite"
        original.rename(root / "database/old.sqlite")
        original.write_bytes(b"different")
        original.chmod(0o600)
        with pytest.raises(ValueError):
            workspace.check()


def test_workspace_parent_replacement_refuses_before_create(tmp_path):
    parent = tmp_path / "parent"
    parent.mkdir(mode=0o700)
    with module.Workspace(parent / "work") as workspace:
        parent.rename(tmp_path / "retained-parent")
        (parent / "work").mkdir(parents=True)
        with pytest.raises(ValueError):
            workspace.write("consumer.log", "must not write")
        assert not (parent / "work/consumer.log").exists()
