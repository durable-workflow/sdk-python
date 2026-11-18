"""Run only under Rust265's frozen native-open interposer, on synthetic files."""

from __future__ import annotations

import base64
import ctypes
import hashlib
import json
import os
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from types import ModuleType


def load(item, name):
    data = base64.b64decode(item["base64"], validate=True)
    if hashlib.sha256(data).hexdigest() != item["sha256"]:
        raise ValueError("native control captured module differs")
    result = ModuleType(name)
    exec(compile(data, name + ".py", "exec"), result.__dict__)
    return result


def main():
    packet = json.load(sys.stdin)
    guard = load(packet["workspace"], "captured_workspace")
    capture = load(packet["capture"], "captured_capture")
    with guard.Workspace(Path(sys.argv[1])) as workspace:
        directory = workspace.directory("database")
        held = workspace.write("sample.sqlite", b"", parent=directory, immutable=False)
        path = workspace.path / "database/sample.sqlite"
        for candidate, value in ((path, "original"), (Path(str(path) + ".replacement"), "replacement")):
            with closing(sqlite3.connect(candidate)) as connection:
                connection.execute("CREATE TABLE sample(value TEXT)")
                connection.execute("INSERT INTO sample VALUES (?)", (value,))
                connection.commit()
            candidate.chmod(0o600)
        before = os.fstat(held["fd"])
        library = ctypes.CDLL(None)
        library.getenv.restype = ctypes.c_char_p
        os.environ["SQLITE_PROBE_TARGET"] = str(path)
        os.environ["SQLITE_PROBE_ARMED"] = "1"
        # Reproduce the lower VFS interval missed by a Python connect wrapper.
        with closing(sqlite3.connect("file:/proc/self/fd/" + str(held["fd"]) + "?mode=ro", uri=True)) as connection:
            observed = connection.execute("SELECT value FROM sample").fetchone()[0]
        workspace.check()
        after = os.fstat(held["fd"])
        if (
            observed != "replacement"
            or library.getenv(b"SQLITE_PROBE_ARMED") is not None
            or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
        ):
            raise ValueError("native-open predecessor counterexample did not reproduce")
        # Both fixture connections are closed; this child is the only fixture
        # writer. Keep the lower native-open attack armed through actual capture.
        os.environ["SQLITE_PROBE_ARMED"] = "1"
        files, proof = capture.capture(workspace, held, lambda: None)
        if library.getenv(b"SQLITE_PROBE_ARMED") != b"1":
            raise ValueError("held capture reopened the mutable main pathname")
        os.environ.pop("SQLITE_PROBE_ARMED")
        print(
            json.dumps(
                {
                    "predecessor_observed": observed,
                    "held_inode_preserved": True,
                    "restored_basename_verified": True,
                    "native_open_not_called_by_capture": True,
                    "proof": proof,
                    "files": {name: base64.b64encode(data).decode() for name, data in files.items()},
                }
            )
        )


if __name__ == "__main__":
    main()
