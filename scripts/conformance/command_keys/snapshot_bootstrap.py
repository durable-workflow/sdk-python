"""Recover captured quiescent main/WAL bytes in a mount-free private container.

The outer controller must prove all database writers stopped or paused before
capturing both files. This program does not make a live byte copy coherent.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import stat
import sys
import time
from contextlib import closing
from types import ModuleType

MAX_FILE = 64 * 1024 * 1024
MAX_PACKET = 180 * 1024 * 1024
HELPER_SHA256 = "6fc6054ce087c136a86d7b6b222daeefaa87c58fa0b613b155a5df39e6a26fad"


def decode(item, maximum):
    if not isinstance(item, dict) or set(item) != {"base64", "sha256"}:
        raise ValueError("captured input shape differs")
    if not isinstance(item["base64"], str) or len(item["base64"]) > 4 * ((maximum + 2) // 3):
        raise ValueError("captured input exceeds its bound")
    data = base64.b64decode(item["base64"], validate=True)
    if len(data) > maximum or hashlib.sha256(data).hexdigest() != item["sha256"]:
        raise ValueError("captured input bytes differ")
    return data


def recover(main, wal):
    if os.getresuid() != (0, 0, 0):
        raise ValueError("private recovery bootstrap requires root")
    directory = os.open("/snapshot", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(directory)
        if info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o700 or os.listdir(directory):
            raise ValueError("recovery directory is not empty and root-private")
        for name, data in (("captured.sqlite", main), ("captured.sqlite-wal", wal)):
            if name.endswith("-wal") and not data:
                continue
            descriptor = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=directory)
            try:
                offset = 0
                while offset < len(data):
                    offset += os.pwrite(descriptor, data[offset:], offset)
                if os.pread(descriptor, MAX_FILE + 1, 0) != data:
                    raise ValueError("private recovery copy differs")
            finally:
                os.close(descriptor)
        memory = sqlite3.connect(":memory:")
        try:
            # Only this trusted bootstrap exists in the container. No host
            # pathname or application code participates in SQLite's native open.
            with closing(sqlite3.connect("file:/snapshot/captured.sqlite?mode=ro", uri=True, timeout=2)) as source:
                source.execute("PRAGMA trusted_schema=OFF")
                source.execute("PRAGMA query_only=ON")
                source.execute("BEGIN")
                source.execute("SELECT count(*) FROM sqlite_schema").fetchone()
                deadline = time.monotonic() + 5

                def progress(_status, _remaining, _total):
                    if time.monotonic() > deadline:
                        raise ValueError("private recovery backup exceeded its bound")

                source.backup(memory, pages=128, progress=progress, sleep=0.01)
            if memory.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise ValueError("private recovered database integrity check failed")
            memory.execute("PRAGMA trusted_schema=OFF")
            memory.execute("PRAGMA query_only=ON")
            memory.execute("BEGIN")
            for name in os.listdir(directory):
                os.chmod(name, 0o444, dir_fd=directory, follow_symlinks=False)
            os.fchmod(directory, 0o555)
            return memory
        except BaseException:
            memory.close()
            raise
    finally:
        os.close(directory)


def drop_privileges():
    os.setgroups([])
    os.setresgid(65534, 65534, 65534)
    os.setresuid(65534, 65534, 65534)
    with open("/proc/self/status", encoding="ascii") as stream:
        status = dict(line.split(":", 1) for line in stream if ":" in line)
    if (
        os.getresuid() != (65534, 65534, 65534)
        or os.getresgid() != (65534, 65534, 65534)
        or os.getgroups()
        or any(int(status[key].strip(), 16) for key in ("CapEff", "CapPrm", "CapAmb"))
        or status["NoNewPrivs"].strip() != "1"
    ):
        raise ValueError("snapshot helper privilege boundary differs")
    try:
        os.rename("/snapshot/captured.sqlite", "/snapshot/substituted.sqlite")
    except PermissionError:
        pass
    else:
        raise ValueError("snapshot helper can replace the private database")


def main():
    packet = sys.stdin.buffer.read(MAX_PACKET + 1)
    if len(packet) > MAX_PACKET:
        raise ValueError("snapshot packet exceeds its bound")
    document = json.loads(packet)
    if set(document) != {"schema", "main", "wal", "helper"} or document["schema"] != "sdk-python.quiesced-snapshot.v1":
        raise ValueError("snapshot packet shape differs")
    main_bytes = decode(document["main"], MAX_FILE)
    wal = decode(document["wal"], MAX_FILE)
    helper = decode(document["helper"], 128 * 1024)
    if not main_bytes or hashlib.sha256(helper).hexdigest() != HELPER_SHA256:
        raise ValueError("snapshot database/helper authority differs")
    with closing(recover(main_bytes, wal)) as connection:
        drop_privileges()
        module = ModuleType("accepted_snapshot_helper")
        module.__file__ = "captured-sqlite-legacy.py"
        exec(compile(helper, module.__file__, "exec"), module.__dict__)
        result = module.snapshot(connection)
    output = json.dumps(
        {
            "schema": "sdk-python.quiesced-snapshot-result.v1",
            "main_sha256": hashlib.sha256(main_bytes).hexdigest(),
            "wal_sha256": hashlib.sha256(wal).hexdigest(),
            "helper_sha256": HELPER_SHA256,
            "sqlite_version": sqlite3.sqlite_version,
            "uid": os.getuid(),
            "snapshot": result,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(output.encode()) > 33 * 1024 * 1024:
        raise ValueError("snapshot output exceeded its bound")
    print(output)


if __name__ == "__main__":
    main()
