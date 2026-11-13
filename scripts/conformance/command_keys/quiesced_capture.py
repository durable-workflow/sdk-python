"""Bounded held-FD capture after the controller proves every writer quiescent."""

from __future__ import annotations

import hashlib
import os
import stat

MAX_FILE = 64 * 1024 * 1024


def identity(info):
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_uid,
        info.st_gid,
        info.st_nlink,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def capture(workspace, database, attest_quiescent):
    """The callback must independently recheck the exact stopped/paused writers.

    Equality alone is not a live snapshot protocol. This function never asks
    SQLite to open any host pathname, including a proc-FD URI.
    """
    workspace.check()
    if workspace.files.get((database["parent"], database["name"])) is not database:
        raise ValueError("capture database is not held by this workspace")
    descriptors = {"main": database["fd"]}
    opened = []
    sidecars = {}
    try:
        attest_quiescent()
        for suffix in ("-wal", "-journal"):
            name = database["name"] + suffix
            try:
                descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=database["parent"])
            except FileNotFoundError:
                sidecars[suffix] = None
                continue
            opened.append(descriptor)
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_nlink != 1
                or info.st_mode & 0o077
                or info.st_size > MAX_FILE
            ):
                raise ValueError("capture sidecar is not a bounded private regular file")
            sidecars[suffix] = (descriptor, identity(info))
            if suffix == "-journal":
                if info.st_size:
                    raise ValueError("rollback journal requires a separate recovery contract")
            else:
                descriptors["wal"] = descriptor

        def recheck():
            attest_quiescent()
            workspace.check()
            for suffix, held in sidecars.items():
                name = database["name"] + suffix
                try:
                    current = os.stat(name, dir_fd=database["parent"], follow_symlinks=False)
                except FileNotFoundError:
                    if held is not None:
                        raise ValueError("capture sidecar vanished") from None
                else:
                    if held is None or identity(current) != held[1] or identity(os.fstat(held[0])) != held[1]:
                        raise ValueError("capture sidecar identity changed")

        before = {role: identity(os.fstat(descriptor)) for role, descriptor in descriptors.items()}
        if not 0 < before["main"][6] <= MAX_FILE:
            raise ValueError("capture main database exceeds its bound")
        first = {role: os.pread(descriptor, MAX_FILE + 1, 0) for role, descriptor in descriptors.items()}
        recheck()
        second = {role: os.pread(descriptor, MAX_FILE + 1, 0) for role, descriptor in descriptors.items()}
        recheck()
        if first != second or any(identity(os.fstat(fd)) != before[role] for role, fd in descriptors.items()):
            raise ValueError("quiescent complete byte captures differ")
        if any(len(first[role]) != before[role][6] for role in descriptors):
            raise ValueError("quiescent byte capture is incomplete")
        first.setdefault("wal", b"")
        return first, {
            role: {"identity": before.get(role), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
            for role, data in first.items()
        }
    finally:
        for descriptor in opened:
            os.close(descriptor)
