"""Capture reviewed local inputs once and detect pathname/inode substitutions."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path


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


def directory_identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid)


class CapturedInput:
    """Retain no-follow descriptors and immutable bytes for one bounded input."""

    def __init__(self, path, role, *, maximum, expected=None):
        self.path = Path(path).absolute()
        self.role = role
        self.handles = []
        self.parents = []
        if ".." in self.path.parts:
            raise ValueError("input path must be canonical")
        try:
            parent = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            self.handles.append(parent)
            for part in self.path.parts[1:-1]:
                descriptor = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                self.handles.append(descriptor)
                info = os.fstat(descriptor)
                sticky_root = info.st_uid == 0 and bool(info.st_mode & stat.S_ISVTX)
                if info.st_uid not in {0, os.getuid()} or (info.st_mode & 0o022 and not sticky_root):
                    raise ValueError("input parent is not owner-controlled")
                self.parents.append((parent, part, descriptor, directory_identity(info)))
                parent = descriptor
            self.parent = parent
            self.descriptor = os.open(self.path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent)
            self.handles.append(self.descriptor)
            info = os.fstat(self.descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_nlink != 1
                or info.st_mode & 0o022
                or not 0 < info.st_size <= maximum
            ):
                raise ValueError("input must be an owned bounded single-link regular file")
            self.original = identity(info)
            self.data = os.pread(self.descriptor, maximum + 1, 0)
            self.sha256 = hashlib.sha256(self.data).hexdigest()
            if len(self.data) != info.st_size or (expected is not None and self.sha256 != expected):
                raise ValueError("captured input identity differs")
            self.recheck()
        except (OSError, ValueError):
            self.close()
            raise ValueError("reviewed input refused: " + role) from None

    def recheck(self):
        for parent, name, descriptor, original in self.parents:
            if (
                directory_identity(os.fstat(descriptor)) != original
                or directory_identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) != original
            ):
                raise ValueError("input parent was replaced: " + self.role)
        current = os.stat(self.path.name, dir_fd=self.parent, follow_symlinks=False)
        if identity(current) != self.original or identity(os.fstat(self.descriptor)) != self.original:
            raise ValueError("held input was replaced or changed: " + self.role)
        if hashlib.sha256(os.pread(self.descriptor, self.original[6] + 1, 0)).hexdigest() != self.sha256:
            raise ValueError("held input bytes changed: " + self.role)

    def close(self):
        for descriptor in reversed(self.handles):
            os.close(descriptor)
        self.handles.clear()

    def __enter__(self):
        return self

    def __exit__(self, *_error):
        self.close()
