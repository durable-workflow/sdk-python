"""Private no-follow result files retained independently of runtime cleanup."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path


class Output:
    def __init__(self, path):
        self.path = Path(path)
        self.path.mkdir(mode=0o700)
        self.directory = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self.identity = os.fstat(self.directory)
        self.files = {}
        self.file_identities = {}
        self.reserve("result.json")

    def check(self):
        current = self.path.lstat()
        if (current.st_dev, current.st_ino) != (self.identity.st_dev, self.identity.st_ino):
            raise ValueError("private output directory changed")

    def reserve(self, name):
        if "/" in name or name in {"", ".", ".."}:
            raise ValueError("output name must be one basename")
        descriptor = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.directory)
        self.files[name] = descriptor
        info = os.fstat(descriptor)
        self.file_identities[name] = (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid, info.st_nlink)
        return descriptor

    def check_file(self, name):
        descriptor = self.files[name]
        current = os.stat(name, dir_fd=self.directory, follow_symlinks=False)
        held = os.fstat(descriptor)
        for info in (current, held):
            shape = (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid, info.st_nlink)
            if shape != self.file_identities[name] or not stat.S_ISREG(info.st_mode):
                raise ValueError("private output file changed")

    def write(self, name, data):
        self.check()
        descriptor = self.files[name] if name in self.files else self.reserve(name)
        self.check_file(name)
        self._write(descriptor, data)

    @staticmethod
    def _write(descriptor, data):
        data = data.encode() if isinstance(data, str) else data
        os.ftruncate(descriptor, 0)
        offset = 0
        while offset < len(data):
            offset += os.pwrite(descriptor, data[offset:], offset)
        os.fsync(descriptor)

    def read_cid(self, name="runtime.cid"):
        try:
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=self.directory)
        except FileNotFoundError:
            return ""
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_size > 128:
                raise ValueError("CID file shape differs")
            return os.pread(descriptor, 129, 0).decode("ascii")
        finally:
            os.close(descriptor)

    def finish(self, report):
        # This held descriptor survives a path refusal; cleanup does not depend
        # on traversing the output pathname to retain a terminal failure receipt.
        try:
            self.check()
            self.check_file("result.json")
        except OSError as error:
            report["output_failure"] = type(error).__name__
            report["complete"] = False
        except ValueError:
            report["output_failure"] = "identity_changed"
            report["complete"] = False
        encoded = json.dumps(report, indent=2) + "\n"
        if len(encoded.encode()) > 128 * 1024:
            raise ValueError("terminal result exceeded its bound")
        self._write(self.files["result.json"], encoded)

    def close(self):
        for descriptor in self.files.values():
            os.close(descriptor)
        os.close(self.directory)
