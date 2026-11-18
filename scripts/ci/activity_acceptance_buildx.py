"""Fresh private Buildx tool state; Docker credential authority stays separate."""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
from pathlib import Path


class BuildxState:
    def __init__(self, environment, parent=None):
        self.path = Path(tempfile.mkdtemp(prefix="published-activity-buildx-", dir=parent))
        self.directory = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self.identity = self.shape(os.fstat(self.directory))
        self.environment = {**environment, "BUILDX_CONFIG": str(self.path)}

    @staticmethod
    def shape(info):
        return info.st_dev, info.st_ino, info.st_uid, info.st_gid, info.st_mode

    def check(self):
        current = self.path.lstat()
        held = os.fstat(self.directory)
        if (
            self.shape(current) != self.identity
            or self.shape(held) != self.identity
            or not stat.S_ISDIR(current.st_mode)
            or current.st_uid != os.getuid()
            or stat.S_IMODE(current.st_mode) != 0o700
        ):
            raise ValueError("private Buildx state directory changed")

    def __enter__(self):
        self.check()
        return self.environment

    def __exit__(self, _error_type, error, _traceback):
        try:
            self.check()
            shutil.rmtree(self.path)
        except Exception as state_error:
            if error is None:
                raise
            # Preserve the command's captured streams when both operations fail.
            error.buildx_state_failure = type(state_error).__name__
        finally:
            os.close(self.directory)
        return False
