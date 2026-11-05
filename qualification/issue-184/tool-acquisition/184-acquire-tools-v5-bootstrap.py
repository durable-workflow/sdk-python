"""Unexecuted reviewed-source bootstrap; a new General decision is still required.

For any future admission, these exact bytes are the inline python -I - stdin
bootstrap. The syntax guard below does not itself grant authorization.
"""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
import stat
from types import ModuleType

OWNER_PATH = Path('/tmp/sdk-python-handoff/184-acquire-tools-v5.py')
OWNER_SHA = '96d4a5b1b5687d11779a244a2a6946555c24020b3b54e93e43e61be2f6722e2c'
OLD_ADMISSION = 'https://github.com/durable-workflow/sdk-python/issues/184#issuecomment-6649046802'
MINIMAL_ENV = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8', 'TZ': 'UTC'}


def require(value: bool, code: str) -> None:
    if not value:
        raise ValueError(code)


def stamp(value: os.stat_result) -> tuple:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_nlink, value.st_uid,
            value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def verified_source(path: Path, expected: str) -> tuple[bytes, tuple]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid()
                and before.st_nlink == 1 and before.st_size <= 65536, 'BOOTSTRAP_SOURCE_TYPE_OR_SIZE')
        raw = stream.read(65537)
        require(stamp(before) == stamp(os.fstat(stream.fileno())), 'BOOTSTRAP_SOURCE_CHANGED_WHILE_READING')
    require(hashlib.sha256(raw).hexdigest() == expected, 'BOOTSTRAP_SOURCE_HASH_MISMATCH')
    require(stamp(os.stat(path, follow_symlinks=False)) == stamp(before), 'BOOTSTRAP_SOURCE_PATH_CHANGED')
    return raw, stamp(before)


def launch(admission: str, proof: dict | None = None) -> dict:
    require(bool(re.fullmatch(r'https://github\.com/durable-workflow/sdk-python/issues/184#issuecomment-[1-9][0-9]{5,17}', admission))
            and admission != OLD_ADMISSION, 'NEW_GENERAL_ADMISSION_REQUIRED_OLD_ATTEMPT_CANNOT_RETRY')
    os.environ.clear()
    os.environ.update(MINIMAL_ENV)
    raw, identity = verified_source(OWNER_PATH, OWNER_SHA)
    code = compile(raw, '<verified-sdk-python-data-owner-v5>', 'exec')
    require(stamp(os.stat(OWNER_PATH, follow_symlinks=False)) == identity, 'BOOTSTRAP_SOURCE_PATH_CHANGED')
    owner = ModuleType('sdk_python_verified_data_owner_v5')
    owner.__file__ = str(OWNER_PATH)
    owner.VERIFIED_SOURCE_SHA256 = OWNER_SHA
    exec(code, owner.__dict__)
    require(stamp(os.stat(OWNER_PATH, follow_symlinks=False)) == identity, 'BOOTSTRAP_SOURCE_PATH_CHANGED')
    return owner.run_guarded_once(admission, proof or {})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--new-general-admission', required=True)
    args = parser.parse_args()
    launch(args.new_general_admission)
