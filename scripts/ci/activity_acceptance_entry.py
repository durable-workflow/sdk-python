"""Captured entry: verify source and the private install before enabling imports."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
import sys
import types
from pathlib import Path

MODE = ""
INVENTORY_SHA256 = ""
CASE_SHA256 = ""
CASE_NAME = ""
WORKER_ENTRY = ""
ENVIRONMENT = Path("/run/acceptance-env")
DATA = Path("/run/acceptance-data")
MAXIMUM_FILE = 64 * 1024 * 1024


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def metadata(path):
    value = path.lstat()
    return [value.st_dev, value.st_ino, value.st_uid, value.st_gid, value.st_mode, value.st_size, value.st_mtime_ns]


def captured_file(path, maximum=MAXIMUM_FILE):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            raise ValueError("captured file shape or size differs")
        chunks, size = [], 0
        while block := os.read(descriptor, min(1024 * 1024, maximum + 1 - size)):
            chunks.append(block)
            size += len(block)
            if size > maximum:
                raise ValueError("captured file exceeds its bound")
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
        ):
            raise ValueError("captured file changed during its read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def snapshot(root):
    before = metadata(root)
    if not stat.S_ISDIR(before[4]):
        raise ValueError("private install root is not a directory")
    entries = {}
    for path in sorted(root.rglob("*")):
        info = metadata(path)
        item = {"metadata": info}
        if stat.S_ISREG(info[4]):
            item["sha256"] = hashlib.sha256(captured_file(path)).hexdigest()
        elif stat.S_ISLNK(info[4]):
            item["target"] = os.readlink(path)
        elif not stat.S_ISDIR(info[4]):
            raise ValueError("private install contains an unsupported entry")
        entries[path.relative_to(root).as_posix()] = item
    if before != metadata(root):
        raise ValueError("private install root changed during capture")
    return {"root": before, "entries": entries}


def guarded_source(path, expected):
    source = captured_file(path, 256 * 1024)
    if hashlib.sha256(source).hexdigest() != expected:
        raise ValueError("captured case source differs")
    return source


def source_module(source):
    module = types.ModuleType("captured_published_case")
    module.__file__ = str(DATA / CASE_NAME)
    sys.modules[module.__name__] = module
    exec(compile(source, module.__file__, "exec", optimize=0), module.__dict__)
    return module


def run_pytest(source, inventory):
    # pytest never opens the writable test or conftest as executable code.
    import pytest

    class CapturedModule(pytest.Module):
        def _getobj(self):
            module = source_module(source)
            module._COLD_BOOTSTRAP = WORKER_ENTRY
            module._COLD_INVENTORY = inventory
            module._COLD_PYTHON = sys.executable
            return module

    class CapturedPlugin:
        @pytest.hookimpl(tryfirst=True)
        def pytest_pycollect_makemodule(self, module_path, parent):
            if module_path != DATA / CASE_NAME:
                raise ValueError("pytest requested an uncaptured module")
            return CapturedModule.from_parent(parent, path=module_path)

        @pytest.fixture(scope="session")
        def server_url(self):
            return os.environ["DURABLE_WORKFLOW_SERVER_URL"]

        @pytest.fixture(scope="session")
        def server_token(self):
            return os.environ["DURABLE_WORKFLOW_AUTH_TOKEN"]

    return pytest.main([
        "-c", "/dev/null", "--noconftest", "-p", "pytest_asyncio.plugin", "-p", "no:cacheprovider",
        "-o", "asyncio_default_fixture_loop_scope=function",
        "--asyncio-mode=auto", "--import-mode=importlib", "-v", "--capture=tee-sys", str(DATA / CASE_NAME),
        "--junitxml=" + str(DATA / "identity.xml"),
    ], plugins=[CapturedPlugin()])


def main():
    if not (sys.flags.isolated and sys.flags.no_site and sys.dont_write_bytecode) or sys.flags.optimize:
        raise ValueError("captured entry requires isolated no-site Python with active assertions")
    payload = sys.stdin.buffer.read(4 * 1024 * 1024 + 1)
    if len(payload) > 4 * 1024 * 1024:
        raise ValueError("captured entry input exceeds its bound")
    packet = json.loads(payload)
    if set(packet) != ({"inventory", "worker"} if MODE == "worker" else {"inventory"}):
        raise ValueError("captured entry input shape differs")
    inventory = packet["inventory"]
    if digest(inventory) != INVENTORY_SHA256:
        raise ValueError("private install commitment differs")
    source = guarded_source(DATA / CASE_NAME, CASE_SHA256)
    if snapshot(ENVIRONMENT) != inventory:
        raise ValueError("private install inventory changed")
    # No site.addsitedir or site.main: .pth and sitecustomize are never run.
    site = ENVIRONMENT / "lib" / ("python" + str(sys.version_info.major) + "." + str(sys.version_info.minor))
    site /= "site-packages"
    if not site.is_dir() or site.is_symlink():
        raise ValueError("verified site directory differs")
    sys.path.insert(0, str(site))
    sys.prefix = sys.exec_prefix = str(ENVIRONMENT)
    if MODE == "worker":
        asyncio.run(source_module(source).cold_worker(packet["worker"]))
        result = 0
    elif MODE == "pytest":
        result = run_pytest(source, inventory)
    else:
        raise ValueError("captured entry mode differs")
    if snapshot(ENVIRONMENT) != inventory:
        raise ValueError("private install changed during execution")
    raise SystemExit(result)


if __name__ == "__main__":
    main()
