"""Trusted argv bootstrap inside the fixed published Python image.

No host interpreter, virtualenv, source directory or credential directory is
mounted. The controller sends only already captured inputs and synthetic data.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import linecache
import os
import stat
import subprocess
import sys
import tempfile
import venv
from pathlib import Path
from types import ModuleType, SimpleNamespace

RESULT_PREFIX = "SDK_PYTHON_CONSUMER_RESULT="
ROLES = {"adapter", "bootstrap", "proxy", "wheel", "requirements"}
MAX_BUNDLE_BYTES = 8 * 1024 * 1024
IMAGE_INTERPRETER = Path("/usr/local/bin/python3.12")


def approved_environment_link(relative, target):
    return (
        (relative == "lib64" and target == "lib")
        or (relative in {"bin/python", "bin/python3"} and target == "python3.12")
        or (relative == "bin/python3.12" and target == str(IMAGE_INTERPRETER))
    )


def decode_inputs(bundle):
    if set(bundle["sources"]) != ROLES:
        raise ValueError("captured source roles differ")
    result = {}
    for role, entry in bundle["sources"].items():
        data = base64.b64decode(entry["base64"], validate=True)
        if hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise ValueError("captured bytes differ: " + role)
        result[role] = data
    return result


def execute_source(data, name):
    module = ModuleType(name)
    module.__file__ = "captured-" + name + ".py"
    sys.modules[name] = module
    linecache.cache[module.__file__] = (len(data), None, data.decode().splitlines(keepends=True), module.__file__)
    # The bytes were captured before runtime start. No executable input path
    # is reopened or imported through a loader.
    exec(compile(data, module.__file__, "exec"), module.__dict__)
    return module


def inventory(root):
    entries = []
    total = 0
    for path in sorted(root.rglob("*")):
        relative = str(path.relative_to(root))
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        if path.is_symlink():
            target = os.readlink(path)
            if not approved_environment_link(relative, target):
                raise ValueError("unexpected installed-environment link")
            entries.append([relative, "symlink", target])
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
            raise ValueError("unexpected installed-environment file")
        total += info.st_size
        if total > 128 * 1024 * 1024 or len(entries) > 10_000:
            raise ValueError("installed-environment inventory exceeds bound")
        entries.append([relative, stat.S_IMODE(info.st_mode), hashlib.sha256(path.read_bytes()).hexdigest()])
    digest = hashlib.sha256(json.dumps(entries, separators=(",", ":")).encode()).hexdigest()
    return {"sha256": digest, "files": len(entries), "bytes": total}


def prepare(bundle, sources):
    if "prepared" in bundle:
        raise ValueError("caller may not supply a prepared interpreter")
    root = Path(tempfile.mkdtemp(prefix="sdk-python-installed-"))
    environment = root / "venv"
    # Scratch remains noexec. These fresh internal links may target only the
    # immutable interpreter in the fixed image, never a host/caller program.
    venv.EnvBuilder(with_pip=True, symlinks=True).create(environment)
    lock = root / "requirements.txt"
    lock.write_bytes(sources["requirements"])
    lock.chmod(0o400)
    python = environment / "bin/python"
    executable = python.lstat()
    if (
        not stat.S_ISLNK(executable.st_mode)
        or executable.st_nlink != 1
        or executable.st_uid != os.getuid()
        or python.resolve() != IMAGE_INTERPRETER
    ):
        raise ValueError("fresh interpreter identity differs")
    image_binary = IMAGE_INTERPRETER.lstat()
    if (
        not stat.S_ISREG(image_binary.st_mode)
        or image_binary.st_uid != 0
        or image_binary.st_nlink != 1
        or image_binary.st_mode & 0o022
    ):
        raise ValueError("immutable image interpreter shape differs")
    binary_sha256 = hashlib.sha256(python.read_bytes()).hexdigest()
    if binary_sha256 != hashlib.sha256(IMAGE_INTERPRETER.read_bytes()).hexdigest():
        raise ValueError("virtualenv interpreter differs from fixed image interpreter")
    clean_environment = {"PATH": os.defpath, "TMPDIR": str(root), "PIP_CONFIG_FILE": "/dev/null"}
    subprocess.run(
        [
            str(python),
            "-I",
            "-B",
            "-m",
            "pip",
            "--isolated",
            "install",
            "--no-compile",
            "--no-cache-dir",
            "--only-binary=:all:",
            "--require-hashes",
            "--disable-pip-version-check",
            "--progress-bar",
            "off",
            "--index-url",
            "https://pypi.org/simple",
            "-r",
            str(lock),
        ],
        check=True,
        env=clean_environment,
        timeout=180,
    )
    before = inventory(environment)
    bundle["prepared"] = {
        "root": str(environment),
        "binary_sha256": binary_sha256,
        "config_sha256": hashlib.sha256((environment / "pyvenv.cfg").read_bytes()).hexdigest(),
        "inventory": before,
    }
    # Both the child program and its inputs come from the same captured bytes.
    # The interpreter links here to the immutable image, never supplied
    # by a host pathname. Its complete environment is checked around the call.
    result = subprocess.run(
        [str(python), "-I", "-B", "-c", sources["bootstrap"].decode(), "--prepared"],
        input=json.dumps(bundle).encode(),
        env=clean_environment,
        timeout=300,
    )
    if inventory(environment) != before:
        raise ValueError("installed environment changed")
    raise SystemExit(result.returncode)


def main():
    raw = sys.stdin.buffer.read(MAX_BUNDLE_BYTES + 1)
    if len(raw) > MAX_BUNDLE_BYTES:
        raise ValueError("captured bundle exceeds bound")
    bundle = json.loads(raw)
    sources = decode_inputs(bundle)
    if sys.argv[1:] != ["--prepared"]:
        prepare(bundle, sources)
    prepared = bundle["prepared"]
    environment = Path(prepared["root"])
    if Path(sys.prefix) != environment or Path(sys.executable).parent != environment / "bin":
        raise ValueError("consumer did not start in its private registry environment")
    if (
        hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest() != prepared["binary_sha256"]
        or hashlib.sha256((environment / "pyvenv.cfg").read_bytes()).hexdigest() != prepared["config_sha256"]
    ):
        raise ValueError("prepared interpreter/config identity changed")
    before = inventory(environment)
    if before != prepared["inventory"]:
        raise ValueError("prepared environment changed before consumer execution")
    root = environment.parent
    credential = root / "fixture.env"
    descriptor = os.open(credential, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write("DW_AUTH_TOKEN=" + bundle["token"] + "\n")
    adapter = execute_source(sources["adapter"], "installed_command_keys")
    try:
        result = asyncio.run(
            adapter.main(
                SimpleNamespace(
                    upstream=bundle["upstream"],
                    credential_file=credential,
                    proxy_data=sources["proxy"],
                    wheel_data=sources["wheel"],
                    adapter_sha256=bundle["sources"]["adapter"]["sha256"],
                    server_version=bundle["server_version"],
                    output=root / "consumer.json",
                )
            )
        )
    finally:
        credential.unlink(missing_ok=True)
    after = inventory(environment)
    if before != after:
        raise ValueError("installed environment changed during consumer execution")
    result["toolchain"] = {
        "interpreter_sha256": prepared["binary_sha256"],
        "venv_config_sha256": prepared["config_sha256"],
        "environment_inventory": before,
        "post_inventory_equal": True,
        "requirements_sha256": bundle["sources"]["requirements"]["sha256"],
    }
    print(RESULT_PREFIX + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
