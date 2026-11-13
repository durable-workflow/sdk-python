"""Verify installed SDK files against the independently approved registry wheel."""

from __future__ import annotations

import base64
import csv
import hashlib
import importlib.metadata
import importlib.util
import io
import json
import stat
import sys
import sysconfig
import zipfile
from pathlib import Path, PurePosixPath


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def record_rows(text):
    result = {}
    for path, checksum, size in csv.reader(io.StringIO(text)):
        if path in result:
            raise ValueError("SDK RECORD repeats a member")
        result[path] = (checksum, size)
    return result


def member(archive, path, checksum, size):
    parts = PurePosixPath(path)
    if parts.is_absolute() or ".." in parts.parts or "\\" in path:
        raise ValueError("SDK wheel member path is not relative")
    info = archive.getinfo(path)
    if stat.S_ISLNK(info.external_attr >> 16):
        raise ValueError("SDK wheel member is a symlink")
    content = archive.read(path)
    digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).decode().rstrip("=")
    if checksum != "sha256=" + digest or int(size) != len(content):
        raise ValueError("SDK wheel member differs from RECORD")
    return content


def wheel_members(data, expected_sha256):
    if sha256(data) != expected_sha256:
        raise ValueError("SDK wheel differs from its reviewed digest")
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        names = archive.namelist()
        records = [name for name in names if name.endswith(".dist-info/RECORD")]
        if len(names) != len(set(names)) or len(records) != 1:
            raise ValueError("SDK wheel membership differs")
        record = records[0]
        rows = record_rows(archive.read(record).decode())
        if set(names) != set(rows) or rows[record] != ("", ""):
            raise ValueError("SDK wheel members differ from RECORD membership")
        expected = {path: member(archive, path, *row) for path, row in rows.items() if path != record}
    return record, expected


def installed_member(site, path, content):
    installed = site / path
    if installed.is_symlink() or not installed.is_file() or installed.read_bytes() != content:
        raise ValueError("installed SDK member differs from reviewed wheel")
    return {"sha256": sha256(content), "bytes": len(content)}


def verify_sdk(site, origin, wheel, expected_sha256):
    site = Path(site).resolve()
    if Path(origin).resolve() != site / "durable_workflow/__init__.py":
        raise ValueError("SDK import origin differs from the fresh environment")
    record, members = wheel_members(wheel, expected_sha256)
    expected = {path: installed_member(site, path, content) for path, content in members.items()}
    package = site / "durable_workflow"
    actual = {str(path.relative_to(site)) for path in package.rglob("*") if path.is_file() or path.is_symlink()}
    package_members = {path for path in expected if path.startswith("durable_workflow/")}
    if actual != package_members:
        raise ValueError("installed SDK has missing or extra package files")
    installed_record = site / record
    if installed_record.is_symlink() or not installed_record.is_file():
        raise ValueError("installed SDK RECORD is unavailable")
    installed_rows = record_rows(installed_record.read_text())
    for path, data in expected.items():
        digest = base64.urlsafe_b64encode(bytes.fromhex(data["sha256"])).decode().rstrip("=")
        if installed_rows.get(path) != ("sha256=" + digest, str(data["bytes"])):
            raise ValueError("installed SDK RECORD differs from the reviewed wheel")
    expected[record] = {"sha256": sha256(installed_record.read_bytes()), "bytes": installed_record.stat().st_size}
    return expected


def main():
    encoded = sys.stdin.buffer.readline(64 * 1024 * 1024 + 1)
    if len(encoded) > 64 * 1024 * 1024 or not encoded.endswith(b"\n"):
        raise ValueError("SDK inventory input exceeds its bound")
    packet = json.loads(encoded)
    if set(packet) != {"wheel_base64", "wheel_sha256", "sdk_version"}:
        raise ValueError("SDK inventory input shape differs")
    wheel = base64.b64decode(packet["wheel_base64"], validate=True)
    spec = importlib.util.find_spec("durable_workflow")
    if spec is None or spec.origin is None:
        raise ValueError("installed SDK is unavailable")
    if importlib.metadata.version("durable-workflow") != packet["sdk_version"]:
        raise ValueError("installed SDK version differs")
    result = verify_sdk(sysconfig.get_path("purelib"), spec.origin, wheel, packet["wheel_sha256"])
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
