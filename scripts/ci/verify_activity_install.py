"""Verify installed SDK files against the independently approved registry wheel."""

from __future__ import annotations

import base64
import configparser
import csv
import hashlib
import io
import os
import stat
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


def generated_members(site, record, members, python, script_template):
    metadata = str(PurePosixPath(record).parent)
    generated = {metadata + "/INSTALLER": b"pip\n", metadata + "/REQUESTED": b""}
    entry_points = members.get(metadata + "/entry_points.txt")
    if entry_points is None:
        return generated
    if python is None or script_template is None:
        raise ValueError("console scripts require the trusted base installer's template")
    parser = configparser.ConfigParser()
    parser.read_string(entry_points.decode())
    if parser.sections() != ["console_scripts"]:
        raise ValueError("SDK entry point groups differ")
    for name, target in parser.items("console_scripts"):
        if not name or "/" in name or "\\" in name or name in {".", ".."}:
            raise ValueError("SDK console script name differs")
        module, function = target.split(":", 1)
        body = script_template % {"module": module, "import_name": function.split(".")[0], "func": function}
        relative = os.path.relpath(Path(python).parent / name, site)
        generated[relative] = ("#!" + str(python) + "\n" + body).encode()
    return generated


def verify_membership(site, record, expected):
    roots = (site / "durable_workflow", site / PurePosixPath(record).parent)
    actual = {
        str(path.relative_to(site)) for root in roots for path in root.rglob("*") if path.is_file() or path.is_symlink()
    }
    distribution = {path for path in expected if not path.startswith("../")} | {record}
    if actual != distribution:
        raise ValueError("installed SDK has missing or extra distribution files")


def verify_installed_record(site, record, expected):
    installed_record = site / record
    if installed_record.is_symlink() or not installed_record.is_file():
        raise ValueError("installed SDK RECORD is unavailable")
    installed_rows = record_rows(installed_record.read_text())
    if set(installed_rows) != set(expected) | {record} or installed_rows[record] != ("", ""):
        raise ValueError("installed SDK RECORD membership differs")
    for path, data in expected.items():
        digest = base64.urlsafe_b64encode(bytes.fromhex(data["sha256"])).decode().rstrip("=")
        if installed_rows[path] != ("sha256=" + digest, str(data["bytes"])):
            raise ValueError("installed SDK RECORD differs from the reviewed distribution")
    expected[record] = {"sha256": sha256(installed_record.read_bytes()), "bytes": installed_record.stat().st_size}
    return expected


def verify_sdk(site, origin, wheel, expected_sha256, *, python=None, script_template=None):
    site = Path(site).resolve()
    if Path(origin).resolve() != site / "durable_workflow/__init__.py":
        raise ValueError("SDK import origin differs from the fresh environment")
    record, members = wheel_members(wheel, expected_sha256)
    generated = generated_members(site, record, members, python, script_template)
    if set(generated) & set(members):
        raise ValueError("installer entries overlap reviewed wheel members")
    members.update(generated)
    verify_membership(site, record, members)
    expected = {path: installed_member(site, path, content) for path, content in members.items()}
    return verify_installed_record(site, record, expected)
