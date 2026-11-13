from __future__ import annotations

import base64
import csv
import hashlib
import importlib.util
import io
import zipfile
from pathlib import Path

import pytest

PATH = Path(__file__).parents[1] / "scripts/ci/verify_activity_install.py"
SPEC = importlib.util.spec_from_file_location("verify_activity_install", PATH)
assert SPEC and SPEC.loader
inventory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(inventory)


def installed_fixture(tmp_path):
    site = tmp_path / "site"
    package = site / "durable_workflow"
    package.mkdir(parents=True)
    content = b"VALUE = 'reviewed wheel'\n"
    (package / "__init__.py").write_bytes(content)
    name = "durable_workflow/__init__.py"
    record = "durable_workflow-1.0.dist-info/RECORD"
    table = io.StringIO()
    writer = csv.writer(table)
    digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).decode().rstrip("=")
    writer.writerow([name, "sha256=" + digest, len(content)])
    writer.writerow([record, "", ""])
    (site / record).parent.mkdir()
    (site / record).write_text(table.getvalue())
    wheel = tmp_path / "sdk.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(name, content)
        archive.writestr(record, table.getvalue())
    return site, package / "__init__.py", wheel, hashlib.sha256(wheel.read_bytes()).hexdigest()


def test_installed_member_matches_the_independently_reviewed_wheel(tmp_path):
    site, origin, wheel, digest = installed_fixture(tmp_path)
    result = inventory.verify_sdk(site, origin, wheel, digest)
    assert "durable_workflow/__init__.py" in result
    assert "durable_workflow-1.0.dist-info/RECORD" in result


@pytest.mark.parametrize("change", ["replace", "extra", "wrong_origin", "wrong_wheel"])
def test_version_and_origin_alone_cannot_approve_substituted_or_extra_sdk_bytes(tmp_path, change):
    site, origin, wheel, digest = installed_fixture(tmp_path)
    if change == "replace":
        origin.write_text("VALUE = 'substituted module'\n")
    elif change == "extra":
        origin.with_name("extra.py").write_text("VALUE = 'ambient file'\n")
    elif change == "wrong_origin":
        origin = tmp_path / "checkout/src/durable_workflow/__init__.py"
    else:
        digest = "0" * 64
    with pytest.raises(ValueError):
        inventory.verify_sdk(site, origin, wheel, digest)


def test_repeated_installed_record_row_cannot_hide_a_replaced_member(tmp_path):
    site, origin, wheel, digest = installed_fixture(tmp_path)
    record = next(site.glob("*.dist-info/RECORD"))
    record.write_text(record.read_text() + record.read_text().splitlines()[0] + "\n")
    with pytest.raises(ValueError, match="RECORD repeats"):
        inventory.verify_sdk(site, origin, wheel, digest)
