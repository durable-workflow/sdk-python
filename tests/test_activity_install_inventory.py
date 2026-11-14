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
assert SPEC is not None
assert SPEC.loader is not None
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
    installed_table = io.StringIO(table.getvalue())
    installed_table.seek(0, 2)
    installed_writer = csv.writer(installed_table)
    for extra, data in (("INSTALLER", b"pip\n"), ("REQUESTED", b"")):
        path = str(Path(record).parent / extra)
        (site / path).write_bytes(data)
        checksum = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
        installed_writer.writerow([path, "sha256=" + checksum, len(data)])
    (site / record).write_text(installed_table.getvalue())
    wheel = tmp_path / "sdk.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(name, content)
        archive.writestr(record, table.getvalue())
    return site, package / "__init__.py", wheel, hashlib.sha256(wheel.read_bytes()).hexdigest()


def test_installed_member_matches_the_independently_reviewed_wheel(tmp_path):
    site, origin, wheel, digest = installed_fixture(tmp_path)
    result = inventory.verify_sdk(site, origin, wheel.read_bytes(), digest)
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
        inventory.verify_sdk(site, origin, wheel.read_bytes(), digest)


def test_repeated_installed_record_row_cannot_hide_a_replaced_member(tmp_path):
    site, origin, wheel, digest = installed_fixture(tmp_path)
    record = next(site.glob("*.dist-info/RECORD"))
    record.write_text(record.read_text() + record.read_text().splitlines()[0] + "\n")
    with pytest.raises(ValueError, match="RECORD repeats"):
        inventory.verify_sdk(site, origin, wheel.read_bytes(), digest)


@pytest.mark.parametrize("change", ["extra_metadata", "foreign_entry_points", "extra_record_row", "installer_bytes"])
def test_unreviewed_distribution_members_and_record_rows_are_refused(tmp_path, change):
    site, origin, wheel, digest = installed_fixture(tmp_path)
    metadata = next(site.glob("*.dist-info"))
    if change == "extra_metadata":
        (metadata / "ambient.py").write_text("unreviewed = True\n")
    elif change == "foreign_entry_points":
        (metadata / "entry_points.txt").write_text("[pytest11]\nambient = foreign:plugin\n")
    elif change == "installer_bytes":
        (metadata / "INSTALLER").write_text("foreign installer\n")
    else:
        record = metadata / "RECORD"
        record.write_text(record.read_text() + "foreign.py,sha256=unreviewed,1\n")
    data = wheel.read_bytes()
    with pytest.raises(ValueError):
        inventory.verify_sdk(site, origin, data, digest)


def test_only_expected_installer_entries_and_exact_generated_console_script_are_admitted(tmp_path):
    site, origin, old_wheel, _old_digest = installed_fixture(tmp_path)
    metadata = "durable_workflow-1.0.dist-info"
    entry_points = (metadata + "/entry_points.txt", b"[console_scripts]\nreviewed-cli = durable_workflow:main\n")
    python = tmp_path / "bin/python"
    python.parent.mkdir()
    template = "from %(module)s import %(import_name)s\n%(func)s()\n"
    content = ("#!" + str(python) + "\nfrom durable_workflow import main\nmain()\n").encode()
    script = python.parent / "reviewed-cli"
    script.write_bytes(content)
    (site / entry_points[0]).write_bytes(entry_points[1])
    record = metadata + "/RECORD"
    with zipfile.ZipFile(old_wheel) as archive:
        members = {name: archive.read(name) for name in archive.namelist() if name != record}
    members[entry_points[0]] = entry_points[1]
    table = io.StringIO()
    writer = csv.writer(table)
    for name, data in members.items():
        checksum = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
        writer.writerow([name, "sha256=" + checksum, len(data)])
    writer.writerow([record, "", ""])
    wheel = tmp_path / "console.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
        archive.writestr(record, table.getvalue())
    for name, data in (
        (metadata + "/INSTALLER", b"pip\n"),
        (metadata + "/REQUESTED", b""),
        ("../bin/reviewed-cli", content),
    ):
        checksum = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).decode().rstrip("=")
        writer.writerow([name, "sha256=" + checksum, len(data)])
    (site / record).write_text(table.getvalue())
    data = wheel.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    result = inventory.verify_sdk(site, origin, data, digest, python=python, script_template=template)
    assert "../bin/reviewed-cli" in result
    # Matching attacker-updated RECORD rows cannot approve changed wrappers.
    changed = b"print('unreviewed console code')\n"
    script.write_bytes(changed)
    checksum = base64.urlsafe_b64encode(hashlib.sha256(changed).digest()).decode().rstrip("=")
    rows = list(csv.reader((site / record).read_text().splitlines()))
    for row in rows:
        if row[0] == "../bin/reviewed-cli":
            row[1:] = ["sha256=" + checksum, str(len(changed))]
    table = io.StringIO()
    csv.writer(table).writerows(rows)
    (site / record).write_text(table.getvalue())
    with pytest.raises(ValueError, match="member differs"):
        inventory.verify_sdk(site, origin, data, digest, python=python, script_template=template)
