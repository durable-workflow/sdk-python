"""Inert first-open controls; no SDK, API, Worker or Docker invocation."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from scripts.ci import activity_acceptance_consumer as consumer
from scripts.ci import activity_acceptance_entry as entry


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    environment, data = tmp_path / "environment", tmp_path / "data"
    site = environment / "lib" / ("python" + str(sys.version_info.major) + "." + str(sys.version_info.minor))
    site = site / "site-packages"
    site.mkdir(parents=True)
    data.mkdir()
    case = data / "test_captured.py"
    case.write_text("async def cold_worker(p): print('fixed inert result', p['marker'])\n")
    source = Path(entry.__file__).read_text()
    source = source.replace('Path("/run/acceptance-env")', "Path(" + repr(str(environment)) + ")")
    source = source.replace('Path("/run/acceptance-data")', "Path(" + repr(str(data)) + ")")
    monkeypatch.setattr(consumer, "CAPTURED_ENTRY", source)
    monkeypatch.setattr(entry, "ENVIRONMENT", environment)
    monkeypatch.setattr(entry, "DATA", data)
    monkeypatch.setattr(entry, "CASE_NAME", case.name)
    return environment, data, site, case


def launch(case, installed, *, no_site=True):
    source = consumer.entry_source(
        "worker", case.name, hashlib.sha256(case.read_bytes()).hexdigest(), installed,
    )
    arguments = [sys.executable, "-I", *(["-S"] if no_site else []), "-B", "-c", source]
    return subprocess.run(
        arguments, input=json.dumps({"inventory": installed, "worker": {"marker": "owned"}}),
        env={"PATH": os.defpath}, capture_output=True, text=True, timeout=10,
    )


def test_no_site_entry_can_execute_only_the_verified_inert_source(sandbox):
    environment, _data, _site, case = sandbox
    installed = entry.snapshot(environment)
    result = launch(case, installed)
    assert result.returncode == 0 and result.stdout == "fixed inert result owned\n"


def test_entry_refuses_normal_site_even_when_other_inputs_match(sandbox):
    environment, _data, _site, case = sandbox
    result = launch(case, entry.snapshot(environment), no_site=False)
    assert result.returncode != 0 and "no-site Python" in result.stderr
    assert not result.stdout


def test_added_startup_hook_refuses_before_any_case_code(sandbox):
    environment, _data, site, case = sandbox
    installed = entry.snapshot(environment)
    (site / "sitecustomize.py").write_text("raise RuntimeError('startup replacement executed')\n")
    result = launch(case, installed)
    assert result.returncode != 0 and "install inventory changed" in result.stderr
    assert "startup replacement executed" not in result.stderr and not result.stdout


def test_predecessor_runs_pth_before_guard_successor_refuses_source_before_effect(sandbox, tmp_path):
    environment, _data, site, case = sandbox
    subprocess.run(
        [sys.executable, "-I", "-B", "-m", "venv", "--without-pip", "--copies", str(environment)],
        check=True, capture_output=True, text=True, timeout=30,
    )
    marker = tmp_path / "pre-open-marker"
    (site / "preopen.pth").write_text(
        "import pathlib; pathlib.Path(" + repr(str(marker)) + ").write_text('pre-open')\n",
    )
    installed = entry.snapshot(environment)
    source = consumer.entry_source("worker", case.name, hashlib.sha256(case.read_bytes()).hexdigest(), installed)
    old = subprocess.run(
        [str(environment / "bin/python"), "-I", "-B", "-c", "raise RuntimeError('late guard')"],
        env={"PATH": os.defpath}, capture_output=True, text=True, timeout=10,
    )
    assert old.returncode != 0 and marker.read_text() == "pre-open"
    marker.unlink()
    case.write_text("raise RuntimeError('foreign case executed')\n")
    corrected = subprocess.run(
        [sys.executable, "-I", "-S", "-B", "-c", source],
        input=json.dumps({"inventory": installed, "worker": {"marker": "owned"}}),
        env={"PATH": os.defpath}, capture_output=True, text=True, timeout=10,
    )
    assert corrected.returncode != 0 and "captured case source differs" in corrected.stderr
    assert "foreign case executed" not in corrected.stderr and not marker.exists() and not corrected.stdout


def test_post_capture_path_replacement_keeps_the_verified_bytes(sandbox):
    _environment, _data, _site, case = sandbox
    case.write_text("chosen = 'captured'\n")
    source = entry.guarded_source(case, hashlib.sha256(case.read_bytes()).hexdigest())
    case.write_text("raise RuntimeError('replacement must not execute')\n")
    assert entry.source_module(source).chosen == "captured"


def test_pytest_collector_uses_captured_bytes_and_no_mutable_conftest(sandbox, monkeypatch):
    _environment, data, _site, case = sandbox
    source = b"def test_captured(): assert 2 + 2 == 4\n"
    case.write_text("raise RuntimeError('mutable test must not execute')\n")
    (data / "conftest.py").write_text("raise RuntimeError('mutable conftest must not execute')\n")
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    assert entry.run_pytest(source, {}) == 0
    assert (data / "identity.xml").is_file()
