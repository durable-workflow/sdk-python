from __future__ import annotations

import runpy
import sys
from pathlib import Path

import pytest
from scripts.docstring_cross_references import (
    REPRESENTATIVE_PAGES,
    SUPPORTED_ROLES,
    render_cross_references,
    validate_rendered_reference,
)


def test_supported_roles_render_as_optional_cross_references() -> None:
    source = " ".join(
        [
            ":class:`Client`",
            ":meth:`~durable_workflow.Client.get_result`",
            ":func:`Replay helper <durable_workflow.workflow.replay>`",
            ":attr:`WorkflowEnvironment.runs`",
            ":mod:`durable_workflow.testing`",
            ":exc:`RuntimeError`",
        ]
    )

    rendered = render_cross_references(
        source,
        resolve=lambda target: f"resolved.{target}",
        origin="durable_workflow.testing",
        filepath="src/durable_workflow/testing.py",
        lineno=1,
    )

    assert all(f":{role}:`" not in rendered for role in SUPPORTED_ROLES)
    assert rendered.count("<autoref ") == len(SUPPORTED_ROLES)
    assert rendered.count(" optional>") == len(SUPPORTED_ROLES)
    assert 'identifier="resolved.Client"' in rendered
    assert "<code>get_result</code>" in rendered
    assert "<code>Replay helper</code>" in rendered


def test_rendered_output_validator_loads_without_optional_docs_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "griffe", None)

    namespace = runpy.run_path("scripts/docstring_cross_references.py", run_name="docstring_cross_reference_check")

    assert callable(namespace["validate_rendered_reference"])


def write_reference_pages(site: Path, *, leaked_page: Path | None = None) -> None:
    for relative in REPRESENTATIVE_PAGES:
        page = site / relative
        page.parent.mkdir(parents=True, exist_ok=True)
        content = '<p><a href="#symbol"><code>Symbol</code></a></p>'
        if relative == leaked_page:
            content = "<p>:class:<code>LeakedSymbol</code></p>"
        page.write_text(content, encoding="utf-8")


@pytest.mark.parametrize("leaked_page", REPRESENTATIVE_PAGES)
def test_rendered_output_gate_detects_role_leaks_on_representative_pages(
    tmp_path: Path,
    leaked_page: Path,
) -> None:
    write_reference_pages(tmp_path, leaked_page=leaked_page)

    with pytest.raises(ValueError, match=str(leaked_page)):
        validate_rendered_reference(tmp_path)


def test_rendered_output_gate_accepts_clean_generated_pages(tmp_path: Path) -> None:
    write_reference_pages(tmp_path)

    assert validate_rendered_reference(tmp_path) == len(REPRESENTATIVE_PAGES)
