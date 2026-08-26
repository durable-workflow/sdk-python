#!/usr/bin/env python3
"""Render and validate cross-references in generated API docstrings."""

from __future__ import annotations

import argparse
import html
import re
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

try:
    from griffe import Extension as GriffeExtension
except ModuleNotFoundError as error:
    if error.name != "griffe":
        raise

    class GriffeExtension:  # type: ignore[no-redef]
        """Fallback base for rendered-output validation without docs extras."""


SUPPORTED_ROLES = ("class", "meth", "func", "attr", "mod", "exc")
SPHINX_ROLE = re.compile(rf":(?P<role>{'|'.join(SUPPORTED_ROLES)}):`(?P<reference>[^`\n]+)`")
EXPLICIT_TITLE = re.compile(r"(?P<title>.+?)\s*<(?P<target>[^<>]+)>$")
LEAKED_RENDERED_ROLE = re.compile(
    rf":(?:{'|'.join(SUPPORTED_ROLES)}):(?=`|<code(?:\s|>))",
    re.IGNORECASE,
)
REPRESENTATIVE_PAGES = (
    Path("reference/client/index.html"),
    Path("reference/serializer/index.html"),
    Path("reference/testing/index.html"),
)


def _reference_parts(reference: str) -> tuple[str, str]:
    reference = reference.strip()
    explicit = EXPLICIT_TITLE.fullmatch(reference)
    if explicit:
        target = explicit.group("target").strip()
        title = explicit.group("title").strip()
    else:
        target = reference
        title = ""

    shortened = target.startswith("~")
    target = target.removeprefix("~")
    if not title:
        title = target.rsplit(".", 1)[-1] if shortened else target
    return title, target


def render_cross_references(
    docstring: str,
    *,
    resolve: Callable[[str], str],
    origin: str,
    filepath: str,
    lineno: int,
) -> str:
    """Translate supported Sphinx roles into optional mkdocs-autorefs nodes."""

    def replace(match: re.Match[str]) -> str:
        title, target = _reference_parts(match.group("reference"))
        attributes = {
            "identifier": resolve(target),
            "domain": "py",
            "role": match.group("role"),
            "origin": origin,
            "filepath": filepath,
            "lineno": str(lineno),
        }
        rendered_attributes = " ".join(
            f'{name}="{html.escape(value, quote=True)}"' for name, value in attributes.items()
        )
        return f"<autoref {rendered_attributes} optional><code>{html.escape(title)}</code></autoref>"

    return SPHINX_ROLE.sub(replace, docstring)


class DocstringCrossReferenceExtension(GriffeExtension):
    """Translate cross-reference roles after Griffe has constructed each object."""

    @staticmethod
    def _render_object(obj: Any) -> None:
        docstring = obj.docstring
        if docstring is None or SPHINX_ROLE.search(docstring.value) is None:
            return

        def resolve(target: str) -> str:
            first, separator, remainder = target.partition(".")
            try:
                resolved = obj.resolve(first)
            except Exception:  # Griffe uses several resolution errors across supported versions.
                resolved = first
            return f"{resolved}.{remainder}" if separator else resolved

        docstring.value = render_cross_references(
            docstring.value,
            resolve=resolve,
            origin=obj.path,
            filepath=str(obj.filepath),
            lineno=docstring.lineno or 0,
        )

    def on_module(self, *, mod: Any, **kwargs: Any) -> None:
        """Render roles on the initially loaded module as well as submodules."""
        self._render_object(mod)

    def on_object(self, *, obj: Any, **kwargs: Any) -> None:
        """Render roles on public members after their object tree is complete."""
        self._render_object(obj)


def validate_rendered_reference(site: Path) -> int:
    """Reject leaked role syntax in the generated public API reference."""
    missing = [relative for relative in REPRESENTATIVE_PAGES if not (site / relative).is_file()]
    if missing:
        paths = ", ".join(str(path) for path in missing)
        raise ValueError(f"generated API reference is missing representative pages: {paths}")

    reference_pages = sorted((site / "reference").glob("*/index.html"))
    if not reference_pages:
        raise ValueError("generated API reference does not contain public module pages")

    leaks: list[str] = []
    for page in reference_pages:
        rendered = html.unescape(page.read_text(encoding="utf-8"))
        if match := LEAKED_RENDERED_ROLE.search(rendered):
            leaks.append(f"{page.relative_to(site)} ({match.group(0)})")
    if leaks:
        raise ValueError("generated API reference leaked Sphinx role syntax: " + ", ".join(leaks))
    return len(reference_pages)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", type=Path, default=Path("site"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        page_count = validate_rendered_reference(args.site)
    except ValueError as error:
        raise SystemExit(str(error)) from error
    print(f"Validated docstring cross-references in {page_count} generated API pages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
