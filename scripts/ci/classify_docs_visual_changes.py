#!/usr/bin/env python3
"""Classify semantic navigation and search changes in Python documentation UI sources."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from collections.abc import Sequence
from pathlib import Path, PurePosixPath

CUSTOMER_FACING_EXTENSIONS = (
    ".css",
    ".htm",
    ".html",
    ".jinja",
    ".jinja2",
    ".js",
    ".jsx",
    ".less",
    ".sass",
    ".scss",
    ".svelte",
    ".tsx",
    ".twig",
    ".vue",
)

SEARCH_EVIDENCE_POLICY_PATHS = frozenset(
    {
        ".github/workflows/docs-visual.yml",
        "scripts/ci/classify_docs_visual_changes.py",
    }
)
SEARCH_SOURCE_PATHS = frozenset(
    {
        "docs/javascripts/search-accessibility.js",
        "overrides/main.html",
    }
)

NAVIGATION_PATTERN = re.compile(
    r"(?ix)(?:"
    r"[.#](?:md-nav(?:[\w-]*)?|nav-(?:drawer|menu|toggle)(?:[\w-]*)?|__drawer)"
    r"|(?:id|for)\s*=\s*[\"']__drawer[\"']"
    r"|data-md-(?:component|toggle)\s*=\s*[\"']?(?:navigation|drawer)"
    r")"
)
SEARCH_PATTERN = re.compile(
    r"(?ix)(?:"
    r"[.#](?:md-search(?:[\w-]*)?|search-(?:dialog|overlay|input|result)(?:[\w-]*)?|__search)"
    r"|(?:id|for)\s*=\s*[\"']__search[\"']"
    r"|data-md-(?:component|toggle)\s*=\s*[\"']?search(?:[\"'\s>]|$)"
    r")"
)

_HTML_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
_JINJA_COMMENT = re.compile(r"\{#.*?#\}", re.DOTALL)
_HTML_TAG = re.compile(r"<[^>]+>", re.DOTALL)
_SCRIPT_BLOCK = re.compile(r"<script\b[^>]*>(.*?)</script\s*>", re.IGNORECASE | re.DOTALL)
_STYLE_BLOCK = re.compile(r"<style\b[^>]*>(.*?)</style\s*>", re.IGNORECASE | re.DOTALL)


def _canonical_repo_path(path: str) -> bool:
    if not path or path.startswith("/") or "\\" in path or any(character in path for character in "\0\r\n"):
        return False
    normalized = PurePosixPath(path)
    return str(normalized) == path and all(part not in {"", ".", ".."} for part in normalized.parts)


def strip_c_like_comments(source: str) -> str:
    """Remove line and block comments while preserving quoted selectors and URLs."""

    output: list[str] = []
    index = 0
    quote: str | None = None
    while index < len(source):
        character = source[index]
        following = source[index + 1] if index + 1 < len(source) else ""
        if quote is not None:
            output.append(character)
            if character == "\\" and following:
                output.append(following)
                index += 2
                continue
            if character == quote:
                quote = None
            index += 1
            continue
        if character in {"'", '"', "`"}:
            quote = character
            output.append(character)
            index += 1
            continue
        if character == "/" and following == "*":
            end = source.find("*/", index + 2)
            index = len(source) if end < 0 else end + 2
            output.append(" ")
            continue
        if character == "/" and following == "/":
            end = source.find("\n", index + 2)
            index = len(source) if end < 0 else end
            output.append("\n")
            continue
        output.append(character)
        index += 1
    return "".join(output)


def semantic_source(path: str, source: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix in {".htm", ".html", ".jinja", ".jinja2", ".svelte", ".twig", ".vue"}:
        uncommented = _JINJA_COMMENT.sub(" ", _HTML_COMMENT.sub(" ", source))
        tags = _HTML_TAG.findall(uncommented)
        scripts = [strip_c_like_comments(block) for block in _SCRIPT_BLOCK.findall(uncommented)]
        styles = [strip_c_like_comments(block) for block in _STYLE_BLOCK.findall(uncommented)]
        return "\n".join([*tags, *scripts, *styles])
    if suffix in {".css", ".js", ".jsx", ".less", ".sass", ".scss", ".tsx"}:
        return strip_c_like_comments(source)
    return ""


def changed_paths(root: Path, base_ref: str) -> tuple[str, ...]:
    result = subprocess.run(
        ["git", "diff", "--name-only", "--no-renames", "--diff-filter=ACDMRTUXB", "-z", f"{base_ref}...HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return tuple(entry.decode("utf-8") for entry in result.stdout.split(b"\0") if entry)


def _file_versions(root: Path, path: str, base_ref: str | None) -> tuple[str, ...]:
    candidate = root / path
    if candidate.is_symlink():
        return ()
    versions: list[str] = []
    if candidate.is_file():
        versions.append(candidate.read_text(encoding="utf-8", errors="replace"))
    if base_ref:
        result = subprocess.run(
            ["git", "show", f"{base_ref}:{path}"],
            cwd=root,
            check=False,
            capture_output=True,
        )
        if result.returncode == 0:
            versions.append(result.stdout.decode("utf-8", errors="replace"))
    return tuple(versions)


def classify_changes(root: Path, paths: Sequence[str], base_ref: str | None = None) -> dict[str, list[str]]:
    matches: dict[str, set[str]] = {"navigation": set(), "search": set()}
    for path in sorted(set(paths)):
        if not _canonical_repo_path(path):
            matches["navigation"].add(path)
            matches["search"].add(path)
            continue
        if path in SEARCH_EVIDENCE_POLICY_PATHS:
            matches["search"].add(path)
            continue
        if not path.lower().endswith(CUSTOMER_FACING_EXTENSIONS):
            continue
        searchable = "\n".join(semantic_source(path, source) for source in _file_versions(root, path, base_ref))
        if NAVIGATION_PATTERN.search(searchable):
            matches["navigation"].add(path)
        if path in SEARCH_SOURCE_PATHS or SEARCH_PATTERN.search(searchable):
            matches["search"].add(path)
    return {interaction: sorted(matched_paths) for interaction, matched_paths in matches.items() if matched_paths}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--base-ref")
    parser.add_argument("--changed-file", action="append", default=[])
    parser.add_argument("--github-output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.root.resolve()
    paths = list(args.changed_file)
    if args.base_ref:
        paths.extend(changed_paths(root, args.base_ref))
    if not paths:
        raise SystemExit("provide --base-ref or at least one --changed-file")

    classification = classify_changes(root, paths, args.base_ref)
    if args.github_output is not None:
        with args.github_output.open("a", encoding="utf-8") as output:
            print(f"required={str(bool(classification)).lower()}", file=output)
            for interaction in ("navigation", "search"):
                print(f"{interaction}={str(interaction in classification).lower()}", file=output)
    print(json.dumps({"classification": classification}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
