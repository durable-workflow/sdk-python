"""MkDocs hooks for manifest-derived API-reference release guidance."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

try:
    from scripts.api_reference_release import load_release_identity, render_release_identity
except ModuleNotFoundError as error:  # pragma: no cover - MkDocs loads hook files outside the package path
    if error.name != "scripts":
        raise
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.api_reference_release import load_release_identity, render_release_identity


def on_page_markdown(markdown: str, page: Any, config: Any, files: Any) -> str:
    """Render the API-reference release tuple from pyproject.toml."""
    del files
    if page.file.src_uri != "index.md":
        return markdown

    repo_root = Path(config.config_file_path).resolve().parent
    return render_release_identity(markdown, load_release_identity(repo_root))
