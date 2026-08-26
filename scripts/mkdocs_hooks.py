"""MkDocs hooks for machine-owned API-reference onboarding commands."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

try:
    from scripts.api_reference_release import render_onboarding_resolvers
except ModuleNotFoundError as error:  # pragma: no cover - MkDocs loads hook files outside the package path
    if error.name != "scripts":
        raise
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.api_reference_release import render_onboarding_resolvers


def on_page_markdown(markdown: str, page: Any, config: Any, files: Any) -> str:
    """Render versionless onboarding resolvers on the landing page."""
    del config, files
    if page.file.src_uri != "index.md":
        return markdown

    return render_onboarding_resolvers(markdown)
