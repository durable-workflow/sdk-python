#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from classify_docs_visual_changes import classify_changes


class DocumentationVisualClassificationTest(unittest.TestCase):
    def test_template_comments_and_unrelated_prose_do_not_create_interactions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            template = root / "overrides" / "partials" / "source.html"
            template.parent.mkdir(parents=True)
            template.write_text(
                "{# Keep repository navigation static; do not add .md-nav or __drawer behavior. #}\n"
                "<!-- Search dialog and data-md-toggle=drawer are documentation terms here. -->\n"
                "<p>Navigation menus and search dialogs are described in this prose.</p>\n"
                '<a class="md-source" data-dw-component="repository-link">Repository</a>\n',
                encoding="utf-8",
            )

            self.assertEqual({}, classify_changes(root, ["overrides/partials/source.html"]))

    def test_css_and_javascript_comments_do_not_create_interactions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stylesheet = root / "docs" / "stylesheets" / "branding.css"
            javascript = root / "docs" / "javascripts" / "branding.js"
            stylesheet.parent.mkdir(parents=True)
            javascript.parent.mkdir(parents=True)
            stylesheet.write_text("/* .md-nav__link and #__drawer */\n.logo { color: blue; }\n", encoding="utf-8")
            javascript.write_text(
                "// document.querySelector('.md-search__input')\nconst help = 'https://example.com/navigation';\n",
                encoding="utf-8",
            )

            self.assertEqual(
                {},
                classify_changes(
                    root,
                    ["docs/stylesheets/branding.css", "docs/javascripts/branding.js"],
                ),
            )

    def test_semantic_selectors_create_their_interaction_classes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stylesheet = root / "docs" / "stylesheets" / "layout.css"
            javascript = root / "docs" / "javascripts" / "controls.js"
            stylesheet.parent.mkdir(parents=True)
            javascript.parent.mkdir(parents=True)
            stylesheet.write_text(".md-nav__link { display: block; }\n", encoding="utf-8")
            javascript.write_text(
                "const input = document.querySelector('.md-search__input');\n",
                encoding="utf-8",
            )

            self.assertEqual(
                {
                    "navigation": ["docs/stylesheets/layout.css"],
                    "search": ["docs/javascripts/controls.js"],
                },
                classify_changes(
                    root,
                    ["docs/stylesheets/layout.css", "docs/javascripts/controls.js"],
                ),
            )

    def test_removing_the_last_selector_still_requires_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root, check=True)
            stylesheet = root / "docs" / "stylesheets" / "layout.css"
            stylesheet.parent.mkdir(parents=True)
            stylesheet.write_text(".md-nav__link { display: block; }\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            subprocess.run(
                [
                    "git",
                    "-c",
                    "user.name=Visual Policy Test",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "-qm",
                    "Add selector",
                ],
                cwd=root,
                check=True,
            )
            base_ref = subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
            ).stdout.strip()
            stylesheet.write_text(".content { display: block; }\n", encoding="utf-8")

            self.assertEqual(
                {"navigation": ["docs/stylesheets/layout.css"]},
                classify_changes(root, ["docs/stylesheets/layout.css"], base_ref),
            )

    def test_policy_source_changes_requalify_the_supported_search_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(
                {
                    "search": ["scripts/ci/classify_docs_visual_changes.py"],
                },
                classify_changes(Path(directory), ["scripts/ci/classify_docs_visual_changes.py"]),
            )


if __name__ == "__main__":
    unittest.main()
