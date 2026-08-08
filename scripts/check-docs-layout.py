#!/usr/bin/env python3
"""Exercise responsive MkDocs search and navigation layout in Chromium."""

from __future__ import annotations

import argparse
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import Browser, Locator, Page, sync_playwright

VIEWPORTS = ((1440, 900), (768, 1024), (390, 844), (640, 360))


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return


@contextmanager
def serve(directory: Path) -> Iterator[str]:
    handler = partial(QuietHandler, directory=str(directory))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}/"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def assert_document_fits(page: Page, state: str) -> None:
    geometry = page.evaluate(
        """() => ({
            viewport: document.documentElement.clientWidth,
            document: document.documentElement.scrollWidth,
        })"""
    )
    assert geometry["document"] <= geometry["viewport"] + 1, f"{state} overflowed: {geometry}"


def assert_control_within_viewport(page: Page, locator: Locator, label: str) -> None:
    assert locator.is_visible(), f"{label} is not visible"
    box = locator.bounding_box()
    assert box is not None, f"{label} has no rendered bounds"
    viewport = page.viewport_size
    assert viewport is not None
    assert box["x"] >= -1 and box["x"] + box["width"] <= viewport["width"] + 1, (
        f"{label} is outside the horizontal viewport: {box}"
    )
    assert box["y"] >= -1 and box["y"] + box["height"] <= viewport["height"] + 1, (
        f"{label} is outside the vertical viewport: {box}"
    )


def exercise_viewport(browser: Browser, url: str, width: int, height: int) -> None:
    context = browser.new_context(viewport={"width": width, "height": height}, reduced_motion="reduce")
    page = context.new_page()
    label = f"{width}x{height}"
    try:
        page.goto(url, wait_until="networkidle")
        assert_document_fits(page, f"closed search at {label}")
        analytics_boundary = page.evaluate(
            """() => ({
                retiredUi: document.querySelectorAll(
                    '.dw-analytics-consent, .dw-analytics-preferences, '
                    + '#durable-workflow-analytics-consent, #durable-workflow-analytics-preferences'
                ).length,
                retiredStorage: localStorage.getItem('durable-workflow.analytics-consent.v1'),
                googleScripts: [...document.scripts].filter(
                    script => /googletagmanager|google-analytics/.test(script.src)
                ).length,
                runtimes: [...document.scripts].filter(
                    script => script.src.endsWith('/javascripts/analytics.js')
                ).length,
            })"""
        )
        assert analytics_boundary == {
            "retiredUi": 0,
            "retiredStorage": None,
            "googleScripts": 0,
            "runtimes": 1,
        }, f"retired analytics boundary rendered at {label}: {analytics_boundary}"

        if width < 960:
            page.locator(".md-header__button[for='__search']").click()
        else:
            page.locator(".md-search__input").click()
        page.wait_for_function("document.querySelector('#__search').checked")

        search_input = page.locator(".md-search__input")
        result_list = page.locator(".md-search-result__list")
        assert_control_within_viewport(page, search_input, f"search input at {label}")
        if width < 960:
            close_control = page.locator(".md-search__icon[for='__search']")
            assert_control_within_viewport(page, close_control, f"search back control at {label}")
        else:
            close_control = page.locator(".md-search__overlay")
            assert close_control.is_visible(), f"search close backdrop is not visible at {label}"

        search_input.press_sequentially("workflow", delay=25)
        result_list.locator("li").first.wait_for(state="visible")
        page.wait_for_timeout(250)
        assert result_list.is_visible(), f"search result list is not visible at {label}"
        assert_document_fits(page, f"open search with results at {label}")

        if width < 960:
            close_control.click()
        else:
            close_control.click(position={"x": 10, "y": 100})
        page.wait_for_function("!document.querySelector('#__search').checked")
        assert_document_fits(page, f"reclosed search at {label}")

        if width < 960:
            page.locator(".md-header__button[for='__drawer']").click()
            page.wait_for_function("document.querySelector('#__drawer').checked")
            assert page.locator(".md-nav--primary").is_visible(), f"navigation drawer is not visible at {label}"
            assert_document_fits(page, f"open navigation at {label}")
    finally:
        context.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("site", type=Path)
    parser.add_argument(
        "--chromium-executable",
        default=os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE"),
        help="Use a system Chromium instead of Playwright's managed browser.",
    )
    args = parser.parse_args()
    site = args.site.resolve()
    if not (site / "index.html").is_file():
        raise SystemExit(f"built documentation not found at {site}")

    with serve(site) as url, sync_playwright() as playwright:
        launch_options = {"headless": True}
        if args.chromium_executable:
            launch_options["executable_path"] = args.chromium_executable
        browser = playwright.chromium.launch(**launch_options)
        try:
            for width, height in VIEWPORTS:
                exercise_viewport(browser, url, width, height)
        finally:
            browser.close()

    print(
        "Validated analytics-free documentation controls at desktop, intermediate, "
        "mobile, and compact-height viewports."
    )


if __name__ == "__main__":
    main()
