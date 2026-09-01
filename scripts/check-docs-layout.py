#!/usr/bin/env python3
"""Check the rendered SDK portal at one desktop and one mobile viewport."""

from __future__ import annotations

import argparse
import contextlib
import functools
import http.server
import threading
from pathlib import Path
from typing import Any

from playwright.sync_api import Browser, Page, sync_playwright


class SiteHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        return

    def do_POST(self) -> None:  # noqa: N802
        if self.path.endswith("/promotion-events"):
            self.send_response(204)
            self.end_headers()
            return
        self.send_error(404)


@contextlib.contextmanager
def serve(directory: Path):
    handler = functools.partial(SiteHandler, directory=str(directory))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def runtime_errors(page: Page) -> list[str]:
    errors: list[str] = []
    page.on("console", lambda message: errors.append(f"console: {message.text}") if message.type == "error" else None)
    page.on("pageerror", lambda error: errors.append(f"page: {error}"))
    page.on(
        "requestfailed",
        lambda request: errors.append(f"request: {request.url} ({request.failure})"),
    )
    return errors


def assert_rendered_health(page: Page, label: str, errors: list[str]) -> None:
    page.wait_for_timeout(500)
    geometry: dict[str, Any] = page.evaluate(
        """
        () => {
          const visible = (element) => {
            const style = getComputedStyle(element);
            const box = element.getBoundingClientRect();
            const drawerClosed = !document.querySelector('#__drawer')?.checked;
            const searchClosed = !document.querySelector('#__search')?.checked;
            if (innerWidth < 960 && drawerClosed && element.closest('.md-sidebar--primary')) return false;
            if (searchClosed && element.closest('.md-search')) return false;
            return style.display !== 'none' && style.visibility !== 'hidden'
              && box.width > 0 && box.height > 0
              && box.right > 0 && box.left < innerWidth && box.bottom > 0 && box.top < innerHeight;
          };
          const controls = [...document.querySelectorAll('a, button, input, select, textarea, summary')]
            .filter(visible)
            .map((element) => {
              const box = element.getBoundingClientRect();
              const style = getComputedStyle(element);
              const scrollable = ['auto', 'scroll'].includes(style.overflowX);
              return {
                label: (element.getAttribute('aria-label') || element.textContent || element.tagName).trim().slice(0, 80),
                left: box.left,
                right: box.right,
                clippedText: ['A', 'BUTTON', 'SUMMARY'].includes(element.tagName)
                  && !scrollable && element.scrollWidth > element.clientWidth + 2,
              };
            })
            .filter((item) => item.left < -1 || item.right > innerWidth + 1 || item.clippedText);
          return {
            documentWidth: document.documentElement.scrollWidth,
            viewportWidth: innerWidth,
            badControls: controls,
          };
        }
        """
    )
    assert geometry["documentWidth"] <= geometry["viewportWidth"] + 1, f"{label} has horizontal overflow: {geometry}"
    assert geometry["badControls"] == [], f"{label} has clipped or off-screen controls: {geometry['badControls']}"
    assert errors == [], f"{label} emitted browser errors: {errors}"


def assert_landing_contract(page: Page) -> None:
    contract = page.evaluate(
        """
        () => {
          const text = (selector) => document.querySelector(selector)?.textContent?.trim() || '';
          const attr = (selector, name) => document.querySelector(selector)?.getAttribute(name) || '';
          return {
            surface: document.querySelectorAll('[data-docs-surface="python-sdk-landing"]').length,
            install: text('[data-docs-action="install"] code'),
            localJourney: text('[data-docs-journey="local-self-hosted"]'),
            primaryServer: document.querySelectorAll('[data-runtime="self-hosted"]').length,
            cloud: document.querySelectorAll('[data-runtime="cloud"]').length,
            roles: [...document.querySelectorAll('[data-sdk-role]')].map((item) => item.dataset.sdkRole).sort(),
            credentials: [...document.querySelectorAll('[data-credential-role]')]
              .map((item) => item.dataset.credentialRole).sort(),
            favicon: attr('link[rel~="icon"]', 'href'),
            ogTitle: attr('meta[property="og:title"]', 'content'),
            ogDescription: attr('meta[property="og:description"]', 'content'),
            twitterTitle: attr('meta[name="twitter:title"]', 'content'),
            twitterDescription: attr('meta[name="twitter:description"]', 'content'),
          };
        }
        """
    )
    assert contract["surface"] == 1
    assert "pip install durable-workflow" in contract["install"]
    assert "durableworkflow/server:2" in contract["localJourney"]
    assert contract["primaryServer"] == 1
    assert contract["cloud"] == 1
    assert contract["roles"] == ["activity", "client", "worker", "workflow"]
    assert contract["credentials"] == [
        "control-plane-api-key",
        "runtime-client-token",
        "runtime-worker-token",
    ]
    assert contract["favicon"].endswith("assets/favicon.svg")
    assert contract["ogTitle"] and contract["ogTitle"] == contract["twitterTitle"]
    assert contract["ogDescription"] and contract["ogDescription"] == contract["twitterDescription"]


def open_page(browser: Browser, base_url: str, path: str, viewport: dict[str, int]) -> tuple[Page, list[str]]:
    page = browser.new_page(viewport=viewport)
    errors = runtime_errors(page)
    response = page.goto(f"{base_url}{path}", wait_until="networkidle")
    assert response is not None and response.ok, f"{path} returned a non-success response"
    return page, errors


def exercise_mobile_controls(page: Page) -> None:
    navigation = page.locator(".md-header__button[for='__drawer']")
    navigation.click()
    page.wait_for_function("document.querySelector('#__drawer').checked")
    page.wait_for_timeout(350)
    drawer = page.locator(".md-sidebar--primary")
    assert drawer.is_visible(), "mobile navigation did not open"
    drawer_box = drawer.bounding_box()
    assert drawer_box is not None and drawer_box["x"] >= -1 and drawer_box["width"] <= 390
    close_navigation = drawer.locator(".dw-navigation__close")
    close_navigation.click()
    page.wait_for_function("!document.querySelector('#__drawer').checked")
    assert navigation.evaluate("element => element === document.activeElement"), "navigation focus was not restored"

    search = page.locator(".md-header__button[for='__search']")
    search.click()
    page.wait_for_function("document.querySelector('#__search').checked")
    page.wait_for_timeout(350)
    search_input = page.locator(".md-search__input")
    search_input.fill("workflow")
    page.wait_for_timeout(250)
    assert search_input.is_visible(), "mobile search did not open"
    search_box = page.locator(".md-search").bounding_box()
    assert search_box is not None and search_box["x"] >= -1 and search_box["width"] <= 391
    page.locator(".md-search__icon[for='__search']").click()
    page.wait_for_function("!document.querySelector('#__search').checked")
    assert search.evaluate("element => element === document.activeElement"), "search focus was not restored"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", nargs="?", type=Path, default=Path("site"))
    args = parser.parse_args()
    site = args.site.resolve()
    if not (site / "index.html").is_file():
        raise SystemExit(f"rendered site is missing {site / 'index.html'}")

    with serve(site) as base_url, sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            desktop, desktop_errors = open_page(browser, base_url, "/", {"width": 1440, "height": 920})
            assert_landing_contract(desktop)
            assert_rendered_health(desktop, "desktop landing", desktop_errors)
            desktop.close()

            for path in ("/sdk-reference/", "/reference/client/"):
                page, errors = open_page(browser, base_url, path, {"width": 1440, "height": 920})
                assert_rendered_health(page, f"desktop {path}", errors)
                page.close()

            mobile, mobile_errors = open_page(browser, base_url, "/", {"width": 390, "height": 844})
            assert_landing_contract(mobile)
            assert_rendered_health(mobile, "mobile landing", mobile_errors)
            exercise_mobile_controls(mobile)
            assert_rendered_health(mobile, "mobile controls", mobile_errors)
            mobile.close()
        finally:
            browser.close()

    print("Validated the Python SDK portal at desktop and mobile viewports.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
