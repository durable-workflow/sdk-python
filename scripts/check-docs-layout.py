#!/usr/bin/env python3
"""Check the rendered SDK portal at one desktop and one mobile viewport."""

from __future__ import annotations

import argparse
import contextlib
import functools
import http.server
import importlib.metadata
import json
import platform
import threading
from pathlib import Path
from typing import Any

import playwright as playwright_package
from playwright.sync_api import Browser, Page, sync_playwright

FOCUSED = "element => element === document.activeElement"


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
                label: (element.getAttribute('aria-label') || element.textContent || element.tagName)
                  .trim().slice(0, 80),
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
    page = browser.new_page(
        viewport=viewport, color_scheme="dark", permissions=["clipboard-read", "clipboard-write"],
    )
    page.route(
        "https://status.durable-workflow.com/api/current.json",
        lambda route: route.fulfill(status=200, content_type="application/json", body="{}"),
    )
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
    width = page.viewport_size["width"] if page.viewport_size else 390
    assert drawer_box is not None and drawer_box["x"] >= -1 and drawer_box["width"] <= width
    close_navigation = drawer.locator(".dw-navigation__close")
    close_navigation.click()
    page.wait_for_function("!document.querySelector('#__drawer').checked")
    assert navigation.evaluate(FOCUSED), "navigation focus was not restored"

    search = page.locator(".md-header__button[for='__search']")
    search.click()
    page.wait_for_function("document.querySelector('#__search').checked")
    page.wait_for_timeout(350)
    search_input = page.locator(".md-search__input")
    search_input.fill("workflow")
    page.wait_for_timeout(250)
    assert search_input.is_visible(), "mobile search did not open"
    search_box = page.locator(".md-search").bounding_box()
    assert search_box is not None and search_box["x"] >= -1 and search_box["width"] <= width + 1
    page.locator(".md-search__icon[for='__search']").click()
    page.wait_for_function("!document.querySelector('#__search').checked")
    assert search.evaluate(FOCUSED), "search focus was not restored"


def exercise_identity(page: Page, theme: str) -> None:
    page.wait_for_function("document.body.dataset.mdColorScheme === 'default'")
    if theme == "dark":
        toggle = page.get_by_role("button", name="Switch to dark mode", exact=True)
        toggle.focus()
        toggle.press("Enter")
        page.wait_for_function("document.body.dataset.mdColorScheme === 'slate'")
        page.wait_for_function(
            "document.querySelector('.md-header__option input:checked')?.nextElementSibling === document.activeElement",
        )
        page.reload(wait_until="networkidle")
        page.wait_for_function("document.body.dataset.mdColorScheme === 'slate'")
    page.evaluate("document.fonts.ready")
    fonts = page.evaluate("[...document.fonts].filter(font => font.status === 'loaded').map(font => font.family)")
    assert {"Inter", "Space Grotesk", "JetBrains Mono"} <= set(fonts), fonts
    assert page.locator(".highlight pre").first.evaluate(
        "element => getComputedStyle(element).backgroundColor",
    ) == "rgb(15, 21, 19)"
    switcher = page.locator("[data-sdk-switcher]")
    trigger = switcher.locator("summary")
    trigger.focus()
    trigger.press("Enter")
    page.wait_for_function("document.querySelector('[data-sdk-switcher]').open")
    assert switcher.get_by_role("link", name="Rust", exact=True).get_attribute("href") == "https://rust.durable-workflow.com/"
    trigger.press("Escape")
    assert not switcher.evaluate("element => element.open")
    assert trigger.evaluate(FOCUSED)
    assert page.locator("[data-status-label]").text_content() == "Status unknown"
    assert page.locator(".dw-global-actions .dw-button").is_visible()


def exercise_copy_and_steps(page: Page) -> None:
    spacing = page.locator(".dw-cloud-promotion__eyebrow").evaluate(
        """element => {
          const style = getComputedStyle(element);
          return parseFloat(style.letterSpacing) / parseFloat(style.fontSize);
        }""",
    )
    assert abs(spacing - 0.08) < 0.001, "Eyebrow tracking differs from the canonical design"
    targets = page.locator("button[data-clipboard-target]").evaluate_all(
        "buttons => buttons.map(button => button.dataset.clipboardTarget)",
    )
    assert len(set(targets)) == len(targets), "Code blocks share a copy target"
    for selector in (".dw-install-card", ".dw-quickstart-code"):
        block = page.locator(selector)
        expected = block.locator("pre code").text_content()
        # Never inspect a shared host's previous clipboard contents.
        page.evaluate("navigator.clipboard.writeText('sdk-python-public-copy-control')")
        block.locator("button[data-clipboard-target]").click()
        page.wait_for_function(
            "async () => (await navigator.clipboard.readText()) !== 'sdk-python-public-copy-control'",
        )
        copied = page.evaluate("navigator.clipboard.readText()")
        assert copied.rstrip("\n") == expected.rstrip("\n"), "Copy changed or truncated the code"
    assert page.locator("#start-server").count() == 1
    assert page.locator("#save-greeter").count() == 1
    assert page.locator("#run-greeter").count() == 1
    assert page.locator(".dw-quickstart-copy a[href='#start-server']").count() == 1
    assert page.locator(".dw-quickstart-copy a[href='#run-greeter']").count() == 1
    code = page.locator(".dw-quickstart-code pre code").text_content() or ""
    assert "asyncio.run(main())" in code
    assert "workflows=[GreeterWorkflow]" in code
    assert "activities=[greet]" in code
    assert "task_queue=TASK_QUEUE" in code


def check_landing_views(browser: Browser, base_url: str, screenshots: Path | None) -> None:
    for width in (1440, 1024, 390, 320):
        for theme in ("light", "dark"):
            page, errors = open_page(browser, base_url, "/", {"width": width, "height": 920})
            exercise_identity(page, theme)
            assert_landing_contract(page)
            assert_rendered_health(page, f"{width} {theme} landing", errors)
            exercise_copy_and_steps(page)
            if width < 960:
                exercise_mobile_controls(page)
            assert_rendered_health(page, f"{width} {theme} controls", errors)
            if screenshots:
                page.screenshot(path=str(screenshots / f"python-{width}-{theme}.png"), full_page=True)
            page.close()


def check_api_views(browser: Browser, base_url: str) -> None:
    for path in ("/sdk-reference/", "/reference/client/"):
        for width in (1440, 390):
            for theme in ("light", "dark"):
                page, errors = open_page(browser, base_url, path, {"width": width, "height": 920})
                exercise_identity(page, theme)
                assert_rendered_health(page, f"{width} {theme} {path}", errors)
                if width < 960:
                    exercise_mobile_controls(page)
                page.close()


def browser_evidence(browser: Browser) -> dict[str, str]:
    manifest_path = Path(playwright_package.__file__).parent / "driver" / "package" / "browsers.json"
    manifest = json.loads(manifest_path.read_text())
    selected = next(item for item in manifest["browsers"] if item["name"] == "chromium-headless-shell")
    proof = {
        "playwright_version": importlib.metadata.version("playwright"),
        "selection": "Playwright default bundled headless Chromium",
        "bundled_revision": selected["revision"],
        "bundled_version": selected["browserVersion"],
        "launched_version": browser.version,
        "os": platform.system(),
        "architecture": platform.machine(),
    }
    print(json.dumps(proof, sort_keys=True))
    assert proof["launched_version"] == proof["bundled_version"], "Launched browser differs from the locked bundle"
    return proof


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", nargs="?", type=Path, default=Path("site"))
    parser.add_argument("--screenshots", type=Path)
    args = parser.parse_args()
    site = args.site.resolve()
    if not (site / "index.html").is_file():
        raise SystemExit(f"rendered site is missing {site / 'index.html'}")

    with serve(site) as base_url, sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            proof = browser_evidence(browser)
            if args.screenshots:
                args.screenshots.mkdir(parents=True, exist_ok=True)
                (args.screenshots / "browser-evidence.json").write_text(json.dumps(proof, indent=2) + "\n")
            check_landing_views(browser, base_url, args.screenshots)
            check_api_views(browser, base_url)

        finally:
            browser.close()

    print("Validated the Python SDK portal at desktop and mobile viewports.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
