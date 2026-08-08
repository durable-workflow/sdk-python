#!/usr/bin/env python3
"""Exercise responsive MkDocs search and navigation behavior in Chromium."""

from __future__ import annotations

import argparse
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import Browser, Locator, Page, Response, sync_playwright

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


def assert_rendered_health(
    page: Page,
    state: str,
    runtime_errors: list[str],
    *,
    check_reachability: bool = True,
) -> None:
    geometry = page.evaluate(
        """() => {
            const visibleFragments = (element) => {
                const style = getComputedStyle(element)
                if (style.display === 'none' || style.visibility === 'hidden') return []
                if (Number.parseFloat(style.opacity || '1') <= 0) return []
                return [...element.getClientRects()].flatMap((fragment) => {
                    if (fragment.width <= 0 || fragment.height <= 0) return []
                    const visible = {
                        left: Math.max(0, fragment.left),
                        right: Math.min(innerWidth, fragment.right),
                        top: Math.max(0, fragment.top),
                        bottom: Math.min(innerHeight, fragment.bottom),
                    }
                    for (let ancestor = element.parentElement; ancestor; ancestor = ancestor.parentElement) {
                        const ancestorStyle = getComputedStyle(ancestor)
                        const ancestorBox = ancestor.getBoundingClientRect()
                        if (ancestorStyle.overflowX !== 'visible') {
                            visible.left = Math.max(visible.left, ancestorBox.left)
                            visible.right = Math.min(visible.right, ancestorBox.right)
                        }
                        if (ancestorStyle.overflowY !== 'visible') {
                            visible.top = Math.max(visible.top, ancestorBox.top)
                            visible.bottom = Math.min(visible.bottom, ancestorBox.bottom)
                        }
                    }
                    return visible.left < visible.right && visible.top < visible.bottom ? [visible] : []
                })
            }
            const relatedHit = (hit, control) => {
                if (!hit) return false
                if (hit === control || control.contains(hit)) return true
                if ([...(control.labels || [])].some((label) => label === hit || label.contains(hit))) return true
                const hitLabel = hit.closest?.('label')
                return Boolean(hitLabel && hitLabel.control === control)
            }
            const interactive = [...document.querySelectorAll(
                'input, select, textarea, button, a[href], summary, [role="button"]'
            )].filter((element) => {
                if (!visibleFragments(element).length || element.matches(':disabled') || element.closest('[inert]')) {
                    return false
                }
                return !element.closest('details:not([open])') || Boolean(element.closest('summary'))
            })
            const fractions = [0.08, 0.29, 0.5, 0.71, 0.92]
            const unreachable = interactive.flatMap((control) => {
                const fragments = visibleFragments(control)
                if (!fragments.length) return []
                const box = fragments.reduce((bounds, fragment) => ({
                    left: Math.min(bounds.left, fragment.left),
                    right: Math.max(bounds.right, fragment.right),
                    top: Math.min(bounds.top, fragment.top),
                    bottom: Math.max(bounds.bottom, fragment.bottom),
                }))
                let reachable = 0
                let centersReachable = true
                let samples = 0
                for (const fragment of fragments) {
                    for (const yFraction of fractions) {
                        for (const xFraction of fractions) {
                            const x = fragment.left + (fragment.right - fragment.left) * xFraction
                            const y = fragment.top + (fragment.bottom - fragment.top) * yFraction
                            const hit = document.elementFromPoint(x, y)
                            const pointReachable = relatedHit(hit, control)
                            if (pointReachable) reachable += 1
                            if (xFraction === 0.5 && yFraction === 0.5) {
                                centersReachable = centersReachable && pointReachable
                            }
                            samples += 1
                        }
                    }
                }
                if (centersReachable && reachable / samples >= 0.5) return []
                return [{
                    tag: control.tagName.toLowerCase(),
                    role: control.getAttribute('role'),
                    name: control.getAttribute('name') || control.getAttribute('aria-label') || '',
                    className: control.className,
                    href: control.getAttribute('href'),
                    box,
                    fragments,
                    reachable,
                    samples,
                }]
            })
            const clippedControls = interactive.flatMap((control) => {
                const widthFits = control.scrollWidth <= control.clientWidth + 1
                const heightFits = control.scrollHeight <= control.clientHeight + 1
                if (widthFits && heightFits) {
                    return []
                }
                const text = (control.value || control.textContent || control.getAttribute('aria-label') || '').trim()
                return text ? [{tag: control.tagName.toLowerCase(), text: text.slice(0, 80)}] : []
            })
            return {
                viewport: document.documentElement.clientWidth,
                document: document.documentElement.scrollWidth,
                clippedControls,
                unreachable,
            }
        }"""
    )
    assert geometry["document"] <= geometry["viewport"] + 1, f"{state} overflowed: {geometry}"
    assert geometry["clippedControls"] == [], f"{state} has clipped control text: {geometry['clippedControls']}"
    if check_reachability:
        assert geometry["unreachable"] == [], f"{state} has unreachable visible controls: {geometry['unreachable']}"
    assert runtime_errors == [], f"{state} emitted browser or HTTP errors: {runtime_errors}"


def assert_background_is_inert(page: Page, label: str) -> None:
    background = page.evaluate(
        """() => [...document.querySelectorAll(
            '.md-header a, .md-header button, .md-header label, .md-sidebar a, .md-content a, .md-footer a'
        )]
            .filter((element) => !element.closest('.md-search'))
            .map((element) => ({
                tag: element.tagName.toLowerCase(),
                className: element.className,
                inert: Boolean(element.closest('[inert]')),
            }))"""
    )
    assert background, f"{label} did not find covered page controls"
    assert all(control["inert"] for control in background), f"{label} left covered controls active: {background}"


def assert_wrapped_inline_control_is_reachable(page: Page, runtime_errors: list[str], label: str) -> None:
    fragment_count = page.evaluate(
        """() => {
            const fixture = document.createElement('div')
            fixture.dataset.reachabilityFixture = 'wrapped-inline'
            fixture.style.cssText = [
                'position: fixed',
                'left: 16px',
                'bottom: 16px',
                'width: 180px',
                'z-index: 9999',
                'background: white',
                'font: 16px/20px sans-serif',
            ].join(';')
            fixture.innerHTML = 'prefix prefix prefix <a href="#fragment-test">reachable wrapped inline link</a>'
            document.body.append(fixture)
            return fixture.querySelector('a').getClientRects().length
        }"""
    )
    try:
        assert fragment_count > 1, f"{label} did not produce a fragmented inline control"
        assert_rendered_health(page, label, runtime_errors)
    finally:
        page.locator('[data-reachability-fixture="wrapped-inline"]').evaluate("element => element.remove()")


def assert_focus_wraps(page: Page, label: str) -> None:
    dialog = page.locator(".md-search[role='dialog']")
    page.locator(".md-search__input").focus()
    for _ in range(4):
        page.keyboard.press("Tab")
        assert dialog.evaluate("(element) => element.contains(document.activeElement)"), (
            f"{label} allowed forward focus to leave search"
        )

    page.locator(".md-search__input").focus()
    page.keyboard.press("Shift+Tab")
    assert dialog.evaluate("(element) => element.contains(document.activeElement)"), (
        f"{label} allowed reverse focus to leave search"
    )


def assert_results_focus_wraps(page: Page, label: str) -> None:
    stops = page.evaluate(
        """() => {
            const dialog = document.querySelector('.md-search[role="dialog"]')
            const controls = [...dialog.querySelectorAll(
                'a[href], button, input, select, textarea, summary, [tabindex]'
            )].filter((element) => {
                if (element.closest('[inert]')) return false
                const isResultControl = element.matches(
                    '.md-search-result a[href], .md-search-result summary'
                )
                if (element.tabIndex < 0 && !isResultControl) return false
                if (element.closest('details:not([open])') && !element.closest('summary')) return false
                const style = getComputedStyle(element)
                const bounds = element.getBoundingClientRect()
                return style.display !== 'none' && style.visibility !== 'hidden'
                    && bounds.width > 0 && bounds.height > 0
            })
            const closeControl = dialog.querySelector('.md-search__icon[for="__search"]')
            const scrollControl = dialog.querySelector('.md-search__scrollwrap')
            const orderedControls = controls.filter(
                (element) => element !== closeControl && element !== scrollControl
            )
            if (controls.includes(scrollControl)) orderedControls.push(scrollControl)
            if (controls.includes(closeControl)) orderedControls.push(closeControl)
            orderedControls.forEach((element, index) => {
                element.dataset.searchTabStop = String(index)
            })
            return orderedControls.map((element) => ({
                tag: element.tagName.toLowerCase(),
                text: (element.value || element.textContent || element.getAttribute('aria-label') || '')
                    .trim().slice(0, 80),
            }))
        }"""
    )
    assert stops, f"{label} has no focusable search controls"
    summary_indexes = [index for index, stop in enumerate(stops) if stop["tag"] == "summary"]
    assert summary_indexes, f"{label} did not render a focusable result expander: {stops}"

    for index, stop in enumerate(stops):
        page.locator(f'[data-search-tab-stop="{index}"]').focus()
        page.keyboard.press("Tab")
        focused = page.evaluate("document.activeElement?.dataset.searchTabStop")
        assert focused == str((index + 1) % len(stops)), (
            f"{label} skipped the search control after {stop}: focused marker {focused}; controls: {stops}"
        )

    for index in reversed(range(len(stops))):
        page.locator(f'[data-search-tab-stop="{index}"]').focus()
        page.keyboard.press("Shift+Tab")
        focused = page.evaluate("document.activeElement?.dataset.searchTabStop")
        assert focused == str((index - 1) % len(stops)), (
            f"{label} skipped a reverse search control before {stops[index]}: "
            f"focused marker {focused}; controls: {stops}"
        )


def exercise_viewport(browser: Browser, url: str, width: int, height: int) -> None:
    context = browser.new_context(viewport={"width": width, "height": height}, reduced_motion="reduce")
    page = context.new_page()
    label = f"{width}x{height}"
    runtime_errors: list[str] = []
    page.on(
        "console",
        lambda message: (
            runtime_errors.append(f"console {message.type}: {message.text}") if message.type == "error" else None
        ),
    )
    page.on("pageerror", lambda error: runtime_errors.append(f"page: {error}"))
    page.on("requestfailed", lambda request: runtime_errors.append(f"request: {request.url} {request.failure}"))

    def record_http_error(response: Response) -> None:
        if response.status >= 400:
            runtime_errors.append(f"http {response.status}: {response.url}")

    page.on("response", record_http_error)

    try:
        response = page.goto(url, wait_until="networkidle")
        assert response is not None and response.ok, f"documentation returned a non-success response at {label}"
        assert_rendered_health(page, f"closed search at {label}", runtime_errors)
        if (width, height) == (640, 360):
            assert_wrapped_inline_control_is_reachable(
                page,
                runtime_errors,
                f"wrapped inline control at {label}",
            )

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
                searchAccessibility: [...document.scripts].filter(
                    script => script.src.endsWith('/javascripts/search-accessibility.js')
                ).length,
            })"""
        )
        assert analytics_boundary == {
            "retiredUi": 0,
            "retiredStorage": None,
            "googleScripts": 0,
            "runtimes": 1,
            "searchAccessibility": 1,
        }, f"documentation runtime boundary is invalid at {label}: {analytics_boundary}"

        opener_selector = ".md-header__button[for='__search']" if width < 960 else ".md-search__input"
        opener = page.locator(opener_selector)
        opener.click()
        page.wait_for_function("document.querySelector('#__search').checked")
        page.wait_for_function("document.querySelector('.md-search').getAttribute('aria-modal') === 'true'")

        search_input = page.locator(".md-search__input")
        result_list = page.locator(".md-search-result__list")
        close_control = page.locator(".md-search__icon[for='__search']")
        assert_control_within_viewport(page, search_input, f"search input at {label}")
        assert_control_within_viewport(page, close_control, f"search close control at {label}")
        assert_background_is_inert(page, f"open search at {label}")
        assert_focus_wraps(page, f"open search at {label}")
        assert_rendered_health(page, f"open search at {label}", runtime_errors)

        search_input.fill("")
        search_input.press_sequentially("workflow", delay=25)
        result_list.locator("li").first.wait_for(state="visible")
        page.wait_for_timeout(1_000)
        assert result_list.is_visible(), f"search result list is not visible at {label}"
        assert_background_is_inert(page, f"populated search at {label}")
        assert_rendered_health(page, f"populated search at {label}", runtime_errors)
        assert_results_focus_wraps(page, f"populated search at {label}")

        close_control.focus()
        page.keyboard.press("Enter")
        page.wait_for_function("!document.querySelector('#__search').checked")
        assert opener.evaluate("(element) => element === document.activeElement"), (
            f"closing search did not restore opener focus at {label}"
        )
        assert page.locator("[inert]").count() == 0, f"closing search left the page inert at {label}"
        assert_rendered_health(page, f"keyboard-closed search at {label}", runtime_errors)

        opener.click()
        page.wait_for_function("document.querySelector('#__search').checked")
        close_control.click()
        page.wait_for_function("!document.querySelector('#__search').checked")
        assert opener.evaluate("(element) => element === document.activeElement"), (
            f"pointer-closing search did not restore opener focus at {label}"
        )

        if width < 960:
            page.locator(".md-header__button[for='__drawer']").click()
            page.wait_for_function("document.querySelector('#__drawer').checked")
            assert page.locator(".md-nav--primary").is_visible(), f"navigation drawer is not visible at {label}"
            assert_rendered_health(
                page,
                f"open navigation at {label}",
                runtime_errors,
                check_reachability=False,
            )
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
        "Validated contained documentation search focus, populated results, navigation, and rendered health "
        "at desktop, intermediate, mobile, and compact-height viewports."
    )


if __name__ == "__main__":
    main()
