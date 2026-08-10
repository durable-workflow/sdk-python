#!/usr/bin/env python3
"""Exercise responsive MkDocs search and navigation behavior in Chromium."""

from __future__ import annotations

import argparse
import json
import os
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from playwright.sync_api import Browser, Locator, Page, Response, Route, sync_playwright

VIEWPORTS = ((1440, 920), (768, 1024), (390, 844), (640, 360))
NESTED_REFERENCE_ROUTES = (
    ("client", "first"),
    ("serializer", "middle"),
    ("testing", "final"),
)
GITHUB_API_ENDPOINTS = (
    "https://api.github.com/repos/durable-workflow/sdk-python",
    "https://api.github.com/repos/durable-workflow/sdk-python/releases/latest",
)
GITHUB_API_ENDPOINT = re.compile(
    r"^https://api\.github\.com/repos/durable-workflow/sdk-python(?:/releases/latest)?(?:\?.*)?$"
)


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


def rendered_geometry(page: Page) -> dict[str, Any]:
    return page.evaluate(
        """() => {
            const renderedFragments = (element) => {
                const style = getComputedStyle(element)
                if (style.display === 'none' || style.visibility === 'hidden') return []
                if (Number.parseFloat(style.opacity || '1') <= 0) return []
                return [...element.getClientRects()]
                    .filter((fragment) => fragment.width > 0 && fragment.height > 0)
                    .map((fragment) => ({
                        left: fragment.left,
                        right: fragment.right,
                        top: fragment.top,
                        bottom: fragment.bottom,
                    }))
            }
            const clipToAncestors = (element) => {
                let fragments = renderedFragments(element)
                for (
                    let ancestor = element.parentElement;
                    ancestor && fragments.length;
                    ancestor = ancestor.parentElement
                ) {
                    const style = getComputedStyle(ancestor)
                    const clipsX = style.overflowX !== 'visible'
                    const clipsY = style.overflowY !== 'visible'
                    if (!clipsX && !clipsY) continue
                    const ancestorBox = ancestor.getBoundingClientRect()
                    const clipped = fragments.map((fragment) => ({
                        left: clipsX ? Math.max(fragment.left, ancestorBox.left) : fragment.left,
                        right: clipsX ? Math.min(fragment.right, ancestorBox.right) : fragment.right,
                        top: clipsY ? Math.max(fragment.top, ancestorBox.top) : fragment.top,
                        bottom: clipsY ? Math.min(fragment.bottom, ancestorBox.bottom) : fragment.bottom,
                    })).filter((fragment) => fragment.right > fragment.left && fragment.bottom > fragment.top)
                    if (!clipped.length) {
                        const scrollsX = clipsX && ['auto', 'scroll'].includes(style.overflowX)
                            && ancestor.scrollWidth > ancestor.clientWidth + 1
                        const scrollsY = clipsY && ['auto', 'scroll'].includes(style.overflowY)
                            && ancestor.scrollHeight > ancestor.clientHeight + 1
                        return {fragments: [], fullyClipped: true, usableScroll: scrollsX || scrollsY}
                    }
                    fragments = clipped
                }
                return {fragments, fullyClipped: false, usableScroll: false}
            }
            const visibleFragments = (element) => {
                const clipped = clipToAncestors(element)
                return clipped.fragments.flatMap((fragment) => {
                    const visible = {
                        left: Math.max(0, fragment.left),
                        right: Math.min(innerWidth, fragment.right),
                        top: Math.max(0, fragment.top),
                        bottom: Math.min(innerHeight, fragment.bottom),
                    }
                    return visible.right - visible.left >= 1 && visible.bottom - visible.top >= 1
                        ? [visible]
                        : []
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
                if (!renderedFragments(element).length || element.matches(':disabled') || element.closest('[inert]')) {
                    return false
                }
                return !element.closest('details:not([open])') || Boolean(element.closest('summary'))
            })
            const fractions = [0.08, 0.29, 0.5, 0.71, 0.92]
            const unreachable = interactive.flatMap((control) => {
                const fragments = visibleFragments(control)
                if (!fragments.length) {
                    const clipping = clipToAncestors(control)
                    const inactiveResponsiveSurface = control.closest('.md-sidebar, .md-search')
                        && !control.closest('[aria-modal="true"]')
                    const bounds = control.getBoundingClientRect()
                    const intersectsViewport = bounds.right > 0 && bounds.left < innerWidth
                        && bounds.bottom > 0 && bounds.top < innerHeight
                    if (
                        !clipping.fullyClipped
                        || clipping.usableScroll
                        || inactiveResponsiveSurface
                        || !intersectsViewport
                    ) return []
                    return [{
                        tag: control.tagName.toLowerCase(),
                        role: control.getAttribute('role'),
                        name: control.getAttribute('name') || control.getAttribute('aria-label') || '',
                        className: control.className,
                        href: control.getAttribute('href'),
                        box: {
                            left: bounds.left,
                            right: bounds.right,
                            top: bounds.top,
                            bottom: bounds.bottom,
                        },
                        fragments: [],
                        reachable: 0,
                        samples: 0,
                        fullyClipped: true,
                    }]
                }
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
            const clippedControls = interactive
                .filter((control) => visibleFragments(control).length)
                .flatMap((control) => {
                    const widthFits = control.scrollWidth <= control.clientWidth + 1
                    const heightFits = control.scrollHeight <= control.clientHeight + 1
                    if (widthFits && heightFits) {
                        return []
                    }
                    const text = (
                        control.value || control.textContent || control.getAttribute('aria-label') || ''
                    ).trim()
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


def assert_rendered_health(
    page: Page,
    state: str,
    runtime_errors: list[str],
    *,
    check_reachability: bool = True,
) -> None:
    geometry = rendered_geometry(page)
    assert geometry["document"] <= geometry["viewport"] + 1, f"{state} overflowed: {geometry}"
    assert geometry["clippedControls"] == [], f"{state} has clipped control text: {geometry['clippedControls']}"
    if check_reachability:
        assert geometry["unreachable"] == [], f"{state} has unreachable controls: {geometry['unreachable']}"
    assert runtime_errors == [], f"{state} emitted browser or HTTP errors: {runtime_errors}"


def assert_reference_panel_uses_drawer(
    page: Page,
    label: str,
    expected_active: str,
) -> dict[str, Any]:
    geometry = reference_panel_geometry(page)
    assert_reference_panel_geometry(geometry, label, expected_active)
    return geometry


def reference_panel_geometry(page: Page) -> dict[str, Any] | None:
    return page.evaluate(
        """() => {
            const drawer = document.querySelector('.md-sidebar--primary')
            let activePanel = drawer.querySelector('.md-nav--primary')
            for (const panelToggle of drawer.querySelectorAll('input.md-nav__toggle:checked')) {
                const panel = panelToggle.parentElement?.querySelector(':scope > nav.md-nav')
                if (panel instanceof HTMLElement && activePanel.contains(panel)) activePanel = panel
            }
            const active = activePanel.querySelector('.md-nav__link--active')
            const list = active?.closest('.md-nav__list')
            const backControl = activePanel.querySelector(':scope > .md-nav__title')
            const backIcon = backControl?.querySelector('.md-nav__icon')
            const closeControl = drawer.querySelector('.dw-navigation__close')
            if (
                !(active instanceof HTMLElement)
                || !(list instanceof HTMLElement)
                || !(backControl instanceof HTMLElement)
                || !(backIcon instanceof HTMLElement)
                || !(closeControl instanceof HTMLElement)
            ) return null

            const drawerBounds = drawer.getBoundingClientRect()
            const listBounds = list.getBoundingClientRect()
            const activeBounds = active.getBoundingClientRect()
            const backIconBounds = backIcon.getBoundingClientRect()
            const closeBounds = closeControl.getBoundingClientRect()
            return {
                actions: {
                    back: {
                        controls: backControl.getAttribute('for'),
                        right: backIconBounds.right,
                    },
                    close: {
                        action: closeControl.dataset.navigationAction,
                        backgroundColor: getComputedStyle(closeControl).backgroundColor,
                        left: closeBounds.left,
                    },
                },
                active: {
                    bottom: activeBounds.bottom,
                    text: active.textContent.trim().replace(/\s+/g, ' '),
                    top: activeBounds.top,
                },
                drawer: {
                    bottom: drawerBounds.bottom,
                    height: drawerBounds.height,
                    left: drawerBounds.left,
                    top: drawerBounds.top,
                    width: drawerBounds.width,
                },
                list: {
                    bottom: listBounds.bottom,
                    clientHeight: list.clientHeight,
                    overflowY: getComputedStyle(list).overflowY,
                    scrollHeight: list.scrollHeight,
                    scrollTop: list.scrollTop,
                    top: listBounds.top,
                },
            }
        }"""
    )


def assert_reference_panel_geometry(
    geometry: dict[str, Any] | None,
    label: str,
    expected_active: str,
) -> None:
    assert geometry is not None, f"{label} did not render an active reference list"
    active = geometry["active"]
    actions = geometry["actions"]
    drawer = geometry["drawer"]
    reference_list = geometry["list"]
    assert active["text"] == expected_active, f"{label} selected the wrong destination: {active}"
    drawer_midpoint = drawer["left"] + drawer["width"] / 2
    assert actions["back"]["controls"].startswith("__nav_"), f"{label} lost nested back navigation: {actions}"
    assert actions["back"]["right"] < drawer_midpoint < actions["close"]["left"], (
        f"{label} did not separate nested back and drawer close actions: {actions}"
    )
    assert actions["close"]["action"] == "close", f"{label} did not identify its close action: {actions}"
    assert actions["close"]["backgroundColor"] != "rgba(0, 0, 0, 0)", (
        f"{label} did not visually frame its close action: {actions}"
    )
    assert active["top"] >= reference_list["top"] - 1, f"{label} hid the active destination above the list"
    assert active["bottom"] <= reference_list["bottom"] + 1, f"{label} hid the active destination below the list"
    assert abs(reference_list["bottom"] - drawer["bottom"]) <= 1, (
        f"{label} did not use the available drawer height: {geometry}"
    )
    assert reference_list["clientHeight"] >= 200, f"{label} retained a constrained list: {geometry}"
    assert reference_list["overflowY"] == "auto", f"{label} lost its bounded overflow region: {geometry}"


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


def assert_navigation_background_is_inert(page: Page, label: str) -> None:
    background = page.evaluate(
        """() => {
            const drawer = document.querySelector('.md-sidebar--primary')
            const backdrop = document.querySelector('.md-overlay[for="__drawer"]')
            const toggle = document.querySelector('#__drawer')
            return [...document.querySelectorAll(
                'input, select, textarea, button, a[href], summary, [role="button"]'
            )]
                .filter((element) => !drawer.contains(element) && element !== backdrop && element !== toggle)
                .map((element) => ({
                    tag: element.tagName.toLowerCase(),
                    className: element.className,
                    inert: Boolean(element.closest('[inert]')),
                }))
        }"""
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


def assert_viewport_edge_fragment_threshold(page: Page, runtime_errors: list[str], label: str) -> None:
    visible_heights = page.evaluate(
        """() => {
            const fixture = document.createElement('div')
            fixture.dataset.reachabilityFixture = 'viewport-edge'

            const addControl = (href, left, visibleHeight) => {
                const control = document.createElement('a')
                control.href = href
                control.textContent = href.slice(1)
                control.style.cssText = [
                    'position: fixed',
                    `left: ${left}px`,
                    `top: calc(100vh - ${visibleHeight}px)`,
                    'width: 160px',
                    'height: 20px',
                    'z-index: 9998',
                    'background: white',
                ].join(';')
                fixture.append(control)
                return control
            }

            const subpixel = addControl('#subpixel-viewport-edge', 16, 0.03125)
            const meaningful = addControl('#meaningful-viewport-edge', 192, 2)
            const blocker = document.createElement('div')
            blocker.style.cssText = [
                'position: fixed',
                'inset: auto 0 0',
                'height: 2px',
                'z-index: 9999',
                'pointer-events: auto',
            ].join(';')
            fixture.append(blocker)
            document.body.append(fixture)

            const clippedHeight = (control) => {
                const bounds = control.getBoundingClientRect()
                return Math.min(innerHeight, bounds.bottom) - Math.max(0, bounds.top)
            }
            return {
                subpixel: clippedHeight(subpixel),
                meaningful: clippedHeight(meaningful),
            }
        }"""
    )
    try:
        assert 0 < visible_heights["subpixel"] < 1, (
            f"{label} did not produce a sub-pixel viewport-edge fragment: {visible_heights}"
        )
        assert visible_heights["meaningful"] >= 1, (
            f"{label} did not produce a meaningfully visible partial control: {visible_heights}"
        )

        geometry = rendered_geometry(page)
        findings = {
            finding["href"]: finding
            for finding in geometry["unreachable"]
            if finding["href"] in {"#subpixel-viewport-edge", "#meaningful-viewport-edge"}
        }
        assert "#subpixel-viewport-edge" not in findings, (
            f"{label} sampled a sub-pixel viewport-edge fragment: {findings}"
        )
        assert "#meaningful-viewport-edge" in findings, (
            f"{label} stopped sampling meaningfully visible partial controls: {findings}"
        )
    finally:
        page.locator('[data-reachability-fixture="viewport-edge"]').evaluate("element => element.remove()")

    assert_rendered_health(page, label, runtime_errors)


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


def assert_navigation_focus_wraps(page: Page, label: str) -> list[dict[str, str]]:
    drawer = page.locator(".md-sidebar--primary")
    close_control = drawer.locator(".dw-navigation__close")
    assert close_control.get_attribute("data-navigation-action") == "close", (
        f"{label} did not expose an explicit drawer close action"
    )
    assert close_control.get_attribute("aria-label") == "Close navigation", (
        f"{label} did not preserve the drawer close control's accessible name"
    )
    assert close_control.evaluate("(element) => element === document.activeElement"), (
        f"{label} did not move focus to the drawer close control"
    )

    traversal = page.evaluate(
        r"""() => {
            const drawer = document.querySelector('.md-sidebar--primary')
            const closeControl = drawer.querySelector('.dw-navigation__close')
            let activePanel = drawer.querySelector('.md-nav--primary')
            for (const panelToggle of drawer.querySelectorAll('input.md-nav__toggle:checked')) {
                const panel = panelToggle.parentElement?.querySelector(':scope > nav.md-nav')
                if (panel instanceof HTMLElement && activePanel.contains(panel)) activePanel = panel
            }
            const inactivePanels = [...drawer.querySelectorAll('input.md-nav__toggle:not(:checked)')]
                .map((panelToggle) => panelToggle.parentElement?.querySelector(':scope > nav.md-nav'))
                .filter((panel) => panel instanceof HTMLElement)
            const controls = [...drawer.querySelectorAll(
                'a[href], button, input, select, textarea, summary, [tabindex]'
            )].filter((element) => {
                if (element.closest('[inert]') || element.tabIndex < 0) return false
                if (element.closest('details:not([open])') && !element.closest('summary')) return false
                const style = getComputedStyle(element)
                const bounds = element.getBoundingClientRect()
                return style.display !== 'none' && style.visibility !== 'hidden'
                    && bounds.width > 0 && bounds.height > 0
            })
            const outsideActivePanel = controls.filter(
                (element) => element !== closeControl && (
                    !activePanel.contains(element)
                    || inactivePanels.some((panel) => panel.contains(element))
                )
            ).map((element) => ({
                tag: element.tagName.toLowerCase(),
                text: (element.value || element.textContent || element.getAttribute('aria-label') || '')
                    .trim().replace(/\s+/g, ' ').slice(0, 80),
            }))
            controls.forEach((element, index) => {
                element.dataset.navigationTabStop = String(index)
            })
            return {
                outsideActivePanel,
                stops: controls.map((element) => ({
                    tag: element.tagName.toLowerCase(),
                    text: (element.value || element.textContent || element.getAttribute('aria-label') || '')
                        .trim().replace(/\s+/g, ' ').slice(0, 80),
                })),
            }
        }"""
    )
    stops = traversal["stops"]
    assert stops, f"{label} has no active drawer controls"
    assert traversal["outsideActivePanel"] == [], (
        f"{label} left controls from an occluded or translated panel in the tab order: "
        f"{traversal['outsideActivePanel']}"
    )

    for index, stop in enumerate(stops):
        page.locator(f'[data-navigation-tab-stop="{index}"]').focus()
        page.keyboard.press("Tab")
        focused = page.evaluate("document.activeElement?.dataset.navigationTabStop")
        assert focused == str((index + 1) % len(stops)), (
            f"{label} skipped the drawer control after {stop}: focused marker {focused}; controls: {stops}"
        )

    for index in reversed(range(len(stops))):
        page.locator(f'[data-navigation-tab-stop="{index}"]').focus()
        page.keyboard.press("Shift+Tab")
        focused = page.evaluate("document.activeElement?.dataset.navigationTabStop")
        assert focused == str((index - 1) % len(stops)), (
            f"{label} skipped a reverse drawer control before {stops[index]}: "
            f"focused marker {focused}; controls: {stops}"
        )

    close_control.focus()
    page.locator(".md-header .md-logo").focus()
    assert close_control.evaluate("(element) => element === document.activeElement"), (
        f"{label} allowed programmatic focus to leave navigation"
    )
    return stops


def assert_control_is_keyboard_and_pointer_reachable(page: Page, selector: str, label: str) -> None:
    control = page.locator(selector).first
    assert control.count() == 1, f"{label} is missing"
    control.scroll_into_view_if_needed()
    assert control.is_visible(), f"{label} is not visible"
    control.focus()
    assert control.evaluate("(element) => element === document.activeElement"), f"{label} cannot receive focus"
    assert control.evaluate(
        """(element) => {
            const bounds = element.getBoundingClientRect()
            const hit = document.elementFromPoint(
                bounds.left + bounds.width / 2,
                bounds.top + bounds.height / 2,
            )
            return hit === element || element.contains(hit)
                || Boolean(hit?.closest('label')?.control === element)
        }"""
    ), f"{label} cannot be reached by pointer"


def exercise_navigation_breakpoint_transition(browser: Browser, url: str) -> None:
    context = browser.new_context(viewport={"width": 390, "height": 844}, reduced_motion="reduce")
    page = context.new_page()
    runtime_errors: list[str] = []
    page.on(
        "console",
        lambda message: (
            runtime_errors.append(f"console {message.type}: {message.text}") if message.type == "error" else None
        ),
    )
    page.on("pageerror", lambda error: runtime_errors.append(f"page: {error}"))
    page.on("requestfailed", lambda request: runtime_errors.append(f"request: {request.url} {request.failure}"))
    page.on(
        "request",
        lambda request: (
            runtime_errors.append(f"external request: {request.url}")
            if request.url.startswith(("http://", "https://")) and not request.url.startswith(url)
            else None
        ),
    )
    page.on(
        "response",
        lambda response: (
            runtime_errors.append(f"http {response.status}: {response.url}") if response.status >= 400 else None
        ),
    )

    try:
        response = page.goto(f"{url}reference/client/", wait_until="networkidle")
        assert response is not None and response.ok, "documentation transition fixture returned a non-success response"

        navigation_opener = page.locator(".md-header__button[for='__drawer']")
        navigation_drawer = page.locator(".md-sidebar--primary")
        navigation_close = navigation_drawer.locator(".dw-navigation__close")
        toggle = page.locator("#__drawer")

        navigation_opener.click()
        page.wait_for_function("document.querySelector('#__drawer').checked")
        page.wait_for_function("document.querySelector('.md-sidebar--primary').getAttribute('aria-modal') === 'true'")
        assert_navigation_background_is_inert(page, "navigation before desktop transition")
        assert_navigation_focus_wraps(page, "navigation before desktop transition")

        page.set_viewport_size({"width": 1440, "height": 900})
        page.wait_for_function("!document.querySelector('#__drawer').checked", timeout=2_000)
        assert not toggle.is_checked(), "desktop transition left the navigation toggle checked"
        assert navigation_drawer.get_attribute("role") is None, "desktop transition left a hidden dialog"
        assert navigation_drawer.get_attribute("aria-modal") is None, "desktop transition left modal semantics"
        assert page.locator("[inert]").count() == 0, "desktop transition left unrelated regions inert"
        assert navigation_close.evaluate("(element) => element !== document.activeElement"), (
            "desktop transition left focus on the hidden drawer close control"
        )

        for selector, label in (
            (".md-header .md-logo", "desktop header link"),
            (".md-sidebar--primary a[href]:visible", "desktop primary navigation link"),
            (".md-content a[href]:visible", "desktop documentation link"),
            (".md-sidebar--secondary a[href]:visible", "desktop table-of-contents link"),
            (".md-footer a[href]:visible", "desktop footer link"),
        ):
            assert_control_is_keyboard_and_pointer_reachable(page, selector, label)
        assert_rendered_health(page, "navigation after desktop transition", runtime_errors)

        page.set_viewport_size({"width": 390, "height": 844})
        page.wait_for_timeout(100)
        assert not toggle.is_checked(), "responsive re-entry restored a stale checked toggle"
        assert navigation_drawer.get_attribute("role") is None, "responsive re-entry restored a stale dialog"
        assert navigation_drawer.get_attribute("aria-modal") is None, "responsive re-entry restored modal semantics"
        assert page.locator("[inert]").count() == 0, "responsive re-entry restored inert state"
        assert navigation_opener.evaluate("(element) => element === document.activeElement"), (
            "responsive re-entry left focus in the hidden navigation drawer"
        )

        navigation_opener.click()
        page.wait_for_function("document.querySelector('#__drawer').checked")
        assert_navigation_focus_wraps(page, "navigation after responsive re-entry")
        page.set_viewport_size({"width": 1440, "height": 900})
        page.wait_for_function("!document.querySelector('#__drawer').checked", timeout=2_000)
        assert page.locator("[inert]").count() == 0, "repeated desktop transition left inert state"
        assert_rendered_health(page, "repeated navigation desktop transition", runtime_errors)
    finally:
        context.close()


def exercise_nested_navigation_viewport(
    browser: Browser,
    url: str,
    route: str,
    position: str,
    width: int,
    height: int,
) -> dict[str, Any]:
    context = browser.new_context(viewport={"width": width, "height": height}, reduced_motion="reduce")
    page = context.new_page()
    runtime_errors: list[str] = []
    page.on(
        "console",
        lambda message: (
            runtime_errors.append(f"console {message.type}: {message.text}") if message.type == "error" else None
        ),
    )
    page.on("pageerror", lambda error: runtime_errors.append(f"page: {error}"))
    page.on("requestfailed", lambda request: runtime_errors.append(f"request: {request.url} {request.failure}"))
    page.on(
        "request",
        lambda request: (
            runtime_errors.append(f"external request: {request.url}")
            if request.url.startswith(("http://", "https://")) and not request.url.startswith(url)
            else None
        ),
    )
    page.on(
        "response",
        lambda response: (
            runtime_errors.append(f"http {response.status}: {response.url}") if response.status >= 400 else None
        ),
    )

    label = f"nested {route} ({position}) {width}x{height}"
    result: dict[str, Any] = {
        "position": position,
        "route": f"/reference/{route}/",
        "viewport": {"width": width, "height": height},
        "states": {"default": {"pointer_unreachable": 0}},
    }
    try:
        response = page.goto(f"{url}reference/{route}/", wait_until="networkidle")
        assert response is not None and response.ok, f"{label} fixture returned a non-success response"
        assert page.locator(f"h1#{route}").count() == 1, f"{label} did not load the requested reference"
        assert_rendered_health(page, f"closed navigation at {label}", runtime_errors)

        if width < 960:
            navigation_opener = page.locator(".md-header__button[for='__drawer']")
            navigation_drawer = page.locator(".md-sidebar--primary")
            navigation_opener.click()
            page.wait_for_function("document.querySelector('#__drawer').checked")
            page.wait_for_function(
                "document.querySelector('.md-sidebar--primary').getAttribute('aria-modal') === 'true'"
            )
            assert_navigation_background_is_inert(page, f"open navigation at {label}")
            reference_panel = assert_reference_panel_uses_drawer(page, label, route.replace("_", " ").title())
            assert_rendered_health(page, f"open navigation at {label}", runtime_errors)
            stops = assert_navigation_focus_wraps(page, f"open navigation at {label}")
            assert navigation_drawer.locator("[inert]").count() >= 3, (
                f"open navigation at {label} did not isolate translated parent-panel controls"
            )
            result["states"]["navigation-open"] = {
                "active_keyboard_controls": stops,
                "pointer_unreachable": 0,
                "reference_panel": reference_panel,
            }
        return result
    finally:
        context.close()


def exercise_nested_navigation_regression(browser: Browser, url: str) -> dict[str, Any]:
    context = browser.new_context(viewport={"width": 390, "height": 844}, reduced_motion="reduce")
    page = context.new_page()
    runtime_errors: list[str] = []
    page.on(
        "console",
        lambda message: (
            runtime_errors.append(f"console {message.type}: {message.text}") if message.type == "error" else None
        ),
    )
    page.on("pageerror", lambda error: runtime_errors.append(f"page: {error}"))

    try:
        response = page.goto(f"{url}reference/{NESTED_REFERENCE_ROUTES[0][0]}/", wait_until="networkidle")
        assert response is not None and response.ok, (
            "nested navigation regression fixture returned a non-success response"
        )
        page.locator(".md-header__button[for='__drawer']").click()
        page.wait_for_function("document.querySelector('#__drawer').checked")
        page.wait_for_function("document.querySelector('.md-sidebar--primary').getAttribute('aria-modal') === 'true'")
        released = page.locator(".md-sidebar--primary [inert]").evaluate_all(
            """elements => {
                for (const element of elements) element.inert = false
                return elements.length
            }"""
        )
        assert released >= 3, "nested navigation regression fixture did not restore affected parent-panel controls"

        pointer_failure = ""
        try:
            assert_rendered_health(page, "affected nested navigation pointer fixture", runtime_errors)
        except AssertionError as error:
            pointer_failure = str(error)
        assert "unreachable controls" in pointer_failure, (
            f"nested navigation pointer regression was not detected: {pointer_failure or 'no failure'}"
        )

        keyboard_failure = ""
        try:
            assert_navigation_focus_wraps(page, "affected nested navigation keyboard fixture")
        except AssertionError as error:
            keyboard_failure = str(error)
        assert "occluded or translated panel" in keyboard_failure, (
            f"nested navigation keyboard regression was not detected: {keyboard_failure or 'no failure'}"
        )
        translated_panel_result = {
            "geometry": "affected fixture rejected",
            "keyboard": "affected fixture rejected",
        }

        response = page.goto(f"{url}reference/{NESTED_REFERENCE_ROUTES[-1][0]}/", wait_until="networkidle")
        assert response is not None and response.ok, "three-row nested navigation fixture returned a failure"
        page.locator(".md-header__button[for='__drawer']").click()
        page.wait_for_function("document.querySelector('#__drawer').checked")
        page.wait_for_function("document.querySelector('.md-sidebar--primary').getAttribute('aria-modal') === 'true'")
        fixture = page.evaluate(
            """() => {
                const drawer = document.querySelector('.md-sidebar--primary')
                let activePanel = drawer.querySelector('.md-nav--primary')
                for (const panelToggle of drawer.querySelectorAll('input.md-nav__toggle:checked')) {
                    const panel = panelToggle.parentElement?.querySelector(':scope > nav.md-nav')
                    if (panel instanceof HTMLElement && activePanel.contains(panel)) activePanel = panel
                }
                const active = activePanel.querySelector('.md-nav__link--active')
                const list = active?.closest('.md-nav__list')
                if (!(list instanceof HTMLElement) || !(active instanceof HTMLElement)) return null
                const rows = [...list.querySelectorAll(':scope > .md-nav__item')]
                    .map((row) => row.querySelector(':scope > .md-nav__link'))
                    .filter((row) => row instanceof HTMLElement)
                if (rows.length < 4) return null
                const listTop = list.getBoundingClientRect().top
                const threeRowHeight = Math.ceil(rows[2].getBoundingClientRect().bottom - listTop)
                list.style.setProperty('height', `${threeRowHeight}px`, 'important')
                list.style.setProperty('max-height', `${threeRowHeight}px`, 'important')
                list.style.setProperty('min-height', `${threeRowHeight}px`, 'important')
                list.style.setProperty('overflow-y', 'clip', 'important')
                list.scrollTop = 0
                const listBounds = list.getBoundingClientRect()
                const visibleRows = rows.filter((row) => {
                    const bounds = row.getBoundingClientRect()
                    return bounds.bottom > listBounds.top + 1 && bounds.top < listBounds.bottom - 1
                }).length
                return {
                    active: active.textContent.trim().replace(/\s+/g, ' '),
                    overflowY: getComputedStyle(list).overflowY,
                    visibleRows,
                }
            }"""
        )
        assert fixture == {"active": "Testing", "overflowY": "clip", "visibleRows": 3}, (
            f"three-row nested navigation fixture was not applied: {fixture}"
        )
        clipped_geometry = reference_panel_geometry(page)

        geometry_failure = ""
        try:
            assert_rendered_health(page, "three-row nested navigation fixture", runtime_errors)
        except AssertionError as error:
            geometry_failure = str(error)
        assert "'fullyClipped': True" in geometry_failure, (
            f"fully clipped nested controls were not detected: {geometry_failure or 'no failure'}"
        )

        interaction_failure = ""
        try:
            assert_reference_panel_geometry(clipped_geometry, "three-row nested navigation fixture", "Testing")
        except AssertionError as error:
            interaction_failure = str(error)
        assert "hid the active destination" in interaction_failure, (
            f"clipped active destination was not rejected: {interaction_failure or 'no failure'}"
        )

        return {
            "translated_parent_panels": translated_panel_result,
            "three_row_list": {
                "geometry": "affected fixture rejected",
                "interaction": "affected fixture rejected",
                "reference_panel": clipped_geometry,
                "visible_rows": fixture["visibleRows"],
            },
        }
    finally:
        context.close()


def exercise_viewport(browser: Browser, url: str, width: int, height: int) -> None:
    context = browser.new_context(viewport={"width": width, "height": height}, reduced_motion="reduce")
    github_api_requests: list[str] = []

    def reject_github_api(route: Route) -> None:
        github_api_requests.append(route.request.url)
        route.fulfill(
            status=403,
            content_type="application/json",
            body='{"message":"API rate limit exceeded"}',
        )

    context.route(GITHUB_API_ENDPOINT, reject_github_api)
    page = context.new_page()
    for endpoint in GITHUB_API_ENDPOINTS:
        intercepted = page.goto(endpoint)
        assert intercepted is not None and intercepted.status == 403, (
            f"GitHub API regression route did not return HTTP 403 for {endpoint}"
        )
    assert github_api_requests == list(GITHUB_API_ENDPOINTS), (
        f"GitHub API regression routes were not exercised: {github_api_requests}"
    )
    github_api_requests.clear()

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
    page.on(
        "request",
        lambda request: (
            runtime_errors.append(f"external request: {request.url}")
            if request.url.startswith(("http://", "https://")) and not request.url.startswith(url)
            else None
        ),
    )

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
            assert_viewport_edge_fragment_threshold(
                page,
                runtime_errors,
                f"viewport-edge controls at {label}",
            )

        runtime_boundary = page.evaluate(
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
                navigationAccessibility: [...document.scripts].filter(
                    script => script.src.endsWith('/javascripts/navigation-accessibility.js')
                ).length,
                repositoryLinks: document.querySelectorAll(
                    '[data-dw-component="repository-link"]'
                ).length,
                liveRepositoryMetadata: document.querySelectorAll(
                    '[data-md-component="source"]'
                ).length,
                externalStylesheets: [...document.querySelectorAll('link[rel="stylesheet"][href]')]
                    .map(link => link.href)
                    .filter(href => new URL(href).origin !== location.origin),
                externalPreconnects: [...document.querySelectorAll('link[rel="preconnect"][href]')]
                    .map(link => link.href)
                    .filter(href => new URL(href).origin !== location.origin),
            })"""
        )
        assert runtime_boundary == {
            "retiredUi": 0,
            "retiredStorage": None,
            "googleScripts": 0,
            "runtimes": 1,
            "searchAccessibility": 1,
            "navigationAccessibility": 1,
            "repositoryLinks": 2,
            "liveRepositoryMetadata": 0,
            "externalStylesheets": [],
            "externalPreconnects": [],
        }, f"documentation runtime boundary is invalid at {label}: {runtime_boundary}"

        promotion = page.locator('[data-promotion-source="sdk-python-reference"]')
        assert promotion.count() == 1, f"Cloud promotion is missing at {label}"
        assert promotion.is_visible(), f"Cloud promotion is hidden at {label}"
        action = promotion.locator('[data-promotion-action="early-access"]')
        assert action.get_attribute("href") == (
            "https://cloud.durable-workflow.com/early-access#source=sdk-python-reference"
        )

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
            navigation_opener = page.locator(".md-header__button[for='__drawer']")
            navigation_drawer = page.locator(".md-sidebar--primary")
            navigation_close = navigation_drawer.locator(".dw-navigation__close")
            navigation_backdrop = page.locator(".md-overlay[for='__drawer']")

            navigation_opener.click()
            page.wait_for_function("document.querySelector('#__drawer').checked")
            page.wait_for_function(
                "document.querySelector('.md-sidebar--primary').getAttribute('aria-modal') === 'true'"
            )
            assert navigation_drawer.is_visible(), f"navigation drawer is not visible at {label}"
            assert_navigation_background_is_inert(page, f"open navigation at {label}")
            assert_navigation_focus_wraps(page, f"open navigation at {label}")
            assert_rendered_health(page, f"open navigation at {label}", runtime_errors)

            navigation_backdrop.click(position={"x": width - 8, "y": height / 2})
            page.wait_for_function("!document.querySelector('#__drawer').checked")
            assert navigation_opener.evaluate("(element) => element === document.activeElement"), (
                f"backdrop-closing navigation did not restore opener focus at {label}"
            )
            assert page.locator("[inert]").count() == 0, f"backdrop-closing navigation left the page inert at {label}"

            navigation_opener.click()
            page.wait_for_function("document.querySelector('#__drawer').checked")
            page.keyboard.press("Escape")
            page.wait_for_function("!document.querySelector('#__drawer').checked")
            assert navigation_opener.evaluate("(element) => element === document.activeElement"), (
                f"Escape-closing navigation did not restore opener focus at {label}"
            )
            assert page.locator("[inert]").count() == 0, f"Escape-closing navigation left the page inert at {label}"

            navigation_opener.click()
            page.wait_for_function("document.querySelector('#__drawer').checked")
            navigation_close.focus()
            page.keyboard.press("Enter")
            page.wait_for_function("!document.querySelector('#__drawer').checked")
            assert navigation_opener.evaluate("(element) => element === document.activeElement"), (
                f"explicitly closing navigation did not restore opener focus at {label}"
            )
            assert page.locator("[inert]").count() == 0, f"explicitly closing navigation left the page inert at {label}"
            assert navigation_drawer.get_attribute("aria-modal") is None, (
                f"closing navigation left modal semantics behind at {label}"
            )
            assert_rendered_health(page, f"closed navigation at {label}", runtime_errors)

        assert github_api_requests == [], (
            f"documentation requested live GitHub repository metadata at {label}: {github_api_requests}"
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
    parser.add_argument(
        "--navigation-transition-only",
        action="store_true",
        help="Exercise only the responsive navigation transition used by visual qualification.",
    )
    parser.add_argument(
        "--nested-navigation-only",
        action="store_true",
        help="Exercise only the nested reference navigation matrix used by visual qualification.",
    )
    parser.add_argument(
        "--transition-evidence",
        type=Path,
        help="Write a structured passing result after the breakpoint transition succeeds.",
    )
    parser.add_argument(
        "--nested-navigation-evidence",
        type=Path,
        help="Write structured pointer, keyboard, and regression evidence for the nested reference route.",
    )
    parser.add_argument(
        "--source-revision",
        help="Bind nested navigation evidence to the captured 40-character source revision.",
    )
    args = parser.parse_args()
    if args.navigation_transition_only and args.nested_navigation_only:
        parser.error("--navigation-transition-only and --nested-navigation-only are mutually exclusive")
    if args.transition_evidence and args.nested_navigation_only:
        parser.error("--transition-evidence cannot be used with --nested-navigation-only")
    if args.nested_navigation_evidence and args.navigation_transition_only:
        parser.error("--nested-navigation-evidence cannot be used with --navigation-transition-only")
    if args.source_revision and (
        args.nested_navigation_evidence is None or re.fullmatch(r"[0-9a-f]{40}", args.source_revision) is None
    ):
        parser.error("--source-revision requires nested navigation evidence and a 40-character revision")
    site = args.site.resolve()
    if not (site / "index.html").is_file():
        raise SystemExit(f"built documentation not found at {site}")

    nested_results: list[dict[str, Any]] = []
    nested_regression: dict[str, str] = {}
    with serve(site) as url, sync_playwright() as playwright:
        launch_options = {"headless": True}
        if args.chromium_executable:
            launch_options["executable_path"] = args.chromium_executable
        browser = playwright.chromium.launch(**launch_options)
        try:
            if not args.navigation_transition_only and not args.nested_navigation_only:
                for width, height in VIEWPORTS:
                    exercise_viewport(browser, url, width, height)
            if not args.navigation_transition_only:
                nested_results = [
                    exercise_nested_navigation_viewport(browser, url, route, position, width, height)
                    for route, position in NESTED_REFERENCE_ROUTES
                    for width, height in VIEWPORTS
                ]
                nested_regression = exercise_nested_navigation_regression(browser, url)
            if not args.nested_navigation_only:
                exercise_navigation_breakpoint_transition(browser, url)
        finally:
            browser.close()

    if args.navigation_transition_only:
        print("Validated navigation state across the responsive-to-desktop breakpoint transition.")
    elif args.nested_navigation_only:
        print(
            "Validated nested reference navigation pointer and keyboard reachability at desktop, intermediate, "
            "mobile, and compact-height viewports."
        )
    else:
        print(
            "Validated contained documentation search and navigation focus, close behavior, and rendered health "
            "on root and nested reference routes at desktop, intermediate, mobile, compact-height, and "
            "responsive-to-desktop transitions."
        )
    if args.transition_evidence:
        evidence = {
            "schema": "durable-workflow.python-docs.navigation-transition/v1",
            "outcome": "pass",
            "viewports": {
                "responsive": {"width": 390, "height": 844},
                "desktop": {"width": 1440, "height": 900},
            },
            "verified": [
                "toggle-state",
                "dialog-semantics",
                "focus",
                "inert-cleanup",
                "keyboard-reachability",
                "pointer-reachability",
                "overflow",
                "browser-errors",
                "responsive-reentry",
            ],
        }
        args.transition_evidence.write_text(f"{json.dumps(evidence, indent=2)}\n", encoding="utf-8")
    if args.nested_navigation_evidence:
        evidence = {
            "schema": "durable-workflow.python-docs.nested-navigation/v3",
            "outcome": "pass",
            "routes": [f"/reference/{route}/" for route, _ in NESTED_REFERENCE_ROUTES],
            "viewports": nested_results,
            "regression": nested_regression,
            "verified": [
                "explicit-route",
                "pointer-reachability",
                "keyboard-traversal",
                "active-panel-isolation",
                "active-destination-visibility",
                "affected-fixture-rejection",
                "browser-errors",
                "available-drawer-height",
                "distinct-navigation-actions",
                "fully-clipped-control-rejection",
                "three-row-list-rejection",
            ],
        }
        if args.source_revision:
            evidence["source"] = {
                "repository": "durable-workflow/sdk-python",
                "revision": args.source_revision,
            }
        args.nested_navigation_evidence.write_text(f"{json.dumps(evidence, indent=2)}\n", encoding="utf-8")


if __name__ == "__main__":
    main()
