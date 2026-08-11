#!/usr/bin/env python3
"""Qualify the deployed Python promotion transport in a real browser."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

# Direct file execution adds ``scripts/`` rather than the repository root to
# sys.path. The deployment workflow uses this entrypoint, so make its local
# package imports resolvable before importing the qualifier dependencies.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from playwright.sync_api import Browser, Request, Response, sync_playwright  # noqa: E402
from scripts.api_reference_release import load_release_identity  # noqa: E402
from scripts.check_api_reference_install import verify_public_deployment  # noqa: E402

DOCS_URL = "https://python.durable-workflow.com/"
RELEASE_AUDIT_URL = f"{DOCS_URL}release-audit.json"
DESTINATION_URL = "https://cloud.durable-workflow.com/early-access#source=sdk-python-reference"
PROMOTION_EVENT_URL = "https://cloud.durable-workflow.com/early-access/promotion-events"
PROMOTION_SOURCE = "sdk-python-reference"
QUALIFICATION_EVENT = "qualification"
VIEWPORTS = (
    ("desktop", 1440, 900),
    ("intermediate", 768, 1024),
    ("mobile", 390, 844),
    ("short-height", 640, 360),
)

QUALIFICATION_REWRITE_SCRIPT = f"""
(() => {{
  const eventUrl = {json.dumps(PROMOTION_EVENT_URL)};
  const source = {json.dumps(PROMOTION_SOURCE)};
  const qualificationEvent = {json.dumps(QUALIFICATION_EVENT)};
  const nativeFetch = window.fetch.bind(window);

  window.fetch = function (input, init) {{
    const requestUrl = typeof input === 'string' ? input : input.url;
    if (requestUrl !== eventUrl) return nativeFetch(input, init);

    const options = init || {{}};
    let initiatedPayload = null;
    try {{
      initiatedPayload = JSON.parse(options.body);
    }} catch (_error) {{
      // The qualification fails on the recorded initiation shape below.
    }}
    window.recordPromotionQualificationInitiation(initiatedPayload);

    return nativeFetch(input, {{
      ...options,
      body: JSON.stringify({{source, event: qualificationEvent}}),
    }});
  }};
}})();
"""


def event_payload(request: Request) -> dict[str, Any] | None:
    try:
        payload = json.loads(request.post_data or "")
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def is_event(response: Response, event: str) -> bool:
    return response.url == PROMOTION_EVENT_URL and event_payload(response.request) == {
        "source": PROMOTION_SOURCE,
        "event": event,
    }


def assert_event_response(response: Response, event: str) -> None:
    request = response.request
    headers = request.all_headers()
    assert request.method == "POST", f"{event} promotion event used {request.method}"
    assert event_payload(request) == {"source": PROMOTION_SOURCE, "event": event}, (
        f"{event} promotion event was not bounded: {request.post_data}"
    )
    assert headers.get("origin") == DOCS_URL.rstrip("/"), (
        f"{event} promotion event omitted or changed its Origin header: {headers.get('origin')}"
    )
    assert headers.get("referer") == DOCS_URL, (
        f"{event} promotion event exposed more than the docs origin: {headers.get('referer')}"
    )
    assert headers.get("content-type", "").startswith("text/plain"), (
        f"{event} promotion event changed content type: {headers.get('content-type')}"
    )
    assert "cookie" not in headers and "authorization" not in headers, f"{event} promotion event sent credentials"
    assert response.status == 204, f"{event} promotion receiver returned HTTP {response.status}"
    assert response.headers.get("access-control-allow-origin") == DOCS_URL.rstrip("/"), (
        f"{event} promotion response did not allow the Python docs origin"
    )
    assert "no-store" in response.headers.get("cache-control", ""), f"{event} promotion response was cacheable"


def qualify_viewport(browser: Browser, name: str, width: int, height: int) -> None:
    context = browser.new_context(viewport={"width": width, "height": height}, reduced_motion="reduce")
    page = context.new_page()
    errors: list[str] = []
    promotion_requests: list[Request] = []
    initiated_events: list[object] = []

    def record_initiated_event(payload: object) -> None:
        initiated_events.append(payload)

    page.expose_function("recordPromotionQualificationInitiation", record_initiated_event)
    page.add_init_script(QUALIFICATION_REWRITE_SCRIPT)

    page.on(
        "console",
        lambda message: errors.append(f"console {message.type}: {message.text}") if message.type == "error" else None,
    )
    page.on("pageerror", lambda error: errors.append(f"page: {error}"))
    page.on(
        "request",
        lambda request: promotion_requests.append(request) if request.url == PROMOTION_EVENT_URL else None,
    )

    try:
        action = page.locator('[data-promotion-action="early-access"]')
        with page.expect_response(lambda response: is_event(response, QUALIFICATION_EVENT), timeout=30_000) as pending:
            document = page.goto(DOCS_URL, wait_until="domcontentloaded", timeout=30_000)
            action.scroll_into_view_if_needed()
        assert document is not None and document.ok, f"deployed docs returned HTTP {document.status} at {name}"
        assert_event_response(pending.value, QUALIFICATION_EVENT)

        action.wait_for(state="visible")
        assert action.get_attribute("href") == DESTINATION_URL, "promotion destination changed"
        with (
            page.expect_response(
                lambda response: is_event(response, QUALIFICATION_EVENT), timeout=30_000
            ) as click_pending,
            page.expect_navigation(wait_until="domcontentloaded", timeout=30_000) as navigation,
        ):
            action.click()
        assert_event_response(click_pending.value, QUALIFICATION_EVENT)

        destination = navigation.value
        assert destination is not None and destination.status == 200, (
            f"public early-access destination returned HTTP {destination.status if destination else 'none'} at {name}"
        )
        resolved = urlparse(page.url)
        expected = urlparse(DESTINATION_URL)
        assert (resolved.scheme, resolved.netloc, resolved.path) == (
            expected.scheme,
            expected.netloc,
            expected.path,
        ), f"promotion resolved to an unexpected destination at {name}: {page.url}"
        assert page.locator("[data-promotion-source-input]").input_value() == PROMOTION_SOURCE, (
            f"promotion source was not retained by the early-access form at {name}"
        )
        assert page.locator('input[name="intent"][value="cohort"]').is_checked(), (
            f"promotion did not select the launch cohort at {name}"
        )

        page.wait_for_timeout(250)
        assert initiated_events == [
            {"source": PROMOTION_SOURCE, "event": "impression"},
            {"source": PROMOTION_SOURCE, "event": "click"},
        ], f"deployed promotion initiated unexpected events at {name}: {initiated_events}"
        observed = [event_payload(request) for request in promotion_requests]
        assert observed == [
            {"source": PROMOTION_SOURCE, "event": QUALIFICATION_EVENT},
            {"source": PROMOTION_SOURCE, "event": QUALIFICATION_EVENT},
        ], f"promotion qualification emitted duplicate or unbounded events at {name}: {observed}"
        assert errors == [], f"promotion emitted browser errors at {name}: {errors}"
    finally:
        context.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--chromium-executable",
        default=os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE"),
        help="Use a system Chromium instead of Playwright's managed browser.",
    )
    parser.add_argument(
        "--source-revision",
        required=True,
        help="Exact deployed source revision required before live qualification begins.",
    )
    parser.add_argument(
        "--release-audit-attempts",
        type=int,
        default=12,
        help="Number of attempts allowed while the live release record converges.",
    )
    parser.add_argument(
        "--release-audit-retry-sleep",
        type=float,
        default=10,
        help="Seconds between live release-record attempts.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    verify_public_deployment(
        RELEASE_AUDIT_URL,
        load_release_identity(REPO_ROOT),
        args.source_revision,
        attempts=args.release_audit_attempts,
        retry_sleep=args.release_audit_retry_sleep,
    )

    with sync_playwright() as playwright:
        launch_options = {"headless": True}
        if args.chromium_executable:
            launch_options["executable_path"] = args.chromium_executable
        browser = playwright.chromium.launch(**launch_options)
        try:
            for name, width, height in VIEWPORTS:
                qualify_viewport(browser, name, width, height)
        finally:
            browser.close()

    print(
        f"Confirmed deployed revision {args.source_revision}, two non-aggregating qualification requests, "
        "the source-attributed impression/click initiation and destination behavior, and no browser errors at "
        "desktop, intermediate, mobile, and short-height viewports."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
