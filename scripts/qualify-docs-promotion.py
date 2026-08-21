#!/usr/bin/env python3
"""Qualify the deployed Python landing and promotion transport in a real browser."""

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
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError  # noqa: E402
from scripts.api_reference_release import load_release_identity  # noqa: E402
from scripts.check_api_reference_install import verify_public_deployment  # noqa: E402

DOCS_URL = "https://python.durable-workflow.com/"
RELEASE_AUDIT_URL = f"{DOCS_URL}release-audit.json"
DESTINATION_URL = "https://cloud.durable-workflow.com/early-access#source=sdk-python-reference"
PROMOTION_EVENT_URL = "https://cloud.durable-workflow.com/early-access/promotion-events"
PROMOTION_SOURCE = "sdk-python-reference"
QUALIFICATION_EVENT = "qualification"
SDK_GUIDE_URL = "https://durable-workflow.com/docs/2.0/polyglot/python/"
PYPI_URL = "https://pypi.org/project/durable-workflow/"
GITHUB_URL = "https://github.com/durable-workflow/sdk-python"
MAIN_DOCS_URL = "https://durable-workflow.com/docs/2.0/introduction/"
CAPABILITIES_URL = "https://durable-workflow.com/docs/2.0/capabilities/"
COMPATIBILITY_URL = "https://durable-workflow.com/docs/2.0/compatibility/"
PLAYGROUND_URL = "https://github.com/durable-workflow/sample-app#symmetric-sdk-playground"
CLOUD_GUIDE_URL = "https://durable-workflow.com/docs/2.0/polyglot/cloud-control-plane/"
REFERENCE_ROUTES = ("reference/client/", "reference/worker/", "reference/workflow/", "reference/activity/")
VIEWPORT_ATTEMPTS = 3
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


def assert_deployed_landing(page: Any, name: str) -> None:
    """Check stable destinations and browser behavior without freezing landing prose."""
    contract = page.evaluate(
        r"""({
            guideUrl, pypiUrl, githubUrl, mainDocsUrl, capabilitiesUrl,
            compatibilityUrl, playgroundUrl, cloudGuideUrl,
        }) => {
            const one = (selector) => document.querySelector(selector)
            const all = (selector) => [...document.querySelectorAll(selector)]
            const href = (selector) => one(selector)?.href || null
            const meta = (selector) => one(selector)?.content || null
            const primary = one('[data-docs-destination="local-self-hosted"]')
            const cloudHero = one('.dw-hero [data-docs-destination="managed-cloud"]')
            const guide = one('[data-docs-destination="sdk-guide"]')
            const reference = one('.dw-hero [data-docs-destination="api-reference"]')
            const selfHosted = one('[data-runtime="self-hosted"]')
            const cloud = one('[data-runtime="cloud"]')
            const install = one('[data-docs-action="install"] code')
            const localJourney = one('[data-docs-journey="local-self-hosted"]')
            const managedJourney = one('[data-docs-journey="managed-cloud"]')
            const destinations = all('[data-docs-destination]').map((element) => ({
                destination: element.dataset.docsDestination,
                href: element.href,
            }))
            return {
                canonical: href('link[rel="canonical"]'),
                cloudAccess: cloud?.dataset.access || null,
                cloudHeroAccess: cloudHero?.dataset.access || null,
                cloudHeroHref: cloudHero?.getAttribute('href') || null,
                cloudBelowFirstViewport: Boolean(cloud && cloud.getBoundingClientRect().top >= innerHeight),
                cloudFollowsSelfHosted: Boolean(
                    cloud && selfHosted
                    && (selfHosted.compareDocumentPosition(cloud) & Node.DOCUMENT_POSITION_FOLLOWING)
                ),
                codeBlocks: all('.dw-landing pre').map((element) => ({
                    clientWidth: element.clientWidth,
                    overflowX: getComputedStyle(element).overflowX,
                    scrollWidth: element.scrollWidth,
                })),
                documentWidth: document.documentElement.scrollWidth,
                capabilitiesLinked: destinations.some((entry) => entry.href === capabilitiesUrl),
                cloudGuideLinked: destinations.some((entry) => entry.href === cloudGuideUrl),
                compatibilityLinked: destinations.some((entry) => entry.href === compatibilityUrl),
                credentialRoles: all('[data-credential-role]')
                    .map((element) => element.dataset.credentialRole).sort(),
                favicon: href('link[rel~="icon"]'),
                githubLinked: destinations.some((entry) => entry.href === githubUrl),
                guideHref: guide?.href || null,
                guideLinked: destinations.some((entry) => entry.href === guideUrl),
                installCommand: install?.textContent.trim().replace(/\s+/g, ' ') || null,
                journeyContract: localJourney ? {
                    activityType: localJourney.dataset.activityType,
                    taskQueue: localJourney.dataset.taskQueue,
                    workflowType: localJourney.dataset.workflowType,
                } : null,
                journeyCode: localJourney
                    ? all('[data-docs-journey="local-self-hosted"] code')
                        .map((element) => element.textContent).join('\n')
                    : '',
                localJourney: Boolean(localJourney),
                mainDocsLinked: destinations.some((entry) => entry.href === mainDocsUrl),
                managedJourney: Boolean(managedJourney),
                ogDescription: meta('meta[property="og:description"]'),
                ogSiteName: meta('meta[property="og:site_name"]'),
                ogTitle: meta('meta[property="og:title"]'),
                ogType: meta('meta[property="og:type"]'),
                ogUrl: meta('meta[property="og:url"]'),
                pageDescription: meta('meta[name="description"]'),
                pageTitle: document.title,
                positiveTabIndexes: all('[tabindex]').filter((element) => Number(element.tabIndex) > 0).length,
                primaryAccess: primary?.dataset.access || null,
                primaryHref: primary?.getAttribute('href') || null,
                pypiLinked: destinations.some((entry) => entry.href === pypiUrl),
                playgroundLinked: destinations.some((entry) => entry.href === playgroundUrl),
                referenceHref: reference?.getAttribute('href') || null,
                roles: all('[data-sdk-role]').map((element) => element.dataset.sdkRole).sort(),
                selfHostedAccess: selfHosted?.dataset.access || null,
                serverCommand: localJourney
                    ? all('[data-docs-journey="local-self-hosted"] code')
                        .map((element) => element.textContent)
                        .find((text) => text.includes('durableworkflow/server:')) || null
                    : null,
                surfaceCount: all('[data-docs-surface="python-sdk-landing"]').length,
                runtimeUrlContracts: all('[data-runtime-url-contract]')
                    .map((element) => element.dataset.runtimeUrlContract),
                twitterCard: meta('meta[name="twitter:card"]'),
                twitterDescription: meta('meta[name="twitter:description"]'),
                twitterTitle: meta('meta[name="twitter:title"]'),
                viewportWidth: document.documentElement.clientWidth,
                workerReadyOutputs: all('[data-worker-ready-output]')
                    .map((element) => element.dataset.workerReadyOutput),
            }
        }""",
        {
            "guideUrl": SDK_GUIDE_URL,
            "pypiUrl": PYPI_URL,
            "githubUrl": GITHUB_URL,
            "mainDocsUrl": MAIN_DOCS_URL,
            "capabilitiesUrl": CAPABILITIES_URL,
            "compatibilityUrl": COMPATIBILITY_URL,
            "playgroundUrl": PLAYGROUND_URL,
            "cloudGuideUrl": CLOUD_GUIDE_URL,
        },
    )
    assert contract["surfaceCount"] == 1, f"deployed {name} landing surface is missing"
    assert contract["documentWidth"] <= contract["viewportWidth"] + 1, f"deployed {name} landing overflows"
    assert contract["primaryAccess"] == contract["selfHostedAccess"] == "no-account-required"
    assert contract["primaryHref"] == "#first-workflow"
    assert contract["cloudAccess"] == "limited"
    assert contract["cloudHeroAccess"] == "limited" and contract["cloudHeroHref"] == "#managed-cloud"
    assert contract["cloudFollowsSelfHosted"], f"deployed {name} landing puts Cloud before self-hosting"
    assert contract["cloudBelowFirstViewport"], f"deployed {name} first viewport promotes limited Cloud access"
    assert contract["localJourney"]
    assert contract["managedJourney"]
    assert contract["runtimeUrlContracts"] == ["provisioned-namespace-root"]
    assert contract["credentialRoles"] == [
        "control-plane-api-key",
        "runtime-client-token",
        "runtime-worker-token",
    ]
    assert contract["journeyContract"] == {
        "activityType": "python.greet",
        "taskQueue": "python-workers",
        "workflowType": "python.greeter",
    }
    assert all(value in contract["journeyCode"] for value in contract["journeyContract"].values())
    assert contract["workerReadyOutputs"] == ["registered"]
    assert contract["roles"] == ["activity", "client", "worker", "workflow"]
    assert contract["guideHref"] == SDK_GUIDE_URL
    assert contract["referenceHref"] == "reference/client/"
    assert contract["guideLinked"] and contract["pypiLinked"] and contract["githubLinked"]
    assert all(
        contract[key]
        for key in (
            "mainDocsLinked",
            "capabilitiesLinked",
            "compatibilityLinked",
            "playgroundLinked",
            "cloudGuideLinked",
        )
    )
    assert contract["installCommand"] == "pip install 'durable-workflow~=2.0.0rc0'"
    assert contract["serverCommand"] and "{{" not in contract["serverCommand"]
    assert contract["codeBlocks"] and all(
        block["scrollWidth"] <= block["clientWidth"] + 1 or block["overflowX"] in {"auto", "scroll"}
        for block in contract["codeBlocks"]
    ), f"deployed {name} landing contains an unscrollable code block"
    assert contract["positiveTabIndexes"] == 0
    assert contract["favicon"] and contract["favicon"].endswith("/assets/favicon.svg")
    assert contract["ogType"] == "website" and contract["ogSiteName"] == "Durable Workflow Python SDK"
    assert contract["ogTitle"] == contract["pageTitle"] == contract["twitterTitle"]
    assert contract["ogDescription"] == contract["pageDescription"] == contract["twitterDescription"]
    assert contract["ogUrl"] == contract["canonical"] == DOCS_URL
    assert contract["twitterCard"] == "summary"

    primary = page.locator('[data-docs-destination="local-self-hosted"]').first
    cloud_path = page.locator('.dw-hero [data-docs-destination="managed-cloud"]')
    primary.focus()
    page.keyboard.press("Tab")
    assert cloud_path.evaluate("(element) => element === document.activeElement"), (
        f"deployed {name} landing focus does not move from the local task to the managed path"
    )
    page.keyboard.press("Shift+Tab")
    assert primary.evaluate("(element) => element === document.activeElement")
    page.evaluate("scrollTo(0, 0)")

    resources = page.evaluate(
        """async ({routes}) => {
            const requested = [...routes, 'search/search_index.json']
            const responses = await Promise.all(requested.map(async (path) => {
                const response = await fetch(new URL(path, location.href), {credentials: 'omit'})
                let searchDocuments = null
                if (path.endsWith('.json') && response.ok) {
                    const payload = await response.clone().json()
                    searchDocuments = Array.isArray(payload.docs) ? payload.docs.length : null
                }
                return {path, status: response.status, searchDocuments}
            }))
            const favicon = document.querySelector('link[rel~="icon"]')
            const faviconResponse = await fetch(favicon.href, {credentials: 'omit'})
            responses.push({
                path: new URL(favicon.href).pathname,
                status: faviconResponse.status,
                type: faviconResponse.headers.get('content-type'),
            })
            return responses
        }""",
        {"routes": list(REFERENCE_ROUTES)},
    )
    assert all(resource["status"] == 200 for resource in resources), (
        f"deployed {name} landing resources failed: {resources}"
    )
    search = next(resource for resource in resources if resource["path"] == "search/search_index.json")
    assert search["searchDocuments"] and search["searchDocuments"] > len(REFERENCE_ROUTES)
    favicon = next(resource for resource in resources if resource["path"].endswith("/assets/favicon.svg"))
    assert "image/svg+xml" in (favicon.get("type") or "")


def exercise_search(page: Any, name: str, width: int) -> None:
    opener_selector = ".md-header__button[for='__search']" if width < 960 else ".md-search__input"
    opener = page.locator(opener_selector)
    opener.click()
    page.wait_for_function("document.querySelector('#__search').checked")
    page.wait_for_function("document.querySelector('.md-search').getAttribute('aria-modal') === 'true'")
    search_input = page.locator(".md-search__input")
    search_input.fill("")
    search_input.press_sequentially("Client", delay=25)
    result_list = page.locator(".md-search-result__list")
    result_list.locator("li").first.wait_for(state="visible")
    client_result = result_list.locator('a[href*="reference/client/"]').first
    client_result.wait_for(state="visible")
    close_control = page.locator(".md-search__icon[for='__search']")
    close_control.click()
    page.wait_for_function("!document.querySelector('#__search').checked")
    assert opener.evaluate("(element) => element === document.activeElement"), (
        f"deployed {name} search did not restore opener focus"
    )


def qualify_viewport_once(
    browser: Browser,
    name: str,
    width: int,
    height: int,
    evidence_directory: Path | None = None,
) -> None:
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
        document = page.goto(DOCS_URL, wait_until="networkidle", timeout=30_000)
        assert document is not None and document.ok, f"deployed docs returned HTTP {document.status} at {name}"
        assert_deployed_landing(page, name)
        exercise_search(page, name, width)
        assert initiated_events == [], f"below-fold Cloud promotion was counted in the {name} first viewport"
        assert promotion_requests == [], f"below-fold Cloud promotion sent a request in the {name} first viewport"
        if evidence_directory is not None:
            page.screenshot(path=evidence_directory / f"landing-{name}-{width}x{height}.png")

        with page.expect_response(lambda response: is_event(response, QUALIFICATION_EVENT), timeout=30_000) as pending:
            action.scroll_into_view_if_needed()
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


def qualify_viewport(
    browser: Browser,
    name: str,
    width: int,
    height: int,
    evidence_directory: Path | None = None,
) -> None:
    """Qualify one viewport, retrying only transient browser transport timeouts."""
    timeouts: list[str] = []
    for attempt in range(1, VIEWPORT_ATTEMPTS + 1):
        try:
            qualify_viewport_once(browser, name, width, height, evidence_directory)
            return
        except PlaywrightTimeoutError as error:
            timeouts.append(f"attempt {attempt}: {error}")

    raise AssertionError(
        f"promotion qualification timed out at {name} after {VIEWPORT_ATTEMPTS} attempts: " + "; ".join(timeouts)
    )


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
    parser.add_argument(
        "--evidence-directory",
        type=Path,
        default=Path("deployed-visual"),
        help="Retain first-viewport screenshots and a source-bound qualification record.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    evidence_directory = args.evidence_directory.resolve()
    evidence_directory.mkdir(parents=True, exist_ok=True)
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
                qualify_viewport(browser, name, width, height, evidence_directory)
        finally:
            browser.close()

    evidence = {
        "schema": "durable-workflow.python-docs.deployed-landing/v1",
        "outcome": "pass",
        "origin": DOCS_URL,
        "source_revision": args.source_revision,
        "viewports": [
            {
                "name": name,
                "width": width,
                "height": height,
                "screenshot": f"landing-{name}-{width}x{height}.png",
            }
            for name, width, height in VIEWPORTS
        ],
        "verified": [
            "account-free-primary-journey",
            "first-screen-runtime-choice",
            "limited-cloud-secondary-journey",
            "managed-runtime-credential-roles",
            "namespace-runtime-url-contract",
            "install-and-compatible-server-commands",
            "workflow-activity-worker-client-journey",
            "positive-worker-registration-output",
            "sdk-role-model",
            "developer-destinations",
            "reference-routes",
            "search",
            "focus-order",
            "code-overflow",
            "social-metadata",
            "favicon",
            "promotion-transport",
        ],
    }
    (evidence_directory / "qualification.json").write_text(
        f"{json.dumps(evidence, indent=2)}\n",
        encoding="utf-8",
    )

    print(
        f"Confirmed deployed revision {args.source_revision}, the general-first landing and developer destinations, "
        "reference routes, search, focus, code overflow, social metadata, favicon, and two successful "
        "non-aggregating promotion requests per viewport at desktop, intermediate, mobile, and short-height sizes."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
