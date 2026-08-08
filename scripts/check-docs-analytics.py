import re
import sys
from pathlib import Path

build_directory = Path(sys.argv[1] if len(sys.argv) > 1 else "site")
runtime = Path("docs/javascripts/analytics.js").read_text(encoding="utf-8")

if (build_directory / "javascripts/analytics.js").read_text(encoding="utf-8") != runtime:
    raise SystemExit("Rendered MkDocs analytics runtime is stale")

for required in (
    "https://static.cloudflareinsights.com/beacon.min.js",
    "document.querySelector(BEACON_SELECTOR)",
    "loader.type = 'module'",
    "loader.dataset.cfBeacon = JSON.stringify({token: TOKEN})",
    "'python.durable-workflow.com'",
    "'cloud.durable-workflow.com': new Set(['/', '/early-access', '/early-access/'])",
    "'status.durable-workflow.com': new Set(['/'])",
):
    if required not in runtime:
        raise SystemExit(f"Analytics runtime is missing required configuration: {required}")

if re.search(r"\bspa\s*:", runtime):
    raise SystemExit("Analytics runtime overrides Cloudflare's supported navigation semantics")

runtime_forbidden = re.compile(
    r"localStorage|sessionStorage|document\.cookie|googletagmanager|google-analytics|"
    r"G-HD1YHT442Y|durable-workflow\.analytics-consent|_ga(?:\b|_)",
    re.IGNORECASE,
)
if runtime_forbidden.search(runtime):
    raise SystemExit("Analytics runtime contains retired Google or browser-storage behavior")

html_files = list(build_directory.rglob("*.html"))
if not html_files:
    raise SystemExit("MkDocs did not render HTML pages")

html_forbidden = re.compile(
    r"googletagmanager|google-analytics|G-HD1YHT442Y|"
    r"durable-workflow\.analytics-consent|durable-workflow-analytics-(?:consent|preferences)|_ga(?:\b|_)",
    re.IGNORECASE,
)

for html_file in html_files:
    html = html_file.read_text(encoding="utf-8")
    if len(re.findall(r'src="[^"]*javascripts/analytics\.js"', html)) != 1:
        raise SystemExit(f"{html_file} must load one cookie-free analytics runtime")
    if not re.search(
        r'<script(?=[^>]*\bsrc="[^"]*javascripts/analytics\.js")(?=[^>]*\btype="module")[^>]*>',
        html,
    ):
        raise SystemExit(f"{html_file} must use module semantics for analytics")
    if "stylesheets/analytics.css" in html:
        raise SystemExit(f"{html_file} still loads retired analytics UI styles")
    if html_forbidden.search(html):
        raise SystemExit(f"{html_file} contains retired Google analytics or consent state")

print(f"Validated cookie-free Cloudflare Web Analytics in {len(html_files)} rendered pages.")
