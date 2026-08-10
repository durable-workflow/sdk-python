import argparse
import re
from pathlib import Path

parser = argparse.ArgumentParser(description="Validate the rendered documentation analytics runtime.")
parser.add_argument("build_directory", nargs="?", type=Path, default=Path("site"))
parser.add_argument(
    "--require-token",
    action="store_true",
    help="Require a canonical token in the final public artifact instead of the build placeholder.",
)
args = parser.parse_args()

build_directory = args.build_directory
runtime = Path("docs/javascripts/analytics.js").read_text(encoding="utf-8")
styles = Path("docs/stylesheets/layout.css").read_text(encoding="utf-8")

if (build_directory / "javascripts/analytics.js").read_text(encoding="utf-8") != runtime:
    raise SystemExit("Rendered MkDocs analytics runtime is stale")
if not re.search(r"\.dw-cloud-promotion__eyebrow\s*\{[^}]*letter-spacing:\s*0;", styles, re.DOTALL):
    raise SystemExit("Promotion eyebrow letter spacing must remain zero")

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

if args.require_token and (
    "__CLOUDFLARE_WEB_ANALYTICS_TOKEN__" in runtime or not re.search(r"\|\|\s*'[a-f0-9]{32}'", runtime)
):
    raise SystemExit("Final public analytics runtime does not contain the canonical site token")

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

home = (build_directory / "index.html").read_text(encoding="utf-8")
if home.count('data-promotion-source="sdk-python-reference"') != 1:
    raise SystemExit("Python reference home must render one bounded Cloud promotion")
if 'href="https://cloud.durable-workflow.com/early-access#source=sdk-python-reference"' not in home:
    raise SystemExit("Python reference promotion must resolve to the public early-access form")

for promotion_boundary in (
    "PROMOTION_SOURCE = 'sdk-python-reference'",
    "credentials: 'omit'",
    "referrerPolicy: 'origin'",
    "JSON.stringify({source: PROMOTION_SOURCE, event})",
):
    if promotion_boundary not in runtime:
        raise SystemExit(f"Promotion analytics is missing its bounded contract: {promotion_boundary}")

instrumentation = " with the canonical site token" if args.require_token else ""
print(f"Validated cookie-free Cloudflare Web Analytics{instrumentation} in {len(html_files)} rendered pages.")
