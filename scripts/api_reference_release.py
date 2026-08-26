"""Release identity used to render the Python API reference."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import tomllib  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - exercised by the Python 3.10 CI cell
    import tomli as tomllib  # type: ignore[import-not-found]


SERVER_IMAGE_RESOLVER_TOKEN = "{{ durable_workflow_server_image_resolver }}"
RELEASE_EVIDENCE_FILENAME = "release-audit.json"
RELEASE_EVIDENCE_SCHEMA = "durable-workflow.python-api-reference.release"
QUICKSTART_CONTRACT_SCHEMA = "durable-workflow.docs.v2.quickstart-execution-contract"
QUICKSTART_CONTRACT_URL = "https://durable-workflow.com/quickstart-execution-contract.json"
SUPPORTED_PRERELEASE_INSTALL_COMMAND = "curl -fsSL https://durable-workflow.com/install-sdk.sh | sh -s -- python"
SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND = rf'''export DW_SERVER_IMAGE="$(
  curl -fsSL "${{DURABLE_WORKFLOW_QUICKSTART_CONTRACT_URL:-{QUICKSTART_CONTRACT_URL}}}" |
    python -c 'import json, re, sys
contract = json.load(sys.stdin)
if contract.get("schema") != "{QUICKSTART_CONTRACT_SCHEMA}":
    raise SystemExit("The public quickstart contract has an unsupported schema.")
server = contract.get("artifacts", {{}}).get("server", {{}})
version = server.get("version")
image = server.get("image")
reference = server.get("reference")
if not isinstance(version, str) or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+", version) is None:
    raise SystemExit("The public quickstart contract has an invalid Server prerelease.")
if not isinstance(image, str) or reference != f"{{image}}:{{version}}":
    raise SystemExit("The public quickstart contract has an invalid Server reference.")
print(reference)'
)"'''


@dataclass(frozen=True)
class ReleaseIdentity:
    package: str
    version: str
    registry_version: str
    server_version: str

    @property
    def exact_requirement(self) -> str:
        return f"{self.package}=={self.registry_version}"

@dataclass(frozen=True)
class QualifiedOnboarding:
    sdk_version: str
    sdk_registry_version: str
    server_version: str
    server_reference: str


def normalize_registry_version(version: str) -> str:
    match = re.fullmatch(
        r"(?P<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-(?P<label>[A-Za-z]+)\.(?P<number>[0-9]+))?",
        version,
    )
    if match is None:
        raise ValueError(f"Unsupported SDK release version: {version}")
    label = match.group("label")
    if label is None:
        return match.group("core")
    return f"{match.group('core')}{label.lower()}{match.group('number')}"


def load_release_identity(repo_root: Path) -> ReleaseIdentity:
    with (repo_root / "pyproject.toml").open("rb") as file:
        manifest = tomllib.load(file)

    project = manifest["project"]
    release = manifest["tool"]["durable-workflow"]
    identity = ReleaseIdentity(
        package=project["name"],
        version=project["version"],
        registry_version=release["registry-version"],
        server_version=release["supported-server-versions"],
    )

    if identity.package != "durable-workflow":
        raise ValueError(f"Unexpected package name: {identity.package}")
    if release["product-train"] != identity.version:
        raise ValueError("project.version and tool.durable-workflow.product-train must match")
    if normalize_registry_version(identity.version) != identity.registry_version:
        raise ValueError("registry-version must be the PEP 440 form of project.version")
    if "-" not in identity.version:
        raise ValueError("API-reference release identity must remain a prerelease before the 2.0 stable cutover")

    return identity


def render_onboarding_resolvers(markdown: str) -> str:
    """Render machine-owned versionless onboarding commands."""
    if SERVER_IMAGE_RESOLVER_TOKEN not in markdown:
        raise ValueError("API-reference template is missing the Server image resolver token")

    rendered = markdown.replace(SERVER_IMAGE_RESOLVER_TOKEN, SUPPORTED_SERVER_IMAGE_RESOLVER_COMMAND)
    if re.search(r"{{\s*durable_workflow_[a-z_]+\s*}}", rendered):
        raise ValueError("API-reference template contains an unresolved onboarding token")
    return rendered


def load_qualified_onboarding(payload: object) -> QualifiedOnboarding:
    """Validate the public contract fields used by Python onboarding."""
    if not isinstance(payload, dict) or payload.get("schema") != QUICKSTART_CONTRACT_SCHEMA:
        raise ValueError("Public quickstart contract has an unsupported schema")

    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("Public quickstart contract does not contain artifacts")
    sdk = artifacts.get("sdk-python")
    server = artifacts.get("server")
    if not isinstance(sdk, dict) or not isinstance(server, dict):
        raise ValueError("Public quickstart contract does not contain the Python SDK/Server pair")

    sdk_version = sdk.get("version")
    server_version = server.get("version")
    server_image = server.get("image")
    server_reference = server.get("reference")
    prerelease = re.compile(r"[0-9]+\.[0-9]+\.[0-9]+-(?:alpha|beta|rc)\.[0-9]+")
    if not isinstance(sdk_version, str) or prerelease.fullmatch(sdk_version) is None:
        raise ValueError("Public quickstart contract has an invalid Python SDK prerelease")
    if not isinstance(server_version, str) or prerelease.fullmatch(server_version) is None:
        raise ValueError("Public quickstart contract has an invalid Server prerelease")
    if not isinstance(server_image, str) or not server_image:
        raise ValueError("Public quickstart contract has an invalid Server image")
    if server_reference != f"{server_image}:{server_version}":
        raise ValueError("Public quickstart contract Server reference does not match its image and version")

    return QualifiedOnboarding(
        sdk_version=sdk_version,
        sdk_registry_version=normalize_registry_version(sdk_version),
        server_version=server_version,
        server_reference=server_reference,
    )


def release_evidence(identity: ReleaseIdentity, source_revision: str) -> dict[str, Any]:
    """Build the public record that binds a deployed page to its release source."""
    if re.fullmatch(r"[0-9a-f]{40}", source_revision) is None:
        raise ValueError("API-reference source revision must be an exact Git object ID")

    return {
        "schema": RELEASE_EVIDENCE_SCHEMA,
        "source_revision": source_revision,
        "pypi_version": identity.registry_version,
        "install_command": SUPPORTED_PRERELEASE_INSTALL_COMMAND,
        "artifact_versions": {
            "sdk-python": identity.version,
            "server": identity.server_version,
        },
    }


def write_release_evidence(site: Path, identity: ReleaseIdentity, source_revision: str) -> Path:
    """Write the release record into a built API-reference site."""
    path = site / RELEASE_EVIDENCE_FILENAME
    path.write_text(
        f"{json.dumps(release_evidence(identity, source_revision), indent=2)}\n",
        encoding="utf-8",
    )
    return path


def validate_release_evidence(
    evidence: object,
    identity: ReleaseIdentity,
    source_revision: str,
) -> dict[str, Any]:
    """Require a deployed release record to match the manifest-derived tuple."""
    expected = release_evidence(identity, source_revision)
    if evidence != expected:
        raise ValueError("Deployed API-reference release evidence does not match the expected release tuple")
    return expected
