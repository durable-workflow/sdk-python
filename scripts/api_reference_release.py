"""Release identity used to render the Python API reference."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib  # type: ignore[import-not-found]
except ModuleNotFoundError:  # pragma: no cover - exercised by the Python 3.10 CI cell
    import tomli as tomllib  # type: ignore[import-not-found]


SDK_VERSION_TOKEN = "{{ durable_workflow_sdk_version }}"
SERVER_VERSION_TOKEN = "{{ durable_workflow_server_version }}"


@dataclass(frozen=True)
class ReleaseIdentity:
    package: str
    version: str
    registry_version: str
    server_version: str

    @property
    def requirement(self) -> str:
        return f"{self.package}=={self.version}"

    @property
    def install_command(self) -> str:
        return f"pip install {self.requirement}"


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


def render_release_identity(markdown: str, identity: ReleaseIdentity) -> str:
    """Replace machine-owned release tokens without coupling tests to prose."""
    missing = [token for token in (SDK_VERSION_TOKEN, SERVER_VERSION_TOKEN) if token not in markdown]
    if missing:
        raise ValueError(f"API-reference release template is missing tokens: {', '.join(missing)}")

    rendered = markdown.replace(SDK_VERSION_TOKEN, identity.version).replace(
        SERVER_VERSION_TOKEN,
        identity.server_version,
    )
    if re.search(r"{{\s*durable_workflow_(?:sdk|server)_version\s*}}", rendered):
        raise ValueError("API-reference release template contains an unresolved release token")
    return rendered
