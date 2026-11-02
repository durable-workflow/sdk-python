"""Release compatibility helpers derived from shipped source."""

from __future__ import annotations

import ast
import re

PROTOCOL_VERSION_PATTERN = re.compile(r"(?P<major>0|[1-9][0-9]*)\.(?P<minor>0|[1-9][0-9]*)")
STABLE_SERVER_VERSION_PATTERN = re.compile(r"([1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")


class CompatibilityContractError(ValueError):
    """Shipped runtime compatibility metadata is invalid."""


def validate_server_release_baselines(legacy: str, baselines: object) -> dict[str, str]:
    """Validate the complete additive map before any consumer selects a major."""

    legacy_match = STABLE_SERVER_VERSION_PATTERN.fullmatch(legacy)
    if legacy_match is None:
        raise CompatibilityContractError("legacy Server baseline must be one canonical stable release")
    if not isinstance(baselines, dict) or not baselines:
        raise CompatibilityContractError("Server release baselines must be a non-empty major-to-release map")
    validated: dict[str, str] = {}
    for major, minimum in baselines.items():
        if not isinstance(major, str) or re.fullmatch(r"[1-9][0-9]*", major) is None:
            raise CompatibilityContractError("Server baseline major keys must be canonical positive integers")
        match = STABLE_SERVER_VERSION_PATTERN.fullmatch(minimum) if isinstance(minimum, str) else None
        if match is None or match.group(1) != major:
            raise CompatibilityContractError("each Server baseline must be a stable release of its keyed major")
        validated[major] = minimum
    if validated.get(legacy_match.group(1)) != legacy:
        raise CompatibilityContractError("Server release baselines contradict or omit the legacy baseline")
    return validated


def declared_runtime_protocol_version(source: str) -> str:
    """Read the literal protocol authority shipped in ``client.py``."""

    try:
        module = ast.parse(source)
    except SyntaxError as error:
        raise CompatibilityContractError("runtime protocol source is not valid Python") from error

    declarations: list[str] = []
    for node in module.body:
        value: ast.expr | None = None
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "PROTOCOL_VERSION" for target in node.targets)
            or (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == "PROTOCOL_VERSION"
            )
        ):
            value = node.value
        if value is not None and isinstance(value, ast.Constant) and isinstance(value.value, str):
            declarations.append(value.value)

    if len(declarations) != 1:
        raise CompatibilityContractError("runtime source must declare one literal PROTOCOL_VERSION")
    if PROTOCOL_VERSION_PATTERN.fullmatch(declarations[0]) is None:
        raise CompatibilityContractError("runtime PROTOCOL_VERSION must be a canonical major.minor version")
    return declarations[0]
