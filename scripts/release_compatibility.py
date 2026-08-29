"""Semantic release-compatibility contracts shared by publication surfaces."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from html.parser import HTMLParser

PROTOCOL_VERSION_PATTERN = re.compile(r"(?P<major>0|[1-9][0-9]*)\.(?P<minor>0|[1-9][0-9]*)")
SDK_RELEASE_IDENTITY = "public-quickstart-contract:artifacts.sdk-python.version"
SERVER_RELEASE_IDENTITY = "public-quickstart-contract:artifacts.server.version"


class CompatibilityContractError(ValueError):
    """A release-facing compatibility surface differs from runtime authority."""


@dataclass(frozen=True)
class WorkerProtocolRequirement:
    """The Server worker-protocol interval accepted by an SDK release."""

    minimum_inclusive: str
    maximum_exclusive: str

    def expression(self) -> str:
        return f">={self.minimum_inclusive},<{self.maximum_exclusive}"


def _protocol_parts(value: str, label: str) -> tuple[int, int]:
    match = PROTOCOL_VERSION_PATTERN.fullmatch(value)
    if match is None:
        raise CompatibilityContractError(f"{label} must be a canonical major.minor protocol version")
    return int(match.group("major")), int(match.group("minor"))


def worker_protocol_requirement(worker_protocol_version: str) -> WorkerProtocolRequirement:
    """Derive the Server interval implemented by the worker compatibility check."""

    major, _minor = _protocol_parts(worker_protocol_version, "worker protocol version")
    return WorkerProtocolRequirement(
        minimum_inclusive=worker_protocol_version,
        maximum_exclusive=f"{major + 1}.0",
    )


def parse_worker_protocol_requirement(value: str) -> WorkerProtocolRequirement:
    """Parse a semantic interval without depending on surrounding prose or layout."""

    constraints: dict[str, str] = {}
    for token in value.split(","):
        match = re.fullmatch(r"\s*(>=|<)\s*([^\s,]+)\s*", token)
        if match is None or match.group(1) in constraints:
            raise CompatibilityContractError("worker protocol guidance must contain one >= and one < constraint")
        constraints[match.group(1)] = match.group(2)
    if set(constraints) != {">=", "<"}:
        raise CompatibilityContractError("worker protocol guidance must contain one >= and one < constraint")

    _protocol_parts(constraints[">="], "worker protocol minimum")
    _protocol_parts(constraints["<"], "worker protocol maximum")
    return WorkerProtocolRequirement(
        minimum_inclusive=constraints[">="],
        maximum_exclusive=constraints["<"],
    )


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
    _protocol_parts(declarations[0], "runtime PROTOCOL_VERSION")
    return declarations[0]


class _ReadmeCompatibilityParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.markers: list[dict[str, str]] = []
        self.protocol_expressions: list[str] = []
        self._container_depth = 0
        self._protocol_depth: int | None = None
        self._protocol_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del tag
        attributes = {key: value or "" for key, value in attrs}
        if attributes.get("data-durable-workflow-compatibility") == "worker-protocol":
            if self._container_depth:
                raise CompatibilityContractError("README compatibility markers must not be nested")
            self.markers.append(attributes)
            self._container_depth = 1
        elif self._container_depth:
            self._container_depth += 1

        if self._container_depth and attributes.get("data-compatibility-role") == "server-worker-protocols":
            if self._protocol_depth is not None:
                raise CompatibilityContractError("README worker protocol guidance must not be nested")
            self._protocol_depth = self._container_depth
            self._protocol_parts = []

    def handle_data(self, data: str) -> None:
        if self._protocol_depth is not None:
            self._protocol_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        del tag
        if not self._container_depth:
            return
        if self._protocol_depth == self._container_depth:
            self.protocol_expressions.append("".join(self._protocol_parts).strip())
            self._protocol_depth = None
            self._protocol_parts = []
        self._container_depth -= 1


def validate_readme_compatibility(
    readme: str,
    worker_protocol_version: str,
) -> WorkerProtocolRequirement:
    """Validate structured, visible README guidance against runtime semantics."""

    expected = worker_protocol_requirement(worker_protocol_version)
    parser = _ReadmeCompatibilityParser()
    parser.feed(readme)
    parser.close()

    if len(parser.markers) != 1:
        raise CompatibilityContractError("README must contain one structured worker protocol compatibility marker")
    marker = parser.markers[0]
    if marker.get("data-sdk-release-identity") != SDK_RELEASE_IDENTITY:
        raise CompatibilityContractError("README compatibility guidance must identify the SDK release authority")
    if marker.get("data-qualified-server-release-identity") != SERVER_RELEASE_IDENTITY:
        raise CompatibilityContractError(
            "README compatibility guidance must identify the qualified Server release authority"
        )

    structured = WorkerProtocolRequirement(
        minimum_inclusive=marker.get("data-minimum-inclusive", ""),
        maximum_exclusive=marker.get("data-maximum-exclusive", ""),
    )
    _protocol_parts(structured.minimum_inclusive, "README worker protocol minimum")
    _protocol_parts(structured.maximum_exclusive, "README worker protocol maximum")
    if structured != expected:
        raise CompatibilityContractError(
            f"README worker protocol contract {structured.expression()} differs from runtime {expected.expression()}"
        )

    if len(parser.protocol_expressions) != 1:
        raise CompatibilityContractError("README must display one structured worker protocol requirement")
    displayed = parse_worker_protocol_requirement(parser.protocol_expressions[0])
    if displayed != structured:
        raise CompatibilityContractError(
            "README visible worker protocol requirement differs from its structured compatibility contract"
        )
    return expected
