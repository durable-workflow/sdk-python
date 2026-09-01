"""Release compatibility helpers derived from shipped source."""

from __future__ import annotations

import ast
import re

PROTOCOL_VERSION_PATTERN = re.compile(r"(?P<major>0|[1-9][0-9]*)\.(?P<minor>0|[1-9][0-9]*)")


class CompatibilityContractError(ValueError):
    """Shipped runtime compatibility metadata is invalid."""


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
