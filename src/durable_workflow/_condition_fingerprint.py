"""Stable identity for ``wait_condition`` predicates (#258).

The Server records the fingerprint a worker sends with ``open_condition_wait``
and replay compares it, so the value must be the same on every Python version
and in every process. Version 2 hashes the predicate's module, qualified name
and a canonical form of its source AST. It never hashes bytecode, constant
reprs, memory addresses, file paths or line numbers.

Version 1 (every release through 2.14.0) hashed bytecode and ``repr`` of code
constants, which change across Python versions and, for nested functions,
across processes. Recorded v1 values cannot be recomputed portably, so replay
treats a v1 value that differs from this process's v1 as unverifiable rather
than as a changed predicate (see :func:`fingerprint_verdict`).
"""

from __future__ import annotations

import ast
import hashlib
import inspect
import linecache
import re
from collections.abc import Callable
from typing import Any, Literal

V2_PREFIX = "dwpy2:sha256:"
_LEGACY = re.compile(r"sha256:[0-9a-f]{64}\Z")
# Fields that carry no predicate meaning or differ between Python versions.
_SKIPPED_FIELDS = frozenset({"ctx", "kind", "type_comment", "type_params", "decorator_list", "returns"})
_parsed: dict[tuple[str, int], ast.Module | None] = {}

Verdict = Literal["match", "mismatch", "unverifiable"]


def legacy_fingerprint(predicate: Callable[[], bool]) -> str:
    """The v1 fingerprint, kept only to recognise histories recorded by 2.14.0 and earlier."""
    h = hashlib.sha256()
    h.update(b"durable-workflow-python.wait-condition.v1\0")
    h.update(f"{getattr(predicate, '__module__', '')}\0".encode())
    h.update(f"{getattr(predicate, '__qualname__', '')}\0".encode())
    original = getattr(predicate, "__typed_workflow_method__", None)
    code = getattr(original if original is not None else predicate, "__code__", None)
    if code is None:
        h.update(repr(predicate).encode())
    else:
        h.update(repr((
            code.co_argcount, code.co_posonlyargcount, code.co_kwonlyargcount, code.co_code,
            code.co_consts, code.co_names, code.co_varnames, code.co_freevars,
        )).encode())
    return f"sha256:{h.hexdigest()}"


def is_legacy(value: Any) -> bool:
    return isinstance(value, str) and _LEGACY.match(value) is not None


def fingerprint(predicate: Callable[[], bool]) -> str | None:
    """Return the v2 fingerprint, or None when the predicate's source is unavailable.

    Without a fingerprint the wait is identified by its key and call order,
    as in sdk-php.
    """
    function = _authored_function(predicate)
    if function is None:
        return None
    node = _predicate_node(function)
    if node is None:
        return None
    h = hashlib.sha256()
    h.update(b"durable-workflow-python.wait-condition.v2\0")
    h.update(f"{getattr(function, '__module__', '')}\0".encode())
    h.update(f"{getattr(function, '__qualname__', '')}\0".encode())
    h.update(node.encode())
    return f"{V2_PREFIX}{h.hexdigest()}"


def fingerprint_verdict(recorded: Any, authored: str | None, predicate: Callable[[], bool]) -> Verdict:
    """Compare a recorded fingerprint with the authored predicate.

    - No recorded value: nothing to compare.
    - An identity supplied explicitly in another format (not v2, not None)
      is compared exactly, as before.
    - A v2 value is compared exactly; without source it cannot be checked.
    - A v1 value equal to this process's v1 matches. Any other v1 value is
      unverifiable, because v1 differs across Python versions and processes.
    - Any other string is compared exactly, as before.
    """
    if not isinstance(recorded, str):
        return "match"
    if authored is not None and not authored.startswith(V2_PREFIX):
        return "match" if recorded == authored else "mismatch"
    if recorded.startswith(V2_PREFIX):
        if authored is None:
            return "unverifiable"
        return "match" if recorded == authored else "mismatch"
    if is_legacy(recorded):
        return "match" if recorded == legacy_fingerprint(predicate) else "unverifiable"
    return "match" if recorded == authored else "mismatch"


def _authored_function(predicate: Any) -> Any:
    target = getattr(predicate, "__typed_workflow_method__", None) or predicate
    if inspect.ismethod(target):
        target = target.__func__
    target = getattr(target, "__typed_workflow_method__", None) or target
    return target if inspect.isfunction(target) else None


def _module_tree(function: Any) -> ast.Module | None:
    try:
        filename = inspect.getsourcefile(function) or inspect.getfile(function)
    except (OSError, TypeError):
        return None
    lines = linecache.getlines(filename, function.__globals__)
    if not lines:
        return None
    source = "".join(lines)
    key = (filename, hash(source))
    if key not in _parsed:
        try:
            _parsed[key] = ast.parse(source)
        except (SyntaxError, ValueError):
            _parsed[key] = None
    return _parsed[key]


def _predicate_node(function: Any) -> str | None:
    tree = _module_tree(function)
    if tree is None:
        return None
    code = function.__code__
    first_line = code.co_firstlineno
    if function.__name__ == "<lambda>":
        lambdas = [node for node in ast.walk(tree) if isinstance(node, ast.Lambda) and node.lineno == first_line]
        lambdas.sort(key=lambda node: node.col_offset)
        if not lambdas:
            return None
        # Lambdas that start on the same line share one fingerprint covering
        # all of them. Telling them apart needs column data that Python 3.10
        # lacks, and the fingerprint must be the same on every version.
        return "|".join(_canonical(node) for node in lambdas)
    definitions = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == function.__name__
        and min([node.lineno, *(decorator.lineno for decorator in node.decorator_list)]) == first_line
    ]
    if len(definitions) != 1:
        return None
    return _canonical(definitions[0])


def _canonical(value: Any) -> str:
    if isinstance(value, ast.AST):
        parts = [type(value).__name__]
        for name in value._fields:
            if name in _SKIPPED_FIELDS:
                continue
            item = getattr(value, name, None)
            if item is None or item == []:
                continue
            parts.append(f"{name}={_canonical(item)}")
        return "(" + " ".join(parts) + ")"
    if isinstance(value, list):
        return "[" + ",".join(_canonical(item) for item in value) + "]"
    if value is None or value is Ellipsis or isinstance(value, str | bytes | int | float | complex | bool):
        return f"{type(value).__name__}:{value!r}"
    return type(value).__name__
