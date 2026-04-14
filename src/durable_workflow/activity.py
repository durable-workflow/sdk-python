"""Activity decorator and registry."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

_REGISTRY: dict[str, Callable[..., Any]] = {}


def defn(*, name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
        fn.__activity_name__ = name  # type: ignore[attr-defined]
        _REGISTRY[name] = fn
        return fn

    return wrap


def registry() -> dict[str, Callable[..., Any]]:
    return dict(_REGISTRY)
