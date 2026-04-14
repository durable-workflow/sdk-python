"""Activity decorator and registry."""
from __future__ import annotations

from typing import Callable


_REGISTRY: dict[str, Callable] = {}


def defn(*, name: str):
    def wrap(fn: Callable) -> Callable:
        fn.__activity_name__ = name
        _REGISTRY[name] = fn
        return fn

    return wrap


def registry() -> dict[str, Callable]:
    return dict(_REGISTRY)
