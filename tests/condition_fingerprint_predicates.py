"""Predicates whose v2 fingerprints are pinned in condition_fingerprints_v2.json (#258).

The golden values were computed on the official python:3.12-slim image. Every
CI Python version must compute the same values, in this process and in a fresh
one, so a wait recorded on one version or process replays on any other.
Keep this module's source unchanged: editing it changes the fingerprints.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any

from durable_workflow import workflow


def _identity(predicate: Callable[[], bool]) -> Callable[[], bool]:
    return predicate


def _typed_method_fixture(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Exercise the unwrapping marker without the unreleased typed workflow API."""
    @wraps(fn)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        return fn(*args, **kwargs)

    wrapped.__typed_workflow_method__ = fn  # type: ignore[attr-defined]
    return wrapped


@workflow.defn(name="tests.fingerprint.typed-ready")
class TypedReady:
    def __init__(self) -> None:
        self.approvals: list[str] = []

    @workflow.query("ready")
    @_typed_method_fixture
    def ready(self, minimum: int = 2) -> bool:
        return len(self.approvals) >= minimum

    def run(self, ctx: workflow.WorkflowContext) -> None:
        return None


class Approvals:
    def __init__(self) -> None:
        self.items: list[int] = [1, 20]
        self.approved = False
        self.name = "x"

    def is_done(self) -> bool:
        return self.approved

    def has_large(self) -> bool:
        return any(n > 10 for n in self.items)

    def _multiline(self) -> Callable[[], bool]:
        return _identity(
            lambda: (
                self.approved
                and len(self.items) > 0
            )
        )

    def _same_line(self) -> tuple[Callable[[], bool], Callable[[], bool]]:
        return (lambda: self.approved), (lambda: not self.approved)

    def _inner(self) -> Callable[[], bool]:
        def ready() -> bool:
            return self.approved and bool(self.items)

        return ready

    # Python 3.12 inlines list, dict and set comprehensions (PEP 709), so the
    # comprehension disappears from these lambdas' __qualname__.
    def _in_listcomp(self) -> Callable[[], bool]:
        return [lambda: self.approved for _ in range(1)][0]

    def _in_dictcomp(self) -> Callable[[], bool]:
        return {key: lambda: bool(self.items) for key in ["only"]}["only"]

    def _in_setcomp(self) -> Callable[[], bool]:
        return next(iter({lambda: len(self.items) > 1 for _ in range(1)}))

    def _same_line_in_listcomp(self) -> list[Callable[[], bool]]:
        return [(lambda: value > 0) if value else (lambda: not value) for value in [1, 0]]  # noqa: B023

    def _same_line_listcomp_bodies(self) -> tuple[Callable[[], bool], Callable[[], bool]]:
        return (lambda: any([n > 0 for n in self.items])), (lambda: any([n < 0 for n in self.items]))

    def _same_line_dictcomp_bodies(self) -> tuple[Callable[[], bool], Callable[[], bool]]:
        items = self.items
        return (lambda: any({n: n > 0 for n in items}.values())), (lambda: any({n: n < 0 for n in items}.values()))

    def _same_line_setcomp_bodies(self) -> tuple[Callable[[], bool], Callable[[], bool]]:
        return (lambda: any({n > 0 for n in self.items})), (lambda: any({n < 0 for n in self.items}))

    def predicates(self) -> dict[str, Any]:
        first, second = self._same_line()
        positive, negative = self._same_line_in_listcomp()
        list_first, list_second = self._same_line_listcomp_bodies()
        dict_first, dict_second = self._same_line_dictcomp_bodies()
        set_first, set_second = self._same_line_setcomp_bodies()
        return {
            "lambda": lambda: self.approved,
            "genexpr": lambda: any(n > 10 for n in self.items),
            "listcomp": lambda: len([n for n in self.items if n]) > 2,
            "nested_lambda": lambda: (lambda value: value > 3)(len(self.items)),
            "fstring": lambda: f"{self.name}-{len(self.items)}" == "x-1",
            "multiline_in_call": self._multiline(),
            "same_line_first": first,
            "same_line_second": second,
            "bound_method": self.is_done,
            "bound_genexpr_method": self.has_large,
            "inner_def": self._inner(),
            "typed_wrapper": TypedReady().ready,
            "lambda_in_listcomp": self._in_listcomp(),
            "lambda_in_dictcomp": self._in_dictcomp(),
            "lambda_in_setcomp": self._in_setcomp(),
            "same_line_in_listcomp_first": positive,
            "same_line_in_listcomp_second": negative,
            "same_line_listcomp_body_first": list_first,
            "same_line_listcomp_body_second": list_second,
            "same_line_dictcomp_body_first": dict_first,
            "same_line_dictcomp_body_second": dict_second,
            "same_line_setcomp_body_first": set_first,
            "same_line_setcomp_body_second": set_second,
        }
