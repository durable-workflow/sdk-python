"""Convert neutral payload values to annotated Python types, and back.

The wire format stays the neutral Avro Value map: dataclasses become string
keyed maps and enums become their value. Decoding is driven only by the
receiver's own annotations, so no class names enter durable history.

Decoding follows payload-evolution rules: keys the target dataclass does not
declare are ignored, and missing fields take their defaults. A value that
cannot be converted raises :class:`TypedValueError` naming the field path.
"""
from __future__ import annotations

import dataclasses
import inspect
import types
from collections.abc import Callable, Mapping, Sequence
from enum import Enum
from typing import Any, Union, get_args, get_origin, get_type_hints

from .serializer import to_avro_payload_value

_MISSING = object()


class TypedValueError(TypeError):
    """A payload value does not match the receiver's declared type."""

    def __init__(self, path: str, expected: str, detail: str) -> None:
        self.path = path
        self.expected = expected
        super().__init__(f"{path}: expected {expected}; {detail}")


def _type_name(annotation: Any) -> str:
    if isinstance(annotation, type):
        return annotation.__qualname__
    return repr(annotation).replace("typing.", "")


def _describe(value: Any) -> str:
    return f"got {type(value).__name__}"


def from_payload_value(value: Any, annotation: Any, path: str = "value") -> Any:
    """Return *value* converted to *annotation*.

    Unannotated (``Any``) and plain scalar targets pass values through
    unchanged, so existing untyped behavior is preserved.
    """
    if annotation is Any or annotation is inspect.Parameter.empty or annotation is object:
        return value

    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        options = get_args(annotation)
        if value is None:
            if type(None) in options:
                return None
            raise TypedValueError(path, _type_name(annotation), "got null")
        concrete = [option for option in options if option is not type(None)]
        if len(concrete) == 1:
            return from_payload_value(value, concrete[0], path)
        errors: list[str] = []
        for option in concrete:
            try:
                return from_payload_value(value, option, path)
            except TypedValueError as error:
                errors.append(str(error))
        raise TypedValueError(path, _type_name(annotation), "no union member matched")

    if origin in (list, Sequence) or annotation is list:
        if not isinstance(value, list):
            raise TypedValueError(path, _type_name(annotation), _describe(value))
        (item_type,) = get_args(annotation) or (Any,)
        return [from_payload_value(item, item_type, f"{path}[{index}]") for index, item in enumerate(value)]

    if origin in (dict, Mapping) or annotation is dict:
        if not isinstance(value, dict):
            raise TypedValueError(path, _type_name(annotation), _describe(value))
        key_type, item_type = get_args(annotation) or (str, Any)
        if key_type is not str:
            raise TypedValueError(path, _type_name(annotation), "only str map keys are supported")
        return {key: from_payload_value(item, item_type, f"{path}[{key!r}]") for key, item in value.items()}

    if isinstance(annotation, type) and issubclass(annotation, Enum):
        if isinstance(value, annotation):
            return value
        try:
            return annotation(value)
        except ValueError:
            raise TypedValueError(path, _type_name(annotation), f"{value!r} is not a member") from None

    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        return _dataclass_from_payload(value, annotation, path)

    if annotation is float and isinstance(value, int) and not isinstance(value, bool):
        return float(value)

    if isinstance(annotation, type) and annotation in (str, int, float, bool, bytes, type(None)):
        if annotation is int and isinstance(value, bool):
            raise TypedValueError(path, "int", _describe(value))
        if not isinstance(value, annotation):
            raise TypedValueError(path, _type_name(annotation), _describe(value))
        return value

    # Other annotations (TypeVars, Protocols, Literal, ...) are not part of
    # this contract; keep the neutral value rather than guessing.
    return value


def _dataclass_from_payload(value: Any, cls: type, path: str) -> Any:
    if isinstance(value, cls):
        return value
    if not isinstance(value, dict):
        raise TypedValueError(path, cls.__qualname__, _describe(value))
    try:
        hints = get_type_hints(cls)
    except (NameError, TypeError) as error:
        raise TypedValueError(path, cls.__qualname__, f"annotations cannot be resolved: {error}") from None
    arguments: dict[str, Any] = {}
    deferred: dict[str, Any] = {}
    for field in dataclasses.fields(cls):  # type: ignore[arg-type]
        field_path = f"{path}.{field.name}"
        if field.name in value:
            converted = from_payload_value(value[field.name], hints.get(field.name, Any), field_path)
            if field.init:
                arguments[field.name] = converted
            else:
                deferred[field.name] = converted
            continue
        has_default = (
            field.default is not dataclasses.MISSING
            or field.default_factory is not dataclasses.MISSING
        )
        if field.init and not has_default:
            raise TypedValueError(field_path, _type_name(hints.get(field.name, Any)), "field is missing")
    # Keys the receiver does not declare are ignored for rolling deploys.
    instance = cls(**arguments)
    for name, converted in deferred.items():
        object.__setattr__(instance, name, converted)
    return instance


def to_payload_value(value: Any) -> Any:
    """Return the neutral payload value for a typed Python value."""
    return to_avro_payload_value(value)


def typed_callable(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap *fn* so positional arguments decode to its annotations.

    The wrapper keeps *fn*'s module and qualified name, so supervised
    callbacks that pickle by reference resolve to the wrapper itself.
    """
    import functools

    signature = inspect.signature(fn)
    try:
        hints = get_type_hints(fn)
    except (NameError, TypeError) as error:
        raise TypeError(f"typed activity {fn.__qualname__} has unresolvable annotations: {error}") from None
    parameters = [
        parameter for parameter in signature.parameters.values()
        if parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    variadic = next(
        (parameter for parameter in signature.parameters.values()
         if parameter.kind is inspect.Parameter.VAR_POSITIONAL),
        None,
    )

    def convert(args: tuple[Any, ...]) -> list[Any]:
        converted: list[Any] = []
        for index, value in enumerate(args):
            if index < len(parameters):
                name = parameters[index].name
                converted.append(from_payload_value(value, hints.get(name, Any), name))
            elif variadic is not None:
                converted.append(
                    from_payload_value(value, hints.get(variadic.name, Any), f"{variadic.name}[{index}]")
                )
            else:
                converted.append(value)
        return converted

    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def typed_async(*args: Any, **kwargs: Any) -> Any:
            return to_payload_value(await fn(*convert(args), **kwargs))

        wrapper: Callable[..., Any] = typed_async
    else:
        @functools.wraps(fn)
        def typed_sync(*args: Any, **kwargs: Any) -> Any:
            return to_payload_value(fn(*convert(args), **kwargs))

        wrapper = typed_sync
    wrapper.__typed_activity__ = fn  # type: ignore[attr-defined]
    return wrapper
