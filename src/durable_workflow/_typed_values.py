"""Opt-in conversion between neutral payload values and annotated Python types.

The wire format stays the neutral Avro Value: a dataclass travels as the
string-keyed map of its fields and an ``Enum`` as its value, exactly as
``dataclasses.asdict()`` and ``.value`` would produce by hand. Decoding is
driven only by the receiver's own annotations, so no class name, tag or
pickled object ever enters durable history.

Decoding follows payload-evolution rules for rolling deploys: keys a target
dataclass does not declare are ignored, and a missing field takes its
default. A value that cannot be converted raises
:class:`~durable_workflow.errors.TypedValueError`, which names the field path
and the expected type but never the value.
"""

from __future__ import annotations

import dataclasses
import functools
import inspect
import types
from collections.abc import Callable
from enum import Enum
from typing import Any, Union, cast, get_args, get_origin, get_type_hints

from .errors import TypedValueError

_SCALARS: tuple[type, ...] = (str, int, float, bool, bytes)
_PASS_THROUGH = (Any, object, inspect.Parameter.empty)


def _type_name(annotation: Any) -> str:
    """A short, module-free name for diagnostics, such as ``list[Item]``."""
    if annotation is type(None) or annotation is None:
        return "None"
    if _is_union(annotation):
        options = get_args(annotation)
        concrete = [option for option in options if option is not type(None)]
        if len(concrete) == 1 and len(options) == 2:
            return f"Optional[{_type_name(concrete[0])}]"
        return " | ".join(_type_name(option) for option in options)
    origin = get_origin(annotation)
    if origin is not None:
        arguments = ", ".join(_type_name(argument) for argument in get_args(annotation))
        name = getattr(origin, "__name__", repr(origin).replace("typing.", ""))
        return f"{name}[{arguments}]" if arguments else name
    if isinstance(annotation, type):
        return annotation.__qualname__
    return repr(annotation).replace("typing.", "")


def _json_kind(value: Any) -> str:
    """The neutral kind of a received value, never the value itself."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, bytes):
        return "bytes"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _optional_member(annotation: Any) -> Any:
    """Return T for Optional[T]; refuse every other union as ambiguous."""
    options = get_args(annotation)
    concrete = [option for option in options if option is not type(None)]
    if len(concrete) != 1 or len(concrete) == len(options):
        raise TypeError(
            f"typed values support only Optional[T] unions; {_type_name(annotation)} is ambiguous"
        )
    return concrete[0]


def _is_union(annotation: Any) -> bool:
    return get_origin(annotation) in (Union, types.UnionType)


def _dataclass_hints(cls: type) -> dict[str, Any]:
    try:
        return get_type_hints(cls)
    except (NameError, TypeError) as error:
        raise TypeError(f"typed values cannot resolve the annotations of {cls.__qualname__}: {error}") from None


def validate_annotation(annotation: Any, where: str, _seen: frozenset[type] = frozenset()) -> None:
    """Refuse an annotation this contract cannot decode unambiguously.

    Called when a typed activity is declared or a ``result_type`` is given, so
    an unsupported type fails at definition time instead of passing a plain
    value through silently.
    """
    if annotation in _PASS_THROUGH or annotation is None or annotation is type(None):
        return
    if _is_union(annotation):
        validate_annotation(_optional_member(annotation), where, _seen)
        return
    origin = get_origin(annotation)
    if annotation is list or origin is list:
        for item in get_args(annotation):
            validate_annotation(item, where, _seen)
        return
    if annotation is dict or origin is dict:
        arguments = get_args(annotation)
        if arguments:
            if arguments[0] is not str:
                raise TypeError(f"{where}: typed maps need str keys, not {_type_name(arguments[0])}")
            validate_annotation(arguments[1], where, _seen)
        return
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        for member in annotation:
            if not isinstance(member.value, str | int | float):
                raise TypeError(
                    f"{where}: {annotation.__qualname__} members must have str, int or float values"
                )
        return
    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        if annotation in _seen:
            return
        hints = _dataclass_hints(annotation)
        for item in dataclasses.fields(annotation):
            if item.init:
                validate_annotation(hints.get(item.name, Any), f"{where}.{item.name}", _seen | {annotation})
        return
    if isinstance(annotation, type) and annotation in _SCALARS:
        return
    raise TypeError(f"{where}: typed values do not support {_type_name(annotation)}")


def decode_payload_value(value: Any, annotation: Any, path: str = "value") -> Any:
    """Decode an untrusted payload value; every refusal is a TypedValueError."""
    try:
        return from_payload_value(value, annotation, path)
    except RecursionError:
        raise TypedValueError(path, _type_name(annotation), "the value is nested too deeply") from None


def from_payload_value(value: Any, annotation: Any, path: str = "value") -> Any:
    """Return *value* converted to *annotation*, which must be validated."""
    if annotation in _PASS_THROUGH:
        return value
    if annotation is None or annotation is type(None):
        if value is not None:
            raise TypedValueError(path, "None", f"got {_json_kind(value)}")
        return None
    if _is_union(annotation):
        if value is None:
            return None
        return from_payload_value(value, _optional_member(annotation), path)
    if value is None:
        raise TypedValueError(path, _type_name(annotation), "got null")

    origin = get_origin(annotation)
    if annotation is list or origin is list:
        if not isinstance(value, list):
            raise TypedValueError(path, _type_name(annotation), f"got {_json_kind(value)}")
        (item_type,) = get_args(annotation) or (Any,)
        return [from_payload_value(item, item_type, f"{path}[{index}]") for index, item in enumerate(value)]

    if annotation is dict or origin is dict:
        if not isinstance(value, dict):
            raise TypedValueError(path, _type_name(annotation), f"got {_json_kind(value)}")
        _, item_type = get_args(annotation) or (str, Any)
        # Map keys are user data (for example email addresses), so a path names
        # an entry by its position, never by its key.
        return {
            key: from_payload_value(item, item_type, f"{path}[entry {index}]")
            for index, (key, item) in enumerate(value.items())
        }

    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return _enum_from_payload(value, annotation, path)

    if isinstance(annotation, type) and dataclasses.is_dataclass(annotation):
        return _dataclass_from_payload(value, annotation, path)

    if annotation is float and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if annotation is int and isinstance(value, bool):
        raise TypedValueError(path, "int", "got boolean")
    if isinstance(annotation, type) and annotation in _SCALARS:
        if not isinstance(value, annotation):
            raise TypedValueError(path, annotation.__qualname__, f"got {_json_kind(value)}")
        return value
    raise TypeError(f"{path}: typed values do not support {_type_name(annotation)}")


def _enum_from_payload(value: Any, cls: type[Enum], path: str) -> Enum:
    if isinstance(value, cls):
        return value
    for member in cls:
        expected = member.value
        same_kind = type(expected) is type(value) or (type(expected) is float and type(value) is int)
        if same_kind and expected == value:
            return member
    # The diagnostic must not echo the received value.
    raise TypedValueError(path, cls.__qualname__, f"the {_json_kind(value)} is not a member")


def _dataclass_from_payload(value: Any, cls: type, path: str) -> Any:
    if isinstance(value, cls):
        return value
    if not isinstance(value, dict):
        raise TypedValueError(path, cls.__qualname__, f"got {_json_kind(value)}")
    hints = _dataclass_hints(cls)
    arguments: dict[str, Any] = {}
    for item in dataclasses.fields(cls):
        if not item.init:
            # Computed fields are derived again by the dataclass itself.
            continue
        field_path = f"{path}.{item.name}"
        annotation = hints.get(item.name, Any)
        if item.name in value:
            arguments[item.name] = from_payload_value(value[item.name], annotation, field_path)
        elif item.default is dataclasses.MISSING and item.default_factory is dataclasses.MISSING:
            raise TypedValueError(field_path, _type_name(annotation), "the field is missing")
    # Keys the receiver does not declare are ignored for rolling deploys.
    try:
        return cls(**arguments)
    except ValueError:
        # A __post_init__ check refused the decoded fields. Its message can
        # carry the values, so it is not repeated here.
        raise TypedValueError(path, cls.__qualname__, "the dataclass refused the decoded fields") from None


def to_payload_value(value: Any) -> Any:
    """Return the neutral value for dataclass and ``Enum`` values, recursively.

    Every other value is returned unchanged, so the codec keeps refusing what
    it refuses today (for example datetimes and tuples). The result encodes
    to the same bytes as ``dataclasses.asdict()`` with each ``Enum`` replaced
    by its ``.value``.
    """
    if isinstance(value, Enum):
        return to_payload_value(value.value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {item.name: to_payload_value(getattr(value, item.name)) for item in dataclasses.fields(value)}
    if isinstance(value, list):
        return [to_payload_value(item) for item in value]
    if isinstance(value, dict):
        return {key: to_payload_value(item) for key, item in value.items()}
    return value


def to_payload_values(values: list[Any]) -> list[Any]:
    return [to_payload_value(value) for value in values]


def _positional_decoder(
    fn: Callable[..., Any], skip: int, where: str,
) -> Callable[[tuple[Any, ...]], list[Any]] | None:
    """Build a decoder for *fn*'s positional arguments after the first *skip*.

    Returns None when no argument needs decoding (none, or only ``Any`` /
    unannotated ones), so the caller can leave *fn* unwrapped.
    """
    signature = inspect.signature(fn)
    try:
        hints = get_type_hints(fn)
    except (NameError, TypeError) as error:
        raise TypeError(f"{where} {fn.__qualname__} has unresolvable annotations: {error}") from None
    positional: list[tuple[str, Any]] = []
    variadic: tuple[str, Any] | None = None
    for parameter in list(signature.parameters.values())[skip:]:
        annotation = hints.get(parameter.name, Any)
        if parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            validate_annotation(annotation, parameter.name)
            positional.append((parameter.name, annotation))
        elif parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            validate_annotation(annotation, parameter.name)
            variadic = (parameter.name, annotation)

    annotations = [annotation for _, annotation in positional] + ([variadic[1]] if variadic is not None else [])
    if all(annotation in _PASS_THROUGH for annotation in annotations):
        return None

    def convert(args: tuple[Any, ...]) -> list[Any]:
        converted: list[Any] = []
        for index, value in enumerate(args):
            if index < len(positional):
                name, annotation = positional[index]
                converted.append(decode_payload_value(value, annotation, name))
            elif variadic is not None:
                name, annotation = variadic
                converted.append(decode_payload_value(value, annotation, f"{name}[{index - len(positional)}]"))
            else:
                converted.append(value)
        return converted

    return convert


def typed_callable(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap an activity so positional arguments decode to its annotations.

    Annotations are validated here, so an unsupported type fails when the
    activity is declared. The wrapper keeps *fn*'s module and qualified name:
    the decorator binds that name to the wrapper, so supervised callbacks that
    pickle by reference resolve to it.
    """
    decoder = _positional_decoder(fn, 0, "typed activity")

    def convert(args: tuple[Any, ...]) -> list[Any]:
        return list(args) if decoder is None else decoder(args)

    wrapper: Any
    if inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def typed_async(*args: Any, **kwargs: Any) -> Any:
            return to_payload_value(await fn(*convert(args), **kwargs))

        wrapper = typed_async
    else:

        @functools.wraps(fn)
        def typed_sync(*args: Any, **kwargs: Any) -> Any:
            return to_payload_value(fn(*convert(args), **kwargs))

        wrapper = typed_sync
    wrapper.__typed_activity__ = fn
    return cast(Callable[..., Any], wrapper)


def typed_method(fn: Callable[..., Any], skip: int) -> Callable[..., Any]:
    """Wrap a workflow method so arguments after the first *skip* decode.

    ``skip`` is 1 for handlers (``self``) and 2 for ``run`` (``self, ctx``). A
    generator ``run`` stays a generator. Results are returned unchanged; the
    commands that carry them send dataclass and Enum values as neutral values.
    A method with nothing to decode is returned unwrapped.
    """
    convert = _positional_decoder(fn, skip, "typed workflow method")
    if convert is None:
        # Nothing to decode: keep the original function, so a handler used as a
        # wait_condition predicate keeps its recorded fingerprint.
        return fn
    wrapper: Any
    if inspect.isgeneratorfunction(fn):

        @functools.wraps(fn)
        def typed_generator(*args: Any, **kwargs: Any) -> Any:
            return (yield from fn(*args[:skip], *convert(args[skip:]), **kwargs))

        wrapper = typed_generator
    elif inspect.iscoroutinefunction(fn):

        @functools.wraps(fn)
        async def typed_async(*args: Any, **kwargs: Any) -> Any:
            return await fn(*args[:skip], *convert(args[skip:]), **kwargs)

        wrapper = typed_async
    else:

        @functools.wraps(fn)
        def typed_sync(*args: Any, **kwargs: Any) -> Any:
            return fn(*args[:skip], *convert(args[skip:]), **kwargs)

        wrapper = typed_sync
    wrapper.__typed_workflow_method__ = fn
    return cast(Callable[..., Any], wrapper)
