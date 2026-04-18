#!/usr/bin/env python3
"""Generate the public Python SDK API reference as Docusaurus Markdown."""

from __future__ import annotations

import argparse
import dataclasses
import inspect
import re
import sys
from collections.abc import Iterable
from pathlib import Path
from types import ModuleType
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))

import durable_workflow as dw  # noqa: E402
from durable_workflow import activity, serializer, sync, workflow  # noqa: E402
from durable_workflow import errors as errors_module  # noqa: E402
from durable_workflow import metrics as metrics_module  # noqa: E402
from durable_workflow import retry_policy as retry_policy_module  # noqa: E402

CLASS_SECTIONS: tuple[tuple[str, tuple[type, ...]], ...] = (
    (
        "Control Plane",
        (
            dw.Client,
            dw.WorkflowHandle,
            dw.WorkflowExecution,
            dw.WorkflowList,
        ),
    ),
    (
        "Schedules",
        (
            dw.ScheduleSpec,
            dw.ScheduleAction,
            dw.ScheduleHandle,
            dw.ScheduleDescription,
            dw.ScheduleList,
            dw.ScheduleTriggerResult,
            dw.ScheduleBackfillResult,
        ),
    ),
    (
        "Workers And Authoring",
        (
            dw.Worker,
            workflow.WorkflowContext,
            dw.ContinueAsNew,
            dw.StartChildWorkflow,
            activity.ActivityContext,
            activity.ActivityInfo,
        ),
    ),
    (
        "Synchronous Facade",
        (
            sync.Client,
            sync.SyncWorkflowHandle,
            sync.SyncScheduleHandle,
        ),
    ),
    (
        "Metrics",
        (
            dw.MetricsRecorder,
            dw.NoopMetrics,
            dw.InMemoryMetrics,
            dw.PrometheusMetrics,
        ),
    ),
    (
        "Retry Policy",
        (
            retry_policy_module.RetryPolicy,
        ),
    ),
)

FUNCTION_SECTIONS: tuple[tuple[str, ModuleType, tuple[str, ...]], ...] = (
    ("Workflow Decorators", workflow, ("defn", "registry")),
    ("Activity Decorators And Context", activity, ("defn", "context", "registry")),
    ("Payload Serialization", serializer, ("encode", "envelope", "decode_envelope", "decode")),
)

ERROR_CLASSES: tuple[type, ...] = tuple(
    getattr(errors_module, name)
    for name in (
        "DurableWorkflowError",
        "ServerError",
        "WorkflowNotFound",
        "WorkflowAlreadyStarted",
        "WorkflowFailed",
        "WorkflowCancelled",
        "WorkflowTerminated",
        "NamespaceNotFound",
        "InvalidArgument",
        "Unauthorized",
        "ScheduleNotFound",
        "ScheduleAlreadyExists",
        "QueryFailed",
        "UpdateRejected",
        "ChildWorkflowFailed",
        "ActivityCancelled",
        "NonRetryableError",
        "AvroNotInstalledError",
    )
)

METRIC_CONSTANTS = (
    metrics_module.CLIENT_REQUESTS,
    metrics_module.CLIENT_REQUEST_DURATION_SECONDS,
    metrics_module.WORKER_POLLS,
    metrics_module.WORKER_POLL_DURATION_SECONDS,
    metrics_module.WORKER_TASKS,
    metrics_module.WORKER_TASK_DURATION_SECONDS,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="Write Markdown to this path instead of stdout.",
    )
    args = parser.parse_args()

    markdown = render_document()
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(markdown, encoding="utf-8")
    else:
        print(markdown)
    return 0


def render_document() -> str:
    lines: list[str] = [
        "---",
        "sidebar_position: 4",
        "title: Python API Reference",
        "---",
        "",
        "# Python SDK API Reference",
        "",
        "This page is generated from the SDK's public symbols and docstrings by "
        "`repos/sdk-python/scripts/generate-api-reference.py`. Do not edit it by hand.",
        "",
        "The SDK is async-first. Use `durable_workflow.Client` and `durable_workflow.Worker` "
        "for deployed code, or `durable_workflow.sync.Client` for scripts that cannot "
        "manage an event loop.",
        "",
    ]

    lines.extend(render_package_exports())
    for title, classes in CLASS_SECTIONS:
        lines.extend(render_class_section(title, classes))
    for title, module, names in FUNCTION_SECTIONS:
        lines.extend(render_function_section(title, module, names))
    lines.extend(render_errors())
    lines.extend(render_metrics())
    lines.append("")
    return "\n".join(lines)


def render_package_exports() -> list[str]:
    exports = ", ".join(f"`{name}`" for name in sorted(dw.__all__))
    return [
        "## Package Exports",
        "",
        "`from durable_workflow import ...` exposes:",
        "",
        exports,
        "",
    ]


def render_class_section(title: str, classes: Iterable[type]) -> list[str]:
    lines = [f"## {title}", ""]
    for cls in classes:
        lines.extend(render_class(cls))
    return lines


def render_class(cls: type) -> list[str]:
    lines = [f"### `{qualified_name(cls)}`", ""]
    constructor = constructor_signature(cls)
    if constructor is not None:
        lines.extend(["```python", constructor, "```", ""])

    doc = own_doc(cls)
    if doc:
        lines.extend([doc, ""])

    if dataclasses.is_dataclass(cls):
        lines.extend(render_dataclass_fields(cls))

    methods = public_methods(cls)
    if methods:
        lines.extend(["#### Methods", ""])
        for name, member in methods:
            lines.extend(render_callable(name, member, method=True))
    return lines


def render_dataclass_fields(cls: type) -> list[str]:
    lines = ["#### Fields", "", "| Field | Type | Default |", "|---|---|---|"]
    for field in dataclasses.fields(cls):
        annotation = type_name(field.type)
        default = field_default(field)
        lines.append(f"| `{field.name}` | `{annotation}` | {default} |")
    lines.append("")
    return lines


def render_function_section(title: str, module: ModuleType, names: Iterable[str]) -> list[str]:
    lines = [f"## {title}", ""]
    for name in names:
        member = getattr(module, name)
        lines.extend(render_callable(name, member, method=False, module=module))
    return lines


def render_callable(
    name: str,
    member: Any,
    *,
    method: bool,
    module: ModuleType | None = None,
) -> list[str]:
    label = name if module is None else f"{module.__name__}.{name}"
    signature = callable_signature(name, member, method=method)
    lines = [f"##### `{label}`", "", "```python", signature, "```", ""]
    doc = clean_doc(inspect.getdoc(member))
    if doc:
        lines.extend([doc, ""])
    return lines


def render_errors() -> list[str]:
    lines = ["## Exceptions", ""]
    for cls in ERROR_CLASSES:
        lines.append(f"### `{qualified_name(cls)}`")
        lines.append("")
        constructor = constructor_signature(cls)
        if constructor is not None:
            lines.extend(["```python", constructor, "```", ""])
        doc = own_doc(cls)
        if doc:
            lines.extend([doc, ""])
        if cls.__base__ is not object:
            lines.extend([f"Inherits from `{qualified_name(cls.__base__)}`.", ""])
    return lines


def render_metrics() -> list[str]:
    return [
        "## Metric Names",
        "",
        "| Constant Value |",
        "|---|",
        *[f"| `{name}` |" for name in METRIC_CONSTANTS],
        "",
    ]


def constructor_signature(cls: type) -> str | None:
    init = getattr(cls, "__init__", None)
    if init is None or init is object.__init__:
        return None
    sig = without_self(signature(init))
    return compact_signature(f"{cls.__name__}{sig}")


def callable_signature(name: str, member: Any, *, method: bool) -> str:
    sig = signature(member)
    if method:
        sig = without_self(sig)
    prefix = "async " if inspect.iscoroutinefunction(member) else ""
    return compact_signature(f"{prefix}{name}{sig}")


def signature(member: Any) -> inspect.Signature:
    try:
        return inspect.signature(member, eval_str=True)
    except (NameError, TypeError):
        return inspect.signature(member)


def compact_signature(value: str) -> str:
    return (
        value
        .replace("typing.", "")
        .replace("collections.abc.", "")
        .replace("durable_workflow.client.", "")
        .replace("durable_workflow.worker.", "")
        .replace("durable_workflow.workflow.", "")
        .replace("durable_workflow.activity.", "")
        .replace("durable_workflow.metrics.", "")
        .replace("durable_workflow.retry_policy.", "")
    )


def without_self(sig: inspect.Signature) -> inspect.Signature:
    params = list(sig.parameters.values())
    if params and params[0].name in {"self", "cls"}:
        params = params[1:]
    return sig.replace(parameters=params)


def public_methods(cls: type) -> list[tuple[str, Any]]:
    methods: list[tuple[str, Any]] = []
    for name, member in inspect.getmembers(cls):
        if name.startswith("_"):
            continue
        raw = inspect.getattr_static(cls, name)
        if isinstance(raw, property):
            methods.append((name, raw.fget))
            continue
        if inspect.isfunction(member) or inspect.iscoroutinefunction(member):
            methods.append((name, member))
    return methods


def qualified_name(obj: Any) -> str:
    module = getattr(obj, "__module__", "")
    name = getattr(obj, "__qualname__", getattr(obj, "__name__", str(obj)))
    if module.startswith("durable_workflow"):
        return f"{module}.{name}"
    return name


def clean_doc(doc: str | None) -> str:
    if not doc:
        return ""
    cleaned = inspect.cleandoc(doc)
    cleaned = re.sub(r":(?:class|func|meth|mod):`~?([^`]+)`", r"`\1`", cleaned)
    return cleaned.replace("``", "`")


def own_doc(obj: Any) -> str:
    return clean_doc(getattr(obj, "__doc__", None))


def field_default(field: dataclasses.Field[Any]) -> str:
    if field.default is not dataclasses.MISSING:
        return f"`{field.default!r}`"
    if field.default_factory is not dataclasses.MISSING:
        factory = field.default_factory  # type: ignore[misc]
        name = getattr(factory, "__name__", repr(factory))
        return f"`{name}()`"
    return "required"


def type_name(annotation: Any) -> str:
    if isinstance(annotation, str):
        return annotation
    name = getattr(annotation, "__name__", None)
    if name:
        return name
    return str(annotation).replace("typing.", "")


if __name__ == "__main__":
    raise SystemExit(main())
