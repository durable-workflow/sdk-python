from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, Protocol, TypeAlias

MetricTags: TypeAlias = Mapping[str, str]
MetricKey: TypeAlias = tuple[str, tuple[tuple[str, str], ...]]

CLIENT_REQUESTS = "durable_workflow_client_requests"
CLIENT_REQUEST_DURATION_SECONDS = "durable_workflow_client_request_duration_seconds"
WORKER_POLLS = "durable_workflow_worker_polls"
WORKER_POLL_DURATION_SECONDS = "durable_workflow_worker_poll_duration_seconds"
WORKER_TASKS = "durable_workflow_worker_tasks"
WORKER_TASK_DURATION_SECONDS = "durable_workflow_worker_task_duration_seconds"


class MetricsRecorder(Protocol):
    """Pluggable counter and histogram recorder used by the client and worker."""

    def increment(self, name: str, value: float = 1.0, tags: MetricTags | None = None) -> None:
        """Increment a counter metric."""

    def record(self, name: str, value: float, tags: MetricTags | None = None) -> None:
        """Record a histogram/sample metric."""


class NoopMetrics:
    """Default metrics recorder that intentionally drops all observations."""

    def increment(self, name: str, value: float = 1.0, tags: MetricTags | None = None) -> None:
        pass

    def record(self, name: str, value: float, tags: MetricTags | None = None) -> None:
        pass


NOOP_METRICS = NoopMetrics()


def _metric_key(name: str, tags: MetricTags | None) -> MetricKey:
    return name, tuple(sorted((str(k), str(v)) for k, v in (tags or {}).items()))


@dataclass
class InMemoryMetrics:
    """Simple recorder useful for tests and custom exporter loops."""

    counters: dict[MetricKey, float] = field(default_factory=dict)
    histograms: dict[MetricKey, list[float]] = field(default_factory=dict)

    def increment(self, name: str, value: float = 1.0, tags: MetricTags | None = None) -> None:
        key = _metric_key(name, tags)
        self.counters[key] = self.counters.get(key, 0.0) + value

    def record(self, name: str, value: float, tags: MetricTags | None = None) -> None:
        self.histograms.setdefault(_metric_key(name, tags), []).append(value)

    def counter_value(self, name: str, tags: MetricTags | None = None) -> float:
        return self.counters.get(_metric_key(name, tags), 0.0)

    def observations(self, name: str, tags: MetricTags | None = None) -> list[float]:
        return list(self.histograms.get(_metric_key(name, tags), []))


class PrometheusMetrics:
    """Metrics recorder backed by the optional prometheus-client package."""

    def __init__(self, *, registry: Any | None = None) -> None:
        try:
            prometheus_client = import_module("prometheus_client")
        except ImportError as exc:
            raise RuntimeError(
                "PrometheusMetrics requires prometheus-client. "
                "Install it with `pip install durable-workflow[prometheus]`."
            ) from exc

        self._counter_cls: Any = prometheus_client.Counter
        self._histogram_cls: Any = prometheus_client.Histogram
        self._registry = registry
        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}
        self._label_names: dict[tuple[str, str], tuple[str, ...]] = {}

    def increment(self, name: str, value: float = 1.0, tags: MetricTags | None = None) -> None:
        tag_values = dict(_metric_key(name, tags)[1])
        counter = self._metric("counter", name, tuple(tag_values))
        if tag_values:
            counter.labels(**tag_values).inc(value)
        else:
            counter.inc(value)

    def record(self, name: str, value: float, tags: MetricTags | None = None) -> None:
        tag_values = dict(_metric_key(name, tags)[1])
        histogram = self._metric("histogram", name, tuple(tag_values))
        if tag_values:
            histogram.labels(**tag_values).observe(value)
        else:
            histogram.observe(value)

    def _metric(self, kind: str, name: str, label_names: tuple[str, ...]) -> Any:
        key = (kind, name)
        existing = self._label_names.get(key)
        if existing is not None and existing != label_names:
            raise ValueError(
                f"metric {name!r} was already registered as a {kind} "
                f"with labels {existing!r}; got {label_names!r}"
            )
        self._label_names[key] = label_names

        store = self._counters if kind == "counter" else self._histograms
        if name in store:
            return store[name]

        kwargs: dict[str, Any] = {}
        if self._registry is not None:
            kwargs["registry"] = self._registry
        metric_cls = self._counter_cls if kind == "counter" else self._histogram_cls
        metric = metric_cls(name, f"{name} {kind}", label_names, **kwargs)
        store[name] = metric
        return metric
