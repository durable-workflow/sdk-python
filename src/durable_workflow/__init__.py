from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("durable-workflow")
except PackageNotFoundError:  # source checkout without installed metadata
    __version__ = "0.0.0+unknown"

from . import activity, sync, workflow
from .activity import ActivityContext, ActivityInfo
from .client import (
    Client,
    ScheduleAction,
    ScheduleBackfillResult,
    ScheduleDescription,
    ScheduleHandle,
    ScheduleList,
    ScheduleSpec,
    ScheduleTriggerResult,
    WorkflowExecution,
    WorkflowHandle,
    WorkflowList,
)
from .errors import (
    ActivityCancelled,
    ChildWorkflowFailed,
    DurableWorkflowError,
    InvalidArgument,
    NamespaceNotFound,
    NonRetryableError,
    QueryFailed,
    ScheduleAlreadyExists,
    ScheduleNotFound,
    ServerError,
    Unauthorized,
    UpdateRejected,
    WorkflowAlreadyStarted,
    WorkflowCancelled,
    WorkflowFailed,
    WorkflowNotFound,
    WorkflowTerminated,
)
from .metrics import (
    InMemoryMetrics,
    MetricsRecorder,
    NoopMetrics,
    PrometheusMetrics,
)
from .retry_policy import RetryPolicy, TransportRetryPolicy
from .worker import Worker
from .workflow import ActivityRetryPolicy, ContinueAsNew, StartChildWorkflow

__all__ = [
    "__version__",
    "ActivityCancelled",
    "ActivityContext",
    "ActivityInfo",
    "ActivityRetryPolicy",
    "ChildWorkflowFailed",
    "Client",
    "ContinueAsNew",
    "NonRetryableError",
    "ScheduleAction",
    "ScheduleAlreadyExists",
    "ScheduleBackfillResult",
    "ScheduleDescription",
    "ScheduleHandle",
    "ScheduleList",
    "ScheduleNotFound",
    "ScheduleSpec",
    "ScheduleTriggerResult",
    "StartChildWorkflow",
    "Worker",
    "WorkflowExecution",
    "WorkflowHandle",
    "WorkflowList",
    "activity",
    "sync",
    "workflow",
    "DurableWorkflowError",
    "InvalidArgument",
    "InMemoryMetrics",
    "MetricsRecorder",
    "NamespaceNotFound",
    "NoopMetrics",
    "QueryFailed",
    "PrometheusMetrics",
    "RetryPolicy",
    "ServerError",
    "TransportRetryPolicy",
    "Unauthorized",
    "UpdateRejected",
    "WorkflowAlreadyStarted",
    "WorkflowCancelled",
    "WorkflowFailed",
    "WorkflowNotFound",
    "WorkflowTerminated",
]
