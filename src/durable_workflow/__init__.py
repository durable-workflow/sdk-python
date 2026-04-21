from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("durable-workflow")
except PackageNotFoundError:  # source checkout without installed metadata
    __version__ = "0.0.0+unknown"

from . import activity, sync, testing, workflow
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
    WorkflowPayloadDecodeError,
    WorkflowTerminated,
)
from .interceptors import (
    ActivityHandler,
    ActivityInterceptorContext,
    PassthroughWorkerInterceptor,
    QueryTaskHandler,
    QueryTaskInterceptorContext,
    WorkerInterceptor,
    WorkflowTaskHandler,
    WorkflowTaskInterceptorContext,
)
from .metrics import (
    InMemoryMetrics,
    MetricsRecorder,
    NoopMetrics,
    PrometheusMetrics,
)
from .retry_policy import RetryPolicy, TransportRetryPolicy
from .serializer import (
    PayloadSizeWarningConfig,
    PayloadSizeWarningContext,
    to_avro_payload_value,
    to_avro_payload_values,
)
from .worker import Worker
from .workflow import ActivityRetryPolicy, ChildWorkflowRetryPolicy, ContinueAsNew, StartChildWorkflow

__all__ = [
    "__version__",
    "ActivityCancelled",
    "ActivityContext",
    "ActivityHandler",
    "ActivityInfo",
    "ActivityInterceptorContext",
    "ActivityRetryPolicy",
    "ChildWorkflowRetryPolicy",
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
    "WorkerInterceptor",
    "WorkflowExecution",
    "WorkflowTaskHandler",
    "WorkflowTaskInterceptorContext",
    "WorkflowHandle",
    "WorkflowList",
    "WorkflowPayloadDecodeError",
    "activity",
    "sync",
    "testing",
    "workflow",
    "DurableWorkflowError",
    "InvalidArgument",
    "InMemoryMetrics",
    "MetricsRecorder",
    "NamespaceNotFound",
    "NoopMetrics",
    "QueryFailed",
    "PrometheusMetrics",
    "PayloadSizeWarningConfig",
    "PayloadSizeWarningContext",
    "PassthroughWorkerInterceptor",
    "QueryTaskHandler",
    "QueryTaskInterceptorContext",
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
    "to_avro_payload_value",
    "to_avro_payload_values",
]
