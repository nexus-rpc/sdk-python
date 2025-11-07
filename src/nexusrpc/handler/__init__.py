"""
Components for implementing Nexus handlers.

Server/worker authors will use this module to create the top-level Nexus handlers
responsible for dispatching requests to Nexus operations.

Nexus service/operation authors will use this module to implement operation handler
methods within service handler classes.
"""

from __future__ import annotations

from ._common import (
    CancelOperationContext,
    OperationContext,
    OperationTaskCancellation,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)
from ._core import Handler, OperationHandlerInterceptor
from ._decorators import operation_handler, service_handler, sync_operation
from ._operation_handler import AwaitableOperationHandler, OperationHandler

__all__ = [
    "AwaitableOperationHandler",
    "CancelOperationContext",
    "Handler",
    "OperationContext",
    "OperationHandler",
    "OperationTaskCancellation",
    "OperationHandlerInterceptor",
    "operation_handler",
    "service_handler",
    "StartOperationContext",
    "StartOperationResultAsync",
    "StartOperationResultSync",
    "sync_operation",
    "operation_handler",
]
