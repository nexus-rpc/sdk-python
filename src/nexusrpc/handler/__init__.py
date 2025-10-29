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
from ._core import Handler as Handler
from ._decorators import operation_handler, service_handler, sync_operation
from ._operation_handler import OperationHandler as OperationHandler

__all__ = [
    "CancelOperationContext",
    "Handler",
    "OperationContext",
    "OperationHandler",
    "OperationTaskCancellation",
    "service_handler",
    "StartOperationContext",
    "StartOperationResultAsync",
    "StartOperationResultSync",
    "sync_operation",
    "operation_handler",
]
