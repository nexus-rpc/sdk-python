"""
Components for implementing Nexus handlers.

Server/worker authors will use this module to create the top-level Nexus handlers
responsible for dispatching requests to Nexus operations.

Nexus service/operation authors will use this module to implement operation handler
methods within service handler classes.
"""
# TODO(preview): pass pyright strict mode "python.analysis.typeCheckingMode": "strict"
# TODO(preview): pass mypy

from __future__ import annotations

from ._common import (
    CancelOperationContext as CancelOperationContext,
    FetchOperationInfoContext as FetchOperationInfoContext,
    FetchOperationResultContext as FetchOperationResultContext,
    OperationContext as OperationContext,
    StartOperationContext as StartOperationContext,
    StartOperationResultAsync as StartOperationResultAsync,
    StartOperationResultSync as StartOperationResultSync,
)
from ._core import Handler as Handler
from ._decorators import (
    service_handler as service_handler,
    sync_operation as sync_operation,
)
from ._operation_handler import OperationHandler as OperationHandler
from ._util import (
    get_start_method_input_and_output_type_annotations as get_start_method_input_and_output_type_annotations,
)

__all__ = [
    "CancelOperationContext",
    "FetchOperationInfoContext",
    "FetchOperationResultContext",
    "get_start_method_input_and_output_type_annotations",
    "Handler",
    "OperationContext",
    "OperationHandler",
    "service_handler",
    "StartOperationContext",
    "StartOperationResultAsync",
    "StartOperationResultSync",
    "sync_operation",
]
