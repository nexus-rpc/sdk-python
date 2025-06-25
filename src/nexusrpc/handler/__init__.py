# TODO(preview): show what it looks like to manually build a service implementation at runtime
# where the operations may be based on some runtime information.

# TODO(preview): pass pyright strict mode "python.analysis.typeCheckingMode": "strict"
# TODO(preview): pass mypy

# TODO(prerelease): docstrings
# TODO(prerelease): check API docs


from __future__ import annotations

from ._common import (
    CancelOperationContext as CancelOperationContext,
    FetchOperationInfoContext as FetchOperationInfoContext,
    FetchOperationResultContext as FetchOperationResultContext,
    HandlerError as HandlerError,
    HandlerErrorType as HandlerErrorType,
    OperationContext as OperationContext,
    OperationError as OperationError,
    OperationErrorState as OperationErrorState,
    StartOperationContext as StartOperationContext,
    StartOperationResultAsync as StartOperationResultAsync,
    StartOperationResultSync as StartOperationResultSync,
)
from ._core import (
    Handler as Handler,
    SyncioHandler as SyncioHandler,
)
from ._decorators import (
    operation_handler as operation_handler,
    service_handler as service_handler,
)
from ._operation_handler import (
    OperationHandler as OperationHandler,
    SyncOperationHandler as SyncOperationHandler,
)
