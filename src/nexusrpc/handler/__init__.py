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
    OperationContext as OperationContext,
    StartOperationContext as StartOperationContext,
    StartOperationResultAsync as StartOperationResultAsync,
    StartOperationResultSync as StartOperationResultSync,
)
from ._core import (
    Handler as Handler,
    SyncioHandler as SyncioHandler,
)
from ._decorators import (
    service_handler as service_handler,
    sync_operation as sync_operation,
)
from ._operation_handler import OperationHandler as OperationHandler
from ._util import (
    get_start_method_input_and_output_type_annotations as get_start_method_input_and_output_type_annotations,
)
