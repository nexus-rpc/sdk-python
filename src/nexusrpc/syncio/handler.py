from __future__ import annotations

from typing import (
    Any,
    Union,
)

import nexusrpc.handler
from nexusrpc.handler import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    OperationInfo,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)
from nexusrpc.handler._util import is_async_callable


class Handler(nexusrpc.handler.BaseHandler):
    """
    A Nexus handler with non-async `def` methods.

    A Nexus handler manages a collection of Nexus service handlers.

    Operation requests are delegated to a :py:class:`ServiceHandler` based on the service
    name in the operation context.
    """

    def start_operation(
        self,
        ctx: StartOperationContext,
        input: nexusrpc.handler.LazyValue,
    ) -> Union[
        StartOperationResultSync[Any],
        StartOperationResultAsync,
    ]:
        """Handle a Start Operation request.

        Args:
            ctx: The operation context.
            input: The input to the operation, as a LazyValue.
        """
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        op = service_handler.service.operations[ctx.operation]
        deserialized_input = input.consume(as_type=op.input_type)

        if is_async_callable(op_handler.start):
            raise RuntimeError(
                "Operation start handler method is an `async def` and "
                "cannot be called from a sync handler. "
            )
        # TODO(preview): apply middleware stack as composed functions
        if not self.sync_executor:
            raise RuntimeError(
                "Operation start handler method is not an `async def` but "
                "no sync executor was provided to the Handler constructor. "
            )
        return self.sync_executor.submit(
            op_handler.start, ctx, deserialized_input
        ).result()

    def cancel_operation(self, ctx: CancelOperationContext, token: str) -> None:
        """Handle a Cancel Operation request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        if is_async_callable(op_handler.cancel):
            raise RuntimeError(
                "Operation cancel handler method is an `async def` and "
                "cannot be called from a sync handler. "
            )
        else:
            if not self.sync_executor:
                raise RuntimeError(
                    "Operation cancel handler method is not an `async def` function but "
                    "no executor was provided to the Handler constructor."
                )
            return self.sync_executor.submit(op_handler.cancel, ctx, token).result()

    def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> OperationInfo:
        raise NotImplementedError

    def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Any:
        raise NotImplementedError
