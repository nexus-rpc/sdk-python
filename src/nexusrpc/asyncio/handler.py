# TODO(preview): show what it looks like to manually build a service implementation at runtime
# where the operations may be based on some runtime information.

# TODO(preview): pass pyright strict mode "python.analysis.typeCheckingMode": "strict"
# TODO(preview): pass mypy


from __future__ import annotations

import inspect
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
    A Nexus handler with `async def` methods.

    A Nexus handler manages a collection of Nexus service handlers.

    Operation requests are delegated to a :py:class:`ServiceHandler` based on the service
    name in the operation context.
    """

    async def start_operation(
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
        deserialized_input = await input.consume(as_type=op.input_type)

        if is_async_callable(op_handler.start):
            # TODO(preview): apply middleware stack as composed awaitables
            return await op_handler.start(ctx, deserialized_input)
        else:
            # TODO(preview): apply middleware stack as composed functions
            if not self.sync_executor:
                raise RuntimeError(
                    "Operation start handler method is not an `async def` but "
                    "no sync executor was provided to the Handler constructor. "
                )
            result = await self.sync_executor.submit_to_event_loop(
                op_handler.start, ctx, deserialized_input
            )
            if inspect.isawaitable(result):
                raise RuntimeError(
                    f"Operation start handler method {op_handler.start} returned an "
                    "awaitable but is not an `async def` coroutine function."
                )
            return result

    async def cancel_operation(self, ctx: CancelOperationContext, token: str) -> None:
        """Handle a Cancel Operation request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        if is_async_callable(op_handler.cancel):
            return await op_handler.cancel(ctx, token)
        else:
            if not self.sync_executor:
                raise RuntimeError(
                    "Operation cancel handler method is not an `async def` function but "
                    "no executor was provided to the Handler constructor."
                )
            result = await self.sync_executor.submit_to_event_loop(
                op_handler.cancel, ctx, token
            )
            if inspect.isawaitable(result):
                raise RuntimeError(
                    f"Operation cancel handler method {op_handler.cancel} returned an "
                    "awaitable but is not an `async def` function."
                )
            return result

    async def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> OperationInfo:
        raise NotImplementedError

    async def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Any:
        raise NotImplementedError
