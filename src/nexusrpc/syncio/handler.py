from __future__ import annotations

from typing import (
    Any,
    Callable,
    Optional,
    Union,
    overload,
)

import nexusrpc
from nexusrpc import InputT, OperationInfo, OutputT
from nexusrpc._serializer import LazyValueT
from nexusrpc._types import ServiceHandlerT
from nexusrpc._util import (
    get_callable_name,
    is_async_callable,
    set_operation_definition,
    set_operation_factory,
)
from nexusrpc.handler._common import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)
from nexusrpc.handler._core import BaseServiceCollectionHandler
from nexusrpc.handler._util import get_start_method_input_and_output_type_annotations

from ..handler._operation_handler import OperationHandler


class Handler(BaseServiceCollectionHandler):
    """
    A Nexus handler with non-async `def` methods.

    A Nexus handler manages a collection of Nexus service handlers.

    Operation requests are delegated to a :py:class:`ServiceHandler` based on the service
    name in the operation context.

    This class uses `def` methods. For `async def` methods, see :py:class:`nexusrpc.handler.Handler`.
    """

    def start_operation(
        self,
        ctx: StartOperationContext,
        input: LazyValueT,
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
        if not self.executor:
            raise RuntimeError(
                "Operation start handler method is not an `async def` but "
                "no executor was provided to the Handler constructor. "
            )
        return self.executor.submit(op_handler.start, ctx, deserialized_input).result()

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
            if not self.executor:
                raise RuntimeError(
                    "Operation cancel handler method is not an `async def` function but "
                    "no executor was provided to the Handler constructor."
                )
            return self.executor.submit(op_handler.cancel, ctx, token).result()

    def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> OperationInfo:
        raise NotImplementedError

    def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Any:
        raise NotImplementedError


class SyncOperationHandler(OperationHandler[InputT, OutputT]):
    """
    An :py:class:`nexusrpc.handler.OperationHandler` that is limited to responding synchronously.

    The name 'SyncOperationHandler' means that it responds synchronously, in the
    sense that the start method delivers the final operation result as its return
    value, rather than returning an operation token representing an in-progress
    operation.

    This version of the class uses `def` methods. For the async version, see
    :py:class:`nexusrpc.handler.SyncOperationHandler`.
    """

    def __init__(self, start: Callable[[StartOperationContext, InputT], OutputT]):
        if is_async_callable(start):
            raise RuntimeError(
                f"{start} is an `async def` method. "
                "SyncOperationHandler must be initialized with a `def` method. "
                "To use `async def` methods, use nexusrpc.handler.SyncOperationHandler."
            )
        self._start = start
        if start.__doc__:
            if start_func := getattr(self.start, "__func__", None):
                start_func.__doc__ = start.__doc__

    def start(
        self, ctx: StartOperationContext, input: InputT
    ) -> StartOperationResultSync[OutputT]:
        """
        Start the operation and return its final result synchronously.
        """
        return StartOperationResultSync(self._start(ctx, input))

    def fetch_info(self, ctx: FetchOperationInfoContext, token: str) -> OperationInfo:
        raise NotImplementedError(
            "Cannot fetch operation info for an operation that responded synchronously."
        )

    def fetch_result(self, ctx: FetchOperationResultContext, token: str) -> OutputT:
        raise NotImplementedError(
            "Cannot fetch the result of an operation that responded synchronously."
        )

    def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        raise NotImplementedError(
            "An operation that responded synchronously cannot be cancelled."
        )


@overload
def sync_operation(
    start: Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT],
) -> Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT]: ...


@overload
def sync_operation(
    *,
    name: Optional[str] = None,
) -> Callable[
    [Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT]],
    Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT],
]: ...


def sync_operation(
    start: Optional[
        Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT]
    ] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT],
    Callable[
        [Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT]],
        Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT],
    ],
]:
    """
    Decorator marking a method as the start method for a synchronous operation.
    """
    if is_async_callable(start):
        raise TypeError(
            "syncio sync_operation decorator must be used on a `def` operation method"
        )

    def decorator(
        start: Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT],
    ) -> Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT]:
        def operation_handler_factory(
            self: ServiceHandlerT,
        ) -> OperationHandler[InputT, OutputT]:
            def _start(ctx: StartOperationContext, input: InputT) -> OutputT:
                return start(self, ctx, input)

            _start.__doc__ = start.__doc__
            return SyncOperationHandler(_start)

        input_type, output_type = get_start_method_input_and_output_type_annotations(
            start
        )

        method_name = get_callable_name(start)
        set_operation_definition(
            operation_handler_factory,
            nexusrpc.Operation(
                name=name or method_name,
                method_name=method_name,
                input_type=input_type,
                output_type=output_type,
            ),
        )

        set_operation_factory(start, operation_handler_factory)
        return start

    if start is None:
        return decorator

    return decorator(start)
