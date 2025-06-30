from __future__ import annotations

import concurrent.futures
from typing import (
    Any,
    Callable,
    Optional,
    Sequence,
    Union,
    overload,
)

from typing_extensions import TypeGuard

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

from ...handler._operation_handler import OperationHandler


class Handler(BaseServiceCollectionHandler):
    """
    A Nexus handler with non-async `def` methods.

    This class does not support user operation handlers that are `async def` methods.
    For a Handler class with `async def` methods that supports `async def` and `def`
    user operation handlers, see :py:class:`nexusrpc.handler.Handler`.

    A Nexus handler manages a collection of Nexus service handlers.

    Operation requests are dispatched to a :py:class:`ServiceHandler` based on the
    service name in the operation context.

    Example:
        .. code-block:: python

            import concurrent.futures
            import nexusrpc.syncio.handler

            # Create service handler instances with sync operations
            my_service = MySyncServiceHandler()

            # Create executor for running sync operations
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)

            # Create syncio handler (requires executor)
            handler = nexusrpc.syncio.handler.Handler([my_service], executor=executor)

            # Use handler to process requests (methods are non-async)
            result = handler.start_operation(ctx, input_lazy_value)

    """

    executor: concurrent.futures.Executor  # type: ignore[assignment]

    def __init__(
        self,
        user_service_handlers: Sequence[Any],
        executor: concurrent.futures.Executor,
    ):
        super().__init__(user_service_handlers, executor)
        self._validate_all_operation_handlers_are_sync()
        if not self.executor:
            raise RuntimeError("A syncio Handler must be initialized with an executor.")

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
        assert self._assert_not_async_callable(op_handler.start)
        # TODO(preview): apply middleware stack
        return self.executor.submit(op_handler.start, ctx, deserialized_input).result()

    def cancel_operation(self, ctx: CancelOperationContext, token: str) -> None:
        """Handle a Cancel Operation request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        assert self._assert_not_async_callable(op_handler.cancel)
        if not self.executor:
            raise RuntimeError(
                "Operation cancel handler method is not an `async def` method but "
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

    def _validate_all_operation_handlers_are_sync(self) -> None:
        for service_handler in self.service_handlers.values():
            for op_handler in service_handler.operation_handlers.values():
                self._assert_not_async_callable(op_handler.start)
                self._assert_not_async_callable(op_handler.cancel)
                self._assert_not_async_callable(op_handler.fetch_info)
                self._assert_not_async_callable(op_handler.fetch_result)

    def _assert_not_async_callable(
        self, method: Callable[..., Any]
    ) -> TypeGuard[Callable[..., Any]]:
        if is_async_callable(method):
            raise RuntimeError(
                f"Operation handler method {method} is an `async def` method, "
                "but you are using nexusrpc.syncio.handler.Handler, "
                "which is for `def` methods. Use nexusrpc.handler.Handler instead."
            )
        return True


# TODO(prerelease): should not be exported
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

    This is the synchronous I/O version using `def` methods.

    Example:
        .. code-block:: python

            import requests
            from nexusrpc.handler import service_handler
            from nexusrpc.syncio.handler import sync_operation

            @service_handler
            class MySyncServiceHandler:
                @sync_operation
                def process_data(
                    self, ctx: StartOperationContext, input: str
                ) -> str:
                    # You can use synchronous I/O libraries
                    response = requests.get("https://api.example.com/data")
                    data = response.json()
                    return f"Processed: {data}"
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
