from __future__ import annotations

from typing import (
    Awaitable,
    Callable,
    Union,
)

from nexusrpc.handler import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    OperationHandler,
    OperationInfo,
    StartOperationContext,
    StartOperationResultSync,
)
from nexusrpc.handler._util import is_async_callable
from nexusrpc.types import InputT, OutputT


class SyncOperationHandler(OperationHandler[InputT, OutputT]):
    """
    An :py:class:`OperationHandler` that is limited to responding synchronously.

    This version of the class uses traditional `def` methods, instead of `async def`.
    For the async version, see :py:class:`nexusrpc.handler.SyncOperationHandler`.
    """

    def __init__(
        self,
        start_method: Callable[[StartOperationContext, InputT], OutputT],
    ):
        if is_async_callable(start_method):
            raise RuntimeError(
                f"{start_method} is an `async def` method. "
                "syncio.SyncOperationHandler must be initialized with a `def` method. "
                "To use `async def` methods, see :py:class:`nexusrpc.handler.SyncOperationHandler`."
            )
        self.start_method = start_method
        if start_method.__doc__:
            self.start.__func__.__doc__ = start_method.__doc__

    def start(
        self, ctx: StartOperationContext, input: InputT
    ) -> StartOperationResultSync[OutputT]:
        """
        The name 'SyncOperationHandler' means that it responds synchronously in the
        sense that the start method delivers the final operation result as its return
        value, rather than returning an operation token representing an in-progress
        operation. This version of the class uses `async def` methods. For the syncio
        version, see :py:class:`nexusrpc.handler.syncio.SyncOperationHandler`.
        """
        output = self.start_method(ctx, input)
        return StartOperationResultSync(output)

    def fetch_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]:
        raise NotImplementedError(
            "Cannot fetch operation info for an operation that responded synchronously."
        )

    def fetch_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Union[OutputT, Awaitable[OutputT]]:
        raise NotImplementedError(
            "Cannot fetch the result of an operation that responded synchronously."
        )

    def cancel(
        self, ctx: CancelOperationContext, token: str
    ) -> Union[None, Awaitable[None]]:
        raise NotImplementedError(
            "An operation that responded synchronously cannot be cancelled."
        )
