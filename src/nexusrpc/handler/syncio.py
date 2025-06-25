from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    Awaitable,
    Callable,
    Optional,
    Union,
)

from nexusrpc import OperationInfo
from nexusrpc.handler import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    OperationHandler,
    StartOperationContext,
    StartOperationResultSync,
)
from nexusrpc.handler._util import is_async_callable
from nexusrpc.types import InputT, OutputT


class SyncOperationHandler(OperationHandler[InputT, OutputT], ABC):
    """
    An :py:class:`OperationHandler` that is limited to responding synchronously.

    This version of the class uses traditional `def` methods, instead of `async def`.
    For the async version, see :py:class:`nexusrpc.handler.SyncOperationHandler`.
    """

    def __init__(
        self,
        start: Optional[Callable[[StartOperationContext, InputT], OutputT]] = None,
    ):
        if start is not None:
            if is_async_callable(start):
                raise RuntimeError(
                    f"{start} is an `async def` method. "
                    "syncio.SyncOperationHandler must be initialized with a `def` method. "
                    "To use `async def` methods, see :py:class:`nexusrpc.handler.SyncOperationHandler`."
                )
            self._start = start
            if start.__doc__:
                self.start.__func__.__doc__ = start.__doc__
        else:
            self._start = None

    @classmethod
    def from_callable(
        cls,
        start: Callable[[StartOperationContext, InputT], OutputT],
    ) -> SyncOperationHandler[InputT, OutputT]:
        return _SyncOperationHandler(start)

    @abstractmethod
    def start(
        self, ctx: StartOperationContext, input: InputT
    ) -> StartOperationResultSync[OutputT]:
        """
        The name 'SyncOperationHandler' means that it responds synchronously in the
        sense that the start method delivers the final operation result as its return
        value, rather than returning an operation token representing an in-progress
        operation. This version of the class uses `def` methods. For the `async def`
        version, see :py:class:`nexusrpc.handler.SyncOperationHandler`.
        """
        ...

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


class _SyncOperationHandler(SyncOperationHandler[InputT, OutputT]):
    def start(
        self, ctx: StartOperationContext, input: InputT
    ) -> StartOperationResultSync[OutputT]:
        if self._start is None:
            raise RuntimeError(
                "Do not use _SyncOperationHandler directly. "
                "Use SyncOperationHandler.from_callable instead."
            )
        output = self._start(ctx, input)
        return StartOperationResultSync(output)
