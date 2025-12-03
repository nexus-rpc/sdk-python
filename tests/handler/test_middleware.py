import concurrent.futures
import logging
import uuid
from typing import Any

import pytest

from nexusrpc import LazyValue
from nexusrpc.handler import (
    CancelOperationContext,
    Handler,
    MiddlewareSafeOperationHandler,
    OperationContext,
    OperationHandler,
    OperationHandlerMiddleware,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
    operation_handler,
    service_handler,
    sync_operation,
)
from tests.helpers import DummySerializer, TestOperationTaskCancellation

_operation_results: dict[str, int] = {}

logger = logging.getLogger()


class MyAsyncOperationHandler(OperationHandler[int, int]):
    async def start(
        self, ctx: StartOperationContext, input: int
    ) -> StartOperationResultAsync:
        token = str(uuid.uuid4())
        _operation_results[token] = input + 1
        return StartOperationResultAsync(token)

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        del _operation_results[token]


@service_handler
class MyService:
    @operation_handler
    def incr(self) -> OperationHandler[int, int]:
        return MyAsyncOperationHandler()


@service_handler
class MyServiceSync:
    @sync_operation
    def incr(self, ctx: StartOperationContext, input: int) -> int:  # type: ignore[reportUnusedParameter]
        return input + 1


class CountingMiddleware(OperationHandlerMiddleware):
    def __init__(self) -> None:
        self.num_start = 0
        self.num_cancel = 0

    def intercept(
        self, ctx: OperationContext, next: MiddlewareSafeOperationHandler
    ) -> MiddlewareSafeOperationHandler:
        return CountingOperationHandler(next, self)


class CountingOperationHandler(MiddlewareSafeOperationHandler):
    """
    An :py:class:`AwaitableOperationHandler` that wraps a counting middleware
    that counts the number of calls to each handler method.
    """

    def __init__(
        self,
        next: MiddlewareSafeOperationHandler,
        middleware: CountingMiddleware,
    ) -> None:
        self._next = next
        self._middleware = middleware

    async def start(
        self, ctx: StartOperationContext, input: Any
    ) -> StartOperationResultSync[Any] | StartOperationResultAsync:
        self._middleware.num_start += 1
        return await self._next.start(ctx, input)

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        self._middleware.num_cancel += 1
        return await self._next.cancel(ctx, token)


class MustBeFirstMiddleware(OperationHandlerMiddleware):
    def __init__(self, counter: CountingMiddleware) -> None:
        self._counter = counter

    def intercept(
        self, ctx: OperationContext, next: MiddlewareSafeOperationHandler
    ) -> MiddlewareSafeOperationHandler:
        return MustBeFirstOperationHandler(next, self._counter)


class MustBeFirstOperationHandler(MiddlewareSafeOperationHandler):
    """
    An :py:class:`AwaitableOperationHandler` that wraps a counting middleware
    and asserts that the wrapped middleware has a count of 0 for each handler method
    """

    def __init__(
        self,
        next: MiddlewareSafeOperationHandler,
        counter: CountingMiddleware,
    ) -> None:
        self._next = next
        self._counter = counter

    async def start(
        self, ctx: StartOperationContext, input: Any
    ) -> StartOperationResultSync[Any] | StartOperationResultAsync:
        assert self._counter.num_start == 0
        logger.info("%s.%s: start operation", ctx.service, ctx.operation)

        result = await self._next.start(ctx, input)

        if isinstance(result, StartOperationResultAsync):
            logger.info(
                "%s.%s: start operation completed async. token=%s",
                ctx.service,
                ctx.operation,
                result.token,
            )
        else:
            logger.info(
                "%s.%s: start operation completed sync. value=%s",
                ctx.service,
                ctx.operation,
                result.value,
            )

        return result

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        assert self._counter.num_cancel == 0
        logger.info("%s.%s: cancel token=%s", ctx.service, ctx.operation, token)
        return await self._next.cancel(ctx, token)


@pytest.mark.asyncio
async def test_async_operation_middleware_applied():
    counting_middleware = CountingMiddleware()
    handler = Handler(
        user_service_handlers=[MyService()],
        middleware=[
            MustBeFirstMiddleware(counting_middleware),
            counting_middleware,
        ],
    )
    start_ctx = StartOperationContext(
        service="MyService",
        operation="incr",
        headers={},
        request_id="request_id",
        task_cancellation=TestOperationTaskCancellation(),
    )
    start_result = await handler.start_operation(
        start_ctx, LazyValue(DummySerializer(1), headers={})
    )
    assert isinstance(start_result, StartOperationResultAsync)
    assert start_result.token

    cancel_ctx = CancelOperationContext(
        service="MyService",
        operation="incr",
        headers={},
        task_cancellation=TestOperationTaskCancellation(),
    )
    await handler.cancel_operation(cancel_ctx, start_result.token)
    assert start_result.token not in _operation_results

    assert counting_middleware.num_start == 1
    assert counting_middleware.num_cancel == 1


@pytest.mark.asyncio
async def test_sync_operation_middleware_applied():
    counting_middleware = CountingMiddleware()
    handler = Handler(
        user_service_handlers=[MyServiceSync()],
        executor=concurrent.futures.ThreadPoolExecutor(),
        middleware=[
            MustBeFirstMiddleware(counting_middleware),
            counting_middleware,
        ],
    )
    start_ctx = StartOperationContext(
        service="MyServiceSync",
        operation="incr",
        headers={},
        request_id="request_id",
        task_cancellation=TestOperationTaskCancellation(),
    )
    start_result = await handler.start_operation(
        start_ctx, LazyValue(DummySerializer(1), headers={})
    )
    assert isinstance(start_result, StartOperationResultSync)
    assert start_result.value == 2

    assert counting_middleware.num_start == 1
    assert counting_middleware.num_cancel == 0
