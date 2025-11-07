import logging
from typing import Any
import uuid

import pytest

from nexusrpc import LazyValue
from nexusrpc.handler import (
    CancelOperationResult,
    CancelOperationContext,
    Handler,
    OperationHandler,
    OperationHandlerInterceptor,
    StartOperationResult,
    StartOperationContext,
    StartOperationResultAsync,
    service_handler,
)
from nexusrpc.handler._common import StartOperationResultSync
from nexusrpc.handler._decorators import operation_handler
from nexusrpc.handler._operation_handler import InterceptedOperationHandler
from tests.helpers import DummySerializer

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


class MyInterceptor(OperationHandlerInterceptor):
    def intercept_operation_handler(
        self, next: InterceptedOperationHandler[Any, Any]
    ) -> InterceptedOperationHandler[Any, Any]:
        return LoggingOperationHandler(next)


class LoggingOperationHandler(InterceptedOperationHandler[Any, Any]):
    def __init__(self, next: InterceptedOperationHandler[Any, Any]) -> None:
        self._next = next

    async def start(
        self, ctx: StartOperationContext, input: Any
    ) -> StartOperationResultSync[Any] | StartOperationResultAsync:
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
        logger.info("%s.%s: cancel token=%s", ctx.service, ctx.operation, token)
        return await self._next.cancel(ctx, token)


@pytest.mark.asyncio
async def test_async_operation_happy_path():
    handler = Handler(
        user_service_handlers=[MyService()], interceptors=[MyInterceptor()]
    )
    start_ctx = StartOperationContext(
        service="MyService",
        operation="incr",
        headers={},
        request_id="request_id",
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
    )
    await handler.cancel_operation(cancel_ctx, start_result.token)
    assert start_result.token not in _operation_results
