import asyncio

import pytest

from nexusrpc import LazyValue
from nexusrpc.handler import (
    CancelOperationContext,
    Handler,
    OperationHandler,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
    operation_handler,
    service_handler,
    sync_operation,
)
from tests.helpers import DummySerializer, TestOperationTaskCancellation


class CancellableAsyncOperationHandler(OperationHandler[None, None]):
    async def start(
        self, ctx: StartOperationContext, input: None
    ) -> StartOperationResultAsync:
        try:
            await asyncio.wait_for(
                ctx.task_cancellation.wait_until_cancelled(), timeout=1
            )
        except TimeoutError as err:
            raise RuntimeError("Expected cancellation") from err

        details = ctx.task_cancellation.cancellation_reason()
        if not details:
            raise RuntimeError("Expected cancellation details")

        # normally you return a token but for this test
        # we use the token to indicate success by returning the expected
        # cancellation details
        return StartOperationResultAsync(details)

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        pass


@service_handler
class MyService:
    @operation_handler
    def cancellable_async(self) -> OperationHandler[None, None]:
        return CancellableAsyncOperationHandler()

    @sync_operation
    async def cancellable_sync(self, ctx: StartOperationContext, _input: None) -> str:
        cancelled = ctx.task_cancellation.wait_until_cancelled_sync(1)
        if not cancelled:
            raise RuntimeError("Expected cancellation")

        details = ctx.task_cancellation.cancellation_reason()
        if not details:
            raise RuntimeError("Expected cancellation details")

        return details


@pytest.mark.asyncio
async def test_cancellation_sync_operation():
    handler = Handler(user_service_handlers=[MyService()])
    cancellation = TestOperationTaskCancellation()
    start_ctx = StartOperationContext(
        service="MyService",
        operation="cancellable_sync",
        headers={},
        request_id="request_id",
        task_cancellation=cancellation,
    )

    operation_task = asyncio.create_task(
        handler.start_operation(
            start_ctx, LazyValue(serializer=DummySerializer(None), headers={})
        )
    )

    cancellation.cancel()
    result = await operation_task
    assert result == StartOperationResultSync("test cancellation occurred")


@pytest.mark.asyncio
async def test_cancellation_async_operation():
    handler = Handler(user_service_handlers=[MyService()])
    cancellation = TestOperationTaskCancellation()
    start_ctx = StartOperationContext(
        service="MyService",
        operation="cancellable_async",
        headers={},
        request_id="request_id",
        task_cancellation=cancellation,
    )

    operation_task = asyncio.create_task(
        handler.start_operation(
            start_ctx, LazyValue(serializer=DummySerializer(None), headers={})
        )
    )

    cancellation.cancel()
    result = await operation_task
    assert result == StartOperationResultAsync("test cancellation occurred")
