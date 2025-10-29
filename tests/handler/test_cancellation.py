import asyncio
import threading
from typing import Optional

import pytest

from nexusrpc import LazyValue
from nexusrpc.handler import (
    CancelOperationContext,
    Handler,
    OperationHandler,
    StartOperationContext,
    StartOperationResultAsync,
    service_handler,
)
from nexusrpc.handler._cancellation import OperationTaskCancellation
from nexusrpc.handler._common import StartOperationResultSync
from nexusrpc.handler._decorators import operation_handler, sync_operation
from tests.helpers import DummySerializer


class CancellableAsyncOperationHandler(OperationHandler[None, None]):
    async def start(
        self, ctx: StartOperationContext, input: None
    ) -> StartOperationResultAsync:
        try:
            await asyncio.wait_for(
                ctx.task_cancellation.wait_until_cancelled_async(), timeout=1
            )
        except TimeoutError as err:
            raise RuntimeError("Expected cancellation") from err

        details = ctx.task_cancellation.cancellation_details()
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
        cancelled = ctx.task_cancellation.wait_until_cancelled(1)
        if not cancelled:
            raise RuntimeError("Expected cancellation")

        details = ctx.task_cancellation.cancellation_details()
        if not details:
            raise RuntimeError("Expected cancellation details")

        return details


class TestOperationTaskCancellation(OperationTaskCancellation):
    # A naive implementation of cancellation for use in tests
    def __init__(self):
        self._details = None
        self._evt = threading.Event()
        self._lock = threading.Lock()

    def is_cancelled(self) -> bool:
        return self._evt.is_set()

    def cancellation_details(self) -> Optional[str]:
        with self._lock:
            return self._details

    def wait_until_cancelled(self, timeout: float | None = None) -> bool:
        return self._evt.wait(timeout)

    async def wait_until_cancelled_async(self):
        while not self.is_cancelled():
            await asyncio.sleep(0.05)

    def cancel(self):
        with self._lock:
            self._details = "test cancellation occurred"
            self._evt.set()


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
