import uuid

import pytest

from nexusrpc import LazyValue
from nexusrpc.handler import (
    CancelOperationContext,
    Handler,
    OperationHandler,
    StartOperationContext,
    StartOperationResultAsync,
    operation_handler,
    service_handler,
)
from tests.helpers import DummySerializer, TestOperationTaskCancellation

_operation_results: dict[str, int] = {}


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


@pytest.mark.asyncio
async def test_async_operation_happy_path():
    handler = Handler(user_service_handlers=[MyService()])
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
