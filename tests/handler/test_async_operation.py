import uuid
from dataclasses import dataclass
from typing import Any, Optional, Type

import pytest

from nexusrpc import Content, LazyValue, OperationInfo, OperationState
from nexusrpc.handler import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    Handler,
    OperationHandler,
    StartOperationContext,
    StartOperationResultAsync,
    operation_handler,
    service_handler,
)


class _TestCase:
    user_service_handler: Any


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

    async def fetch_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> OperationInfo:
        assert token in _operation_results
        return OperationInfo(
            token=token,
            state=OperationState.RUNNING,
        )

    async def fetch_result(self, ctx: FetchOperationResultContext, token: str) -> int:
        return _operation_results[token]


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
    )
    start_result = await handler.start_operation(
        start_ctx, LazyValue(DummySerializer(1), headers={})
    )
    assert isinstance(start_result, StartOperationResultAsync)
    assert start_result.token

    fetch_info_ctx = FetchOperationInfoContext(
        service="MyService",
        operation="incr",
    )
    info = await handler.fetch_operation_info(fetch_info_ctx, start_result.token)
    assert info.state == OperationState.RUNNING

    fetch_result_ctx = FetchOperationResultContext(
        service="MyService",
        operation="incr",
    )
    result = await handler.fetch_operation_result(fetch_result_ctx, start_result.token)
    assert result == 2

    cancel_ctx = CancelOperationContext(
        service="MyService",
        operation="incr",
    )
    await handler.cancel_operation(cancel_ctx, start_result.token)
    assert start_result.token not in _operation_results


@dataclass
class DummySerializer:
    value: int

    async def serialize(self, value: Any) -> Content:
        raise NotImplementedError

    async def deserialize(
        self, content: Content, as_type: Optional[Type[Any]] = None
    ) -> Any:
        return self.value
