from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Optional, Type

import pytest

from nexusrpc.handler import (
    StartOperationContext,
    SyncExecutor,
    service_handler,
)
from nexusrpc.handler._common import StartOperationResultSync
from nexusrpc.handler._decorators import sync_operation_handler
from nexusrpc._serializer import Content
from nexusrpc.syncio import LazyValue
from nexusrpc.syncio.handler import Handler


class _TestCase:
    user_service_handler: Any


class SyncHandlerHappyPath:
    @service_handler
    class MyService:
        @sync_operation_handler
        def incr(self, ctx: StartOperationContext, input: int) -> int:
            return input + 1

    user_service_handler = MyService()


@pytest.mark.parametrize("test_case", [SyncHandlerHappyPath])
def test_sync_handler_happy_path(test_case: Type[_TestCase]):
    handler = Handler(
        user_service_handlers=[test_case.user_service_handler],
        sync_executor=SyncExecutor(executor=ThreadPoolExecutor(max_workers=1)),
    )
    ctx = StartOperationContext(
        service="MyService",
        operation="incr",
    )
    result = handler.start_operation(ctx, LazyValue(DummySerializer(1), headers={}))
    assert isinstance(result, StartOperationResultSync)
    assert result.value == 2


@dataclass
class DummySerializer:
    value: int

    def serialize(self, value: Any) -> Content:
        raise NotImplementedError

    def deserialize(self, content: Content, as_type: Optional[Type[Any]] = None) -> Any:
        return self.value
