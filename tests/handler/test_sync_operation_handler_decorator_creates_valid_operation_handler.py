from unittest import mock

import pytest

from nexusrpc.handler import (
    OperationHandler,
    StartOperationContext,
    StartOperationResultSync,
    SyncOperationHandler,
    operation_handler,
    service_handler,
)
from nexusrpc.handler._util import is_async_callable
from nexusrpc.handler.syncio import SyncOperationHandler as SyncioSyncOperationHandler


@service_handler
class MyServiceHandler:
    def __init__(self):
        self.mutable_container = []

    @operation_handler
    def my_def_op(self) -> OperationHandler[int, int]:
        def start(ctx: StartOperationContext, input: int) -> int:
            """
            This is the docstring for the `my_def_op` sync operation.
            """
            self.mutable_container.append(input)
            return input + 1

        return SyncioSyncOperationHandler.from_callable(start)

    @operation_handler
    def my_async_def_op(self) -> OperationHandler[int, int]:
        async def start(ctx: StartOperationContext, input: int) -> int:
            """
            This is the docstring for the `my_async_def_op` sync operation.
            """
            self.mutable_container.append(input)
            return input + 2

        return SyncOperationHandler.from_callable(start)


def test_def_sync_handler():
    user_instance = MyServiceHandler()
    op_handler = user_instance.my_def_op()
    assert not is_async_callable(op_handler.start)
    assert (
        str(op_handler.start.__doc__).strip()
        == "This is the docstring for the `my_def_op` sync operation."
    )
    assert not user_instance.mutable_container
    ctx = mock.Mock(spec=StartOperationContext)
    result = op_handler.start(ctx, 1)
    assert isinstance(result, StartOperationResultSync)
    assert result.value == 2
    assert user_instance.mutable_container == [1]


@pytest.mark.asyncio
async def test_async_def_sync_handler():
    user_instance = MyServiceHandler()
    op_handler = user_instance.my_async_def_op()
    assert is_async_callable(op_handler.start)
    assert (
        str(op_handler.start.__doc__).strip()
        == "This is the docstring for the `my_async_def_op` sync operation."
    )
    assert not user_instance.mutable_container
    ctx = mock.Mock(spec=StartOperationContext)
    result = await op_handler.start(ctx, 1)
    assert isinstance(result, StartOperationResultSync)
    assert result.value == 3
    assert user_instance.mutable_container == [1]
