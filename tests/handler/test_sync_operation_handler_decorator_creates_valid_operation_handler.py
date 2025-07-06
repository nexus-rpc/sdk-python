from unittest import mock

import pytest

from nexusrpc._util import get_operation_factory, is_async_callable
from nexusrpc.handler import (
    StartOperationContext,
    StartOperationResultSync,
    service_handler,
    sync_operation,
)


@service_handler
class MyServiceHandler:
    def __init__(self):
        self.mutable_container = []

    @sync_operation
    def my_def_op(self, ctx: StartOperationContext, input: int) -> int:
        """
        This is the docstring for the `my_def_op` sync operation.
        """
        self.mutable_container.append(input)
        return input + 1

    @sync_operation(name="foo")
    async def my_async_def_op(self, ctx: StartOperationContext, input: int) -> int:
        """
        This is the docstring for the `my_async_def_op` sync operation.
        """
        self.mutable_container.append(input)
        return input + 2


def test_def_sync_handler():
    user_instance = MyServiceHandler()
    op_handler_factory, _ = get_operation_factory(user_instance.my_def_op)
    assert op_handler_factory
    op_handler = op_handler_factory(user_instance)
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
    op_handler_factory, _ = get_operation_factory(user_instance.my_async_def_op)
    assert op_handler_factory
    op_handler = op_handler_factory(user_instance)
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
