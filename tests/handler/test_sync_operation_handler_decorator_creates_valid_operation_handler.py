from unittest import mock

import pytest

import nexusrpc._handler
from nexusrpc._handler._util import is_async_callable


@nexusrpc._handler.service_handler
class MyServiceHandler:
    def __init__(self):
        self.mutable_container = []

    @nexusrpc._handler.sync_operation_handler
    def my_def_op(
        self, ctx: nexusrpc._handler.StartOperationContext, input: int
    ) -> int:
        """
        This is the docstring for the `my_def_op` sync operation.
        """
        self.mutable_container.append(input)
        return input + 1

    @nexusrpc._handler.sync_operation_handler
    async def my_async_def_op(
        self, ctx: nexusrpc._handler.StartOperationContext, input: int
    ) -> int:
        """
        This is the docstring for the `my_async_def_op` sync operation.
        """
        self.mutable_container.append(input)
        return input + 2


def test_def_sync_handler():
    user_instance = MyServiceHandler()
    op_handler = user_instance.my_def_op()
    assert not is_async_callable(op_handler.start)
    assert (
        str(op_handler.start.__doc__).strip()
        == "This is the docstring for the `my_def_op` sync operation."
    )
    assert not user_instance.mutable_container
    ctx = mock.Mock(spec=nexusrpc._handler.StartOperationContext)
    result = op_handler.start(ctx, 1)
    assert isinstance(result, nexusrpc._handler.StartOperationResultSync)
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
    ctx = mock.Mock(spec=nexusrpc._handler.StartOperationContext)
    result = await op_handler.start(ctx, 1)
    assert isinstance(result, nexusrpc._handler.StartOperationResultSync)
    assert result.value == 3
    assert user_instance.mutable_container == [1]
