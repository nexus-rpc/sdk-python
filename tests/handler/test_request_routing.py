from typing import Any, Type, cast

import pytest

import nexusrpc
from nexusrpc import LazyValue
from nexusrpc._util import get_operation_factory, get_service_definition
from nexusrpc.handler import (
    Handler,
    StartOperationContext,
    service_handler,
    sync_operation,
)
from nexusrpc.handler._common import StartOperationResultSync

from ..helpers import DummySerializer


class _TestCase:
    UserService: Type[Any]
    UserServiceHandler: Type[Any]
    # `expected` is (request, handler), where both request and handler are
    # (service_name, op_name)
    supported_request: tuple[str, str]


class NoOverrides(_TestCase):
    @nexusrpc.service
    class UserService:
        op: nexusrpc.Operation[None, bool]

    @service_handler(service=UserService)
    class UserServiceHandler:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            assert (service_defn := get_service_definition(self.__class__))
            _, op_defn = get_operation_factory(self.op)
            assert op_defn
            assert ctx.service == service_defn.name
            assert ctx.operation == op_defn.name
            return True

    supported_request = ("UserService", "op")


class OverrideServiceName(_TestCase):
    @nexusrpc.service(name="UserServiceNameOverride")
    class UserService:
        op: nexusrpc.Operation[None, bool]

    @service_handler(service=UserService)
    class UserServiceHandler:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            assert (service_defn := get_service_definition(self.__class__))
            _, op_defn = get_operation_factory(self.op)
            assert op_defn
            assert ctx.service == service_defn.name
            assert ctx.operation == op_defn.name
            return True

    supported_request = ("UserServiceNameOverride", "op")


@pytest.mark.parametrize(
    "test_case",
    [
        NoOverrides,
        OverrideServiceName,
    ],
)
@pytest.mark.asyncio
async def test_request_routing_with_service_definition(test_case: _TestCase):
    handler = Handler(user_service_handlers=[test_case.UserServiceHandler()])
    request_service, request_op = test_case.supported_request
    ctx = StartOperationContext(
        service=request_service,
        operation=request_op,
        headers={},
        request_id="request-id",
    )
    result = await handler.start_operation(
        ctx, LazyValue(serializer=DummySerializer(None), headers={})
    )
    assert cast(StartOperationResultSync[bool], result).value is True
