from typing import Any, Callable, Type, cast

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
    # (service_name, op_name)
    supported_request: tuple[str, str]

    class UserServiceHandler:
        op: Callable[..., Any]

        async def _op_impl(self, ctx: StartOperationContext, input: None) -> bool:
            assert (service_defn := get_service_definition(self.__class__))
            assert ctx.service == service_defn.name
            _, op_handler_op_defn = get_operation_factory(self.op)
            assert op_handler_op_defn
            assert service_defn.operations.get(ctx.operation)
            return True


class NoOverrides(_TestCase):
    @nexusrpc.service
    class UserService:
        op: nexusrpc.Operation[None, bool]

    @service_handler(service=UserService)
    class UserServiceHandler(_TestCase.UserServiceHandler):
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            return await self._op_impl(ctx, input)

    supported_request = ("UserService", "op")


class OverrideServiceName(_TestCase):
    @nexusrpc.service(name="UserService-renamed")
    class UserService:
        op: nexusrpc.Operation[None, bool]

    @service_handler(service=UserService)
    class UserServiceHandler(_TestCase.UserServiceHandler):
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            return await self._op_impl(ctx, input)

    supported_request = ("UserService-renamed", "op")


class OverrideOperationName(_TestCase):
    @nexusrpc.service
    class UserService:
        op: nexusrpc.Operation[None, bool] = nexusrpc.Operation(name="op-renamed")

    @service_handler(service=UserService)
    class UserServiceHandler(_TestCase.UserServiceHandler):
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            return await self._op_impl(ctx, input)

    supported_request = ("UserService", "op-renamed")


class OverrideServiceAndOperationName(_TestCase):
    @nexusrpc.service(name="UserService-renamed")
    class UserService:
        op: nexusrpc.Operation[None, bool] = nexusrpc.Operation(name="op-renamed")

    @service_handler(service=UserService)
    class UserServiceHandler(_TestCase.UserServiceHandler):
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            return await self._op_impl(ctx, input)

    supported_request = ("UserService-renamed", "op-renamed")


@pytest.mark.parametrize(
    "test_case",
    [
        NoOverrides,
        OverrideServiceName,
        OverrideOperationName,
        OverrideServiceAndOperationName,
    ],
)
@pytest.mark.asyncio
async def test_request_routing_with_service_definition(
    test_case: _TestCase,
):
    request_service, request_op = test_case.supported_request
    ctx = StartOperationContext(
        service=request_service,
        operation=request_op,
        headers={},
        request_id="request-id",
    )
    handler = Handler(user_service_handlers=[test_case.UserServiceHandler()])
    result = await handler.start_operation(
        ctx, LazyValue(serializer=DummySerializer(None), headers={})
    )
    assert cast(StartOperationResultSync[bool], result).value is True


class NoServiceDefinitionNoOverrides(_TestCase):
    @service_handler
    class UserServiceHandler(_TestCase.UserServiceHandler):
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            return await self._op_impl(ctx, input)

    supported_request = ("UserServiceHandler", "op")


class NoServiceDefinitionOverrideServiceName(_TestCase):
    @service_handler(name="UserServiceHandler-renamed")
    class UserServiceHandler(_TestCase.UserServiceHandler):
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            return await self._op_impl(ctx, input)

    supported_request = ("UserServiceHandler-renamed", "op")


class NoServiceDefinitionOverrideOperationName(_TestCase):
    @service_handler
    class UserServiceHandler(_TestCase.UserServiceHandler):
        @sync_operation(name="op-renamed")
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            return await self._op_impl(ctx, input)

    supported_request = ("UserServiceHandler", "op-renamed")


class NoServiceDefinitionOverrideServiceAndOperationName(_TestCase):
    @service_handler(name="UserServiceHandler-renamed")
    class UserServiceHandler(_TestCase.UserServiceHandler):
        @sync_operation(name="op-renamed")
        async def op(self, ctx: StartOperationContext, input: None) -> bool:
            return await self._op_impl(ctx, input)

    supported_request = ("UserServiceHandler-renamed", "op-renamed")


@pytest.mark.parametrize(
    "test_case",
    [
        NoServiceDefinitionNoOverrides,
        NoServiceDefinitionOverrideServiceName,
        NoServiceDefinitionOverrideOperationName,
        NoServiceDefinitionOverrideServiceAndOperationName,
    ],
)
@pytest.mark.asyncio
async def test_request_routing_without_service_definition(
    test_case: _TestCase,
):
    request_service, request_op = test_case.supported_request
    ctx = StartOperationContext(
        service=request_service,
        operation=request_op,
        headers={},
        request_id="request-id",
    )
    handler = Handler(user_service_handlers=[test_case.UserServiceHandler()])
    result = await handler.start_operation(
        ctx, LazyValue(serializer=DummySerializer(None), headers={})
    )
    assert cast(StartOperationResultSync[bool], result).value is True
