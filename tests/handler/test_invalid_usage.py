"""
Tests for invalid ways that users may attempt to write service definition and service
handler implementations.
"""

import concurrent.futures
from typing import Any, Callable

import pytest

import nexusrpc
from nexusrpc.handler import (
    Handler,
    StartOperationContext,
    service_handler,
    sync_operation,
)
from nexusrpc.syncio.handler import (
    Handler as SyncioHandler,
    sync_operation as syncio_sync_operation,
)


class _TestCase:
    build: Callable[..., Any]
    error_message: str


class OperationHandlerOverridesNameInconsistentlyWithServiceDefinition(_TestCase):
    @staticmethod
    def build():
        @nexusrpc.service
        class SD:
            my_op: nexusrpc.Operation[None, None]

        @service_handler(service=SD)
        class SH:
            @sync_operation(name="foo")
            async def my_op(self, ctx: StartOperationContext, input: None) -> None: ...

    error_message = "Operation handlers may not override the name of an operation in the service definition"


class ServiceDefinitionHasExtraOp(_TestCase):
    @staticmethod
    def build():
        @nexusrpc.service
        class SD:
            my_op_1: nexusrpc.Operation[None, None]
            my_op_2: nexusrpc.Operation[None, None]

        @service_handler(service=SD)
        class SH:
            @sync_operation
            async def my_op_1(
                self, ctx: StartOperationContext, input: None
            ) -> None: ...

    error_message = "does not implement an operation with method name 'my_op_2'"


class ServiceHandlerHasExtraOp(_TestCase):
    @staticmethod
    def build():
        @nexusrpc.service
        class SD:
            my_op_1: nexusrpc.Operation[None, None]

        @service_handler(service=SD)
        class SH:
            @sync_operation
            async def my_op_1(
                self, ctx: StartOperationContext, input: None
            ) -> None: ...

            @sync_operation
            async def my_op_2(
                self, ctx: StartOperationContext, input: None
            ) -> None: ...

    error_message = "does not match an operation method name in the service definition"


class ServiceDefinitionOperationHasNoTypeParams(_TestCase):
    @staticmethod
    def build():
        @nexusrpc.service
        class SD:
            my_op: nexusrpc.Operation

        @service_handler(service=SD)
        class SH:
            @sync_operation
            async def my_op(self, ctx: StartOperationContext, input: None) -> None: ...

    error_message = "has 0 type parameters"


class AsyncioDecoratorWithSyncioMethod(_TestCase):
    @staticmethod
    def build():
        @nexusrpc.service
        class SD:
            my_op: nexusrpc.Operation[None, None]

        @service_handler(service=SD)
        class SH:
            @sync_operation  # assert-type-error: 'Argument 1 to "sync_operation" has incompatible type'
            def my_op(self, ctx: StartOperationContext, input: None) -> None: ...

    error_message = (
        "sync_operation decorator must be used on an `async def` operation method"
    )


class SyncioDecoratorWithAsyncioMethod(_TestCase):
    @staticmethod
    def build():
        @nexusrpc.service
        class SD:
            my_op: nexusrpc.Operation[None, None]

        @service_handler(service=SD)
        class SH:
            @syncio_sync_operation
            async def my_op(self, ctx: StartOperationContext, input: None) -> None: ...

    error_message = (
        "syncio sync_operation decorator must be used on a `def` operation method"
    )


class AsyncioHandlerWithSyncioOperation(_TestCase):
    @staticmethod
    def build():
        @service_handler
        class SH:
            @syncio_sync_operation
            def my_op(self, ctx: StartOperationContext, input: None) -> None: ...

        Handler([SH()])

    error_message = "Use nexusrpc.syncio.handler.Handler instead"


class SyncioHandlerWithAsyncioOperation(_TestCase):
    @staticmethod
    def build():
        @service_handler
        class SH:
            @sync_operation
            async def my_op(self, ctx: StartOperationContext, input: None) -> None: ...

        SyncioHandler([SH()], concurrent.futures.ThreadPoolExecutor())

    error_message = "Use nexusrpc.handler.Handler instead"


@pytest.mark.parametrize(
    "test_case",
    [
        OperationHandlerOverridesNameInconsistentlyWithServiceDefinition,
        ServiceDefinitionOperationHasNoTypeParams,
        ServiceDefinitionHasExtraOp,
        ServiceHandlerHasExtraOp,
        AsyncioDecoratorWithSyncioMethod,
        SyncioDecoratorWithAsyncioMethod,
        AsyncioHandlerWithSyncioOperation,
        SyncioHandlerWithAsyncioOperation,
    ],
)
def test_invalid_usage(test_case: _TestCase):
    if test_case.error_message:
        with pytest.raises(Exception, match=test_case.error_message):
            test_case.build()
    else:
        test_case.build()
