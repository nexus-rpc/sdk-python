"""
Tests for invalid ways that users may attempt to write service definition and service
handler implementations.
"""

from typing import Any, Callable

import pytest
from typing_extensions import dataclass_transform

import nexusrpc
from nexusrpc.handler import (
    Handler,
    StartOperationContext,
    service_handler,
    sync_operation,
)
from nexusrpc.handler._decorators import operation_handler
from nexusrpc.handler._operation_handler import OperationHandler


@dataclass_transform()
class _BaseTestCase:
    pass


class _TestCase(_BaseTestCase):
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
            async def my_op(
                self, _ctx: StartOperationContext, _input: None
            ) -> None: ...

        _ = SH

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
                self, _ctx: StartOperationContext, _input: None
            ) -> None: ...

        _ = SH

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
                self, _ctx: StartOperationContext, _input: None
            ) -> None: ...

            @sync_operation
            async def my_op_2(
                self, _ctx: StartOperationContext, _input: None
            ) -> None: ...

        _ = SH

    error_message = "does not match an operation method name in the service definition"


class ServiceDefinitionOperationHasNoTypeParams(_TestCase):
    @staticmethod
    def build():
        @nexusrpc.service
        class SD:
            my_op: nexusrpc.Operation  # type: ignore

        @service_handler(service=SD)
        class SH:
            @sync_operation
            async def my_op(
                self, _ctx: StartOperationContext, _input: None
            ) -> None: ...

        _ = SH

    error_message = "has 0 type parameters"


class AsyncioHandlerWithSyncioOperation(_TestCase):
    @staticmethod
    def build():
        @service_handler
        class SH:
            @sync_operation
            def my_op(self, _ctx: StartOperationContext, _input: None) -> None: ...

        _ = Handler([SH()])

    error_message = "you have not supplied an executor"


class ServiceDefinitionHasDuplicateMethodNames(_TestCase):
    @staticmethod
    def build():
        @nexusrpc.service
        class SD:
            my_op: nexusrpc.Operation[None, None] = nexusrpc.Operation(
                name="my_op",
                method_name="my_op",
                input_type=None,
                output_type=None,
            )
            my_op_2: nexusrpc.Operation[None, None] = nexusrpc.Operation(
                name="my_op_2",
                method_name="my_op",
                input_type=None,
                output_type=None,
            )

        _ = SD

    error_message = "Operation method name 'my_op' is not unique"


class OperationHandlerNoInputOutputTypeAnnotationsWithoutServiceDefinition(_TestCase):
    @staticmethod
    def build():
        @service_handler
        class SubclassingNoInputOutputTypeAnnotationsWithoutServiceDefinition:
            @operation_handler
            def op(self) -> OperationHandler: ...  # type: ignore

        _ = SubclassingNoInputOutputTypeAnnotationsWithoutServiceDefinition

    error_message = r"has no input type"


@pytest.mark.parametrize(
    "test_case",
    [
        OperationHandlerOverridesNameInconsistentlyWithServiceDefinition,
        ServiceDefinitionOperationHasNoTypeParams,
        ServiceDefinitionHasExtraOp,
        ServiceHandlerHasExtraOp,
        AsyncioHandlerWithSyncioOperation,
        ServiceDefinitionHasDuplicateMethodNames,
        OperationHandlerNoInputOutputTypeAnnotationsWithoutServiceDefinition,
    ],
)
def test_invalid_usage(test_case: _TestCase):
    if test_case.error_message:
        with pytest.raises(Exception, match=test_case.error_message):
            test_case.build()
    else:
        test_case.build()
