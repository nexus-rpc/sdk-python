from __future__ import annotations

from typing import Any, Type

import pytest

import nexusrpc
from nexusrpc._util import get_service_definition
from nexusrpc.handler import (
    OperationHandler,
    service_handler,
)
from nexusrpc.handler._core import ServiceHandler
from nexusrpc.handler._decorators import operation_handler


class _DecoratorValidationTestCase:
    UserService: Type[Any]
    UserServiceHandler: Type[Any]
    expected_error_message_pattern: str


class MissingOperationFromDefinition(_DecoratorValidationTestCase):
    @nexusrpc.service
    class UserService:
        op_A: nexusrpc.Operation[int, str]
        op_B: nexusrpc.Operation[bool, float]

    class UserServiceHandler:
        @operation_handler
        def op_A(self) -> OperationHandler[int, str]: ...

    expected_error_message_pattern = (
        r"does not implement an operation with method name 'op_B'"
    )


class MethodNameDoesNotMatchDefinition(_DecoratorValidationTestCase):
    @nexusrpc.service
    class UserService:
        op_A: nexusrpc.Operation[int, str] = nexusrpc.Operation(name="foo")

    class UserServiceHandler:
        @operation_handler
        def op_A_incorrect_method_name(
            self,
        ) -> OperationHandler[int, str]: ...

    expected_error_message_pattern = (
        r"does not match an operation method name in the service definition."
    )


@pytest.mark.parametrize(
    "test_case",
    [
        MissingOperationFromDefinition,
        MethodNameDoesNotMatchDefinition,
    ],
)
def test_decorator_validates_definition_compliance(
    test_case: _DecoratorValidationTestCase,
):
    with pytest.raises(TypeError, match=test_case.expected_error_message_pattern):
        service_handler(service=test_case.UserService)(test_case.UserServiceHandler)


class _ServiceHandlerInheritanceTestCase:
    UserServiceHandler: Type[Any]
    expected_operations: set[str]


class ServiceHandlerInheritanceWithServiceDefinition(
    _ServiceHandlerInheritanceTestCase
):
    @nexusrpc.service
    class BaseUserService:
        base_op: nexusrpc.Operation[int, str]

    @nexusrpc.service
    class UserService:
        base_op: nexusrpc.Operation[int, str]
        child_op: nexusrpc.Operation[bool, float]

    @service_handler(service=BaseUserService)
    class BaseUserServiceHandler:
        @operation_handler
        def base_op(self) -> OperationHandler[int, str]: ...

    @service_handler(service=UserService)
    class UserServiceHandler(BaseUserServiceHandler):
        @operation_handler
        def child_op(self) -> OperationHandler[bool, float]: ...

    expected_operations = {"base_op", "child_op"}


class ServiceHandlerInheritanceWithoutDefinition(_ServiceHandlerInheritanceTestCase):
    @service_handler
    class BaseUserServiceHandler:
        @operation_handler
        def base_op_nc(self) -> OperationHandler[int, str]: ...

    @service_handler
    class UserServiceHandler(BaseUserServiceHandler):
        @operation_handler
        def child_op_nc(self) -> OperationHandler[bool, float]: ...

    expected_operations = {"base_op_nc", "child_op_nc"}


@pytest.mark.parametrize(
    "test_case",
    [
        ServiceHandlerInheritanceWithServiceDefinition,
        ServiceHandlerInheritanceWithoutDefinition,
    ],
)
def test_service_implementation_inheritance(
    test_case: _ServiceHandlerInheritanceTestCase,
):
    service_handler = ServiceHandler.from_user_instance(test_case.UserServiceHandler())

    assert set(service_handler.operation_handlers) == test_case.expected_operations
    assert set(service_handler.service.operations) == test_case.expected_operations


class _ServiceDefinitionInheritanceTestCase:
    UserService: Type[Any]
    expected_ops: set[str]


class ServiceDefinitionInheritance(_ServiceDefinitionInheritanceTestCase):
    @nexusrpc.service
    class BaseUserService:
        op_from_base_definition: nexusrpc.Operation[int, str]

    @nexusrpc.service
    class UserService(BaseUserService):
        op_from_child_definition: nexusrpc.Operation[bool, float]

    expected_ops = {
        "op_from_base_definition",
        "op_from_child_definition",
    }


@pytest.mark.parametrize(
    "test_case",
    [
        ServiceDefinitionInheritance,
    ],
)
def test_service_definition_inheritance_behavior(
    test_case: _ServiceDefinitionInheritanceTestCase,
):
    service_defn = get_service_definition(test_case.UserService)

    assert service_defn is not None, (
        f"{test_case.UserService.__name__} lacks __nexus_service__ attribute."
    )
    assert isinstance(service_defn, nexusrpc.ServiceDefinition), (
        "__nexus_service__ is not a nexusrpc.ServiceDefinition instance."
    )

    assert set(service_defn.operations) == test_case.expected_ops

    with pytest.raises(
        TypeError,
        match="does not implement an operation with method name 'op_from_child_definition'",
    ):

        @service_handler(service=test_case.UserService)
        class HandlerMissingChildOp:
            @operation_handler
            def op_from_base_definition(
                self,
            ) -> OperationHandler[int, str]: ...
