# TODO(prerelease): The service definition decorator does not support forward type reference
# from __future__ import annotations

from typing import Any, Type

import pytest

import nexusrpc
import nexusrpc._service_definition
import nexusrpc.handler
from nexusrpc.handler._core import ServiceHandler

# TODO(prerelease): check return type of op methods including fetch_result and fetch_info
#         temporalio.common._type_hints_from_func(hello_nexus.hello2().fetch_result),


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
        @nexusrpc.handler.operation_handler
        def op_A(self) -> nexusrpc.handler.OperationHandler[int, str]: ...

    expected_error_message_pattern = r"does not implement operation 'op_B'"


class MethodNameDoesNotMatchDefinition(_DecoratorValidationTestCase):
    @nexusrpc.service
    class UserService:
        op_A: nexusrpc.Operation[int, str] = nexusrpc.Operation(name="foo")

    class UserServiceHandler:
        @nexusrpc.handler.operation_handler
        def op_A_incorrect_method_name(
            self,
        ) -> nexusrpc.handler.OperationHandler[int, str]: ...

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
        nexusrpc.handler.service_handler(service=test_case.UserService)(
            test_case.UserServiceHandler
        )


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

    @nexusrpc.handler.service_handler(service=BaseUserService)
    class BaseUserServiceHandler:
        @nexusrpc.handler.operation_handler
        def base_op(self) -> nexusrpc.handler.OperationHandler[int, str]: ...

    @nexusrpc.handler.service_handler(service=UserService)
    class UserServiceHandler(BaseUserServiceHandler):
        @nexusrpc.handler.operation_handler
        def child_op(self) -> nexusrpc.handler.OperationHandler[bool, float]: ...

    expected_operations = {"base_op", "child_op"}


class ServiceHandlerInheritanceWithoutDefinition(_ServiceHandlerInheritanceTestCase):
    @nexusrpc.handler.service_handler
    class BaseUserServiceHandler:
        @nexusrpc.handler.operation_handler
        def base_op_nc(self) -> nexusrpc.handler.OperationHandler[int, str]: ...

    @nexusrpc.handler.service_handler
    class UserServiceHandler(BaseUserServiceHandler):
        @nexusrpc.handler.operation_handler
        def child_op_nc(self) -> nexusrpc.handler.OperationHandler[bool, float]: ...

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
    service_defn = getattr(test_case.UserService, "__nexus_service__", None)

    assert service_defn is not None, (
        f"{test_case.UserService.__name__} lacks __nexus_service__ attribute."
    )
    assert isinstance(service_defn, nexusrpc.ServiceDefinition), (
        "__nexus_service__ is not a nexusrpc.ServiceDefinition instance."
    )

    assert set(service_defn.operations) == test_case.expected_ops

    with pytest.raises(
        TypeError, match="does not implement operation 'op_from_child_definition'"
    ):

        @nexusrpc.handler.service_handler(service=test_case.UserService)
        class HandlerMissingChildOp:
            @nexusrpc.handler.operation_handler
            def op_from_base_definition(
                self,
            ) -> nexusrpc.handler.OperationHandler[int, str]: ...
