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


# Test Case for Decorator Validation
class _DecoratorValidationTestCase:
    UserServiceDefinition: Type[Any]
    UserServiceHandler: Type[Any]
    expected_error_message_pattern: str


class MissingOperationFromDefinition(_DecoratorValidationTestCase):
    @nexusrpc.service
    class ServiceDefinition:
        op_A: nexusrpc.Operation[int, str]
        op_B: nexusrpc.Operation[bool, float]

    UserServiceDefinition = ServiceDefinition

    class HandlerMissingOpB:
        @nexusrpc.handler.operation_handler
        def op_A(self) -> nexusrpc.handler.OperationHandler[int, str]: ...

        # op_B is missing

    UserServiceHandler = HandlerMissingOpB
    expected_error_message_pattern = r"does not implement operation 'op_B'"


class MethodNameDoesNotMatchDefinition(_DecoratorValidationTestCase):
    @nexusrpc.service
    class ServiceDefinition:
        op_A: nexusrpc.Operation[int, str] = nexusrpc.Operation(name="foo")

    UserServiceDefinition = ServiceDefinition

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
        nexusrpc.handler.service_handler(service=test_case.UserServiceDefinition)(
            test_case.UserServiceHandler
        )


# Test Cases for Service Implementation Inheritance
class _ServiceImplInheritanceTestCase:
    test_case_name: str
    BaseImpl: Type[Any]
    ChildImpl: Type[Any]
    expected_operations_in_child_handler: set[str]


class ServiceImplInheritanceWithDefinition(_ServiceImplInheritanceTestCase):
    test_case_name = "ServiceImplInheritanceWithContracts"

    @nexusrpc.service
    class ContractA:
        base_op: nexusrpc.Operation[int, str]

    @nexusrpc.service
    class ContractB:
        base_op: nexusrpc.Operation[int, str]
        child_op: nexusrpc.Operation[bool, float]

    @nexusrpc.handler.service_handler(service=ContractA)
    class AImplementation:
        @nexusrpc.handler.operation_handler
        def base_op(self) -> nexusrpc.handler.OperationHandler[int, str]: ...

    @nexusrpc.handler.service_handler(service=ContractB)
    class BImplementation(AImplementation):
        @nexusrpc.handler.operation_handler
        def child_op(self) -> nexusrpc.handler.OperationHandler[bool, float]: ...

    BaseImpl = AImplementation
    ChildImpl = BImplementation
    expected_operations_in_child_handler = {"base_op", "child_op"}


class ServiceImplInheritanceWithoutDefinition(_ServiceImplInheritanceTestCase):
    test_case_name = "ServiceImplInheritanceWithoutDefinition"

    @nexusrpc.handler.service_handler
    class BaseImplWithoutDefinition:
        @nexusrpc.handler.operation_handler
        def base_op_nc(self) -> nexusrpc.handler.OperationHandler[int, str]: ...

    @nexusrpc.handler.service_handler
    class ChildImplWithoutDefinition(BaseImplWithoutDefinition):
        @nexusrpc.handler.operation_handler
        def child_op_nc(self) -> nexusrpc.handler.OperationHandler[bool, float]: ...

    BaseImpl = BaseImplWithoutDefinition
    ChildImpl = ChildImplWithoutDefinition
    expected_operations_in_child_handler = {"base_op_nc", "child_op_nc"}


@pytest.mark.parametrize(
    "test_case",
    [
        ServiceImplInheritanceWithDefinition,
        ServiceImplInheritanceWithoutDefinition,
    ],
)
def test_service_implementation_inheritance(test_case: _ServiceImplInheritanceTestCase):
    child_instance = test_case.ChildImpl()
    service_handler_meta = ServiceHandler.from_user_instance(child_instance)

    assert (
        set(service_handler_meta.operation_handlers.keys())
        == test_case.expected_operations_in_child_handler
    )
    assert (
        set(service_handler_meta.service.operations.keys())
        == test_case.expected_operations_in_child_handler
    )


# Test Cases for Service Definition Inheritance (Current Behavior due to TODO)
class _ServiceDefinitionInheritanceTestCase:
    test_case_name: str
    ChildDefinitionInheriting: Type[Any]  # We only need to inspect the child definition
    expected_ops_in_child_definition: set[str]


class ServiceDefinitionInheritance(_ServiceDefinitionInheritanceTestCase):
    test_case_name = "ServiceDefinitionInheritance"

    @nexusrpc.service
    class BaseDef:
        op_from_base_definition: nexusrpc.Operation[int, str]

    @nexusrpc.service
    class ChildDefInherits(BaseDef):
        op_from_child_definition: nexusrpc.Operation[bool, float]

    ChildDefinitionInheriting = ChildDefInherits
    expected_ops_in_child_definition = {
        "op_from_base_definition",
        "op_from_child_definition",
    }


@pytest.mark.parametrize(
    "test_case",
    [
        ServiceDefinitionInheritance,
    ],
)
@pytest.mark.skip(
    reason="TODO(prerelease): service definition inheritance is not supported yet"
)
def test_service_definition_inheritance_behavior(
    test_case: _ServiceDefinitionInheritanceTestCase,
):
    child_service_definition = getattr(
        test_case.ChildDefinitionInheriting, "__nexus_service__", None
    )

    assert child_service_definition is not None, (
        f"{test_case.ChildDefinitionInheriting.__name__} lacks __nexus_service__ attribute."
    )
    assert isinstance(child_service_definition, nexusrpc.ServiceDefinition), (
        "__nexus_service__ is not a nexusrpc.ServiceDefinition instance."
    )

    assert (
        set(child_service_definition.operations.keys())
        == test_case.expected_ops_in_child_definition
    )

    with pytest.raises(
        TypeError, match="does not implement operation 'op_from_base_definition'"
    ):

        @nexusrpc.handler.service_handler(service=test_case.ChildDefinitionInheriting)
        class HandlerMissingChildOp:
            @nexusrpc.handler.operation_handler
            def op_from_base_definition(
                self,
            ) -> nexusrpc.handler.OperationHandler[int, str]: ...
