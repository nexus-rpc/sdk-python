from typing import Any, Type, TypeVar

import pytest

import nexusrpc
import nexusrpc.contract
import nexusrpc.handler
from nexusrpc.handler._core import ServiceHandler

# For OperationHandler generic types, though not strictly needed for test structure itself
I = TypeVar("I")
O = TypeVar("O")


# Test Case for Decorator Validation (Contract Compliance)
class _DecoratorValidationTestCase:
    Contract: Type[Any]
    UserServiceHandler: Type[Any]
    expected_error_message_pattern: str


class MissingOperationFromContract(_DecoratorValidationTestCase):
    @nexusrpc.contract.service
    class ServiceContract:
        op_A: nexusrpc.contract.Operation[int, str]
        op_B: nexusrpc.contract.Operation[bool, float]

    Contract = ServiceContract

    class HandlerMissingOpB:
        @nexusrpc.handler.operation_handler
        def op_A(self) -> nexusrpc.handler.OperationHandler[int, str]: ...

        # op_B is missing

    UserServiceHandler = HandlerMissingOpB
    expected_error_message_pattern = r"does not implement operation 'op_B'"


@pytest.mark.parametrize(
    "test_case",
    [
        MissingOperationFromContract,
    ],
)
def test_decorator_validates_contract_compliance(
    test_case: _DecoratorValidationTestCase,
):
    with pytest.raises(TypeError, match=test_case.expected_error_message_pattern):
        nexusrpc.handler.service_handler(service=test_case.Contract)(
            test_case.UserServiceHandler
        )


# Test Cases for Service Implementation Inheritance
class _ServiceImplInheritanceTestCase:
    test_case_name: str
    BaseImpl: Type[Any]
    ChildImpl: Type[Any]
    expected_operations_in_child_handler: set[str]


class ServiceImplInheritanceWithContracts(_ServiceImplInheritanceTestCase):
    test_case_name = "ServiceImplInheritanceWithContracts"

    @nexusrpc.contract.service
    class ContractA:
        base_op: nexusrpc.contract.Operation[int, str]

    @nexusrpc.contract.service
    class ContractB:
        base_op: nexusrpc.contract.Operation[int, str]
        child_op: nexusrpc.contract.Operation[bool, float]

    @nexusrpc.handler.service_handler(service=ContractA)
    class AnImplementation:
        @nexusrpc.handler.operation_handler
        def base_op(self) -> nexusrpc.handler.OperationHandler[int, str]: ...

    @nexusrpc.handler.service_handler(service=ContractB)
    class BImplementation(AnImplementation):
        @nexusrpc.handler.operation_handler
        def child_op(self) -> nexusrpc.handler.OperationHandler[bool, float]: ...

    BaseImpl = AnImplementation
    ChildImpl = BImplementation
    expected_operations_in_child_handler = {"base_op", "child_op"}


class ServiceImplInheritanceNoContracts(_ServiceImplInheritanceTestCase):
    test_case_name = "ServiceImplInheritanceNoContracts"

    @nexusrpc.handler.service_handler
    class BaseImplNoContract:
        @nexusrpc.handler.operation_handler
        def base_op_nc(self) -> nexusrpc.handler.OperationHandler[int, str]: ...

    @nexusrpc.handler.service_handler
    class ChildImplNoContract(BaseImplNoContract):
        @nexusrpc.handler.operation_handler
        def child_op_nc(self) -> nexusrpc.handler.OperationHandler[bool, float]: ...

    BaseImpl = BaseImplNoContract
    ChildImpl = ChildImplNoContract
    expected_operations_in_child_handler = {"base_op_nc", "child_op_nc"}


@pytest.mark.parametrize(
    "test_case",
    [
        ServiceImplInheritanceWithContracts,
        ServiceImplInheritanceNoContracts,
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


# Test Cases for Service Contract Inheritance (Current Behavior due to TODO)
class _ServiceContractInheritanceTestCase:
    test_case_name: str
    ChildContractDefInheriting: Type[Any]  # We only need to inspect the child contract
    expected_ops_in_child_contract: set[str]


class ContractInheritanceShowsNoActualInheritance(_ServiceContractInheritanceTestCase):
    test_case_name = "ContractInheritanceShowsNoActualInheritance (Current Behavior)"

    @nexusrpc.contract.service
    class BaseDef:
        op_from_base_contract: nexusrpc.contract.Operation[int, str]

    @nexusrpc.contract.service
    class ChildDefInherits(BaseDef):
        op_from_child_contract: nexusrpc.contract.Operation[bool, float]

    ChildContractDefInheriting = ChildDefInherits
    expected_ops_in_child_contract = {"op_from_child_contract"}


@pytest.mark.parametrize(
    "test_case",
    [
        ContractInheritanceShowsNoActualInheritance,
    ],
)
def test_service_contract_inheritance_behavior(
    test_case: _ServiceContractInheritanceTestCase,
):
    child_contract_nexus_service = getattr(
        test_case.ChildContractDefInheriting, "__nexus_service__", None
    )

    assert child_contract_nexus_service is not None, (
        f"{test_case.ChildContractDefInheriting.__name__} lacks __nexus_service__ attribute."
    )
    assert isinstance(child_contract_nexus_service, nexusrpc.contract.Service), (
        "__nexus_service__ is not a nexusrpc.contract.Service instance."
    )

    assert (
        set(child_contract_nexus_service.operations.keys())
        == test_case.expected_ops_in_child_contract
    )

    # Ensure a handler is validated against this understanding of the child contract
    # (i.e., it only needs to implement ops directly defined in ChildDefInherits)

    op_name_in_child_contract = list(test_case.expected_ops_in_child_contract)[0]
    with pytest.raises(
        TypeError, match=rf"does not implement operation '{op_name_in_child_contract}'"
    ):

        @nexusrpc.handler.service_handler(service=test_case.ChildContractDefInheriting)
        class HandlerMissingChildOp:
            pass

    @nexusrpc.handler.service_handler(service=test_case.ChildContractDefInheriting)
    class HandlerCorrectForChildContract:
        # Dynamically define the method to match the op_name_in_child_contract
        # This is a bit tricky for a class body. Simpler to use a fixed name if only one op.
        # Assuming op_from_child_contract is the one.
        @nexusrpc.handler.operation_handler
        def op_from_child_contract(
            self,
        ) -> nexusrpc.handler.OperationHandler[bool, float]: ...

    assert HandlerCorrectForChildContract is not None

    @nexusrpc.handler.service_handler(service=test_case.ChildContractDefInheriting)
    class HandlerWithExtraOpsForChildContract:
        @nexusrpc.handler.operation_handler
        def op_from_child_contract(
            self,
        ) -> nexusrpc.handler.OperationHandler[bool, float]: ...
        @nexusrpc.handler.operation_handler
        def op_from_base_contract(
            self,
        ) -> nexusrpc.handler.OperationHandler[int, str]: ...

    assert HandlerWithExtraOpsForChildContract is not None
