"""
Test that operation decorators result in operation definitions with the correct name and
input/ouput types.
"""

from dataclasses import dataclass
from typing import Any, Optional, Type

import pytest

import nexusrpc.contract
import nexusrpc.handler


@dataclass
class Input:
    pass


@dataclass
class Output:
    pass


@dataclass
class _TestCase:
    Service: Type[Any]
    expected_operations: dict[str, nexusrpc.contract.Operation]
    Contract: Optional[Type[nexusrpc.contract.Service]] = None


class ManualOperationHandler(_TestCase):
    @nexusrpc.handler.service_handler
    class Service:
        @nexusrpc.handler.operation_handler
        def operation(self) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.contract.Operation._create(
            name="operation",
            input_type=Input,
            output_type=Output,
        ),
    }


class ManualOperationHandlerWithNameOverride(_TestCase):
    @nexusrpc.handler.service_handler
    class Service:
        @nexusrpc.handler.operation_handler(name="operation-name")
        def operation(self) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.contract.Operation._create(
            name="operation-name",
            input_type=Input,
            output_type=Output,
        ),
    }


class SyncOperation(_TestCase):
    @nexusrpc.handler.service_handler
    class Service:
        @nexusrpc.handler.sync_operation_handler
        def sync_operation_handler(
            self, ctx: nexusrpc.handler.StartOperationContext, input: Input
        ) -> Output: ...

    expected_operations = {
        "sync_operation_handler": nexusrpc.contract.Operation._create(
            name="sync_operation_handler",
            input_type=Input,
            output_type=Output,
        ),
    }


class SyncOperationWithOperationNameOverride(_TestCase):
    @nexusrpc.handler.service_handler
    class Service:
        @nexusrpc.handler.sync_operation_handler(name="sync-operation-name")
        async def sync_operation_handler(
            self, ctx: nexusrpc.handler.StartOperationContext, input: Input
        ) -> Output: ...

    expected_operations = {
        "sync_operation_handler": nexusrpc.contract.Operation._create(
            name="sync-operation-name",
            input_type=Input,
            output_type=Output,
        ),
    }


class ManualOperationWithContract(_TestCase):
    @nexusrpc.contract.service
    class Contract:
        operation: nexusrpc.contract.Operation[Input, Output]

    @nexusrpc.handler.service_handler(service=Contract)
    class Service:
        @nexusrpc.handler.operation_handler
        def operation(self) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.contract.Operation._create(
            name="operation",
            input_type=Input,
            output_type=Output,
        ),
    }


class ManualOperationWithContractAndOperationNameOverride(_TestCase):
    @nexusrpc.contract.service
    class Contract:
        operation: nexusrpc.contract.Operation[Input, Output] = (
            nexusrpc.contract.Operation(
                name="operation-override",
            )
        )

    @nexusrpc.handler.service_handler(service=Contract)
    class Service:
        @nexusrpc.handler.operation_handler(name="operation-override")
        def operation(self) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.contract.Operation._create(
            name="operation-override",
            input_type=Input,
            output_type=Output,
        ),
    }


class SyncOperationWithCallableInstance(_TestCase):
    @nexusrpc.contract.service
    class Contract:
        sync_operation_with_callable_instance: nexusrpc.contract.Operation[
            Input, Output
        ]

    @nexusrpc.handler.service_handler(service=Contract)
    class Service:
        class CallableInstanceStartMethod:
            def __call__(
                self,
                _handler: Any,
                ctx: nexusrpc.handler.StartOperationContext,
                input: Input,
            ) -> Output: ...

        sync_operation_with_callable_instance = nexusrpc.handler.sync_operation_handler(
            CallableInstanceStartMethod(),
            name="sync_operation_with_callable_instance",
        )

    expected_operations = {
        "sync_operation_with_callable_instance": nexusrpc.contract.Operation._create(
            name="sync_operation_with_callable_instance",
            input_type=Input,
            output_type=Output,
        ),
    }


@pytest.mark.parametrize(
    "test_case",
    [
        ManualOperationHandler,
        ManualOperationHandlerWithNameOverride,
        SyncOperation,
        SyncOperationWithOperationNameOverride,
        ManualOperationWithContract,
        ManualOperationWithContractAndOperationNameOverride,
        # TODO(dan): make callable instances work. Input type is not inferred due to
        # signature differing from normal mathod. See also
        # SyncHandlerHappyPathWithNonAsyncCallableInstance in temporal tests.
        # SyncOperationWithCallableInstance,
    ],
)
@pytest.mark.asyncio
async def test_collected_operation_definitions(
    test_case: Type[_TestCase],
):
    service: nexusrpc.contract.Service = getattr(test_case.Service, "__nexus_service__")
    assert isinstance(service, nexusrpc.contract.Service)
    assert (
        service.name == "Service"
        if test_case.Contract is None
        else test_case.Contract.__nexus_service__.name  # type: ignore
    )
    for method_name, expected_op in test_case.expected_operations.items():
        actual_op = getattr(test_case.Service, method_name).__nexus_operation__
        assert isinstance(actual_op, nexusrpc.contract.Operation)
        assert actual_op.name == expected_op.name
        assert actual_op.input_type == expected_op.input_type
        assert actual_op.output_type == expected_op.output_type


def test_operation_without_decorator():
    class Service:
        def operation(self) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    with pytest.warns(
        UserWarning,
        match=r"Did you forget to apply the @nexusrpc.handler.operation_handler decorator\?",
    ):
        nexusrpc.handler.service_handler(Service)


def test_service_does_not_implement_operation_name():
    @nexusrpc.contract.service
    class Contract:
        operation_a: nexusrpc.contract.Operation[Input, Output]

    class Service:
        @nexusrpc.handler.operation_handler
        def operation_b(self) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    with pytest.raises(
        TypeError,
        match="does not implement operation 'operation_a' in interface",
    ):
        nexusrpc.handler.service_handler(service=Contract)(Service)
