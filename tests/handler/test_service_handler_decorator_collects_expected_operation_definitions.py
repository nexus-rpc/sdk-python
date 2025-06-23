"""
Test that operation decorators result in operation definitions with the correct name and
input/ouput types.
"""

from dataclasses import dataclass
from typing import Any, Optional, Type

import pytest

import nexusrpc._service
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
    expected_operations: dict[str, nexusrpc.Operation]
    Contract: Optional[Type[nexusrpc.ServiceDefinition]] = None
    skip: Optional[str] = None


class ManualOperationHandler(_TestCase):
    @nexusrpc.handler.service_handler
    class Service:
        @nexusrpc.handler.operation_handler
        def operation(self) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.Operation(
            name="operation",
            method_name="operation",
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
        "operation": nexusrpc.Operation(
            name="operation-name",
            method_name="operation",
            input_type=Input,
            output_type=Output,
        ),
    }


class SyncOperation(_TestCase):
    @nexusrpc.handler.service_handler
    class Service:
        @nexusrpc.handler.operation_handler
        def sync_operation_handler(
            self,
        ) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    expected_operations = {
        "sync_operation_handler": nexusrpc.Operation(
            name="sync_operation_handler",
            method_name="sync_operation_handler",
            input_type=Input,
            output_type=Output,
        ),
    }


class SyncOperationWithOperationHandlerNameOverride(_TestCase):
    @nexusrpc.handler.service_handler
    class Service:
        @nexusrpc.handler.operation_handler(name="sync-operation-name")
        def sync_operation_handler(
            self,
        ) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    expected_operations = {
        "sync_operation_handler": nexusrpc.Operation(
            name="sync-operation-name",
            method_name="sync_operation_handler",
            input_type=Input,
            output_type=Output,
        ),
    }


class ManualOperationWithContract(_TestCase):
    @nexusrpc.service
    class Contract:
        operation: nexusrpc.Operation[Input, Output]

    @nexusrpc.handler.service_handler(service=Contract)
    class Service:
        @nexusrpc.handler.operation_handler
        def operation(self) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.Operation(
            name="operation",
            method_name="operation",
            input_type=Input,
            output_type=Output,
        ),
    }


class ManualOperationWithContractNameOverrideAndOperationHandlerNameOverride(_TestCase):
    @nexusrpc.service
    class Contract:
        operation: nexusrpc.Operation[Input, Output] = nexusrpc.Operation(
            name="operation-override",
        )

    @nexusrpc.handler.service_handler(service=Contract)
    class Service:
        @nexusrpc.handler.operation_handler(name="operation-override")
        def operation(self) -> nexusrpc.handler.OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.Operation(
            name="operation-override",
            method_name="operation",
            input_type=Input,
            output_type=Output,
        ),
    }


if False:

    class SyncOperationWithCallableInstance(_TestCase):
        skip = "TODO(prerelease): update this test after decorator change"

        @nexusrpc.service
        class Contract:
            sync_operation_with_callable_instance: nexusrpc.Operation[Input, Output]

        @nexusrpc.handler.service_handler(service=Contract)
        class Service:
            class sync_operation_with_callable_instance:
                def __call__(
                    self,
                    _handler: Any,
                    ctx: nexusrpc.handler.StartOperationContext,
                    input: Input,
                ) -> Output: ...

            # TODO(preview): improve the DX here. The decorator cannot be placed on the
            # callable class itself, because the user must be responsible for instantiating
            # the class to obtain the callable instance.
            sync_operation_with_callable_instance = nexusrpc.handler.operation_handler(
                sync_operation_with_callable_instance()  # type: ignore
            )

        expected_operations = {
            "sync_operation_with_callable_instance": nexusrpc.Operation(
                name="sync_operation_with_callable_instance",
                method_name="CallableInstanceStartMethod",
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
        SyncOperationWithOperationHandlerNameOverride,
        ManualOperationWithContract,
        ManualOperationWithContractNameOverrideAndOperationHandlerNameOverride,
        # TODO(prerelease): make callable instances work. Input type is not inferred due to
        # signature differing from normal mathod. See also
        # SyncHandlerHappyPathWithNonAsyncCallableInstance in temporal tests.
        # SyncOperationWithCallableInstance,
    ],
)
@pytest.mark.asyncio
async def test_collected_operation_definitions(
    test_case: Type[_TestCase],
):
    if test_case.skip:
        pytest.skip(test_case.skip)

    service: nexusrpc.ServiceDefinition = getattr(
        test_case.Service, "__nexus_service__"
    )
    assert isinstance(service, nexusrpc.ServiceDefinition)
    assert (
        service.name == "Service"
        if test_case.Contract is None
        else test_case.Contract.__nexus_service__.name  # type: ignore
    )
    for method_name, expected_op in test_case.expected_operations.items():
        actual_op = getattr(test_case.Service, method_name).__nexus_operation__
        assert isinstance(actual_op, nexusrpc.Operation)
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
