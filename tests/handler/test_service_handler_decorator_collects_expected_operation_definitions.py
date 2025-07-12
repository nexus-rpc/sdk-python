"""
Test that operation decorators result in operation definitions with the correct name and
input/ouput types.
"""

from dataclasses import dataclass
from typing import Any, Optional

import pytest

import nexusrpc
from nexusrpc._util import get_operation, get_service_definition
from nexusrpc.handler import (
    OperationHandler,
    service_handler,
)
from nexusrpc.handler._decorators import operation_handler


@dataclass
class Input:
    pass


@dataclass
class Output:
    pass


@dataclass
class _TestCase:
    Service: type[Any]
    expected_operations: dict[str, nexusrpc.Operation[Any, Any]]
    Contract: Optional[type[Any]] = None


class ManualOperationHandler(_TestCase):
    @service_handler
    class Service:
        @operation_handler
        def operation(self) -> OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.Operation(
            name="operation",
            method_name="operation",
            input_type=Input,
            output_type=Output,
        ),
    }


class ManualOperationHandlerWithNameOverride(_TestCase):
    @service_handler
    class Service:
        @operation_handler(name="operation-name")
        def operation(self) -> OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.Operation(
            name="operation-name",
            method_name="operation",
            input_type=Input,
            output_type=Output,
        ),
    }


class SyncOperation(_TestCase):
    @service_handler
    class Service:
        @operation_handler
        def sync_operation_handler(
            self,
        ) -> OperationHandler[Input, Output]: ...

    expected_operations = {
        "sync_operation_handler": nexusrpc.Operation(
            name="sync_operation_handler",
            method_name="sync_operation_handler",
            input_type=Input,
            output_type=Output,
        ),
    }


class SyncOperationWithOperationHandlerNameOverride(_TestCase):
    @service_handler
    class Service:
        @operation_handler(name="sync-operation-name")
        def sync_operation_handler(
            self,
        ) -> OperationHandler[Input, Output]: ...

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

    @service_handler(service=Contract)
    class Service:
        @operation_handler
        def operation(self) -> OperationHandler[Input, Output]: ...

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

    @service_handler(service=Contract)
    class Service:
        @operation_handler(name="operation-override")
        def operation(self) -> OperationHandler[Input, Output]: ...

    expected_operations = {
        "operation": nexusrpc.Operation(
            name="operation-override",
            method_name="operation",
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
    ],
)
@pytest.mark.asyncio
async def test_collected_operation_definitions(
    test_case: type[_TestCase],
):
    service = get_service_definition(test_case.Service)
    assert isinstance(service, nexusrpc.ServiceDefinition)
    if test_case.Contract:
        defn = get_service_definition(test_case.Contract)
        assert isinstance(defn, nexusrpc.ServiceDefinition)
        assert defn.name == service.name
    else:
        assert service.name == "Service"

    for method_name, expected_op in test_case.expected_operations.items():
        actual_op = get_operation(getattr(test_case.Service, method_name))
        assert isinstance(actual_op, nexusrpc.Operation)
        assert actual_op.name == expected_op.name
        assert actual_op.input_type == expected_op.input_type
        assert actual_op.output_type == expected_op.output_type
