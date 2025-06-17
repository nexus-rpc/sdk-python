"""
Test that operation decorators result in operation factories that return the correct result.
"""

from dataclasses import dataclass
from typing import Any, Type, Union, cast

import pytest

import nexusrpc._service
import nexusrpc._handler
from nexusrpc.types import InputT, OutputT
from nexusrpc._handler._core import collect_operation_handler_factories
from nexusrpc._handler._util import is_async_callable


@dataclass
class _TestCase:
    Service: Type[Any]
    expected_operation_factories: dict[str, Any]


class ManualOperationDefinition(_TestCase):
    @nexusrpc._handler.service_handler
    class Service:
        @nexusrpc._handler.operation_handler
        def operation(self) -> nexusrpc._handler.OperationHandler[int, int]:
            class OpHandler(nexusrpc._handler.SyncOperationHandler[int, int]):
                async def start(
                    self, ctx: nexusrpc._handler.StartOperationContext, input: int
                ) -> nexusrpc._handler.StartOperationResultSync[int]:
                    return nexusrpc._handler.StartOperationResultSync(7)

            return OpHandler()

    expected_operation_factories = {"operation": 7}


class SyncOperation(_TestCase):
    @nexusrpc._handler.service_handler
    class Service:
        @nexusrpc._handler.sync_operation_handler
        def sync_operation_handler(
            self, ctx: nexusrpc._handler.StartOperationContext, input: int
        ) -> int:
            return 7

    expected_operation_factories = {"sync_operation_handler": 7}


@pytest.mark.parametrize(
    "test_case",
    [
        ManualOperationDefinition,
        SyncOperation,
    ],
)
@pytest.mark.asyncio
async def test_collected_operation_factories_match_service_definition(
    test_case: Type[_TestCase],
):
    service: nexusrpc.ServiceDefinition = getattr(
        test_case.Service, "__nexus_service__"
    )
    assert isinstance(service, nexusrpc.ServiceDefinition)
    assert service.name == "Service"
    operation_factories = collect_operation_handler_factories(
        test_case.Service, service
    )
    assert operation_factories.keys() == test_case.expected_operation_factories.keys()
    ctx = nexusrpc._handler.StartOperationContext(
        service="Service",
        operation="operation",
    )

    async def execute(
        op: nexusrpc._handler.OperationHandler[InputT, OutputT],
        ctx: nexusrpc._handler.StartOperationContext,
        input: InputT,
    ) -> Union[
        nexusrpc._handler.StartOperationResultSync[OutputT],
        nexusrpc._handler.StartOperationResultAsync,
    ]:
        if is_async_callable(op.start):
            return await op.start(ctx, input)
        else:
            return cast(
                nexusrpc._handler.StartOperationResultSync[OutputT],
                op.start(ctx, input),
            )

    for op_name, expected_result in test_case.expected_operation_factories.items():
        op_factory = operation_factories[op_name]
        op = op_factory(test_case.Service)
        result = await execute(op, ctx, 0)
        assert isinstance(result, nexusrpc._handler.StartOperationResultSync)
        assert result.value == expected_result
