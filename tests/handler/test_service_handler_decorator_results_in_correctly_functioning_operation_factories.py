"""
Test that operation decorators result in operation factories that return the correct result.
"""

from dataclasses import dataclass
from typing import Any, Type, Union, cast

import pytest

import nexusrpc._service
import nexusrpc.handler
from nexusrpc.handler._core import collect_operation_handler_factories
from nexusrpc.handler._operation_handler import SyncOperationHandler
from nexusrpc.handler._util import is_async_callable
from nexusrpc.types import InputT, OutputT


@dataclass
class _TestCase:
    Service: Type[Any]
    expected_operation_factories: dict[str, Any]


class ManualOperationDefinition(_TestCase):
    @nexusrpc.handler.service_handler
    class Service:
        @nexusrpc.handler.operation_handler
        def operation(self) -> nexusrpc.handler.OperationHandler[int, int]:
            class OpHandler(nexusrpc.handler.OperationHandler[int, int]):
                async def start(
                    self, ctx: nexusrpc.handler.StartOperationContext, input: int
                ) -> nexusrpc.handler.StartOperationResultSync[int]:
                    return nexusrpc.handler.StartOperationResultSync(7)

                def fetch_info(
                    self, ctx: nexusrpc.handler.FetchOperationInfoContext, token: str
                ) -> nexusrpc.OperationInfo:
                    raise NotImplementedError

                def fetch_result(
                    self, ctx: nexusrpc.handler.FetchOperationResultContext, token: str
                ) -> int:
                    raise NotImplementedError

                def cancel(
                    self, ctx: nexusrpc.handler.CancelOperationContext, token: str
                ) -> None:
                    raise NotImplementedError

            return OpHandler()

    expected_operation_factories = {"operation": 7}


class SyncOperation(_TestCase):
    @nexusrpc.handler.service_handler
    class Service:
        @nexusrpc.handler.operation_handler
        def sync_operation_handler(self) -> nexusrpc.handler.OperationHandler[int, int]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: int
            ) -> int:
                return 7

            return SyncOperationHandler.from_callable(start)

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
    ctx = nexusrpc.handler.StartOperationContext(
        service="Service",
        operation="operation",
    )

    async def execute(
        op: nexusrpc.handler.OperationHandler[InputT, OutputT],
        ctx: nexusrpc.handler.StartOperationContext,
        input: InputT,
    ) -> Union[
        nexusrpc.handler.StartOperationResultSync[OutputT],
        nexusrpc.handler.StartOperationResultAsync,
    ]:
        if is_async_callable(op.start):
            return await op.start(ctx, input)
        else:
            return cast(
                nexusrpc.handler.StartOperationResultSync[OutputT],
                op.start(ctx, input),
            )

    for op_name, expected_result in test_case.expected_operation_factories.items():
        op_factory = operation_factories[op_name]
        op = op_factory(test_case.Service)
        result = await execute(op, ctx, 0)
        assert isinstance(result, nexusrpc.handler.StartOperationResultSync)
        assert result.value == expected_result
