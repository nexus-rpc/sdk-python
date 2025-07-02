"""
Test that operation decorators result in operation factories that return the correct result.
"""

from dataclasses import dataclass
from typing import Any, Type, Union, cast

import pytest

import nexusrpc
from nexusrpc import InputT, OutputT
from nexusrpc._util import get_service_definition, is_async_callable
from nexusrpc.handler import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    OperationHandler,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
    service_handler,
    sync_operation,
)
from nexusrpc.handler._core import collect_operation_handler_factories_by_method_name
from nexusrpc.handler._decorators import operation_handler


@dataclass
class _TestCase:
    Service: Type[Any]
    expected_operation_factories: dict[str, Any]


class ManualOperationDefinition(_TestCase):
    @service_handler
    class Service:
        @operation_handler
        def operation(self) -> OperationHandler[int, int]:
            class OpHandler(OperationHandler[int, int]):
                async def start(
                    self, ctx: StartOperationContext, input: int
                ) -> StartOperationResultSync[int]:
                    return StartOperationResultSync(7)

                def fetch_info(
                    self, ctx: FetchOperationInfoContext, token: str
                ) -> nexusrpc.OperationInfo:
                    raise NotImplementedError

                def fetch_result(
                    self, ctx: FetchOperationResultContext, token: str
                ) -> int:
                    raise NotImplementedError

                def cancel(self, ctx: CancelOperationContext, token: str) -> None:
                    raise NotImplementedError

            return OpHandler()

    expected_operation_factories = {"operation": 7}


class SyncOperation(_TestCase):
    @service_handler
    class Service:
        @sync_operation
        async def sync_operation_handler(
            self, ctx: StartOperationContext, input: int
        ) -> int:
            return 7

    expected_operation_factories = {"sync_operation_handler": 7}  # type: ignore


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
    service = get_service_definition(test_case.Service)
    assert isinstance(service, nexusrpc.ServiceDefinition)
    assert service.name == "Service"
    operation_factories = collect_operation_handler_factories_by_method_name(
        test_case.Service, service
    )
    assert operation_factories.keys() == test_case.expected_operation_factories.keys()
    ctx = StartOperationContext(
        service="Service",
        operation="operation",
        headers={},
        request_id="request_id",
    )

    async def execute(
        op: OperationHandler[InputT, OutputT],
        ctx: StartOperationContext,
        input: InputT,
    ) -> Union[
        StartOperationResultSync[OutputT],
        StartOperationResultAsync,
    ]:
        if is_async_callable(op.start):
            return await op.start(ctx, input)
        else:
            return cast(
                StartOperationResultSync[OutputT],
                op.start(ctx, input),
            )

    for op_name, expected_result in test_case.expected_operation_factories.items():
        op_factory = operation_factories[op_name]
        op = op_factory(test_case.Service)
        result = await execute(op, ctx, 0)
        assert isinstance(result, StartOperationResultSync)
        assert result.value == expected_result
