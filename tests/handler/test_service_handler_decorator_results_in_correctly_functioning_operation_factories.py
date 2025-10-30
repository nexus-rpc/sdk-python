"""
Test that operation decorators result in operation factories that return the correct result.
"""

from typing import Any, Union, cast

import pytest
from typing_extensions import dataclass_transform

import nexusrpc
from nexusrpc import InputT, OutputT
from nexusrpc._util import get_service_definition, is_async_callable
from nexusrpc.handler import (
    CancelOperationContext,
    OperationHandler,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
    service_handler,
    sync_operation,
)
from nexusrpc.handler._decorators import operation_handler
from nexusrpc.handler._operation_handler import (
    collect_operation_handler_factories_by_method_name,
)
from tests.helpers import TestOperationTaskCancellation


@dataclass_transform()
class _BaseTestCase:
    pass


class _TestCase(_BaseTestCase):
    Service: type[Any]
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

                def cancel(self, ctx: CancelOperationContext, token: str) -> None:
                    raise NotImplementedError

            return OpHandler()

    expected_operation_factories = {"operation": 7}


class SyncOperation(_TestCase):
    @service_handler
    class Service:
        @sync_operation
        async def sync_operation_handler(
            self, _ctx: StartOperationContext, _input: int
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
    test_case: type[_TestCase],
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
        task_cancellation=TestOperationTaskCancellation(),
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
