"""
Test runtime behavior of operation handlers invoked through Handler.start_operation().

This file tests actual execution behavior, distinct from:
- Decoration-time validation (test_service_handler_decorator_validates_against_service_contract.py)
- Handler constructor validation (test_handler_validates_service_handler_collection.py)
"""

import pytest

from nexusrpc import LazyValue, Operation, service
from nexusrpc.handler import (
    CancelOperationContext,
    Handler,
    OperationHandler,
    StartOperationContext,
    StartOperationResultSync,
    operation_handler,
    service_handler,
)
from nexusrpc.handler._decorators import sync_operation
from tests.helpers import DummySerializer, TestOperationTaskCancellation


@pytest.mark.asyncio
async def test_handler_can_return_covariant_type():
    class Superclass:
        pass

    class Subclass(Superclass):
        pass

    @service
    class CovariantService:
        op_handler: Operation[None, Superclass]
        inline: Operation[None, Superclass]

    class ValidOperationHandler(OperationHandler[None, Superclass]):
        async def start(
            self, ctx: StartOperationContext, input: None
        ) -> StartOperationResultSync[Subclass]:
            return StartOperationResultSync(Subclass())

        async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
            pass

    @service_handler(service=CovariantService)
    class CovariantServiceHandler:
        @operation_handler
        def op_handler(self) -> OperationHandler[None, Superclass]:
            return ValidOperationHandler()

        @sync_operation
        async def inline(self, ctx: StartOperationContext, input: None) -> Superclass:  # pyright: ignore[reportUnusedParameter]
            return Subclass()

    handler = Handler([CovariantServiceHandler()])

    result = await handler.start_operation(
        StartOperationContext(
            service=CovariantService.__name__,
            operation=CovariantService.op_handler.name,
            headers={},
            request_id="test-req",
            task_cancellation=TestOperationTaskCancellation(),
            request_deadline=None,
            callback_url=None,
        ),
        LazyValue(
            serializer=DummySerializer(None),
            headers={},
            stream=None,
        ),
    )
    assert type(result) is StartOperationResultSync
    assert type(result.value) is Subclass

    result = await handler.start_operation(
        StartOperationContext(
            service=CovariantService.__name__,
            operation=CovariantService.inline.name,
            headers={},
            request_id="test-req",
            task_cancellation=TestOperationTaskCancellation(),
            request_deadline=None,
            callback_url=None,
        ),
        LazyValue(
            serializer=DummySerializer(None),
            headers={},
            stream=None,
        ),
    )
    assert type(result) is StartOperationResultSync
    assert type(result.value) is Subclass
