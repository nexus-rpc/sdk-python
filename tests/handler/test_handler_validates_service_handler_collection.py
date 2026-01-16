"""
Test that the Handler constructor processes the supplied collection of service handlers
correctly.
"""

import pytest

from nexusrpc.handler import (
    CancelOperationContext,
    Handler,
    OperationHandler,
    StartOperationContext,
    StartOperationResultSync,
    service_handler,
    operation_handler,
)
from nexusrpc import LazyValue, HandlerError, service, Operation, get_service_definition
from nexusrpc.handler._decorators import sync_operation
from tests.helpers import DummySerializer, TestOperationTaskCancellation


def test_service_must_use_decorator():
    class Service:
        pass

    with pytest.raises(RuntimeError):
        _ = Handler([Service()])


def test_services_are_collected():
    class OpHandler(OperationHandler[int, int]):
        async def start(
            self,
            ctx: StartOperationContext,
            input: int,
        ) -> StartOperationResultSync[int]: ...

        async def cancel(
            self,
            ctx: CancelOperationContext,
            token: str,
        ) -> None: ...

    @service_handler
    class Service1:
        @operation_handler
        def op(self) -> OperationHandler[int, int]:
            return OpHandler()

    service_handlers = Handler([Service1()])
    assert service_handlers.service_handlers.keys() == {"Service1"}
    assert service_handlers.service_handlers["Service1"].service.name == "Service1"
    assert service_handlers.service_handlers["Service1"].operation_handlers.keys() == {
        "op"
    }


def test_service_names_must_be_unique():
    @service_handler(name="a")
    class Service1:
        pass

    @service_handler(name="a")
    class Service2:
        pass

    with pytest.raises(RuntimeError):
        _ = Handler([Service1(), Service2()])


@pytest.mark.asyncio
async def test_operations_must_have_decorator():
    @service_handler
    class TestService:
        async def op(self, _ctx: StartOperationContext, input: str) -> str:
            return input

    handler = Handler([TestService()])

    with pytest.raises(HandlerError, match="has no operation 'op'"):
        _ = await handler.start_operation(
            StartOperationContext(
                service=TestService.__name__,
                operation=TestService.op.__name__,
                headers={},
                request_id="test-req",
                task_cancellation=TestOperationTaskCancellation(),
                request_deadline=None,
                callback_url=None,
            ),
            LazyValue(
                serializer=DummySerializer(value="test"),
                headers={},
                stream=None,
            ),
        )


def test_covariance():
    class Foo:
        def __init__(self) -> None:
            self.name = "foo"

    class Bar(Foo):
        def __init__(self) -> None:
            super().__init__()
            self.name = "bar"

    @service
    class CovariantService:
        op_handler: Operation[None, Foo]
        inline: Operation[None, Foo]

    class ValidOperationHandler(OperationHandler[None, Foo]):
        async def start(
            self, ctx: StartOperationContext, input: None
        ) -> StartOperationResultSync[Bar]:
            return StartOperationResultSync(Bar())

        async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
            pass

    class InvalidOperationHandler(OperationHandler[None, Bar]):
        async def start(
            self, ctx: StartOperationContext, input: None
        ) -> StartOperationResultSync[Bar]:
            return StartOperationResultSync(Bar())

        async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
            pass

    # This service returns Bar but keeps the operation type as Foo
    @service_handler(service=CovariantService)
    class CovariantServiceImplValid:
        @operation_handler
        def op_handler(self) -> OperationHandler[None, Foo]:
            return ValidOperationHandler()

        @sync_operation
        async def inline(self, ctx: StartOperationContext, input: None) -> Foo:
            return Bar()

    with pytest.raises(TypeError):
        # This impl changes the output type in an obviously invalid way
        # it raises as appropriate
        @service_handler(service=CovariantService)
        class ServiceImplInvalid:
            @operation_handler
            def op_handler(self) -> OperationHandler[None, Bar]:
                return InvalidOperationHandler()

            @sync_operation
            async def inline(self, ctx: StartOperationContext, input: None) -> int:
                return 1

    with pytest.raises(TypeError):
        # This impl changes the output type to Bar instead of Foo for both operations
        # it does not raise, though this would in Java/.Net
        @service_handler(service=CovariantService)
        class CovariantServiceImplInvalid:
            @operation_handler
            def op_handler(self) -> OperationHandler[None, Bar]:
                return InvalidOperationHandler()

            @sync_operation
            async def inline(self, ctx: StartOperationContext, input: None) -> Bar:
                return Bar()
