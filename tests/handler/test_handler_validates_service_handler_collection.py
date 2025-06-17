"""
Test that the Handler constructor processes the supplied collection of service handlers
correctly.
"""

import pytest

import nexusrpc._handler
from nexusrpc.asyncio.handler import Handler


def test_service_must_use_decorator():
    class Service:
        pass

    with pytest.raises(RuntimeError):
        Handler([Service()])


def test_services_are_collected():
    class OpHandler(nexusrpc._handler.SyncOperationHandler[int, int]):
        async def start(
            self,
            ctx: nexusrpc._handler.StartOperationContext,
            input: int,
        ) -> nexusrpc._handler.StartOperationResultSync[int]: ...

        async def cancel(
            self,
            ctx: nexusrpc._handler.CancelOperationContext,
            token: str,
        ) -> None: ...

    @nexusrpc._handler.service_handler
    class Service1:
        @nexusrpc._handler.operation_handler
        def op(self) -> nexusrpc._handler.OperationHandler[int, int]:
            return OpHandler()

    service_handlers = Handler([Service1()])
    assert service_handlers.service_handlers.keys() == {"Service1"}
    assert service_handlers.service_handlers["Service1"].service.name == "Service1"
    assert service_handlers.service_handlers["Service1"].operation_handlers.keys() == {
        "op"
    }


def test_service_names_must_be_unique():
    @nexusrpc._handler.service_handler(name="a")
    class Service1:
        pass

    @nexusrpc._handler.service_handler(name="a")
    class Service2:
        pass

    with pytest.raises(RuntimeError):
        Handler([Service1(), Service2()])
