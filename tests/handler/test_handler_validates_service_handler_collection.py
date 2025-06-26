"""
Test that the Handler constructor processes the supplied collection of service handlers
correctly.
"""

import pytest

from nexusrpc import OperationInfo
from nexusrpc.handler import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    Handler,
    OperationHandler,
    StartOperationContext,
    StartOperationResultSync,
    service_handler,
)
from nexusrpc.handler._decorators import operation_handler


def test_service_must_use_decorator():
    class Service:
        pass

    with pytest.raises(RuntimeError):
        Handler([Service()])


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

        async def fetch_info(
            self,
            ctx: FetchOperationInfoContext,
            token: str,
        ) -> OperationInfo: ...

        async def fetch_result(
            self,
            ctx: FetchOperationResultContext,
            token: str,
        ) -> int: ...

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
        Handler([Service1(), Service2()])
