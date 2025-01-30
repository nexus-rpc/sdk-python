import pytest

import nexusrpc
from nexusrpc.handler import Handler


def test_service_must_use_decorator():
    class Service:
        pass

    with pytest.raises(RuntimeError):
        Handler([Service()])


def test_services_are_collected():
    @nexusrpc.handler.service_handler
    class Service1:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[int, int]: ...

    service_handlers = Handler([Service1()])
    assert service_handlers.service_handlers.keys() == {"Service1"}
    assert service_handlers.service_handlers["Service1"].name == "Service1"
    assert service_handlers.service_handlers["Service1"].operation_handlers.keys() == {
        "op"
    }


def test_service_names_must_be_unique():
    @nexusrpc.handler.service_handler(name="a")
    class Service1:
        pass

    @nexusrpc.handler.service_handler(name="a")
    class Service2:
        pass

    with pytest.raises(RuntimeError):
        Handler([Service1(), Service2()])
