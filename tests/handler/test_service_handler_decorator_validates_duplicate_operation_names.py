from typing import Any, Type

import pytest

import nexusrpc.handler


class _TestCase:
    UserServiceHandler: Type[Any]
    expected_error_message: str


class DuplicateOperationName(_TestCase):
    class UserServiceHandler:
        @nexusrpc.handler.operation_handler(name="a")
        def op_1(self) -> nexusrpc.handler.OperationHandler[int, int]: ...

        @nexusrpc.handler.operation_handler(name="a")
        def op_2(self) -> nexusrpc.handler.OperationHandler[str, int]: ...

    expected_error_message = (
        "Operation 'a' in service 'UserServiceHandler' is defined multiple times."
    )


@pytest.mark.parametrize(
    "test_case",
    [
        DuplicateOperationName,
    ],
)
def test_service_handler_decorator(test_case: _TestCase):
    with pytest.raises(RuntimeError, match=test_case.expected_error_message):
        nexusrpc.handler.service_handler(test_case.UserServiceHandler)
