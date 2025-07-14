from typing import Any

import pytest
from typing_extensions import dataclass_transform

from nexusrpc.handler import (
    OperationHandler,
    service_handler,
)
from nexusrpc.handler._decorators import operation_handler


@dataclass_transform()
class _BaseTestCase:
    pass


class _TestCase(_BaseTestCase):
    UserServiceHandler: type[Any]
    expected_error_message: str


class DuplicateOperationName(_TestCase):
    class UserServiceHandler:
        @operation_handler(name="a")
        def op_1(self) -> OperationHandler[int, int]: ...

        @operation_handler(name="a")
        def op_2(self) -> OperationHandler[str, int]: ...

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
        service_handler(test_case.UserServiceHandler)
