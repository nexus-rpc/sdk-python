import pytest
from typing_extensions import dataclass_transform

import nexusrpc


class Output:
    pass


@dataclass_transform()
class _BaseTestCase:
    pass


class _TestCase(_BaseTestCase):
    Contract: type
    expected_error: Exception


class DuplicateOperationNameOverride(_TestCase):
    class Contract:
        a: nexusrpc.Operation[None, Output] = nexusrpc.Operation(name="a")
        b: nexusrpc.Operation[int, str] = nexusrpc.Operation(name="a")

    expected_error = RuntimeError(
        r"Operation 'a' in service .* is defined multiple times"
    )


@pytest.mark.parametrize(
    "test_case",
    [
        DuplicateOperationNameOverride,
    ],
)
def test_operation_validation(
    test_case: type[_TestCase],
):
    with pytest.raises(
        type(test_case.expected_error),
        match=str(test_case.expected_error),
    ):
        nexusrpc.service(test_case.Contract)
