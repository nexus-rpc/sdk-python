from typing import Type

import pytest

import nexusrpc


class Output:
    pass


class _TestCase:
    Contract: Type
    expected_error: Exception


class DuplicateOperationNameOverride(_TestCase):
    class Contract:
        a: nexusrpc.Operation[None, Output] = nexusrpc.Operation(name="a")
        b: nexusrpc.Operation[int, str] = nexusrpc.Operation(name="a")

    expected_error = ValueError(r"Operation 'a' in class .* is defined multiple times")


@pytest.mark.parametrize(
    "test_case",
    [
        DuplicateOperationNameOverride,
    ],
)
def test_operation_validation(
    test_case: Type[_TestCase],
):
    with pytest.raises(
        type(test_case.expected_error),
        match=str(test_case.expected_error),
    ):
        nexusrpc.service(test_case.Contract)
