from typing import Any

import pytest
from typing_extensions import dataclass_transform

import nexusrpc
from nexusrpc._util import get_service_definition


class Output:
    pass


@dataclass_transform()
class _BaseTestCase:
    pass


class OperationDeclarationTestCase(_BaseTestCase):
    Interface: type
    expected_ops: dict[str, tuple[type[Any], type[Any]]]


class OperationDeclarations(OperationDeclarationTestCase):
    @nexusrpc.service
    class Interface:
        a: nexusrpc.Operation[None, Output]
        b: nexusrpc.Operation[int, str] = nexusrpc.Operation(name="b-name")

    expected_ops = {
        "a": (type(None), Output),
        "b-name": (int, str),
    }


@pytest.mark.parametrize(
    "test_case",
    [
        OperationDeclarations,
    ],
)
def test_interface_operation_declarations(
    test_case: type[OperationDeclarationTestCase],
):
    defn = get_service_definition(test_case.Interface)
    assert isinstance(defn, nexusrpc.ServiceDefinition)
    actual_ops = {
        op.name: (op.input_type, op.output_type)
        for op in test_case.Interface.__dict__.values()
        if isinstance(op, nexusrpc.Operation)
    }
    assert actual_ops == test_case.expected_ops
