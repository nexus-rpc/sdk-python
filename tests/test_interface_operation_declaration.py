from typing import Any, Type

import pytest

import nexusrpc.contract


class Output:
    pass


class OperationDeclarationTestCase:
    Interface: Type
    expected_ops: dict[str, tuple[Type[Any], Type[Any]]]


class OperationDeclarations(OperationDeclarationTestCase):
    @nexusrpc.contract.service
    class Interface:
        a: nexusrpc.contract.Operation[None, Output]
        b: nexusrpc.contract.Operation[int, str] = nexusrpc.contract.Operation(
            name="b-name"
        )

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
    test_case: Type[OperationDeclarationTestCase],
):
    metadata = getattr(test_case.Interface, "__nexus_service__")
    assert isinstance(metadata, nexusrpc.contract.Service)
    actual_ops = {
        op.name: (op.input_type, op.output_type)
        for op in test_case.Interface.__dict__.values()
        if isinstance(op, nexusrpc.contract.Operation)
    }
    assert actual_ops == test_case.expected_ops
