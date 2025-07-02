# It's important to test that retrieving annotation works both with and without `from __future__ import annotations`.
# Currently we do so by applying it in some test files and not others.
# See https://docs.python.org/3/howto/annotations.html#accessing-the-annotations-dict-of-an-object-in-python-3-9-and-older
from __future__ import annotations

from typing import Any, Optional, Type

import pytest

import nexusrpc
from nexusrpc import Operation, ServiceDefinition
from nexusrpc._util import get_service_definition

# See https://docs.python.org/3/howto/annotations.html


class _TestCase:
    UserService: Type[Any]
    expected_operation_names: set[str]
    expected_error: Optional[str] = None


class TypeAnnotationsOnly(_TestCase):
    @nexusrpc.service
    class A1:
        a: Operation[int, str]

    class A2(A1):
        b: Operation[int, str]

    UserService = A2
    expected_operation_names = {"a", "b"}


class TypeAnnotationsWithValues(_TestCase):
    @nexusrpc.service
    class A1:
        a: Operation[int, str] = Operation[int, str](name="a-name")

    class A2(A1):
        b: Operation[int, str] = Operation[int, str](name="b-name")

    UserService = A2
    expected_operation_names = {"a-name", "b-name"}


class TypeAnnotationsWithValuesAllFromParentClass(_TestCase):
    # See https://docs.python.org/3/howto/annotations.html#accessing-the-annotations-dict-of-an-object-in-python-3-9-and-older
    # A2.__annotations__ returns annotations from parent
    @nexusrpc.service
    class A1:
        a: Operation[int, str] = Operation[int, str](name="a-name")
        b: Operation[int, str] = Operation[int, str](name="b-name")

    class A2(A1):
        pass

    UserService = A2
    expected_operation_names = {"a-name", "b-name"}


class TypeAnnotationWithInheritedInstance(_TestCase):
    @nexusrpc.service
    class A1:
        a: Operation[int, str] = Operation[int, str](name="a-name")

    class A2(A1):
        a: Operation[int, str]

    UserService = A2
    expected_operation_names = {"a-name", "b-name"}


class InstanceWithoutTypeAnnotationIsAnError(_TestCase):
    class A1:
        a = Operation[int, str](name="a-name")

    UserService = A1
    expected_error = (
        "Operation 'a-name' has no input type, Operation 'a-name' has no output type"
    )


class InvalidUseOfTypeAsValue(_TestCase):
    class A1:
        a = Operation[int, str]

    UserService = A1
    expected_error = "Did you accidentally use '=' instead of ':'"


class ChildClassSynthesizedWithTypeValues(_TestCase):
    @nexusrpc.service
    class A1:
        a: Operation[int, str]

    A2 = type("A2", (A1,), {name: Operation[int, str] for name in ["b"]})

    UserService = A2
    expected_error = "Did you accidentally use '=' instead of ':'"


@pytest.mark.parametrize(
    "test_case",
    [
        TypeAnnotationsOnly,
        TypeAnnotationsWithValues,
        TypeAnnotationsWithValuesAllFromParentClass,
        InstanceWithoutTypeAnnotationIsAnError,
        InvalidUseOfTypeAsValue,
        ChildClassSynthesizedWithTypeValues,
    ],
)
def test_user_service_definition_inheritance(test_case: Type[_TestCase]):
    if test_case.expected_error:
        with pytest.raises(Exception, match=test_case.expected_error):
            nexusrpc.service(test_case.UserService)
        return

    service_defn = get_service_definition(nexusrpc.service(test_case.UserService))
    assert isinstance(service_defn, ServiceDefinition)
    assert set(service_defn.operations) == test_case.expected_operation_names
    for op in service_defn.operations.values():
        assert op.input_type is int
        assert op.output_type is str
