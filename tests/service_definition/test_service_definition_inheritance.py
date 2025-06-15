from pprint import pprint
from typing import Any, Type

import pytest

from nexusrpc import Operation, ServiceDefinition, service
from nexusrpc._util import get_annotations


class _TestCase:
    UserService: Type[Any]
    expected_operation_names: set[str]


class TypeAnnotationsOnly:
    class A1:
        a: Operation[int, int]

    class A2(A1):
        b: Operation[int, int]

    UserService = A2
    expected_operation_names = {"a", "b"}


# https://docs.python.org/3/howto/annotations.html#accessing-the-annotations-dict-of-an-object-in-python-3-9-and-older


class TypeAnnotationsWithValues:
    class A1:
        a: Operation[int, int] = Operation[int, int](name="a-name")

    class A2(A1):
        b: Operation[int, int] = Operation[int, int](name="b-name")

    UserService = A2
    expected_operation_names = {"a-name", "b-name"}


class TypeAnnotationsWithValuesAllFromParentClass:
    class A1:
        a: Operation[int, int] = Operation[int, int](name="a-name")
        b: Operation[int, int] = Operation[int, int](name="b-name")

    class A2(A1):
        pass

    UserService = A2
    expected_operation_names = {"a-name", "b-name"}


class TypeValuesOnly:
    class A1:
        a = Operation[int, int]

    UserService = A1
    expected_operation_names = {"a"}


class ChildClassSynthesizedWithTypeValues:
    class A1:
        a: Operation[int, int]

    A2 = type("A2", (A1,), {name: Operation[int, int] for name in ["b"]})

    UserService = A2
    expected_operation_names = {"a", "b"}


@pytest.mark.parametrize(
    "test_case",
    [
        TypeAnnotationsOnly,
        TypeAnnotationsWithValues,
        TypeAnnotationsWithValuesAllFromParentClass,
        TypeValuesOnly,
        ChildClassSynthesizedWithTypeValues,
    ],
)
def test_user_service_definition_inheritance(test_case: Type[_TestCase]):
    print(f"\n\n{test_case.UserService.__name__}:")
    print("\n__annotations__")
    pprint(get_annotations(test_case.UserService))
    print("\n__dict__")
    pprint(test_case.UserService.__dict__)

    service_defn = getattr(service(test_case.UserService), "__nexus_service__", None)
    assert isinstance(service_defn, ServiceDefinition)
    assert set(service_defn.operations) == test_case.expected_operation_names
