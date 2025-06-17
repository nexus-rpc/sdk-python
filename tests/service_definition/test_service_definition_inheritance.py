from pprint import pprint
from typing import Any, Optional, Type

import pytest

from nexusrpc import Operation, ServiceDefinition, service
from nexusrpc._util import get_annotations

# See https://docs.python.org/3/howto/annotations.html


class _TestCase:
    UserService: Type[Any]
    expected_operation_names: set[str]
    expected_error: Optional[str] = None


class TypeAnnotationsOnly(_TestCase):
    class A1:
        a: Operation[int, str]

    class A2(A1):
        b: Operation[int, str]

    UserService = A2
    expected_operation_names = {"a", "b"}


class TypeAnnotationsWithValues(_TestCase):
    class A1:
        a: Operation[int, str] = Operation[int, str](name="a-name")

    class A2(A1):
        b: Operation[int, str] = Operation[int, str](name="b-name")

    UserService = A2
    expected_operation_names = {"a-name", "b-name"}


class TypeAnnotationsWithValuesAllFromParentClass(_TestCase):
    # See https://docs.python.org/3/howto/annotations.html#accessing-the-annotations-dict-of-an-object-in-python-3-9-and-older
    # A2.__annotations__ returns annotations from parent
    class A1:
        a: Operation[int, str] = Operation[int, str](name="a-name")
        b: Operation[int, str] = Operation[int, str](name="b-name")

    class A2(A1):
        pass

    UserService = A2
    expected_operation_names = {"a-name", "b-name"}


class TypeAnnotationWithInheritedInstance(_TestCase):
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
    class A1:
        a: Operation[int, str]

    A2 = type("A2", (A1,), {name: Operation[int, str] for name in ["b"]})

    UserService = A2
    expected_error = "Did you accidentally use '=' instead of ':'"


# TODO: test mro is honored: that synonymous operation definition in child class wins
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
    print(f"\n\n{test_case.UserService.__name__}:")
    print("\n__annotations__")
    pprint(get_annotations(test_case.UserService))
    print("\n__dict__")
    pprint(test_case.UserService.__dict__)

    if test_case.expected_error:
        with pytest.raises(Exception, match=test_case.expected_error):
            service(test_case.UserService)
        return

    service_defn = getattr(service(test_case.UserService), "__nexus_service__", None)
    assert isinstance(service_defn, ServiceDefinition)
    assert set(service_defn.operations) == test_case.expected_operation_names
    for op in service_defn.operations.values():
        assert op.input_type == int
        assert op.output_type == str
