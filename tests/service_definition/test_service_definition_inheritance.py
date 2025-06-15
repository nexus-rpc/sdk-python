import sys
from pprint import pprint
from typing import Generic, TypeVar

import pytest

from nexusrpc import ServiceDefinition, service

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")


class Operation(Generic[InputT, OutputT]):
    pass


@service
class A1:
    a: Operation[int, int]


@service
class A2(A1):
    b: Operation[int, int]


print(sys.version, end="\n\n")
print("A2: child class with class attribute type annotations only")
print("\n__annotations__")
pprint(A2.__annotations__)
print("\n__dict__")
pprint(A2.__dict__)


# https://docs.python.org/3/howto/annotations.html#accessing-the-annotations-dict-of-an-object-in-python-3-9-and-older


@service
class B1:
    a: Operation[int, int] = Operation[int, int]()


@service
class B2(B1):
    b: Operation[int, int] = Operation[int, int]()


print("\n\nB2: child class with class attribute type annotations with values")
print("\n__annotations__")
pprint(B2.__annotations__)
print("\n__dict__")
pprint(B2.__dict__)


@service
class C1:
    a: Operation[int, int] = Operation[int, int]()
    b: Operation[int, int] = Operation[int, int]()


@service
class C2(C1):
    pass


print("\n\nC2: child class with class attribute type annotations with values")
print("\n__annotations__")
pprint(C2.__annotations__)
print("\n__dict__")
pprint(C2.__dict__)


# ops = {name: nexusrpc.Operation[int, int] for name in op_names}
# service_cls = nexusrpc.service(type("ServiceContract", (), ops))


@service
class D1:
    a: Operation[int, int]


d2_ops = {name: Operation[int, int] for name in ["b"]}

D2 = service(type("D2", (D1,), d2_ops))

print("\n\nD2: child class synthesized from class attribute type annotations only")
print("\n__annotations__")
pprint(D2.__annotations__)
print("\n__dict__")
pprint(D2.__dict__)


@pytest.mark.parametrize("user_service", [A2, B2, C2, D2])
def test_user_service_definition_inheritance(user_service):
    service_defn = getattr(user_service, "__nexus_service__", None)
    assert isinstance(service_defn, ServiceDefinition)
    assert set(service_defn.operations) == {"a", "b"}
