from pprint import pprint

import pytest

from nexusrpc import Operation, ServiceDefinition, service


class A1:
    a: Operation[int, int]


class A2(A1):
    b: Operation[int, int]


# https://docs.python.org/3/howto/annotations.html#accessing-the-annotations-dict-of-an-object-in-python-3-9-and-older


class B1:
    a: Operation[int, int] = Operation[int, int](name="a-name")


class B2(B1):
    b: Operation[int, int] = Operation[int, int](name="b-name")


class C1:
    a: Operation[int, int] = Operation[int, int](name="a-name")
    b: Operation[int, int] = Operation[int, int](name="b-name")


class C2(C1):
    pass


# ops = {name: nexusrpc.Operation[int, int] for name in op_names}
# service_cls = nexusrpc.service(type("ServiceContract", (), ops))


class D1:
    a: Operation[int, int]


d2_ops = {name: Operation[int, int] for name in ["b"]}

D2 = type("D2", (D1,), d2_ops)


@pytest.mark.parametrize("user_service", [A2, B2, C2, D2])
def test_user_service_definition_inheritance(user_service):
    print(f"\n\n{user_service.__name__}:")
    print("\n__annotations__")
    pprint(user_service.__annotations__)
    print("\n__dict__")
    pprint(user_service.__dict__)

    service_defn = getattr(service(user_service), "__nexus_service__", None)
    assert isinstance(service_defn, ServiceDefinition)
    assert set(service_defn.operations) == {"a", "b"}
