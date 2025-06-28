from typing import Type

import pytest

import nexusrpc
from nexusrpc._util import get_service_definition


class NameOverrideTestCase:
    Interface: Type
    expected_name: str


class NotCalled(NameOverrideTestCase):
    @nexusrpc.service
    class Interface:
        pass

    expected_name = "Interface"


class CalledWithoutArgs(NameOverrideTestCase):
    @nexusrpc.service()
    class Interface:
        pass

    expected_name = "Interface"


class CalledWithNameArg(NameOverrideTestCase):
    @nexusrpc.service(name="my-service-interface-🌈")
    class Interface:
        pass

    expected_name = "my-service-interface-🌈"


@pytest.mark.parametrize(
    "test_case",
    [
        NotCalled,
        CalledWithoutArgs,
        CalledWithNameArg,
    ],
)
def test_interface_name_overrides(test_case: Type[NameOverrideTestCase]):
    defn = get_service_definition(test_case.Interface)
    assert defn
    assert defn.name == test_case.expected_name


def test_name_must_not_be_empty():
    with pytest.raises(ValueError):
        nexusrpc.service(name="")(object)
