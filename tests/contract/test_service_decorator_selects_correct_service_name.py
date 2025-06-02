from typing import Type

import pytest

import nexusrpc.contract


class NameOverrideTestCase:
    Interface: Type
    expected_name: str


class NotCalled(NameOverrideTestCase):
    @nexusrpc.contract.service
    class Interface:
        pass

    expected_name = "Interface"


class CalledWithoutArgs(NameOverrideTestCase):
    @nexusrpc.contract.service()
    class Interface:
        pass

    expected_name = "Interface"


class CalledWithNameArg(NameOverrideTestCase):
    @nexusrpc.contract.service(name="my-service-interface-🌈")
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
    metadata = getattr(test_case.Interface, "__nexus_service__")
    assert metadata.name == test_case.expected_name


def test_name_must_not_be_empty():
    with pytest.raises(ValueError):
        nexusrpc.contract.service(name="")(object)
