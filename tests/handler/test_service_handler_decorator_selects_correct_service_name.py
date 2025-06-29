from typing import Optional, Type

import pytest

import nexusrpc
from nexusrpc._util import get_service_definition
from nexusrpc.handler import service_handler


@nexusrpc.service
class ServiceInterface:
    pass


@nexusrpc.service(name="Service-With-Name-Override")
class ServiceInterfaceWithNameOverride:
    pass


class _NameOverrideTestCase:
    ServiceImpl: Type
    expected_name: str
    expected_error: Optional[Type[Exception]] = None


class NotCalled(_NameOverrideTestCase):
    @service_handler
    class ServiceImpl:
        pass

    expected_name = "ServiceImpl"


class CalledWithoutArgs(_NameOverrideTestCase):
    @service_handler()
    class ServiceImpl:
        pass

    expected_name = "ServiceImpl"


class CalledWithNameArg(_NameOverrideTestCase):
    @service_handler(name="my-service-impl-🌈")
    class ServiceImpl:
        pass

    expected_name = "my-service-impl-🌈"


class CalledWithInterface(_NameOverrideTestCase):
    @service_handler(service=ServiceInterface)
    class ServiceImpl:
        pass

    expected_name = "ServiceInterface"


class CalledWithInterfaceWithNameOverride(_NameOverrideTestCase):
    @service_handler(service=ServiceInterfaceWithNameOverride)
    class ServiceImpl:
        pass

    expected_name = "Service-With-Name-Override"


@pytest.mark.parametrize(
    "test_case",
    [
        NotCalled,
        CalledWithoutArgs,
        CalledWithNameArg,
        CalledWithInterface,
        CalledWithInterfaceWithNameOverride,
    ],
)
def test_service_decorator_name_overrides(test_case: Type[_NameOverrideTestCase]):
    service = get_service_definition(test_case.ServiceImpl)
    assert isinstance(service, nexusrpc.ServiceDefinition)
    assert service.name == test_case.expected_name


def test_name_must_not_be_empty():
    with pytest.raises(ValueError):
        service_handler(name="")(object)


def test_name_and_interface_are_mutually_exclusive():
    with pytest.raises(ValueError):
        # Type error due to deliberately violating overload
        service_handler(name="my-service-impl-🌈", service=ServiceInterface)  # type: ignore
