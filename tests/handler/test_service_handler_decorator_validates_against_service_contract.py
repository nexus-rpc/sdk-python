from typing import Optional, Type

import pytest

import nexusrpc.contract
import nexusrpc.handler


class _InterfaceImplementationTestCase:
    Interface: Type
    Impl: Type
    error_message: Optional[str]


class ValidImpl(_InterfaceImplementationTestCase):
    @nexusrpc.contract.service
    class Interface:
        op: nexusrpc.contract.Operation[None, None]

        def unrelated_method(self) -> None: ...

    class Impl:
        @nexusrpc.handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc.handler.StartOperationContext, input: None
        ) -> None: ...

    error_message = None


class ValidImplWithEmptyInterfaceAndExtraOperation(_InterfaceImplementationTestCase):
    @nexusrpc.contract.service
    class Interface:
        pass

    class Impl:
        @nexusrpc.handler.sync_operation_handler
        async def extra_op(
            self, ctx: nexusrpc.handler.StartOperationContext, input: None
        ) -> None: ...

        def unrelated_method(self) -> None: ...

    error_message = None


class ValidImplWithoutTypeAnnotations(_InterfaceImplementationTestCase):
    @nexusrpc.contract.service
    class Interface:
        op: nexusrpc.contract.Operation[int, str]

    class Impl:
        @nexusrpc.handler.sync_operation_handler
        async def op(self, ctx, input): ...

    error_message = None


class MissingOperation(_InterfaceImplementationTestCase):
    @nexusrpc.contract.service
    class Interface:
        op: nexusrpc.contract.Operation[None, None]

    class Impl:
        pass

    error_message = "does not implement operation 'op'"


class MissingInputAnnotation(_InterfaceImplementationTestCase):
    @nexusrpc.contract.service
    class Interface:
        op: nexusrpc.contract.Operation[None, None]

    class Impl:
        @nexusrpc.handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc.handler.StartOperationContext, input
        ) -> None: ...

    error_message = None


class MissingOptionsAnnotation(_InterfaceImplementationTestCase):
    @nexusrpc.contract.service
    class Interface:
        op: nexusrpc.contract.Operation[None, None]

    class Impl:
        @nexusrpc.handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc.handler.StartOperationContext, input: str
        ) -> None: ...

    error_message = "does not match the input type"


class WrongOutputType(_InterfaceImplementationTestCase):
    @nexusrpc.contract.service
    class Interface:
        op: nexusrpc.contract.Operation[None, int]

    class Impl:
        @nexusrpc.handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc.handler.StartOperationContext, input: None
        ) -> str: ...

    error_message = "does not match the output type"


@pytest.mark.parametrize(
    "test_case",
    [
        ValidImpl,
        ValidImplWithEmptyInterfaceAndExtraOperation,
        ValidImplWithoutTypeAnnotations,
        MissingOperation,
        MissingInputAnnotation,
        MissingOptionsAnnotation,
        WrongOutputType,
    ],
)
def test_service_decorator_enforces_interface_implementation(
    test_case: Type[_InterfaceImplementationTestCase],
):
    if test_case.error_message:
        with pytest.raises(Exception) as ei:
            nexusrpc.handler.service_handler(service=test_case.Interface)(
                test_case.Impl
            )
        err = ei.value
        assert test_case.error_message in str(err)
    else:
        nexusrpc.handler.service_handler(service=test_case.Interface)(test_case.Impl)


# TODO(dan): duplicate test?
def test_service_does_not_implement_operation_name():
    @nexusrpc.contract.service
    class Contract:
        operation_a: nexusrpc.contract.Operation[None, None]

    class Service:
        @nexusrpc.handler.operation_handler
        def operation_b(self) -> nexusrpc.handler.OperationHandler[None, None]: ...

    with pytest.raises(
        TypeError,
        match="does not implement operation 'operation_a' in interface",
    ):
        nexusrpc.handler.service_handler(service=Contract)(Service)
