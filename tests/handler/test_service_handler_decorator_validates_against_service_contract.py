from dataclasses import dataclass

import pytest

import nexusrpc
from nexusrpc.handler import (
    StartOperationContext,
    service_handler,
    sync_operation,
)


@dataclass()
class _InterfaceImplementationTestCase:
    Interface: type
    Impl: type
    error_message: str | None


class _InvalidInputTestCase(_InterfaceImplementationTestCase):
    error_message = "OperationHandler input type mismatch"


class _InvalidOutputTestCase(_InterfaceImplementationTestCase):
    error_message = "OperationHandler output type mismatch"


class _ValidTestCase(_InterfaceImplementationTestCase):
    error_message = None


class ValidImpl(_ValidTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

        def unrelated_method(self) -> None: ...

    class Impl:
        @sync_operation
        async def op(self, _ctx: StartOperationContext, _input: None) -> None: ...


class ValidImplWithEmptyInterfaceAndExtraOperation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        pass

    class Impl:
        @sync_operation
        async def extra_op(self, _ctx: StartOperationContext, _input: None) -> None: ...

        def unrelated_method(self) -> None: ...

    error_message = "does not match an operation method name in the service definition"


class ValidImplWithoutTypeAnnotations(_ValidTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[int, str]

    class Impl:
        @sync_operation
        async def op(self, ctx, input): ...  # type: ignore[reportMissingParameterType]


class MissingOperation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    class Impl:
        pass

    error_message = "does not implement an operation with method name 'op'"


class MissingInputAnnotation(_ValidTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input) -> None: ...  # type: ignore[reportMissingParameterType]


class MissingContextAnnotation(_ValidTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    class Impl:
        @sync_operation
        async def op(self, ctx, input: None) -> None: ...  # type: ignore[reportMissingParameterType]


class WrongOutputType(_InvalidOutputTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, int]

    class Impl:
        @sync_operation
        async def op(self, _ctx: StartOperationContext, _input: None) -> str: ...


class WrongOutputTypeWithNone(_InvalidOutputTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[str, None]

    class Impl:
        @sync_operation
        async def op(self, _ctx: StartOperationContext, _input: str) -> str: ...


class ValidImplWithNone(_ValidTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[str, None]

    class Impl:
        @sync_operation
        async def op(self, _ctx: StartOperationContext, _input: str) -> None: ...


class X:
    pass


class SuperClass:
    pass


class Subclass(SuperClass):
    pass


class OutputCovarianceImplOutputCannotBeSubclass(_InvalidOutputTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, SuperClass]

    class Impl:
        @sync_operation
        async def op(self, _ctx: StartOperationContext, _input: X) -> Subclass: ...


class OutputCovarianceImplOutputCannotBeStrictSuperclass(_InvalidOutputTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, Subclass]

    class Impl:
        @sync_operation
        async def op(self, _ctx: StartOperationContext, _input: X) -> SuperClass: ...


class InputContravarianceImplInputCannotBeSuperclass(_InvalidInputTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[Subclass, X]

    class Impl:
        @sync_operation
        async def op(self, _ctx: StartOperationContext, _input: SuperClass) -> X: ...


class InputContravarianceImplInputCannotBeSubclass(_InvalidInputTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[SuperClass, X]

    class Impl:
        @sync_operation
        async def op(self, _ctx: StartOperationContext, _input: Subclass) -> X: ...


@pytest.mark.parametrize(
    "test_case",
    [
        ValidImpl,
        ValidImplWithEmptyInterfaceAndExtraOperation,
        ValidImplWithoutTypeAnnotations,
        MissingOperation,
        MissingInputAnnotation,
        MissingContextAnnotation,
        WrongOutputType,
        WrongOutputTypeWithNone,
        ValidImplWithNone,
        OutputCovarianceImplOutputCannotBeSubclass,
        OutputCovarianceImplOutputCannotBeStrictSuperclass,
        InputContravarianceImplInputCannotBeSuperclass,
    ],
)
def test_service_decorator_enforces_interface_implementation(
    test_case: type[_InterfaceImplementationTestCase],
):
    if test_case.error_message:
        with pytest.raises(Exception) as ei:
            service_handler(service=test_case.Interface)(test_case.Impl)
        err = ei.value
        assert test_case.error_message in str(err)
    else:
        service_handler(service=test_case.Interface)(test_case.Impl)


# TODO(preview): duplicate test?
def test_service_does_not_implement_operation_name():
    @nexusrpc.service
    class Contract:
        operation_a: nexusrpc.Operation[None, None]

    class Service:
        @sync_operation
        async def operation_b(
            self, _ctx: StartOperationContext, _input: None
        ) -> None: ...

    with pytest.raises(
        TypeError,
        match="does not match an operation method name in the service definition",
    ):
        _ = service_handler(service=Contract)(Service)
