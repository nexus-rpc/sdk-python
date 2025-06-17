from typing import Any, Optional, Type

import pytest

import nexusrpc
import nexusrpc._handler


class _InterfaceImplementationTestCase:
    Interface: Type
    Impl: Type
    error_message: Optional[str]


class ValidImpl(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

        def unrelated_method(self) -> None: ...

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: None
        ) -> None: ...

    error_message = None


class ValidImplWithEmptyInterfaceAndExtraOperation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        pass

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        async def extra_op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: None
        ) -> None: ...

        def unrelated_method(self) -> None: ...

    error_message = "does not match an operation method name in the service definition"


class ValidImplWithoutTypeAnnotations(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[int, str]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        async def op(self, ctx, input): ...

    error_message = None


class MissingOperation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    class Impl:
        pass

    error_message = "does not implement operation 'op'"


class MissingInputAnnotation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input
        ) -> None: ...

    error_message = None


class MissingOptionsAnnotation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: str
        ) -> None: ...

    error_message = "is not compatible with the input type"


class WrongOutputType(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, int]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: None
        ) -> str: ...

    error_message = "is not compatible with the output type"


class WrongOutputTypeWithNone(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[str, None]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: str
        ) -> str: ...

    error_message = "is not compatible with the output type"


class ValidImplWithNone(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[str, None]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: str
        ) -> None: ...

    error_message = None


class MoreSpecificImplAllowed(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[Any, Any]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        async def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: str
        ) -> str: ...

    error_message = None


class X:
    pass


class SuperClass:
    pass


class Subclass(SuperClass):
    pass


class OutputCovarianceImplOutputCanBeSameType(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, X]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        def op(self, ctx: nexusrpc._handler.StartOperationContext, input: X) -> X: ...

    error_message = None


class OutputCovarianceImplOutputCanBeSubclass(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, SuperClass]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: X
        ) -> Subclass: ...

    error_message = None


class OutputCovarianceImplOutputCannnotBeStrictSuperclass(
    _InterfaceImplementationTestCase
):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, Subclass]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: X
        ) -> SuperClass: ...

    error_message = "is not compatible with the output type"


class InputContravarianceImplInputCanBeSameType(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, X]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        def op(self, ctx: nexusrpc._handler.StartOperationContext, input: X) -> X: ...

    error_message = None


class InputContravarianceImplInputCanBeSuperclass(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[Subclass, X]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: SuperClass
        ) -> X: ...

    error_message = None


class InputContravarianceImplInputCannotBeSubclass(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[SuperClass, X]

    class Impl:
        @nexusrpc._handler.sync_operation_handler
        def op(
            self, ctx: nexusrpc._handler.StartOperationContext, input: Subclass
        ) -> X: ...

    error_message = "is not compatible with the input type"


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
        WrongOutputTypeWithNone,
        ValidImplWithNone,
        MoreSpecificImplAllowed,
        OutputCovarianceImplOutputCanBeSameType,
        OutputCovarianceImplOutputCanBeSubclass,
        OutputCovarianceImplOutputCannnotBeStrictSuperclass,
        InputContravarianceImplInputCanBeSameType,
        InputContravarianceImplInputCanBeSuperclass,
        # ValidSubtyping,
        # InvalidOutputSupertype,
        # InvalidInputSubtype,
    ],
)
def test_service_decorator_enforces_interface_implementation(
    test_case: Type[_InterfaceImplementationTestCase],
):
    if test_case.error_message:
        with pytest.raises(Exception) as ei:
            nexusrpc._handler.service_handler(service=test_case.Interface)(
                test_case.Impl
            )
        err = ei.value
        assert test_case.error_message in str(err)
    else:
        nexusrpc._handler.service_handler(service=test_case.Interface)(test_case.Impl)


# TODO(preview): duplicate test?
def test_service_does_not_implement_operation_name():
    @nexusrpc.service
    class Contract:
        operation_a: nexusrpc.Operation[None, None]

    class Service:
        @nexusrpc._handler.operation_handler
        def operation_b(self) -> nexusrpc._handler.OperationHandler[None, None]: ...

    with pytest.raises(
        TypeError,
        match="does not match an operation method name in the service definition",
    ):
        nexusrpc._handler.service_handler(service=Contract)(Service)
