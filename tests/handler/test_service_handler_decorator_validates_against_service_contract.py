from typing import Any, Optional, Type

import pytest

import nexusrpc
import nexusrpc.handler
from nexusrpc.handler._operation_handler import SyncOperationHandler


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
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[None, None]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: None
            ) -> None: ...

            return SyncOperationHandler(start)

    error_message = None


class ValidImplWithEmptyInterfaceAndExtraOperation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        pass

    class Impl:
        @nexusrpc.handler.operation_handler
        def extra_op(self) -> nexusrpc.handler.OperationHandler[None, None]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: None
            ) -> None: ...

            return SyncOperationHandler(start)

        def unrelated_method(self) -> None: ...

    error_message = "does not match an operation method name in the service definition"


class ValidImplWithoutTypeAnnotations(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[int, str]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self):
            async def start(ctx, input): ...

            return SyncOperationHandler(start)

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
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[None, None]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input
            ) -> None: ...

            return SyncOperationHandler(start)

    error_message = None


class MissingOptionsAnnotation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[str, None]:
            async def start(
                # TODO(prerelease) isn't this supposed to be missing the ctx annotation?
                ctx: nexusrpc.handler.StartOperationContext,
                input: str,
            ) -> None: ...

            return SyncOperationHandler(start)

    error_message = "is not compatible with the input type"


class WrongOutputType(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, int]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[None, str]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: None
            ) -> str: ...

            return SyncOperationHandler(start)

    error_message = "is not compatible with the output type"


class WrongOutputTypeWithNone(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[str, None]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[str, str]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: str
            ) -> str: ...

            return SyncOperationHandler(start)

    error_message = "is not compatible with the output type"


class ValidImplWithNone(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[str, None]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[str, None]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: str
            ) -> None: ...

            return SyncOperationHandler(start)

    error_message = None


class MoreSpecificImplAllowed(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[Any, Any]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[str, str]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: str
            ) -> str: ...

            return SyncOperationHandler(start)

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
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[X, X]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: X
            ) -> X: ...

            return SyncOperationHandler(start)

    error_message = None


class OutputCovarianceImplOutputCanBeSubclass(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, SuperClass]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[X, Subclass]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: X
            ) -> Subclass: ...

            return SyncOperationHandler(start)

    error_message = None


class OutputCovarianceImplOutputCannnotBeStrictSuperclass(
    _InterfaceImplementationTestCase
):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, Subclass]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[X, SuperClass]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: X
            ) -> SuperClass: ...

            return SyncOperationHandler(start)

    error_message = "is not compatible with the output type"


class InputContravarianceImplInputCanBeSameType(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, X]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[X, X]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: X
            ) -> X: ...

            return SyncOperationHandler(start)

    error_message = None


class InputContravarianceImplInputCanBeSuperclass(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[Subclass, X]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[SuperClass, X]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: SuperClass
            ) -> X: ...

            return SyncOperationHandler(start)

    error_message = None


class InputContravarianceImplInputCannotBeSubclass(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[SuperClass, X]

    class Impl:
        @nexusrpc.handler.operation_handler
        def op(self) -> nexusrpc.handler.OperationHandler[Subclass, X]:
            async def start(
                ctx: nexusrpc.handler.StartOperationContext, input: Subclass
            ) -> X: ...

            return SyncOperationHandler(start)

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
            nexusrpc.handler.service_handler(service=test_case.Interface)(
                test_case.Impl
            )
        err = ei.value
        assert test_case.error_message in str(err)
    else:
        nexusrpc.handler.service_handler(service=test_case.Interface)(test_case.Impl)


# TODO(preview): duplicate test?
def test_service_does_not_implement_operation_name():
    @nexusrpc.service
    class Contract:
        operation_a: nexusrpc.Operation[None, None]

    class Service:
        @nexusrpc.handler.operation_handler
        def operation_b(self) -> nexusrpc.handler.OperationHandler[None, None]: ...

    with pytest.raises(
        TypeError,
        match="does not match an operation method name in the service definition",
    ):
        nexusrpc.handler.service_handler(service=Contract)(Service)
