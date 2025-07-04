import warnings
from typing import Any, Optional, Type

import pytest

import nexusrpc
from nexusrpc.handler import (
    StartOperationContext,
    service_handler,
    sync_operation,
)


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
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> None: ...

    error_message = None


class ValidImplWithEmptyInterfaceAndExtraOperation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        pass

    class Impl:
        @sync_operation
        async def extra_op(self, ctx: StartOperationContext, input: None) -> None: ...

        def unrelated_method(self) -> None: ...

    error_message = "does not match an operation method name in the service definition"


class ValidImplWithoutTypeAnnotations(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[int, str]

    with warnings.catch_warnings(record=True) as _warnings:
        warnings.simplefilter("always")

        class Impl:
            @sync_operation
            async def op(self, ctx, input): ...

    captured_warnings = _warnings
    expected_warning = (
        "to have exactly 2 type-annotated parameters (ctx and input), but it has 0"
    )

    error_message = None


class MissingOperation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    class Impl:
        pass

    error_message = "does not implement an operation with method name 'op'"


class MissingInputAnnotation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    with warnings.catch_warnings(record=True) as _warnings:
        warnings.simplefilter("always")

        class Impl:
            @sync_operation
            async def op(self, ctx: StartOperationContext, input) -> None: ...

    captured_warnings = _warnings
    expected_warning = (
        "to have exactly 2 type-annotated parameters (ctx and input), but it has 1"
    )

    error_message = None


class MissingContextAnnotation(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, None]

    with warnings.catch_warnings(record=True) as _warnings:
        warnings.simplefilter("always")

        class Impl:
            @sync_operation
            async def op(self, ctx, input: None) -> None: ...

    captured_warnings = _warnings
    expected_warning = (
        "to have exactly 2 type-annotated parameters (ctx and input), but it has 1"
    )

    error_message = None


class WrongOutputType(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[None, int]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: None) -> str: ...

    error_message = "is not compatible with the output type"


class WrongOutputTypeWithNone(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[str, None]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: str) -> str: ...

    error_message = "is not compatible with the output type"


class ValidImplWithNone(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[str, None]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: str) -> None: ...

    error_message = None


class MoreSpecificImplAllowed(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[Any, Any]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: str) -> str: ...

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
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: X) -> X: ...

    error_message = None


class OutputCovarianceImplOutputCanBeSubclass(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, SuperClass]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: X) -> Subclass: ...

    error_message = None


class OutputCovarianceImplOutputCannnotBeStrictSuperclass(
    _InterfaceImplementationTestCase
):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, Subclass]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: X) -> SuperClass: ...

    error_message = "is not compatible with the output type"


class InputContravarianceImplInputCanBeSameType(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[X, X]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: X) -> X: ...

    error_message = None


class InputContravarianceImplInputCanBeSuperclass(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[Subclass, X]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: SuperClass) -> X: ...

    error_message = None


class InputContravarianceImplInputCannotBeSubclass(_InterfaceImplementationTestCase):
    @nexusrpc.service
    class Interface:
        op: nexusrpc.Operation[SuperClass, X]

    class Impl:
        @sync_operation
        async def op(self, ctx: StartOperationContext, input: Subclass) -> X: ...

    error_message = "is not compatible with the input type"


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
        MoreSpecificImplAllowed,
        OutputCovarianceImplOutputCanBeSameType,
        OutputCovarianceImplOutputCanBeSubclass,
        OutputCovarianceImplOutputCannnotBeStrictSuperclass,
        InputContravarianceImplInputCanBeSameType,
        InputContravarianceImplInputCanBeSuperclass,
    ],
)
def test_service_decorator_enforces_interface_implementation(
    test_case: Type[_InterfaceImplementationTestCase],
):
    if test_case.error_message:
        with pytest.raises(Exception) as ei:
            service_handler(service=test_case.Interface)(test_case.Impl)
        err = ei.value
        assert test_case.error_message in str(err)
    else:
        if expected_warning := getattr(test_case, "expected_warning", None):
            [warning] = getattr(test_case, "captured_warnings", [])
            assert expected_warning in str(warning.message)
            assert issubclass(warning.category, UserWarning)

        service_handler(service=test_case.Interface)(test_case.Impl)


# TODO(preview): duplicate test?
def test_service_does_not_implement_operation_name():
    @nexusrpc.service
    class Contract:
        operation_a: nexusrpc.Operation[None, None]

    class Service:
        @sync_operation
        async def operation_b(
            self, ctx: StartOperationContext, input: None
        ) -> None: ...

    with pytest.raises(
        TypeError,
        match="does not match an operation method name in the service definition",
    ):
        service_handler(service=Contract)(Service)
