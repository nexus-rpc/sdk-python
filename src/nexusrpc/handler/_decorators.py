from __future__ import annotations

import inspect
import types
import typing
import warnings
from functools import wraps
from typing import (
    Any,
    Awaitable,
    Callable,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)

import nexusrpc

from ._common import (
    StartOperationContext,
    StartOperationResultSync,
)
from ._core import (
    OperationHandler,
    collect_operation_handler_methods,
    service_from_operation_handler_methods,
    validate_operation_handler_methods,
)
from ._types import MISSING_TYPE, I, O, S
from ._util import get_input_and_output_types_from_sync_operation_start_method


@overload
def service_handler(cls: Type[S]) -> Type[S]: ...


# TODO(dan): allow service to be provided as positional argument?
@overload
def service_handler(
    *,
    service: Optional[Type[Any]] = None,
) -> Callable[[Type[S]], Type[S]]: ...


@overload
def service_handler(*, name: str) -> Callable[[Type[S]], Type[S]]: ...


def service_handler(
    cls: Optional[Type[S]] = None,
    service: Optional[Type[Any]] = None,
    *,
    name: Optional[str] = None,
) -> Union[Type[S], Callable[[Type[S]], Type[S]]]:
    """Decorator that marks a class as a Nexus service implementation.

    The class should implement Nexus operation handlers as methods decorated with
    operation handler decorators such as :py:func:`@nexusrpc.handler.operation_handler` or
    :py:func:`@nexusrpc.handler.sync_operation_handler`.

    Args:
        cls: The service implementation class to decorate.
        service: The service contract (interface) that the service implements.
        name: The name of the service. If not provided, the service name or class name
              will be used.

    `service` and `name` are mutually exclusive.

    Example:
        ```python @nexusrpc.handler.service_handler class MyServiceHandler:
            ...
        ```

        ```python @nexusrpc.handler.service_handler(service=MyService) class
        MyServiceHandler:
            ...
        ```

        ```python @nexusrpc.handler.service_handler(name="my-service") class
        MyServiceHandler:
            ...
        ```
    """
    if service and name:
        raise ValueError(
            "You cannot specify both service and name: "
            "if you provide a service then the name will be taken from the service."
        )
    _service = None
    if service:
        _service = getattr(service, "__nexus_service__", None)
        if not _service:
            raise ValueError(
                f"{service} is not a valid Nexus service contract. "
                f"Use the @nexusrpc.contract.service decorator on your class to define a Nexus service contract."
            )

    def decorator(cls: Type[S]) -> Type[S]:
        # The name by which the service must be addressed in Nexus requests.
        _name = (
            _service.name if _service else name if name is not None else cls.__name__
        )
        if not _name:
            raise ValueError("Service name must not be empty.")

        op_factories = collect_operation_handler_methods(cls)
        service = _service or service_from_operation_handler_methods(
            _name, op_factories
        )
        validate_operation_handler_methods(cls, op_factories, service)
        cls.__nexus_service__ = service  # type: ignore
        return cls

    if cls is None:
        return decorator

    return decorator(cls)


# TODO(dan): move these to top of file with a forward reference?
F = TypeVar("F", bound=Callable[[Any], OperationHandler[Any, Any]])


@overload
def operation_handler(method: F) -> F: ...


@overload
def operation_handler(*, name: Optional[str] = None) -> Callable[[F], F]: ...


# TODO(dan): This is following workflow.defn but check that invalid decorator
# usage is prevented by this implementation style.
def operation_handler(
    method: Optional[F] = None,
    *,
    name: Optional[str] = None,
) -> Union[F, Callable[[F], F]]:
    """
    Decorator that marks a method as an operation factory in a Nexus service implementation.

    Args:
        method: The method to decorate.
        name: The name of the operation. If not provided, the method name will be used.

    Examples:
        ```
        @nexusrpc.handler.operation_handler
        def my_operation(self) -> Operation[MyInput, MyOutput]:
            ...
        ```

        ```
        @nexusrpc.handler.operation_handler(name="my-operation")
        def my_operation(self) -> Operation[MyInput, MyOutput]:
            ...
        ```
    """

    def decorator(method: F) -> F:
        # Extract input and output types from the return type annotation
        input_type = MISSING_TYPE
        output_type = MISSING_TYPE

        return_type = typing.get_type_hints(method).get("return")
        if typing.get_origin(return_type) == OperationHandler:
            type_args = typing.get_args(return_type)
            if len(type_args) == 2:
                input_type, output_type = type_args
            else:
                warnings.warn(
                    f"Operations must have two type parameters (input and output type), "
                    f"but operation {method.__name__} has {len(type_args)} type parameters: {type_args}"
                )

        method.__nexus_operation__ = nexusrpc.contract.Operation._create(
            name=name or method.__name__,
            input_type=input_type,
            output_type=output_type,
        )
        return method

    if method is None:
        return decorator

    return decorator(method)


# TODO(dan): docstrings
# TODO(dan): check API docs
# TODO(dan): how do we help users that accidentally use @sync_operation_handler on a function that
# returns nexusrpc.handler.Operation[Input, Output]?
def sync_operation_handler(
    start_method: Optional[
        Callable[[S, StartOperationContext, I], Union[O, Awaitable[O]]]
    ] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    Callable[[S], OperationHandler[I, O]],
    Callable[
        [Callable[[S, StartOperationContext, I], Union[O, Awaitable[O]]]],
        Callable[[S], OperationHandler[I, O]],
    ],
]:
    """Define a sync operation handler by specifying the start method.

    Define a Nexus operation handler that returns a sync result by applying this decorator
    to the start method.
    """

    def decorator(
        start_method: Callable[[S, StartOperationContext, I], Union[O, Awaitable[O]]],
    ) -> Callable[[S], OperationHandler[I, O]]:
        def factory(service: S) -> OperationHandler[I, O]:
            op = OperationHandler()
            # Non-async functions returning Awaitable are not supported
            if inspect.iscoroutinefunction(start_method) or inspect.iscoroutinefunction(
                start_method.__call__
            ):
                start_method_async = cast(
                    Callable[
                        [S, StartOperationContext, I],
                        Awaitable[O],
                    ],
                    start_method,
                )

                # TODO: get rid of first parameter?
                # TODO(dan): what is wraps actually doing here?
                @wraps(start_method)
                async def start_async(
                    _, ctx: StartOperationContext, input: I
                ) -> StartOperationResultSync[O]:
                    result = await start_method_async(service, ctx, input)
                    return StartOperationResultSync(result)

                op.start = types.MethodType(start_async, op)
            else:
                start_method_sync = cast(
                    Callable[[S, StartOperationContext, I], O], start_method
                )

                # TODO(dan): what is wraps actually doing here?
                @wraps(start_method)
                def start(
                    _, ctx: StartOperationContext, input: I
                ) -> StartOperationResultSync[O]:
                    result = start_method_sync(service, ctx, input)
                    return StartOperationResultSync(result)

                op.start = types.MethodType(start, op)

            return op

        input_type, output_type = (
            get_input_and_output_types_from_sync_operation_start_method(start_method)
        )
        nonlocal name
        name = name or getattr(start_method, "__name__", None)
        if not name:
            if cls := getattr(start_method, "__class__", None):
                name = cls.__name__
        if not name:
            raise ValueError(
                f"Could not determine operation name: expected {start_method} to be a function or callable instance"
            )
        factory.__nexus_operation__ = nexusrpc.contract.Operation._create(
            name=name,
            input_type=input_type,
            output_type=output_type,
        )

        return factory

    if start_method is None:
        return decorator

    return decorator(start_method)
