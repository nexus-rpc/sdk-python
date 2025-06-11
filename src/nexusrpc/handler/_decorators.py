from __future__ import annotations

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
from nexusrpc.types import InputT, OutputT, ServiceHandlerT

from ._common import (
    CancelOperationContext,
    StartOperationContext,
    StartOperationResultSync,
)
from ._core import (
    OperationHandler,
    SyncOperationHandler,
    collect_operation_handler_factories,
    service_from_operation_handler_methods,
    validate_operation_handler_methods,
)
from ._util import (
    get_start_method_input_and_output_types_annotations,
    is_async_callable,
)


@overload
def service_handler(cls: Type[ServiceHandlerT]) -> Type[ServiceHandlerT]: ...


# TODO(preview): allow service to be provided as positional argument?
@overload
def service_handler(
    *,
    service: Optional[Type[Any]] = None,
) -> Callable[[Type[ServiceHandlerT]], Type[ServiceHandlerT]]: ...


@overload
def service_handler(
    *, name: str
) -> Callable[[Type[ServiceHandlerT]], Type[ServiceHandlerT]]: ...


def service_handler(
    cls: Optional[Type[ServiceHandlerT]] = None,
    service: Optional[Type[Any]] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    Type[ServiceHandlerT], Callable[[Type[ServiceHandlerT]], Type[ServiceHandlerT]]
]:
    """Decorator that marks a class as a Nexus service handler.

    A service handler is a class that implements the Nexus service by providing
    operation handler implementations for all operations in the service.

    The class should implement Nexus operation handlers as methods decorated with
    operation handler decorators such as :py:func:`@nexusrpc.handler.operation_handler`
    or :py:func:`@nexusrpc.handler.sync_operation_handler`.

    Args:
        cls: The service handler class to decorate.
        service: The service definition that the service handler implements.
        name: Optional name to use for the service, if a service definition is not provided.
              `service` and `name` are mutually exclusive. If neither is provided, the
              class name will be used.

    Example:
        .. code-block:: python

            @nexusrpc.handler.service_handler class MyServiceHandler:
                ...

        .. code-block:: python

            @nexusrpc.handler.service_handler(service=MyService) class MyServiceHandler:
                ...

        .. code-block:: python

            @nexusrpc.handler.service_handler(name="my-service") class MyServiceHandler:
                ...
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
                f"{service} is not a valid Nexus service definition. "
                f"Use the @nexusrpc.service decorator on your class to define a Nexus service definition."
            )

    def decorator(cls: Type[ServiceHandlerT]) -> Type[ServiceHandlerT]:
        # The name by which the service must be addressed in Nexus requests.
        _name = (
            _service.name if _service else name if name is not None else cls.__name__
        )
        if not _name:
            raise ValueError("Service name must not be empty.")

        op_factories = collect_operation_handler_factories(cls, _service)
        service = _service or service_from_operation_handler_methods(
            _name, op_factories
        )
        validate_operation_handler_methods(cls, op_factories, service)
        cls.__nexus_service__ = service  # type: ignore
        return cls

    if cls is None:
        return decorator

    return decorator(cls)


OperationHandlerFactoryT = TypeVar(
    "OperationHandlerFactoryT", bound=Callable[[Any], OperationHandler[Any, Any]]
)


@overload
def operation_handler(
    method: OperationHandlerFactoryT,
) -> OperationHandlerFactoryT: ...


@overload
def operation_handler(
    *, name: Optional[str] = None
) -> Callable[[OperationHandlerFactoryT], OperationHandlerFactoryT]: ...


def operation_handler(
    method: Optional[OperationHandlerFactoryT] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    OperationHandlerFactoryT,
    Callable[[OperationHandlerFactoryT], OperationHandlerFactoryT],
]:
    """
    Decorator marking an operation handler factory method in a service handler class.

    An operation handler factory method is a method that takes no arguments other than
    `self` and returns an :py:class:`OperationHandler` instance.

    Args:
        method: The method to decorate.
        name: Optional name for the operation. If not provided, the method name will be used.

    Examples:
        .. code-block:: python

            @nexusrpc.handler.operation_handler
            def my_operation(self) -> Operation[MyInput, MyOutput]:
                ...

        .. code-block:: python

            @nexusrpc.handler.operation_handler(name="my-operation")
            def my_operation(self) -> Operation[MyInput, MyOutput]:
                ...
    """

    def decorator(
        method: OperationHandlerFactoryT,
    ) -> OperationHandlerFactoryT:
        # Extract input and output types from the return type annotation
        input_type = None
        output_type = None

        return_type = typing.get_type_hints(method).get("return")
        if typing.get_origin(return_type) == OperationHandler:
            type_args = typing.get_args(return_type)
            if len(type_args) == 2:
                input_type, output_type = type_args
            else:
                warnings.warn(
                    f"OperationHandler return type should have two type parameters (input and output type), "
                    f"but operation {method.__name__} has {len(type_args)} type parameters: {type_args}"
                )

        method.__nexus_operation__ = nexusrpc.Operation._create(
            name=name,
            method_name=method.__name__,
            input_type=input_type,
            output_type=output_type,
        )
        return method

    if method is None:
        return decorator

    return decorator(method)


@overload
def sync_operation_handler(
    start_method: Callable[
        [ServiceHandlerT, StartOperationContext, InputT],
        Union[OutputT, Awaitable[OutputT]],
    ],
) -> Callable[[ServiceHandlerT], OperationHandler[InputT, OutputT]]: ...


@overload
def sync_operation_handler(
    *,
    name: Optional[str] = None,
) -> Callable[
    [
        Callable[
            [ServiceHandlerT, StartOperationContext, InputT],
            Union[OutputT, Awaitable[OutputT]],
        ]
    ],
    Callable[[ServiceHandlerT], OperationHandler[InputT, OutputT]],
]: ...


# TODO(preview): how do we help users that accidentally use @sync_operation_handler on a function that
# returns nexusrpc.handler.Operation[Input, Output]?
def sync_operation_handler(
    start_method: Optional[
        Callable[
            [ServiceHandlerT, StartOperationContext, InputT],
            Union[OutputT, Awaitable[OutputT]],
        ]
    ] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    Callable[[ServiceHandlerT], OperationHandler[InputT, OutputT]],
    Callable[
        [
            Callable[
                [ServiceHandlerT, StartOperationContext, InputT],
                Union[OutputT, Awaitable[OutputT]],
            ]
        ],
        Callable[[ServiceHandlerT], OperationHandler[InputT, OutputT]],
    ],
]:
    """Decorator marking a start method as a synchronous operation handler.

    Apply this decorator to a start method to convert it into an operation handler
    factory method.

    Args:
        start_method: The start method to decorate.
        name: Optional name for the operation. If not provided, the method name will be used.

    Examples:
        .. code-block:: python

            @nexusrpc.handler.sync_operation_handler
            def my_operation(self, ctx: StartOperationContext, input: InputT) -> OutputT:
                ...
    """

    def decorator(
        start_method: Callable[
            [ServiceHandlerT, StartOperationContext, InputT],
            Union[OutputT, Awaitable[OutputT]],
        ],
    ) -> Callable[[ServiceHandlerT], OperationHandler[InputT, OutputT]]:
        def factory(service: ServiceHandlerT) -> OperationHandler[InputT, OutputT]:
            op = SyncOperationHandler[InputT, OutputT]()

            # Non-async functions returning Awaitable are not supported
            if is_async_callable(start_method):
                start_method_async = cast(
                    Callable[
                        [ServiceHandlerT, StartOperationContext, InputT],
                        Awaitable[OutputT],
                    ],
                    start_method,
                )

                @wraps(start_method)
                async def start_async(
                    _, ctx: StartOperationContext, input: InputT
                ) -> StartOperationResultSync[OutputT]:
                    result = await start_method_async(service, ctx, input)
                    return StartOperationResultSync(result)

                op.start = types.MethodType(start_async, op)

                async def cancel_async(_, ctx: CancelOperationContext, token: str):
                    raise NotImplementedError(
                        "An operation that responded synchronously cannot be cancelled."
                    )

                op.cancel = types.MethodType(cancel_async, op)

            else:
                start_method_sync = cast(
                    Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT],
                    start_method,
                )

                @wraps(start_method)
                def start(
                    _, ctx: StartOperationContext, input: InputT
                ) -> StartOperationResultSync[OutputT]:
                    result = start_method_sync(service, ctx, input)
                    return StartOperationResultSync(result)

                op.start = types.MethodType(start, op)

                def cancel(_, ctx: CancelOperationContext, token: str):
                    raise NotImplementedError(
                        "An operation that responded synchronously cannot be cancelled."
                    )

                op.cancel = types.MethodType(cancel, op)
            return op

        input_type, output_type = get_start_method_input_and_output_types_annotations(
            start_method
        )
        method_name = getattr(start_method, "__name__", None)
        if (
            not method_name
            and callable(start_method)
            and hasattr(start_method, "__call__")
        ):
            method_name = start_method.__class__.__name__
        if not method_name:
            raise TypeError(
                f"Could not determine operation method name: "
                f"expected {start_method} to be a function or callable instance."
            )

        factory.__nexus_operation__ = nexusrpc.Operation._create(
            name=name,
            method_name=method_name,
            input_type=input_type,
            output_type=output_type,
        )

        return factory

    if start_method is None:
        return decorator

    return decorator(start_method)
