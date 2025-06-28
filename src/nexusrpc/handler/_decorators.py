from __future__ import annotations

import typing
import warnings
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
from nexusrpc import InputT, OutputT
from nexusrpc._types import ServiceHandlerT
from nexusrpc.handler._common import StartOperationContext
from nexusrpc.handler._util import (
    get_callable_name,
    get_service_definition,
    get_start_method_input_and_output_type_annotations,
    is_async_callable,
    set_service_definition,
)

from ._operation_handler import (
    OperationHandler,
    SyncioSyncOperationHandler,
    SyncOperationHandler,
    collect_operation_handler_factories,
    service_definition_from_operation_handler_methods,
    validate_operation_handler_methods,
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
    operation handler decorators such as :py:func:`@nexusrpc.handler.operation_handler`.

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
        _service = get_service_definition(service)
        if not _service:
            raise ValueError(
                f"{service} is not a valid Nexus service definition. "
                f"Use the @nexusrpc.service decorator on a class to define a Nexus service definition."
            )

    def decorator(cls: Type[ServiceHandlerT]) -> Type[ServiceHandlerT]:
        # The name by which the service must be addressed in Nexus requests.
        _name = (
            _service.name if _service else name if name is not None else cls.__name__
        )
        if not _name:
            raise ValueError("Service name must not be empty.")

        op_factories = collect_operation_handler_factories(cls, _service)
        service = _service or service_definition_from_operation_handler_methods(
            _name, op_factories
        )
        validate_operation_handler_methods(cls, op_factories, service)
        set_service_definition(cls, service)
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

        method.__nexus_operation__ = nexusrpc.Operation(
            name=name or method.__name__,
            method_name=method.__name__,
            input_type=input_type,
            output_type=output_type,
        )
        return method

    if method is None:
        return decorator

    return decorator(method)


@overload
def sync_operation(
    start: Callable[
        [ServiceHandlerT, StartOperationContext, InputT],
        Union[OutputT, Awaitable[OutputT]],
    ],
) -> Callable[
    [ServiceHandlerT, StartOperationContext, InputT],
    Union[OutputT, Awaitable[OutputT]],
]: ...


@overload
def sync_operation(
    *,
    name: Optional[str] = None,
) -> Callable[
    [
        Callable[
            [ServiceHandlerT, StartOperationContext, InputT],
            Union[OutputT, Awaitable[OutputT]],
        ]
    ],
    Callable[
        [ServiceHandlerT, StartOperationContext, InputT],
        Union[OutputT, Awaitable[OutputT]],
    ],
]: ...


def sync_operation(
    start: Optional[
        Callable[
            [ServiceHandlerT, StartOperationContext, InputT],
            Union[OutputT, Awaitable[OutputT]],
        ]
    ] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    Callable[
        [ServiceHandlerT, StartOperationContext, InputT],
        Union[OutputT, Awaitable[OutputT]],
    ],
    Callable[
        [
            Callable[
                [ServiceHandlerT, StartOperationContext, InputT],
                Union[OutputT, Awaitable[OutputT]],
            ]
        ],
        Callable[
            [ServiceHandlerT, StartOperationContext, InputT],
            Union[OutputT, Awaitable[OutputT]],
        ],
    ],
]:
    """
    Decorator marking a method as the start method for a synchronous operation.
    """

    def decorator(
        start: Callable[
            [ServiceHandlerT, StartOperationContext, InputT],
            Union[OutputT, Awaitable[OutputT]],
        ],
    ) -> Callable[
        [ServiceHandlerT, StartOperationContext, InputT],
        Union[OutputT, Awaitable[OutputT]],
    ]:
        def operation_handler_factory(
            self: ServiceHandlerT,
        ) -> OperationHandler[InputT, OutputT]:
            if is_async_callable(start):
                start_async = start

                async def _start(ctx: StartOperationContext, input: InputT) -> OutputT:
                    return await start_async(self, ctx, input)

                _start.__doc__ = start.__doc__
                return SyncOperationHandler(_start)
            else:
                start_sync = cast(Callable[..., OutputT], start)

                def _start_sync(ctx: StartOperationContext, input: InputT) -> OutputT:
                    return start_sync(self, ctx, input)

                _start_sync.__doc__ = start.__doc__
                return SyncioSyncOperationHandler(_start_sync)

        input_type, output_type = get_start_method_input_and_output_type_annotations(
            start
        )

        method_name = get_callable_name(start)
        operation_handler_factory.__nexus_operation__ = nexusrpc.Operation(
            name=name or method_name,
            method_name=method_name,
            input_type=input_type,
            output_type=output_type,
        )

        start.__nexus_operation_factory__ = operation_handler_factory
        return start

    if start is None:
        return decorator

    return decorator(start)
