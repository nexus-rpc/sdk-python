from __future__ import annotations

import asyncio
import inspect
import typing
import warnings
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Optional,
    Sequence,
    Type,
    Union,
)

from typing_extensions import Self

import nexusrpc
import nexusrpc._service_definition
from nexusrpc.handler._util import is_async_callable
from nexusrpc.types import InputT, OutputT, ServiceHandlerT

from ._common import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    OperationInfo,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)
from ._serializer import LazyValue


class UnknownServiceError(RuntimeError):
    """Raised when a request contains a service name that does not match a service handler."""

    pass


class UnknownOperationError(RuntimeError):
    """Raised when a request contains an operation name that does not match an operation handler."""

    pass


@dataclass
class Handler:
    """
    A Nexus handler, managing a collection of Nexus service handlers.

    Operation requests are delegated to a :py:class:`ServiceHandler` based on the service
    name in the operation context.
    """

    service_handlers: dict[str, ServiceHandler]

    def __init__(
        self,
        user_service_handlers: Sequence[Any],
        sync_executor: Optional[SyncExecutor] = None,
    ):
        """Initialize a :py:class:`Handler` instance from user service handler instances.

        The user service handler instances must have been decorated with the
        :py:func:`@nexusrpc.handler.service_handler` decorator.

        Args:
            user_service_handlers: A sequence of user service handlers.
            sync_executor: An executor to run non-`async def` operation handlers in.
        """
        self.sync_executor = sync_executor
        self.service_handlers = {}
        for sh in user_service_handlers:
            if isinstance(sh, type):
                raise TypeError(
                    f"Expected a service instance, but got a class: {type(sh)}. "
                    "Nexus service handlers must be supplied as instances, not classes."
                )
            # Users may register ServiceHandler instances directly.
            if not isinstance(sh, ServiceHandler):
                # It must be a user service handler instance (i.e. an instance of a class
                # decorated with @nexusrpc.handler.service_handler).
                sh = ServiceHandler.from_user_instance(sh)
            if sh.service.name in self.service_handlers:
                raise RuntimeError(
                    f"Service '{sh.service.name}' has already been registered."
                )
            if self.sync_executor is None:
                for op_name, operation_handler in sh.operation_handlers.items():
                    if not is_async_callable(operation_handler.start):
                        raise RuntimeError(
                            f"Service '{sh.service.name}' operation '{op_name}' start must be an `async def` if no executor is provided."
                        )
                    if not is_async_callable(operation_handler.cancel):
                        raise RuntimeError(
                            f"Service '{sh.service.name}' operation '{op_name}' cancel must be an `async def` if no executor is provided."
                        )
            self.service_handlers[sh.service.name] = sh

    async def start_operation(
        self,
        ctx: StartOperationContext,
        input: LazyValue,
    ) -> Union[
        StartOperationResultSync[Any],
        StartOperationResultAsync,
    ]:
        """Handle a Start Operation request.

        Args:
            ctx: The operation context.
            input: The input to the operation, as a LazyValue.
        """
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        op = service_handler.service.operations[ctx.operation]
        input = await input.consume(as_type=op.input_type)

        if is_async_callable(op_handler.start):
            # TODO(preview): apply middleware stack as composed awaitables
            return await op_handler.start(ctx, input)
        else:
            # TODO(preview): apply middleware stack as composed functions
            if not self.sync_executor:
                raise RuntimeError(
                    "Operation start handler method is not an `async def` but "
                    "no sync executor was provided to the Handler constructor. "
                )
            result = await self.sync_executor.run_sync(op_handler.start, ctx, input)
            if inspect.isawaitable(result):
                raise RuntimeError(
                    f"Operation start handler method {op_handler.start} returned an "
                    "awaitable but is not an `async def` coroutine function."
                )
            return result

    async def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> OperationInfo:
        """Handle a Fetch Operation Info request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        raise NotImplementedError

    async def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Any:
        """Handle a Fetch Operation Result request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        raise NotImplementedError

    async def cancel_operation(self, ctx: CancelOperationContext, token: str) -> None:
        """Handle a Cancel Operation request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        if is_async_callable(op_handler.cancel):
            return await op_handler.cancel(ctx, token)
        else:
            if not self.sync_executor:
                raise RuntimeError(
                    "Operation cancel handler method is not an `async def` function but "
                    "no executor was provided to the Handler constructor."
                )
            result = await self.sync_executor.run_sync(op_handler.cancel, ctx, token)
            if inspect.isawaitable(result):
                raise RuntimeError(
                    f"Operation cancel handler method {op_handler.cancel} returned an "
                    "awaitable but is not an `async def` function."
                )
            return result

    def _get_service_handler(self, service_name: str) -> ServiceHandler:
        """Return a service handler, given the service name."""
        service = self.service_handlers.get(service_name)
        if service is None:
            # TODO(prerelease): can this raise HandlerError directly or is HandlerError always a
            # wrapper? I have currently made its __cause__ required but if it's not a
            # wrapper then that is wrong.
            raise UnknownServiceError(f"No handler for service '{service_name}'.")
        return service


@dataclass
class ServiceHandler:
    """Internal representation of a user's Nexus service implementation instance.

    A user's service implementation is a class decorated with
    :py:func:`@nexusrpc.handler.service_handler` that defines operation handler methods
    using decorators such as :py:func:`@nexusrpc.handler.operation_handler` or
    :py:func:`@nexusrpc.handler.sync_operation_handler`.

    Instances of this class are created automatically from user service handler instances
    on creation of a Handler instance, at Nexus handler start time. While the user's class
    defines operation handlers as factory methods to be called at handler start time, this
    class contains the :py:class:`OperationHandler` instances themselves.

    You may create instances of this class manually and pass them to the Handler
    constructor, for example when programatically creating Nexus service implementations.
    """

    service: nexusrpc.ServiceDefinition
    operation_handlers: dict[str, OperationHandler[Any, Any]]

    @classmethod
    def from_user_instance(cls, user_instance: Any) -> Self:
        """Create a :py:class:`ServiceHandler` from a user service instance."""

        service = getattr(user_instance.__class__, "__nexus_service__", None)
        if not isinstance(service, nexusrpc.ServiceDefinition):
            raise RuntimeError(
                f"Service '{user_instance}' does not have a service definition. "
                f"Use the :py:func:`@nexusrpc.handler.service_handler` decorator on your class to define "
                f"a Nexus service implementation."
            )
        op_handlers = {
            name: factory(user_instance)
            for name, factory in collect_operation_handler_factories(
                user_instance.__class__, service
            ).items()
        }
        return cls(
            service=service,
            operation_handlers=op_handlers,
        )

    def _get_operation_handler(self, operation: str) -> OperationHandler[Any, Any]:
        """Return an operation handler, given the operation name."""
        if operation not in self.service.operations:
            msg = (
                f"Nexus service definition '{self.service.name}' has no operation '{operation}'. "
                f"There are {len(self.service.operations)} operations in the definition."
            )
            if self.service.operations:
                msg += f": {', '.join(sorted(self.service.operations.keys()))}"
            msg += "."
            raise UnknownOperationError(msg)
        operation_handler = self.operation_handlers.get(operation)
        if operation_handler is None:
            # This should not be possible. If a service definition was supplied then
            # this was checked; if not then the definition was generated from the
            # operation handlers.
            msg = (
                f"Nexus service implementation '{self.service.name}' has no handler for operation '{operation}'. "
                f"There are {len(self.operation_handlers)} available operation handlers"
            )
            if self.operation_handlers:
                msg += f": {', '.join(sorted(self.operation_handlers.keys()))}"
            msg += "."
            raise UnknownOperationError(msg)
        return operation_handler


class OperationHandler(ABC, Generic[InputT, OutputT]):
    """
    Base class for an operation handler in a Nexus service implementation.

    To define a Nexus operation handler, create a method on your service handler class
    that takes `self` and returns an instance of :py:class:`OperationHandler`, and apply
    the :py:func:`@nexusrpc.handler.operation_handler` decorator.

    Alternatively, to create an operation handler that is limited to returning
    synchronously, create the start method of the :py:class:`OperationHandler` on your
    service handler class and apply the
    :py:func:`@nexusrpc.handler.sync_operation_handler` decorator.
    """

    # TODO(preview): We are using `def` signatures with union return types in this abstract
    # base class to represent both `def` and `async` def implementations in child classes.
    # However, this causes VSCode to autocomplete the methods with non-sensical signatures
    # such as
    #
    # async def fetch_result(self, ctx: FetchOperationResultContext, token: str) -> Output | asyncio.Awaitable[Output]
    #
    # Can we improve this DX?

    @abstractmethod
    def start(
        self, ctx: StartOperationContext, input: InputT
    ) -> Union[
        StartOperationResultSync[OutputT],
        Awaitable[StartOperationResultSync[OutputT]],
        StartOperationResultAsync,
        Awaitable[StartOperationResultAsync],
    ]:
        """
        Start the operation, completing either synchronously or asynchronously.

        Returns the result synchronously, or returns an operation token. Which path is
        taken may be decided at operation handling time.
        """
        ...

    @abstractmethod
    def fetch_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]:
        """
        Return information about the current status of the operation.
        """
        ...

    @abstractmethod
    def fetch_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Union[OutputT, Awaitable[OutputT]]:
        """
        Return the result of the operation.
        """
        ...

    @abstractmethod
    def cancel(
        self, ctx: CancelOperationContext, token: str
    ) -> Union[None, Awaitable[None]]:
        """
        Cancel the operation.
        """
        ...


class SyncOperationHandler(OperationHandler[InputT, OutputT]):
    """
    An :py:class:`OperationHandler` that is limited to responding synchronously.
    """

    def start(
        self, ctx: StartOperationContext, input: InputT
    ) -> Union[
        StartOperationResultSync[OutputT],
        Awaitable[StartOperationResultSync[OutputT]],
    ]:
        """
        Start the operation and return its final result synchronously.

        Note that this method may be either `async def` or `def`. The name
        'SyncOperationHandler' means that the operation responds synchronously according
        to the Nexus protocol; it doesn't refer to whether or not the implementation of
        the start method is an `async def` or `def`.
        """
        raise NotImplementedError(
            "Start method must be implemented by subclasses of SyncOperationHandler."
        )

    def fetch_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]:
        raise NotImplementedError(
            "Cannot fetch operation info for an operation that responded synchronously."
        )

    def fetch_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Union[OutputT, Awaitable[OutputT]]:
        raise NotImplementedError(
            "Cannot fetch the result of an operation that responded synchronously."
        )

    def cancel(
        self, ctx: CancelOperationContext, token: str
    ) -> Union[None, Awaitable[None]]:
        raise NotImplementedError(
            "An operation that responded synchronously cannot be cancelled."
        )


def collect_operation_handler_factories(
    user_service_cls: Type[ServiceHandlerT],
    service: Optional[nexusrpc.ServiceDefinition],
) -> dict[str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]]:
    """
    Collect operation handler methods from a user service handler class.
    """
    factories = {}
    op_defn_method_names = (
        {op.method_name for op in service.operations.values()} if service else set()
    )
    for _, method in inspect.getmembers(user_service_cls, inspect.isfunction):
        op_defn = getattr(method, "__nexus_operation__", None)
        if isinstance(op_defn, nexusrpc.Operation):
            # This is a method decorated with one of the *operation_handler decorators
            # assert op_defn.name == name
            if op_defn.name in factories:
                raise RuntimeError(
                    f"Operation '{op_defn.name}' in service '{user_service_cls.__name__}' "
                    f"is defined multiple times."
                )
            if service and op_defn.method_name not in op_defn_method_names:
                _names = ", ".join(f"'{s}'" for s in sorted(op_defn_method_names))
                raise TypeError(
                    f"Operation method name '{op_defn.method_name}' in service handler {user_service_cls} "
                    f"does not match an operation method name in the service definition. "
                    f"Available method names in the service definition: {_names}."
                )

            factories[op_defn.name] = method
        # Check for accidentally missing decorator on an OperationHandler factory
        # TODO(preview): support disabling warning in @service_handler decorator?
        elif (
            typing.get_origin(typing.get_type_hints(method).get("return"))
            == OperationHandler
        ):
            warnings.warn(
                f"Method '{method}' in class '{user_service_cls}' "
                f"returns OperationHandler but has not been decorated. "
                f"Did you forget to apply the @nexusrpc.handler.operation_handler decorator?",
                UserWarning,
                stacklevel=2,
            )
    return factories


def validate_operation_handler_methods(
    user_service_cls: Type[ServiceHandlerT],
    user_methods: dict[str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]],
    service_definition: nexusrpc.ServiceDefinition,
) -> None:
    """Validate operation handler methods against a service definition."""
    for op_name, op_defn in service_definition.operations.items():
        method = user_methods.get(op_name)
        if not method:
            raise TypeError(
                f"Service '{user_service_cls}' does not implement operation '{op_name}' in interface '{service_definition}'. "
            )
        op = getattr(method, "__nexus_operation__", None)
        if not isinstance(op, nexusrpc.Operation):
            raise RuntimeError(
                f"Method '{method}' in class '{user_service_cls.__name__}' "
                f"does not have a valid __nexus_operation__ attribute. "
                f"Did you forget to decorate the operation method with an operation handler decorator such as "
                f":py:func:`@nexusrpc.handler.operation_handler` or "
                f":py:func:`@nexusrpc.handler.sync_operation_handler`?"
            )
        # Input type is contravariant: op handler input must be superclass of op defn output
        if (
            op.input_type is not None
            and op_defn.input_type is not None
            and Any not in (op.input_type, op_defn.input_type)
            and not (
                op_defn.input_type == op.input_type
                or issubclass(op_defn.input_type, op.input_type)
            )
        ):
            raise TypeError(
                f"Operation '{op_name}' in service '{user_service_cls}' has input type '{op.input_type}', "
                f"which is not compatible with the input type '{op_defn.input_type}' in interface '{service_definition}'. "
                f"The input type must be the same as or a superclass of the operation definition input type."
            )
        # Output type is covariant: op handler output must be subclass of op defn output
        if (
            op.output_type is not None
            and op_defn.output_type is not None
            and Any not in (op.output_type, op_defn.output_type)
            and not issubclass(op.output_type, op_defn.output_type)
        ):
            raise TypeError(
                f"Operation '{op_name}' in service '{user_service_cls}' has output type '{op.output_type}', "
                f"which is not compatible with the output type '{op_defn.output_type}' in interface '{service_definition}'. "
                f"The output type must be the same as or a subclass of the operation definition output type."
            )
    if service_definition.operations.keys() > user_methods.keys():
        raise TypeError(
            f"Service '{user_service_cls}' does not implement all operations in interface '{service_definition}'. "
            f"Missing operations: {service_definition.operations.keys() - user_methods.keys()}"
        )
    if user_methods.keys() > service_definition.operations.keys():
        raise TypeError(
            f"Service '{user_service_cls}' implements more operations than the interface '{service_definition}'. "
            f"Extra operations: {user_methods.keys() - service_definition.operations.keys()}"
        )


def service_from_operation_handler_methods(
    service_name: str,
    user_methods: dict[str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]],
) -> nexusrpc.ServiceDefinition:
    """
    Create a service definition from operation handler factory methods.

    In general, users should have access to, or define, a service definition, and validate
    their service handler against it by passing the service definition to the
    :py:func:`@nexusrpc.handler.service_handler` decorator. This function is used when
    that is not the case.
    """
    operations: dict[str, nexusrpc.Operation[Any, Any]] = {}
    for name, method in user_methods.items():
        op = getattr(method, "__nexus_operation__", None)
        if not isinstance(op, nexusrpc.Operation):
            raise RuntimeError(
                f"In service '{service_name}', could not locate operation definition for "
                f"user operation handler method '{name}'. Did you forget to decorate the operation "
                f"method with an operation handler decorator such as "
                f":py:func:`@nexusrpc.handler.operation_handler` or "
                f":py:func:`@nexusrpc.handler.sync_operation_handler`?"
            )
        operations[op.name] = op

    return nexusrpc.ServiceDefinition(name=service_name, operations=operations)


class SyncExecutor:
    """
    Run a synchronous function asynchronously.
    """

    def __init__(self, executor: ThreadPoolExecutor):
        self._executor = executor

    def run_sync(self, fn: Callable[..., Any], *args: Any) -> Awaitable[Any]:
        return asyncio.get_event_loop().run_in_executor(self._executor, fn, *args)
