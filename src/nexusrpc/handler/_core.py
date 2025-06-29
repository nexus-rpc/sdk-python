"""
This module contains Handler classes. A Handler manages a collection of Nexus
service handlers. It receives and responds to incoming Nexus requests, dispatching to
the corresponding operation handler.

A description of the dispatch logic follows.

There are two cases:

Case 1: Every user service handler class has a corresponding service definition
==============================================================================

I.e., there are service definitions that look like

@service
class MyServiceDefinition:
    my_op: nexusrpc.Operation[I, O]


and every service handler class looks like

@service_handler(service=MyServiceDefinition)
class MyServiceHandler:
    @sync_operation
    def my_op(self, ...)


Import time
-----------

1. The @service decorator builds a ServiceDefinition instance and attaches it to
   MyServiceDefinition.

   The ServiceDefinition contains `name` and a map of Operation instances,
   keyed by Operation.name (this is the publicly advertised name).

   An Operation contains `name`, `method_name`, and input and output types.

2. The @sync_operation decorator builds a second Operation instance and attaches
   it to a factory method that is attached to the my_op method object.

3. The @service_handler decorator acquires the ServiceDefinition instance from
   MyServiceDefinition and attaches it to the MyServiceHandler class.


Handler-registration time
-------------------------

4. Handler.__init__ is called with [MyServiceHandler()]

5. A ServiceHandler instance is built from the user service handler class. This comprises a
   ServiceDefinition and a map {op.name: OperationHandler}. The map is built by taking
   every operation in the service definition and locating the operation handler factory method
   whose *method name* matches the method name of the operation in the service definition.

6. Finally we build a map {service_definition.name: ServiceHandler} using the service definition
   in each ServiceHandler.

Request-handling time
---------------------

Now suppose a request has arrived for service S and operation O.

6. The Handler does self.service_handlers[S], yielding an instance of ServiceHandler.

7. The ServiceHandler does self.operation_handlers[O], yielding an instance of
   OperationHandler

Therefore we require that Handler.service_handlers and ServiceHandler.operation_handlers
are keyed by the publicly advertised service and operation name respectively. This was achieved
at steps (6) and (5) respectively.


Case 2: There exists a user service handler class without a corresonding service definition
===========================================================================================

I.e., at least one user service handler class looks like

@service_handler
class MyServiceHandler:
    @sync_operation
    def my_op(...)

This follows Case 1 with the following differences:

- Step (1) does not occur.
- At step (3) the ServiceDefinition is synthesized by the @service_handler decorator from
  MyServiceHandler.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    Awaitable,
    Callable,
    Optional,
    Sequence,
    Union,
)

from typing_extensions import Self

import nexusrpc
from nexusrpc import HandlerError, HandlerErrorType, LazyValue, OperationInfo
from nexusrpc._util import get_service_definition
from nexusrpc.handler._util import is_async_callable

from ._common import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)
from ._operation_handler import (
    OperationHandler,
    collect_operation_handler_factories_by_method_name,
)

# TODO(preview): show what it looks like to manually build a service implementation at runtime
# where the operations may be based on some runtime information.

# TODO(preview): pass pyright strict mode "python.analysis.typeCheckingMode": "strict"
# TODO(preview): pass mypy


class BaseHandler(ABC):
    """
    A Nexus handler, managing a collection of Nexus service handlers.

    Operation requests are delegated to a :py:class:`ServiceHandler` based on the service
    name in the operation context.
    """

    def __init__(
        self,
        user_service_handlers: Sequence[Any],
        executor: Optional[concurrent.futures.Executor] = None,
    ):
        """Initialize a :py:class:`Handler` instance from user service handler instances.

        The user service handler instances must have been decorated with the
        :py:func:`@nexusrpc.handler.service_handler` decorator.

        Args:
            user_service_handlers: A sequence of user service handlers.
            executor: A concurrent.futures.Executor in which to run non-`async def` operation handlers.
        """
        self.executor = _Executor(executor) if executor else None
        self.service_handlers: dict[str, ServiceHandler] = {}
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
            if self.executor is None:
                for op_name, operation_handler in sh.operation_handlers.items():
                    if not is_async_callable(operation_handler.start):
                        raise RuntimeError(
                            f"Service '{sh.service.name}' operation '{op_name}' start method must be an `async def` if no executor is provided."
                        )
                    if not is_async_callable(operation_handler.cancel):
                        raise RuntimeError(
                            f"Service '{sh.service.name}' operation '{op_name}' cancel method must be an `async def` if no executor is provided."
                        )
            self.service_handlers[sh.service.name] = sh

    def _get_service_handler(self, service_name: str) -> ServiceHandler:
        """Return a service handler, given the service name."""
        service = self.service_handlers.get(service_name)
        if service is None:
            raise HandlerError(
                f"No handler for service '{service_name}'.",
                type=HandlerErrorType.NOT_FOUND,
            )
        return service

    @abstractmethod
    def start_operation(
        self,
        ctx: StartOperationContext,
        input: LazyValue,
    ) -> Union[
        StartOperationResultSync[Any],
        StartOperationResultAsync,
        Awaitable[StartOperationResultSync[Any]],
        Awaitable[StartOperationResultAsync],
    ]: ...

    @abstractmethod
    def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]:
        """Handle a Fetch Operation Info request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        ...

    @abstractmethod
    def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Union[Any, Awaitable[Any]]:
        """Handle a Fetch Operation Result request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        ...

    @abstractmethod
    def cancel_operation(
        self, ctx: CancelOperationContext, token: str
    ) -> Union[None, Awaitable[None]]:
        """Handle a Cancel Operation request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        ...


class Handler(BaseHandler):
    """
    A Nexus handler manages a collection of Nexus service handlers.

    Operation requests are delegated to a :py:class:`ServiceHandler` based on the service
    name in the operation context.
    """

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
        deserialized_input = await input.consume(as_type=op.input_type)

        if is_async_callable(op_handler.start):
            # TODO(preview): apply middleware stack as composed awaitables
            return await op_handler.start(ctx, deserialized_input)
        else:
            # TODO(preview): apply middleware stack as composed functions
            if not self.executor:
                raise RuntimeError(
                    "Operation start handler method is not an `async def` but "
                    "no executor was provided to the Handler constructor. "
                )
            result = await self.executor.submit_to_event_loop(
                op_handler.start, ctx, deserialized_input
            )
            if inspect.isawaitable(result):
                raise RuntimeError(
                    f"Operation start handler method {op_handler.start} returned an "
                    "awaitable but is not an `async def` coroutine function."
                )
            return result

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
            if not self.executor:
                raise RuntimeError(
                    "Operation cancel handler method is not an `async def` function but "
                    "no executor was provided to the Handler constructor."
                )
            result = await self.executor.submit_to_event_loop(
                op_handler.cancel, ctx, token
            )
            if inspect.isawaitable(result):
                raise RuntimeError(
                    f"Operation cancel handler method {op_handler.cancel} returned an "
                    "awaitable but is not an `async def` function."
                )
            return result

    async def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> OperationInfo:
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        if is_async_callable(op_handler.fetch_info):
            return await op_handler.fetch_info(ctx, token)
        else:
            if not self.executor:
                raise RuntimeError(
                    "Operation fetch_info handler method is not an `async def` function but "
                    "no executor was provided to the Handler constructor."
                )
            result = await self.executor.submit_to_event_loop(
                op_handler.fetch_info, ctx, token
            )
            if inspect.isawaitable(result):
                raise RuntimeError(
                    f"Operation fetch_info handler method {op_handler.fetch_info} returned an "
                    "awaitable but is not an `async def` function."
                )
            return result

    async def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Any:
        if ctx.wait is not None or ctx.headers.get("request-timeout"):
            raise NotImplementedError(
                "The Nexus SDK is in pre-release and does not support the fetch result "
                "wait parameter or request-timeout header."
            )
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        if is_async_callable(op_handler.fetch_result):
            return await op_handler.fetch_result(ctx, token)
        else:
            if not self.executor:
                raise RuntimeError(
                    "Operation fetch_result handler method is not an `async def` function but "
                    "no executor was provided to the Handler constructor."
                )
            result = await self.executor.submit_to_event_loop(
                op_handler.fetch_result, ctx, token
            )
            if inspect.isawaitable(result):
                raise RuntimeError(
                    f"Operation fetch_result handler method {op_handler.fetch_result} returned an "
                    "awaitable but is not an `async def` function."
                )
            return result


# TODO(prerelease): we have a syncio module now housing the syncio version of
# SyncOperationHandler. If we're retaining that then this (and an async version of
# LazyValue) should go in there.
class SyncioHandler(BaseHandler):
    """
    A Nexus handler with non-async `def` methods.

    A Nexus handler manages a collection of Nexus service handlers.

    Operation requests are delegated to a :py:class:`ServiceHandler` based on the service
    name in the operation context.
    """

    def start_operation(
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
        deserialized_input = input.consume_sync(as_type=op.input_type)

        if is_async_callable(op_handler.start):
            raise RuntimeError(
                "Operation start handler method is an `async def` and "
                "cannot be called from a sync handler. "
            )
        # TODO(preview): apply middleware stack as composed functions
        if not self.executor:
            raise RuntimeError(
                "Operation start handler method is not an `async def` but "
                "no executor was provided to the Handler constructor. "
            )
        return self.executor.submit(op_handler.start, ctx, deserialized_input).result()

    def cancel_operation(self, ctx: CancelOperationContext, token: str) -> None:
        """Handle a Cancel Operation request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        if is_async_callable(op_handler.cancel):
            raise RuntimeError(
                "Operation cancel handler method is an `async def` and "
                "cannot be called from a sync handler. "
            )
        else:
            if not self.executor:
                raise RuntimeError(
                    "Operation cancel handler method is not an `async def` function but "
                    "no executor was provided to the Handler constructor."
                )
            return self.executor.submit(op_handler.cancel, ctx, token).result()

    def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> OperationInfo:
        raise NotImplementedError

    def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Any:
        raise NotImplementedError


@dataclass(frozen=True)
class ServiceHandler:
    """Internal representation of a user's Nexus service implementation instance.

    A user's service implementation is a class decorated with
    :py:func:`@nexusrpc.handler.service_handler` that defines operation handler methods
    using decorators such as :py:func:`@nexusrpc.handler.operation_handler`.

    Instances of this class are created automatically from user service implementation instances
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

        service = get_service_definition(user_instance.__class__)
        if not isinstance(service, nexusrpc.ServiceDefinition):
            raise RuntimeError(
                f"Service '{user_instance}' does not have a service definition. "
                f"Use the :py:func:`@nexusrpc.handler.service_handler` decorator on your class to define "
                f"a Nexus service implementation."
            )

        # Construct a map of operation handlers keyed by the op name from the service
        # definition (i.e. by the name by which the operation can be requested)
        factories_by_method_name = collect_operation_handler_factories_by_method_name(
            user_instance.__class__, service
        )
        op_handlers = {
            op_name: factories_by_method_name[op.method_name](user_instance)
            for op_name, op in service.operations.items()
            # TODO(preview): op.method_name should be non-nullable
            if op.method_name
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
                f"There are {len(self.service.operations)} operations in the definition"
            )
            if self.service.operations:
                msg += f": {', '.join(sorted(self.service.operations.keys()))}"
            msg += "."
            raise HandlerError(msg, type=HandlerErrorType.NOT_FOUND)
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
            raise HandlerError(msg, type=HandlerErrorType.NOT_FOUND)
        return operation_handler


# TODO(prerelease): Do we want to require users to create this wrapper? Two
# alternatives:
#
# 1. Require them to pass in a `concurrent.futures.Executor`. This is what
#    `run_in_executor` is documented to require. This would mean that nexusrpc would
#    initially have a hard-coded dependency on the asyncio event loop. But perhaps that
#    is not a problem: if we ever want to support other event loops, we can add the
#    ability to pass in an event loop implementation at the level of Handler. And in
#    fact perhaps that's better than having the user choose their event loop once in
#    their Executor, and also in other places in nexusrpc.
#    https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
#
# 2. Define an interface (typing.Protocol), containing `def submit(...)` and perhaps
#    nothing else, and require them to pass in anything that implements the interface.
#    But this seems dangerous/a non-starter: run_in_executor is documented to require a
#    `concurrent.futures.Executor`, even if it is currently typed as taking Any.
#
# I've switched to alternative (1). The following class is no longer in the public API
# of nexusrpc.
class _Executor:
    """An executor for synchronous functions."""

    def __init__(self, executor: concurrent.futures.Executor):
        self._executor = executor

    def submit_to_event_loop(
        self, fn: Callable[..., Any], *args: Any
    ) -> Awaitable[Any]:
        return asyncio.get_event_loop().run_in_executor(self._executor, fn, *args)

    def submit(
        self, fn: Callable[..., Any], *args: Any
    ) -> concurrent.futures.Future[Any]:
        return self._executor.submit(fn, *args)
