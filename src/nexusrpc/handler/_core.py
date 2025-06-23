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
import nexusrpc._service
from nexusrpc.handler._util import is_async_callable

from .._serializer import LazyValue
from ._common import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    HandlerError,
    HandlerErrorType,
    OperationInfo,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)
from ._operation_handler import (
    OperationHandler,
    collect_operation_handler_factories,
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
            if self.executor is None:
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

    def _get_service_handler(self, service_name: str) -> ServiceHandler:
        """Return a service handler, given the service name."""
        service = self.service_handlers.get(service_name)
        if service is None:
            raise HandlerError(
                f"No handler for service '{service_name}'.",
                type=HandlerErrorType.NOT_FOUND,
            )
        return service


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
        raise NotImplementedError

    async def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Any:
        raise NotImplementedError


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
