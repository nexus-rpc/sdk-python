from __future__ import annotations

import asyncio
import concurrent
import inspect
import typing
import warnings
from abc import ABC, abstractmethod
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
import nexusrpc.contract

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
from ._types import MISSING_TYPE, I, O, S


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

    Operation requests are delegated to a ServiceHandler based on the service name in the
    OperationContext, and then to an OperationHandler based on the operation name in the
    OperationContext.
    """

    service_handlers: dict[str, ServiceHandler]

    def __init__(
        self,
        user_service_handlers: Sequence[Any],
        executor: Optional[SyncFuncExecutor] = None,
    ):
        """Initialize a Handler instance from user service handler instances.

        The user service handler instances must have been decorated with the
        @nexusrpc.handler.service_handler decorator.

        Args:
            user_service_handlers: A sequence of user service handlers.
        """
        self.executor = executor
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
                    if not inspect.iscoroutinefunction(operation_handler.start):
                        raise RuntimeError(
                            f"Service '{sh.service.name}' operation '{op_name}' start must be async if no executor is provided."
                        )
                    # Cancel isn't currently made async on SyncOperationHandler I think
                    if not inspect.iscoroutinefunction(operation_handler.cancel):
                        raise RuntimeError(
                            f"Service '{sh.service.name}' operation '{op_name}' cancel must be async if no executor is provided."
                        )
            self.service_handlers[sh.service.name] = sh

    async def start_operation(
        self,
        ctx: StartOperationContext,
        service: str,
        operation: str,
        lazy_value: LazyValue,  # TODO(dan): what should the name of this parameter be?
    ) -> Union[
        StartOperationResultSync[Any],
        StartOperationResultAsync,
    ]:
        """Handle a start operation request.

        Args:
            ctx: The operation context.
            service: The name of the service to handle the operation.
            operation: The name of the operation to handle.
            lazy_value: The serialized input to the operation.
        """
        service_handler = self._get_service_handler(service)
        op_handler = service_handler._get_operation_handler(operation)
        op = service_handler.service.operations[operation]
        input = await lazy_value.consume(as_type=op.input_type)
        if inspect.iscoroutinefunction(op_handler.start):
            # TODO(dan): apply middleware stack as composed awaitables
            return await op_handler.start(ctx, input)
        else:
            # TODO(dan): apply middleware stack as composed functions
            # TODO(dan): support passing executor for non-async start methods
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.executor, op_handler.start, ctx, input
            )

    async def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, service: str, operation: str, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]:
        """Handle a fetch operation info request.

        Args:
            ctx: The operation context.
            service: The name of the service.
            operation: The name of the operation.
            token: The operation token.
        """
        raise NotImplementedError

    async def fetch_operation_result(
        self, ctx: FetchOperationResultContext, service: str, operation: str, token: str
    ) -> Union[Any, Awaitable[Any]]:
        """Handle a fetch operation result request.

        Args:
            ctx: The operation context.
            service: The name of the service.
            operation: The name of the operation.
            token: The operation token.
        """
        raise NotImplementedError

    async def cancel_operation(
        self, ctx: CancelOperationContext, service: str, operation: str, token: str
    ) -> Union[None, Awaitable[None]]:
        """Handle a cancel operation request.

        Args:
            ctx: The operation context.
            service: The name of the service.
            operation: The name of the operation.
            token: The operation token.
        """
        service_handler = self._get_service_handler(service)
        op_handler = service_handler._get_operation_handler(operation)
        if inspect.iscoroutinefunction(
            op_handler.cancel
        ) or inspect.iscoroutinefunction(op_handler.cancel.__call__):
            # pyright does not infer awaitable from iscoroutinefunction(__call__)
            return await op_handler.cancel(ctx, token)  # type: ignore
        else:
            raise NotImplementedError(
                "Nexus operation cancel method must be an `async def`."
            )
            op_handler.cancel(ctx, token)

    def _get_service_handler(self, service_name: str) -> ServiceHandler:
        """Return a service handler, given the service name."""
        service = self.service_handlers.get(service_name)
        if service is None:
            # TODO(dan): can this raise HandlerError directly or is HandlerError always a
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
    class contains the OperationHandler instances themselves.

    You may create instances of this class manually and pass them to the Handler
    constructor, for example when programatically creating Nexus service implementations.
    """

    service: nexusrpc.contract.Service
    operation_handlers: dict[str, OperationHandler[Any, Any]]

    @classmethod
    def from_user_instance(cls, user_instance: Any) -> Self:
        """Create a ServiceHandler from a user service instance."""

        service = getattr(user_instance.__class__, "__nexus_service__", None)
        if not isinstance(service, nexusrpc.contract.Service):
            raise RuntimeError(
                f"Service '{user_instance}' does not have a service contract definition. "
                f"Use the :py:func:`@nexusrpc.handler.service_handler` decorator on your class to define "
                f"a Nexus service implementation."
            )
        # TODO(dan): looks like this isn't using name overrides; test coverage?
        op_handlers = {
            name: factory(user_instance)
            for name, factory in collect_operation_handler_methods(
                user_instance.__class__
            ).items()
        }

        return cls(
            service=service,
            operation_handlers=op_handlers,
        )

    def _get_operation_handler(self, operation: str) -> OperationHandler:
        """Return an operation handler, given the operation name."""
        if operation not in self.service.operations:
            msg = (
                f"Nexus service contract '{self.service.name}' has no operation '{operation}'. "
                f"There are {len(self.service.operations)} operations defined in the contract"
            )
            if self.service.operations:
                msg += f": {', '.join(sorted(self.service.operations.keys()))}"
            msg += "."
            raise UnknownOperationError(msg)
        operation_handler = self.operation_handlers.get(operation)
        if operation_handler is None:
            # TODO(dan): This should not be possible. If contract was supplied then this was checked; if
            # not then contract was generated from the operation handlers.
            msg = (
                f"Nexus service implementation '{self.service.name}' has no handler for operation '{operation}'. "
                f"There are {len(self.operation_handlers)} available operation handlers"
            )
            if self.operation_handlers:
                msg += f": {', '.join(sorted(self.operation_handlers.keys()))}"
            msg += "."
            raise UnknownOperationError(msg)
        return operation_handler


class OperationHandler(ABC, Generic[I, O]):
    """
    Base class for an operation handler in a Nexus service implementation.

    To define a Nexus operation handler, create a method on your service handler class
    that takes `self` and returns an instance of OperationHandler, and apply the
    :py:func:`@nexusrpc.handler.operation_handler` decorator.

    Alternatively, to create an operation handler that is limited to returning
    synchronously, create the start method of the OperationHandler on your service handler
    class and apply the :py:func:`@nexusrpc.handler.sync_operation_handler` decorator.
    """

    @abstractmethod
    def start(
        self, ctx: StartOperationContext, input: I
    ) -> Union[
        StartOperationResultSync[O],
        Awaitable[StartOperationResultSync[O]],
        Awaitable[StartOperationResultAsync],
    ]:
        """
        Start the operation, completing either synchronously or asynchronously.

        Either returns the result synchronously, or returns an operation token. Which
        path is taken may be decided at operation handling time.
        """
        ...

    # TODO(dan): test coverage
    @abstractmethod
    def fetch_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]:
        """
        Return information about the current status of the operation.
        """
        ...

    # TODO(dan): test coverage
    @abstractmethod
    def fetch_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Union[O, Awaitable[O]]:
        """
        Fetch the result of the operation.
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


class SyncOperationHandler(OperationHandler[I, O]):
    """
    An OperationHandler that is limited to responding synchronously.
    """

    def start(
        self, ctx: StartOperationContext, input: I
    ) -> Union[
        StartOperationResultSync[O],
        Awaitable[StartOperationResultSync[O]],
        Awaitable[StartOperationResultAsync],
    ]:
        """
        Note that this method may be either `async def` or `def`. 'SyncOperationHandler'
        means that the operation responds synchronously according to the Nexus protocol;
        it doesn't refer to whether the implementation of the method uses an event loop.
        However, a `def` method returning an Awaitable is not supported; use `async def`
        instead.
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
    ) -> Union[O, Awaitable[O]]:
        raise NotImplementedError(
            "Cannot fetch the result of an operation that responded synchronously."
        )

    async def cancel(
        self, ctx: CancelOperationContext, token: str
    ) -> Union[None, Awaitable[None]]:
        raise NotImplementedError(
            "An operation that responded synchronously cannot be cancelled."
        )


def collect_operation_handler_methods(
    user_service_cls: Type[S],
) -> dict[str, Callable[[S], OperationHandler[Any, Any]]]:
    """
    Collect operation handler methods from a user service handler class.
    """
    impl_ops = {}
    for name, method in inspect.getmembers(user_service_cls, inspect.isfunction):
        if op := getattr(method, "__nexus_operation__", None):
            impl_ops[op.name] = method
        # Check for accidentally missing decorator on an OperationHandler factory
        # TODO(dan): support disabling warning in @service_handler decorator?
        elif (
            typing.get_origin(typing.get_type_hints(method).get("return"))
            == OperationHandler
        ):
            warnings.warn(
                f"Method '{name}' in class '{user_service_cls.__name__}' "
                f"returns OperationHandler but has not been decorated. "
                f"Did you forget to apply the @nexusrpc.handler.operation_handler decorator?",
                UserWarning,
                stacklevel=2,
            )
    return impl_ops


def validate_operation_handler_methods(
    user_service_cls: Type[S],
    user_methods: dict[str, Callable[[S], OperationHandler[Any, Any]]],
    service: nexusrpc.contract.Service,
):
    """Validate operation handler methods against a service contract."""
    for op_name, op_contract in service.operations.items():
        if op_name not in user_methods:
            raise TypeError(
                f"Service '{user_service_cls}' does not implement operation '{op_name}' in interface '{service}'. "
            )
        method = user_methods[op_name]
        op: nexusrpc.contract.Operation = method.__nexus_operation__
        if op.input_type != op_contract.input_type and op.input_type != MISSING_TYPE:
            raise TypeError(
                f"Operation '{op_name}' in service '{user_service_cls}' has input type '{op.input_type}', "
                f"which does not match the input type '{op_contract.input_type}' in interface '{service}'."
            )
        if op.output_type != op_contract.output_type and op.output_type != MISSING_TYPE:
            raise TypeError(
                f"Operation '{op_name}' in service '{user_service_cls}' has output type '{op.output_type}', "
                f"which does not match the output type '{op_contract.output_type}' in interface '{service}'."
            )
    # TODO(dan): warn on superfluous ops?
    if not (user_methods.keys() >= service.operations.keys()):
        raise TypeError(
            f"Service '{user_service_cls}' does not implement all operations in interface '{service}'. "
            f"Missing operations: {service.operations.keys() - user_methods.keys()}"
        )


def service_from_operation_handler_methods(
    service_name: str,
    user_methods: dict[str, Callable[[S], OperationHandler[Any, Any]]],
) -> nexusrpc.contract.Service:
    """
    Create a service contract definition from operation handler factory methods.

    In general, users should have access to, or define, a service contract, and validate
    their service handler against it by passing the service contract to the
    @service_handler decorator. This function is used when that is not the case.
    """
    operations = {}
    for name, method in user_methods.items():
        op = getattr(method, "__nexus_operation__", None)
        if not isinstance(op, nexusrpc.contract.Operation):
            raise RuntimeError(
                f"In service '{service_name}', could not locate operation definition for "
                f"user operation handler method '{name}'. Did you forget to decorate the operation "
                f"method with an operation handler decorator such as "
                f":py:func:`@nexusrpc.handler.operation_handler` or "
                f":py:func:`@nexusrpc.handler.sync_operation_handler`?"
            )
        operations[op.name] = op

    return nexusrpc.contract.Service(name=service_name, operations=operations)


class SyncFuncExecutor:
    """
    Run a synchronous function asynchronously.
    """

    @abstractmethod
    def run_sync(self, fn, *args) -> Awaitable[Any]:
        """
        Run a synchronous function asynchronously.
        """
        ...
