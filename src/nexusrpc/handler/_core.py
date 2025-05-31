from __future__ import annotations

import inspect
import typing
import warnings
from dataclasses import dataclass
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
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
    OperationContext,
    OperationInfo,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)
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

    def __init__(self, user_service_handlers: Sequence[Any]):
        """Initialize a Handler instance from user service handler instances.

        The user service handler instances must have been decorated with the
        @nexusrpc.handler.service_handler decorator.

        Args:
            user_service_handlers: A sequence of user service handlers.
        """
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
            if sh.name in self.service_handlers:
                raise RuntimeError(f"Service '{sh.name}' has already been registered.")
            self.service_handlers[sh.name] = sh

    async def start_operation(
        self, ctx: StartOperationContext, service: str, operation: str, input: Any
    ) -> Union[
        StartOperationResultSync[Any],
        Awaitable[StartOperationResultSync[Any]],
        Awaitable[StartOperationResultAsync],
    ]:
        """Handle a start operation request.

        Args:
            ctx: The operation context.
            service: The name of the service to handle the operation.
            operation: The name of the operation to handle.
            input: The serialized input to the operation.
        """
        op_handler = self.get_operation_handler(ctx)
        if inspect.iscoroutinefunction(op_handler.start):
            return await op_handler.start(ctx, input)
        else:
            return op_handler.start(ctx, input)

    def _start_operation_sync(
        self, ctx: StartOperationContext, service: str, operation: str, input: Any
    ) -> Union[StartOperationResultSync[Any], StartOperationResultAsync]:
        op_handler = self.get_operation_handler(ctx)
        assert not inspect.iscoroutinefunction(op_handler.start)
        return op_handler.start(ctx, input)

    async def _start_operation_async(
        self, ctx: StartOperationContext, service: str, operation: str, input: Any
    ) -> Union[StartOperationResultSync[Any], StartOperationResultAsync]:
        op_handler = self.get_operation_handler(ctx)
        return await op_handler.start(ctx, input)

    def fetch_operation_info(
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

    def fetch_operation_result(
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

    def cancel_operation(
        self, ctx: CancelOperationContext, service: str, operation: str, token: str
    ) -> Union[None, Awaitable[None]]:
        """Handle a cancel operation request.

        Args:
            ctx: The operation context.
            service: The name of the service.
            operation: The name of the operation.
            token: The operation token.
        """
        raise NotImplementedError

    def get_operation_handler(self, ctx: OperationContext) -> OperationHandler:
        """Return an operation handler, given the service and operation names from context."""
        service = self.service_handlers.get(ctx.service)
        if service is None:
            # TODO(dan): can this raise HandlerError directly or is HandlerError always a
            # wrapper? I have currently made its __cause__ required but if it's not a
            # wrapper then that is wrong.
            raise UnknownServiceError(
                f"Nexus service '{ctx.service}' has not been registered."
            )
        operation_handler = service.operation_handlers.get(ctx.operation)
        if operation_handler is None:
            raise UnknownOperationError(
                f"Nexus service '{ctx.service}' has no operation '{ctx.operation}'."
            )
        return operation_handler


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

    name: str
    operation_handlers: dict[str, OperationHandler[Any, Any]]

    @classmethod
    def from_user_instance(cls, user_instance: Any) -> Self:
        """Create a ServiceHandler from a user service instance."""

        # set on the class by @service_handler
        service = getattr(user_instance.__class__, "__nexus_service__", None)
        if not isinstance(service, nexusrpc.contract.Service):
            raise RuntimeError(
                f"Service '{user_instance}' does not have a service contract definition. "
                f"Use the :py:func:`@nexusrpc.handler.service_handler` decorator on your class to define "
                f"a Nexus service implementation."
            )
        op_factories = collect_operation_handler_methods(user_instance.__class__)
        return cls(
            name=service.name,
            operation_handlers={
                # TODO(dan): looks like this isn't using name overrides; test coverage?
                op_name: op_factory(user_instance)
                for op_name, op_factory in op_factories.items()
            },
        )


# TODO(dan): ABC? *_operation decorator impls will need adjusting
class OperationHandler(Generic[I, O]):
    """
    Base class for an operation handler in a Nexus service implementation.

    To define a Nexus operation handler, create a method on your service handler class
    that takes `self` and returns an instance of OperationHandler, and apply the
    :py:func:`@nexusrpc.handler.operation_handler` decorator.

    Alternatively, to create an operation handler that is limited to returning
    synchronously, create the start method of the OperationHandler on your service handler
    class and apply the :py:func:`@nexusrpc.handler.sync_operation_handler` decorator.
    """

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
        raise NotImplementedError

    def fetch_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]:
        """
        Return information about the current status of the operation.
        """
        raise NotImplementedError

    def fetch_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Union[O, Awaitable[O]]:
        """
        Fetch the result of the operation.
        """
        raise NotImplementedError

    def cancel(
        self, ctx: CancelOperationContext, token: str
    ) -> Union[None, Awaitable[None]]:
        """
        Cancel the operation.
        """
        raise NotImplementedError


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
            typing.get_origin(inspect.signature(method).return_annotation)
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
