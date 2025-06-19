from __future__ import annotations

import inspect
import typing
import warnings
from abc import ABC, abstractmethod
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Optional,
    Type,
    Union,
)

import nexusrpc
import nexusrpc._service
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
        {
            op.method_name
            for op in service.operations.values()
            if op.method_name is not None
        }
        if service
        else set()
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
                f"which is not compatible with the input type '{op_defn.input_type}' "
                f" in interface '{service_definition.name}'. The input type must be the same as or a "
                f"superclass of the operation definition input type."
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
