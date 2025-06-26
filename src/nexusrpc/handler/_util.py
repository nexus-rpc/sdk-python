from __future__ import annotations

import functools
import inspect
import typing
import warnings
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Optional, Type, Union

from typing_extensions import TypeGuard

import nexusrpc
from nexusrpc.handler._common import StartOperationContext
from nexusrpc.types import InputT, OutputT, ServiceHandlerT

if TYPE_CHECKING:
    from nexusrpc.handler._operation_handler import OperationHandler


def get_start_method_input_and_output_type_annotations(
    start: Callable[
        [ServiceHandlerT, StartOperationContext, InputT],
        Union[OutputT, Awaitable[OutputT]],
    ],
) -> tuple[
    Optional[Type[InputT]],
    Optional[Type[OutputT]],
]:
    """Return operation input and output types.

    `start` must be a type-annotated start method that returns a synchronous result.
    """
    try:
        type_annotations = typing.get_type_hints(start)
    except TypeError:
        # TODO(preview): stacklevel
        warnings.warn(
            f"Expected decorated start method {start} to have type annotations"
        )
        return None, None
    output_type = type_annotations.pop("return", None)

    if len(type_annotations) != 2:
        # TODO(preview): stacklevel
        suffix = f": {type_annotations}" if type_annotations else ""
        warnings.warn(
            f"Expected decorated start method {start} to have exactly 2 "
            f"type-annotated parameters (ctx and input), but it has {len(type_annotations)}"
            f"{suffix}."
        )
        input_type = None
    else:
        ctx_type, input_type = type_annotations.values()
        if not issubclass(ctx_type, StartOperationContext):
            # TODO(preview): stacklevel
            warnings.warn(
                f"Expected first parameter of {start} to be an instance of "
                f"StartOperationContext, but is {ctx_type}."
            )
            input_type = None

    return input_type, output_type


def get_operation_factory(
    obj: Any,
) -> tuple[
    Optional[Callable[[Any], OperationHandler[InputT, OutputT]]],
    Optional[nexusrpc.Operation[InputT, OutputT]],
]:
    op_defn = getattr(obj, "__nexus_operation__", None)
    if op_defn:
        factory = obj
    else:
        if factory := getattr(obj, "__nexus_operation_factory__", None):
            op_defn = getattr(factory, "__nexus_operation__", None)
    if not isinstance(op_defn, nexusrpc.Operation):
        return None, None
    return factory, op_defn


# Copied from https://github.com/modelcontextprotocol/python-sdk
#
# Copyright (c) 2024 Anthropic, PBC.
#
# Modified to use TypeGuard.
#
# This file is licensed under the MIT License.
def is_async_callable(obj: Any) -> TypeGuard[Callable[..., Awaitable[Any]]]:
    """
    Return True if `obj` is an async callable.

    Supports partials of async callable class instances.
    """
    while isinstance(obj, functools.partial):
        obj = obj.func

    return inspect.iscoroutinefunction(obj) or (
        callable(obj) and inspect.iscoroutinefunction(getattr(obj, "__call__", None))
    )
