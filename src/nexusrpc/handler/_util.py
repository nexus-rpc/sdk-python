from __future__ import annotations

import functools
import inspect
import typing
import warnings
from typing import (
    Any,
    Awaitable,
    Callable,
    Optional,
    Type,
    Union,
)

from typing_extensions import TypeGuard

from nexusrpc.types import InputT, OutputT, ServiceHandlerT

from ._common import StartOperationContext


def get_start_method_input_and_output_types_annotations(
    start_method: Callable[
        [ServiceHandlerT, StartOperationContext, InputT],
        Union[OutputT, Awaitable[OutputT]],
    ],
) -> tuple[
    Optional[Type[InputT]],
    Optional[Type[OutputT]],
]:
    """Return operation input and output types.

    `start_method` must be a type-annotated start method that returns a synchronous result.
    """
    try:
        type_annotations = typing.get_type_hints(start_method)
    except TypeError:
        # TODO(preview): stacklevel
        warnings.warn(
            f"Expected decorated start method {start_method} to have type annotations"
        )
        return None, None
    output_type = type_annotations.pop("return", None)

    if len(type_annotations) != 2:
        # TODO(preview): stacklevel
        warnings.warn(
            f"Expected decorated start method {start_method} to have exactly two "
            f"type-annotated parameters (ctx and input), but has {len(type_annotations)}: "
            f"{type_annotations}."
        )
        input_type = None
    else:
        ctx_type, input_type = type_annotations.values()
        if not issubclass(ctx_type, StartOperationContext):
            # TODO(preview): stacklevel
            warnings.warn(
                f"Expected first parameter of {start_method} to be an instance of "
                f"StartOperationContext, but is {ctx_type}."
            )
            input_type = None

    return input_type, output_type


# Copied from https://github.com/modelcontextprotocol/python-sdk
#
# Copyright (c) 2024 Anthropic, PBC.
#
# Modified to use TypeIs.
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
