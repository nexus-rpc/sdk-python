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

from nexusrpc.types import InputT, OutputT

from ._common import StartOperationContext


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
