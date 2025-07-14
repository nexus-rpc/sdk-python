from __future__ import annotations

import typing
import warnings
from collections.abc import Awaitable
from typing import (
    TYPE_CHECKING,
    Callable,
    Optional,
    TypeVar,
    Union,
)

from nexusrpc.handler import StartOperationContext

if TYPE_CHECKING:
    from nexusrpc import InputT, OutputT


ServiceHandlerT = TypeVar("ServiceHandlerT")


# TODO(preview): is it ever valid for this to return None (as opposed to NoneType)?
def get_start_method_input_and_output_type_annotations(
    start: Callable[
        [ServiceHandlerT, StartOperationContext, InputT],
        Union[OutputT, Awaitable[OutputT]],
    ],
) -> tuple[
    Optional[type[InputT]],
    Optional[type[OutputT]],
]:
    """Return operation input and output types.

    `start` must be a type-annotated start method that returns a synchronous result.
    """
    try:
        type_annotations = typing.get_type_hints(start)
    except TypeError:
        return None, None
    output_type = type_annotations.pop("return", None)

    if len(type_annotations) != 2:
        input_type = None
    else:
        ctx_type, input_type = type_annotations.values()
        if not issubclass(ctx_type, StartOperationContext):
            warnings.warn(
                f"Expected first parameter of {start} to be an instance of "
                f"StartOperationContext, but is {ctx_type}."
            )
            input_type = None

    return input_type, output_type
