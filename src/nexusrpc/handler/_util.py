from __future__ import annotations

import typing
import warnings
from typing import (
    Awaitable,
    Callable,
    Type,
    Union,
)

from ._common import StartOperationContext
from ._types import MISSING_TYPE, I, O, S


# TODO(dan): naming, visibility, make this less awkward
# TODO(dan): are we counting number of params or annotations?
# TODO(dan): support sync start method
def get_input_and_output_types_from_sync_operation_start_method(
    start_method: Callable[[S, StartOperationContext, I], Union[O, Awaitable[O]]],
) -> tuple[
    Union[Type[I], Type[MISSING_TYPE]],
    Union[Type[O], Type[MISSING_TYPE]],
]:
    """Return operation input and output types.

    `start_method` must be a type-annotated start method that returns a synchronous result.
    """
    try:
        type_annotations = typing.get_type_hints(start_method)
    except TypeError:
        # TODO(dan): stacklevel
        warnings.warn(
            f"Expected decorated start method {start_method} to have type annotations"
        )
        return MISSING_TYPE, MISSING_TYPE
    output_type = type_annotations.pop("return", MISSING_TYPE)

    if len(type_annotations) != 2:
        # TODO(dan): stacklevel
        warnings.warn(
            f"Expected decorated start method {start_method} to have exactly two type-annotated parameters (ctx and input) "
            f"but has {len(type_annotations)}: {type_annotations}"
        )
        input_type = MISSING_TYPE
    else:
        ctx_type, input_type = type_annotations.values()
        if not issubclass(ctx_type, StartOperationContext):
            # TODO(dan): stacklevel
            warnings.warn(
                f"Expected first parameter of {start_method} to be an instance of StartOperationContext, "
                f"but is {ctx_type}"
            )
            input_type = MISSING_TYPE

    return input_type, output_type
