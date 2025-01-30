import warnings
from typing import (
    Any,
    Awaitable,
    Callable,
    Type,
    Union,
    get_args,
    get_origin,
)

import pytest

from nexusrpc.handler import (
    MISSING_TYPE,
    StartOperationContext,
    get_input_and_output_types_from_sync_operation_start_method,
)


class Input:
    pass


class Output:
    pass


class _TestCase:
    start: Callable
    expected_types: tuple[Type[Any], Type[Any]]


class SyncMethod(_TestCase):
    def start(self, ctx: StartOperationContext, i: Input) -> Output: ...

    expected_types = (Input, Output)


class AsyncMethod(_TestCase):
    async def start(self, ctx: StartOperationContext, i: Input) -> Output: ...

    expected_types = (Input, Output)


class UnionMethod(_TestCase):
    def start(
        self, ctx: StartOperationContext, i: Input
    ) -> Union[Output, Awaitable[Output]]: ...

    expected_types = (Input, Union[Output, Awaitable[Output]])


class MissingInputAnnotationInUnionMethod(_TestCase):
    def start(
        self, ctx: StartOperationContext, i
    ) -> Union[Output, Awaitable[Output]]: ...

    expected_types = (MISSING_TYPE, Union[Output, Awaitable[Output]])


class TooFewParams(_TestCase):
    def start(self, i: Input) -> Output: ...

    expected_types = (MISSING_TYPE, Output)


class TooManyParams(_TestCase):
    def start(self, ctx: StartOperationContext, i: Input, extra: int) -> Output: ...

    expected_types = (MISSING_TYPE, Output)


class WrongOptionsType(_TestCase):
    def start(self, ctx: int, i: Input) -> Output: ...

    expected_types = (MISSING_TYPE, Output)


class NoReturnHint(_TestCase):
    def start(self, ctx: StartOperationContext, i: Input): ...

    expected_types = (Input, MISSING_TYPE)


class NoInputAnnotation(_TestCase):
    def start(self, ctx: StartOperationContext, i) -> Output: ...

    expected_types = (MISSING_TYPE, Output)


class NoOptionsAnnotation(_TestCase):
    def start(self, ctx, i: Input) -> Output: ...

    expected_types = (MISSING_TYPE, Output)


class AllAnnotationsMissing(_TestCase):
    def start(self, ctx: StartOperationContext, i: Input): ...

    expected_types = (Input, MISSING_TYPE)


@pytest.mark.parametrize(
    "test_case",
    [
        SyncMethod,
        AsyncMethod,
        UnionMethod,
        TooFewParams,
        TooManyParams,
        WrongOptionsType,
        NoReturnHint,
        NoInputAnnotation,
        NoOptionsAnnotation,
        MissingInputAnnotationInUnionMethod,
        AllAnnotationsMissing,
    ],
)
def test_get_input_and_output_types(test_case: Type[_TestCase]):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        input_type, output_type = (
            get_input_and_output_types_from_sync_operation_start_method(test_case.start)
        )
        expected_input_type, expected_output_type = test_case.expected_types
        assert input_type is expected_input_type

        expected_origin = get_origin(expected_output_type)
        if expected_origin:  # Awaitable and Union cases
            assert get_origin(output_type) is expected_origin
            assert get_args(output_type) == get_args(expected_output_type)
        else:
            assert output_type is expected_output_type
