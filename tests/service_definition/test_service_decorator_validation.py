import pytest
from typing_extensions import dataclass_transform

import nexusrpc


class Output:
    pass


@dataclass_transform()
class _BaseTestCase:
    pass


class _TestCase(_BaseTestCase):
    Contract: type
    expected_error: Exception


class DuplicateOperationNameOverride(_TestCase):
    class Contract:
        a: nexusrpc.Operation[None, Output] = nexusrpc.Operation(name="a")
        b: nexusrpc.Operation[int, str] = nexusrpc.Operation(name="a")

    expected_error = RuntimeError(
        r"Operation 'a' in service .* is defined multiple times"
    )


@pytest.mark.parametrize(
    "test_case",
    [
        DuplicateOperationNameOverride,
    ],
)
def test_operation_validation(
    test_case: type[_TestCase],
):
    with pytest.raises(
        type(test_case.expected_error),
        match=str(test_case.expected_error),
    ):
        nexusrpc.service(test_case.Contract)


def test_empty_service_name_raises():
    """Empty string passed to @service(name='') should raise."""
    with pytest.raises(ValueError, match=r"Service name '' must not be empty"):

        @nexusrpc.service(name="")
        class MyService:  # pyright: ignore[reportUnusedClass]
            op: nexusrpc.Operation[str, str]


def test_whitespace_only_service_name_raises():
    """Whitespace-only service name should raise."""
    with pytest.raises(ValueError, match=r"Service name '   ' must not be empty"):

        @nexusrpc.service(name="   ")
        class MyService:  # pyright: ignore[reportUnusedClass]
            op: nexusrpc.Operation[str, str]


def test_non_url_encodable_service_name_raises():
    """Service name with non-URL-encodable characters should raise."""
    with pytest.raises(
        ValueError,
        match=r"Service name .* contains characters that cannot be URL-encoded",
    ):

        @nexusrpc.service(name="invalid\ud800surrogate")
        class MyService:  # pyright: ignore[reportUnusedClass]
            op: nexusrpc.Operation[str, str]


def test_valid_service_name_with_special_chars_succeeds():
    """Service names with URL-encodable special characters should succeed."""

    @nexusrpc.service(name="my service")
    class ServiceWithSpace:
        op: nexusrpc.Operation[str, str]

    _ = ServiceWithSpace

    @nexusrpc.service(name="service/with/slashes")
    class ServiceWithSlashes:
        op: nexusrpc.Operation[str, str]

    _ = ServiceWithSlashes

    @nexusrpc.service(name="日本語サービス")
    class ServiceWithUnicode:
        op: nexusrpc.Operation[str, str]

    _ = ServiceWithUnicode

    @nexusrpc.service(name="service?query=value")
    class ServiceWithQueryChars:
        op: nexusrpc.Operation[str, str]

    _ = ServiceWithQueryChars


def test_empty_operation_name_raises():
    """Empty operation name should raise."""
    with pytest.raises(ValueError, match=r"Operation name '' must not be empty"):

        @nexusrpc.service
        class MyService:  # pyright: ignore[reportUnusedClass]
            op: nexusrpc.Operation[str, str] = nexusrpc.Operation(name="")


def test_whitespace_only_operation_name_raises():
    """Whitespace-only operation name should raise."""
    with pytest.raises(ValueError, match=r"Operation name '   ' must not be empty"):

        @nexusrpc.service
        class MyService:  # pyright: ignore[reportUnusedClass]
            op: nexusrpc.Operation[str, str] = nexusrpc.Operation(name="   ")


def test_non_url_encodable_operation_name_raises():
    """Operation name with non-URL-encodable characters should raise."""
    with pytest.raises(
        ValueError,
        match=r"Operation name .* contains characters that cannot be URL-encoded",
    ):

        @nexusrpc.service
        class MyService:  # pyright: ignore[reportUnusedClass]
            op: nexusrpc.Operation[str, str] = nexusrpc.Operation(
                name="invalid\ud800surrogate"
            )


def test_valid_operation_name_with_special_chars_succeeds():
    """Operation names with URL-encodable special characters should succeed."""

    @nexusrpc.service
    class MyService:
        op_with_space: nexusrpc.Operation[str, str] = nexusrpc.Operation(
            name="my operation"
        )
        op_with_slash: nexusrpc.Operation[str, str] = nexusrpc.Operation(
            name="op/with/slashes"
        )
        op_unicode: nexusrpc.Operation[str, str] = nexusrpc.Operation(name="日本語操作")
        op_query: nexusrpc.Operation[str, str] = nexusrpc.Operation(
            name="op?param=value"
        )

    _ = MyService
