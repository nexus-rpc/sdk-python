from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from logging import getLogger
from typing import TypeVar

logger = getLogger(__name__)

InputT = TypeVar("InputT", contravariant=True)
"""Operation input type"""

OutputT = TypeVar("OutputT", covariant=True)
"""Operation output type"""

ServiceHandlerT = TypeVar("ServiceHandlerT")
"""A user's service handler class, typically decorated with @service_handler"""

ServiceT = TypeVar("ServiceT")
"""A user's service definition class, typically decorated with @service"""


class HandlerError(Exception):
    """
    A Nexus handler error.

    This exception is used to represent errors that occur during the handling of a
    Nexus operation that should be reported to the caller as a handler error.

    Example:
        .. code-block:: python

            import nexusrpc

            # Raise a bad request error
            raise nexusrpc.HandlerError(
                "Invalid input provided",
                error_type=nexusrpc.HandlerErrorType.BAD_REQUEST
            )

            # Raise a retryable internal error
            raise nexusrpc.HandlerError(
                "Database unavailable",
                error_type=nexusrpc.HandlerErrorType.INTERNAL,
                retryable_override=True
            )
    """

    def __init__(
        self,
        message: str,
        *,
        error_type: HandlerErrorType | str,
        retryable_override: bool | None = None,
    ):
        """
        Initialize a new HandlerError.

        :param message: A descriptive message for the error. This will become
                        the `message` in the resulting Nexus Failure object.

        :param error_type: The :py:class:`HandlerErrorType` of the error, or a
                          string representation of the error type. If a string is
                          provided and doesn't match a known error type, it will
                          be treated as UNKNOWN and a warning will be logged.

        :param retryable_override: Optionally set whether the error should be
                                   retried. By default, the error type is used
                                   to determine this.
        """
        super().__init__(message)

        # Handle string error types
        if isinstance(error_type, str):
            raw_error_type = error_type
            try:
                error_type = HandlerErrorType[error_type]
            except KeyError:
                logger.warning(f"Unknown Nexus HandlerErrorType: {error_type}")
                error_type = HandlerErrorType.UNKNOWN
        else:
            raw_error_type = error_type.value

        self.error_type = error_type
        self.raw_error_type = raw_error_type
        self.retryable_override = retryable_override

    @property
    def retryable(self) -> bool:
        """
        Whether this error should be retried.

        If :py:attr:`retryable_override` is None, then the default behavior for the
        error type is used. See
        https://github.com/nexus-rpc/api/blob/main/SPEC.md#predefined-handler-errors
        """
        if self.retryable_override is not None:
            return self.retryable_override

        match self.error_type:
            case (
                HandlerErrorType.BAD_REQUEST
                | HandlerErrorType.UNAUTHENTICATED
                | HandlerErrorType.UNAUTHORIZED
                | HandlerErrorType.NOT_FOUND
                | HandlerErrorType.CONFLICT
                | HandlerErrorType.NOT_IMPLEMENTED
            ):
                return False
            case (
                HandlerErrorType.RESOURCE_EXHAUSTED
                | HandlerErrorType.REQUEST_TIMEOUT
                | HandlerErrorType.INTERNAL
                | HandlerErrorType.UNAVAILABLE
                | HandlerErrorType.UPSTREAM_TIMEOUT
                | HandlerErrorType.UNKNOWN
            ):
                return True


class HandlerErrorType(Enum):
    """Nexus handler error types.

    See https://github.com/nexus-rpc/api/blob/main/SPEC.md#predefined-handler-errors
    """

    UNKNOWN = "UNKNOWN"
    """
    The error type is unknown. Subsequent requests by the client are permissible.
    """

    BAD_REQUEST = "BAD_REQUEST"
    """
    The handler cannot or will not process the request due to an apparent client error.

    Clients should not retry this request unless advised otherwise.
    """

    UNAUTHENTICATED = "UNAUTHENTICATED"
    """
    The client did not supply valid authentication credentials for this request.

    Clients should not retry this request unless advised otherwise.
    """

    UNAUTHORIZED = "UNAUTHORIZED"
    """
    The caller does not have permission to execute the specified operation.

    Clients should not retry this request unless advised otherwise.
    """

    NOT_FOUND = "NOT_FOUND"
    """
    The requested resource could not be found but may be available in the future.

    Subsequent requests by the client are permissible but not advised.
    """

    REQUEST_TIMEOUT = "REQUEST_TIMEOUT"
    """
    Returned by the server when it has given up handling a request. This may occur by enforcing a client
    provided `Request-Timeout` or for any arbitrary reason such as enforcing some configurable limit.

    Subsequent requests by the client are permissible.
    """

    CONFLICT = "CONFLICT"
    """
    The request could not be made due to a conflict. This may happen when trying to create an operation that
    has already been started.

    Clients should not retry this request unless advised otherwise.
    """

    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    """
    Some resource has been exhausted, perhaps a per-user quota, or perhaps the entire file system is out of space.

    Subsequent requests by the client are permissible.
    """

    INTERNAL = "INTERNAL"
    """
    An internal error occurred.

    Subsequent requests by the client are permissible.
    """

    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    """
    The handler either does not recognize the request method, or it lacks the ability to fulfill the request.

    Clients should not retry this request unless advised otherwise.
    """

    UNAVAILABLE = "UNAVAILABLE"
    """
    The service is currently unavailable.

    Subsequent requests by the client are permissible.
    """

    UPSTREAM_TIMEOUT = "UPSTREAM_TIMEOUT"
    """
    Used by gateways to report that a request to an upstream handler has timed out.

    Subsequent requests by the client are permissible.
    """


class OperationError(Exception):
    """
    An error that represents "failed" and "canceled" operation results.

    :param message: A descriptive message for the error. This will become the
                    `message` in the resulting Nexus Failure object.

    :param state:

    Example:
        .. code-block:: python

            import nexusrpc

            # Indicate operation failed
            raise nexusrpc.OperationError(
                "Processing failed due to invalid data",
                state=nexusrpc.OperationErrorState.FAILED
            )

            # Indicate operation was canceled
            raise nexusrpc.OperationError(
                "Operation was canceled by user request",
                state=nexusrpc.OperationErrorState.CANCELED
            )
    """

    def __init__(self, message: str, *, state: OperationErrorState):
        super().__init__(message)
        self._state = state

    @property
    def state(self) -> OperationErrorState:
        """
        The state of the operation.
        """
        return self._state


class OperationErrorState(Enum):
    """
    The state of an operation as described by an :py:class:`OperationError`.
    """

    FAILED = "failed"
    """
    The operation failed.
    """

    CANCELED = "canceled"
    """
    The operation was canceled.
    """


@dataclass(frozen=True)
class Link:
    """
    A Link contains a URL and a type that can be used to decode the URL.

    The URL may contain arbitrary data (percent-encoded). It can be used to pass
    information about the caller to the handler, or vice versa.
    """

    url: str
    """
    Link URL.

    Must be percent-encoded.
    """

    type: str
    """
    A data type for decoding the URL.

    Valid chars: alphanumeric, '_', '.', '/'
    """
