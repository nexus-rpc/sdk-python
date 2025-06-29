from dataclasses import dataclass
from enum import Enum
from typing import Optional

from ._serializer import Content as Content, LazyValue as LazyValue
from ._service import (
    Operation as Operation,
    ServiceDefinition as ServiceDefinition,
    service as service,
)
from ._types import InputT as InputT, OutputT as OutputT
from ._util import (
    get_operation_factory as get_operation_factory,
    get_service_definition as get_service_definition,
)


@dataclass(frozen=True)
class Link:
    """
    Link contains a URL and a Type that can be used to decode the URL.
    Links can contain any arbitrary information as a percent-encoded URL.
    It can be used to pass information about the caller to the handler, or vice versa.
    """

    url: str
    """
    Link URL. Must be percent-encoded.
    """

    type: str
    """
    Can describe an data type for decoding the URL.

    Valid chars: alphanumeric, '_', '.', '/'
    """


class OperationState(Enum):
    """
    Describes the current state of an operation.
    """

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"
    RUNNING = "running"


@dataclass(frozen=True)
class OperationInfo:
    """
    Information about an operation.
    """

    token: str
    """
    Token identifying the operation (returned on operation start).
    """

    state: OperationState
    """
    The operation's state.
    """


class OperationErrorState(Enum):
    """
    The state of an operation as described by an OperationError.
    """

    FAILED = "failed"
    CANCELED = "canceled"


class OperationError(Exception):
    """
    An error that represents "failed" and "canceled" operation results.
    """

    def __init__(self, message: str, *, state: OperationErrorState):
        super().__init__(message)
        self.state = state


class HandlerErrorType(Enum):
    """Nexus handler error types.

    See https://github.com/nexus-rpc/api/blob/main/SPEC.md#predefined-handler-errors
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


class HandlerError(Exception):
    """
    A Nexus handler error.

    This exception is used to represent errors that occur during the handling of a
    Nexus operation that should be reported to the caller as a handler error.
    """

    def __init__(
        self,
        message: str,
        *,
        type: HandlerErrorType,
        retryable: Optional[bool] = None,
    ):
        """
        Initializes a new HandlerError.

        :param message: A descriptive message for the error. This will become the `message`
                        in the resulting Nexus Failure object.
        :param type: The type of handler error.
        :param retryable: Whether this error should be retried. If not
                          provided, the default behavior for the error type is used.
        """
        super().__init__(message)
        self._type = type
        self._retryable = retryable

    @property
    def retryable(self) -> Optional[bool]:
        """
        Whether this error should be retried.

        If None, then the default behavior for the error type should be used.
        See https://github.com/nexus-rpc/api/blob/main/SPEC.md#predefined-handler-errors
        """
        return self._retryable

    @property
    def type(self) -> HandlerErrorType:
        """
        The type of handler error.

        See :py:class:`HandlerErrorType` and
        https://github.com/nexus-rpc/api/blob/main/SPEC.md#predefined-handler-errors.
        """
        return self._type
