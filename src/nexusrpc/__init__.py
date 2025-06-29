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
    UNAUTHENTICATED = "UNAUTHENTICATED"
    UNAUTHORIZED = "UNAUTHORIZED"
    NOT_FOUND = "NOT_FOUND"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    INTERNAL = "INTERNAL"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    UNAVAILABLE = "UNAVAILABLE"
    UPSTREAM_TIMEOUT = "UPSTREAM_TIMEOUT"


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
        cause: Optional[BaseException] = None,
        # Whether this error should be considered retryable. If not specified, retry
        # behavior is determined from the error type. For example, INTERNAL is retryable
        # by default unless specified otherwise.
        retryable: Optional[bool] = None,
    ):
        """
        Initializes a new HandlerError.

        :param message: A descriptive message for the error. This will become the `message`
                        in the resulting Nexus Failure object.
        :param type: The type of handler error.
        :param cause: The original exception that caused this handler error, if any.
                      This will be encoded in the `details` of the Nexus Failure object.
        :param retryable: Whether this error should be retried. If not
                          provided, the default behavior for the error type is used.
        """
        super().__init__(message)
        self.__cause__ = cause
        self.type = type
        self.retryable = retryable
