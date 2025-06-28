from dataclasses import dataclass
from enum import Enum

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

    # The URL must be percent-encoded.
    url: str
    # Can describe an actual data type for decoding the URL. Valid chars: alphanumeric, '_', '.',
    # '/'
    type: str


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

    # Token identifying the operation (returned on operation start).
    token: str

    # The operation's state
    state: OperationState


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
