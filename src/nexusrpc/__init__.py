from dataclasses import dataclass
from enum import Enum

from ._serializer import Content as Content
from ._serializer import LazyValueAsync as LazyValueAsync
from ._serializer import LazyValueSync as LazyValueSync
from ._service import Operation as Operation
from ._service import ServiceDefinition as ServiceDefinition
from ._service import service as service


@dataclass
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
