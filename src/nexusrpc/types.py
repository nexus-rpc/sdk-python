from __future__ import annotations

from typing import TypeVar

# Operation input type
InputT = TypeVar("InputT", contravariant=True)

# Operation output type
OutputT = TypeVar("OutputT", covariant=True)

# A user's service handler class, typically decorated with @service_handler
ServiceHandlerT = TypeVar("ServiceHandlerT")

# A user's service definition class, typically decorated with @service
ServiceDefinitionT = TypeVar("ServiceDefinitionT")


class MISSING_TYPE:
    """
    A missing input or output type.

    A sentinel type used to indicate an input or output type that is not specified by an
    operation.
    """

    pass
