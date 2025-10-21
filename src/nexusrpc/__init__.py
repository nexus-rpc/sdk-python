"""
nexusrpc is a library for building Nexus handlers.

See https://github.com/nexus-rpc and https://github.com/nexus-rpc/api/blob/main/SPEC.md.

Nexus is a synchronous RPC protocol. Arbitrary duration operations are modeled on top of
a set of pre-defined synchronous RPCs.

A Nexus caller calls a handler. The handler may respond inline (synchronous response) or
return a token referencing the ongoing operation (asynchronous response). The caller can
cancel an asynchronous operation using this token. The caller can also specify a callback
URL, which the handler uses to deliver the result ofan asynchronous operation when it is ready.
"""

from __future__ import annotations

from . import handler
from ._common import (
    HandlerError,
    HandlerErrorType,
    InputT,
    Link,
    OperationError,
    OperationErrorState,
    OperationState,
    OutputT,
)
from ._serializer import Content, LazyValue
from ._service import Operation, OperationDefinition, ServiceDefinition, service
from ._util import (
    get_operation,
    get_service_definition,
    set_operation,
)

__all__ = [
    "Content",
    "get_operation",
    "get_service_definition",
    "handler",
    "HandlerError",
    "HandlerErrorType",
    "InputT",
    "LazyValue",
    "Link",
    "Operation",
    "OperationDefinition",
    "OperationError",
    "OperationErrorState",
    "OperationState",
    "OutputT",
    "service",
    "ServiceDefinition",
    "set_operation",
]
