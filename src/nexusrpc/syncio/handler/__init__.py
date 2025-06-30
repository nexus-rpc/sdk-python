"""
Components for implementing Nexus handlers that use synchronous I/O.

See :py:mod:`nexusrpc.handler` for the asynchronous I/O version of this module.

Server/worker authors will use this module to create the top-level Nexus handlers
responsible for dispatching requests to Nexus operations.

Nexus service/operation authors will use this module to implement operation handler
methods within service handler classes.
"""

from ._core import Handler, sync_operation

__all__ = [
    "Handler",
    "sync_operation",
]
