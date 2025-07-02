"""
Components for implementing Nexus handlers that use synchronous I/O.

By default the components of the nexusrpc library use asynchronous I/O (`async def`).
This module provides alternative components based on traditional synchronous I/O
(`def`).

Server/worker authors will use this module to create top-level Nexus handlers that
expose `def` methods such as `start_operation` and `cancel_operation`.

Nexus service/operation authors will use this module to obtain a synchronous I/O
version of the `sync_operation` decorator.
"""

from ._serializer import LazyValue

__all__ = [
    "LazyValue",
]
