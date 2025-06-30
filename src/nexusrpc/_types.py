from __future__ import annotations

from typing import TypeVar

InputT = TypeVar("InputT", contravariant=True)
"""Operation input type"""

OutputT = TypeVar("OutputT", covariant=True)
"""Operation output type"""

ServiceHandlerT = TypeVar("ServiceHandlerT")
"""A user's service handler class, typically decorated with @service_handler"""

ServiceT = TypeVar("ServiceT")
"""A user's service definition class, typically decorated with @service"""
