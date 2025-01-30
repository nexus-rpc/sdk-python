from __future__ import annotations

from typing import TypeVar

I = TypeVar("I", contravariant=True)  # operation input
O = TypeVar("O", covariant=True)  # operation output
S = TypeVar("S")  # a service


class MISSING_TYPE:
    pass
