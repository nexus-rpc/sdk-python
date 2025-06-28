from dataclasses import dataclass
from typing import Any, Optional, Type

from nexusrpc import Content


@dataclass
class DummySerializer:
    value: Any

    async def serialize(self, value: Any) -> Content:
        raise NotImplementedError

    async def deserialize(
        self, content: Content, as_type: Optional[Type[Any]] = None
    ) -> Any:
        return self.value
