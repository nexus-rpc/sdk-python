from dataclasses import dataclass
from typing import Any, Optional

from nexusrpc import Content


@dataclass
class DummySerializer:
    value: Any

    async def serialize(self, value: Any) -> Content:
        raise NotImplementedError

    async def deserialize(
        self, content: Content, as_type: Optional[type[Any]] = None
    ) -> Any:
        return self.value
