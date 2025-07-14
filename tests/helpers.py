from dataclasses import dataclass
from typing import Any, Optional

from nexusrpc import Content


@dataclass
class DummySerializer:
    value: Any

    async def serialize(self, value: Any) -> Content:  # pyright: ignore[reportUnusedParameter]
        raise NotImplementedError

    async def deserialize(
        self,
        content: Content,  # pyright: ignore[reportUnusedParameter]
        as_type: Optional[type[Any]] = None,  # pyright: ignore[reportUnusedParameter]
    ) -> Any:
        return self.value
