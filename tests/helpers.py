import asyncio
import threading
from dataclasses import dataclass
from typing import Any, Optional

from nexusrpc import Content
from nexusrpc.handler import OperationTaskCancellation


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


class TestOperationTaskCancellation(OperationTaskCancellation):
    __test__ = False

    # A naive implementation of cancellation for use in tests
    def __init__(self):
        self._details = None
        self._evt = threading.Event()
        self._lock = threading.Lock()

    def is_cancelled(self) -> bool:
        return self._evt.is_set()

    def cancellation_reason(self) -> Optional[str]:
        with self._lock:
            return self._details

    def wait_until_cancelled_sync(self, timeout: float | None = None) -> bool:
        return self._evt.wait(timeout)

    async def wait_until_cancelled(self):
        while not self.is_cancelled():
            await asyncio.sleep(0.05)

    def cancel(self):
        with self._lock:
            self._details = "test cancellation occurred"
            self._evt.set()
