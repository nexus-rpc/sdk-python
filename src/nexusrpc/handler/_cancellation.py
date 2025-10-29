from abc import ABC, abstractmethod
from typing import Optional


class OperationTaskCancellation(ABC):
    """
    Indicates whether a a Nexus task has been cancelled during a sync operation or before an async operation has
    returned a token.

    Nexus worker implementations are expected to provide an implementation that enables
    cooperative cancellation for both sync and async operation handlers.
    """

    @abstractmethod
    def is_cancelled(self) -> bool:
        """Return True if the associated task has been cancelled."""
        raise NotImplementedError

    @abstractmethod
    def cancellation_details(self) -> Optional[str]:
        """Provide additional context for the cancellation, if available."""
        raise NotImplementedError

    @abstractmethod
    def wait_until_cancelled(self, timeout: Optional[float] = None) -> bool:
        """Block until cancellation occurs or the optional timeout elapses."""
        raise NotImplementedError

    @abstractmethod
    async def wait_until_cancelled_async(self) -> None:
        """Await cancellation using async primitives."""
        raise NotImplementedError


class UncancellableOperationTaskCancellation(OperationTaskCancellation):
    """An :py:class:`OperationTaskCancellation` that never cancels. Used by default if a Nexus worker implementation does not implement task cancellation."""

    def is_cancelled(self) -> bool:
        """Always report not cancelled."""
        return False

    def cancellation_details(self) -> Optional[str]:
        """Return None because no cancellation data is ever available."""
        return None

    def wait_until_cancelled(self, timeout: Optional[float] = None):
        """Never block because cancellation will not occur."""
        return False

    async def wait_until_cancelled_async(self):
        """Never await cancellation because it cannot be triggered."""
        pass
