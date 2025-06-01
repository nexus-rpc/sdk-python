from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterable,
    Mapping,
    Optional,
    Protocol,
)


@dataclass
class Content:
    """
    A container for a map of headers and a byte array of data.

    It is used by the SDK's Serializer interface implementations.
    """

    headers: Mapping[str, str]
    """
    Header that should include information on how to deserialize this content.
    Headers constructed by the framework always have lower case keys.
    User provided keys are treated case-insensitively.
    """

    data: Optional[bytes] = None
    """Request or response data. May be undefined for empty data."""


class Serializer(Protocol):
    """
    Serializer is used by the framework to serialize/deserialize input and output.
    """

    # TODO(dan): support non-async def?

    async def serialize(self, value: Any) -> Content:
        """Serialize encodes a value into a Content."""
        ...

    async def deserialize(self, content: Content) -> Any:
        """Deserialize decodes a Content into a value."""
        ...


class LazyValue:
    """
    A container for a value encoded in an underlying stream.
    It is used to stream inputs and outputs in the various client and server APIs.
    """

    def __init__(
        self,
        serializer: Serializer,
        headers: Mapping[str, str],
        stream: Optional[AsyncIterable[bytes]] = None,
    ) -> None:
        """
        Args:
            serializer: The serializer to use for consuming the value.
            headers: Headers that include information on how to process the stream's content.
                     Headers constructed by the framework always have lower case keys.
                     User provided keys are treated case-insensitively.
            stream: AsyncIterable that contains request or response data. None means empty data.
        """
        self.serializer = serializer
        self.headers = headers
        self.stream = stream

    async def consume(self) -> Any:
        """
        Consume the underlying reader stream, deserializing via the embedded serializer.
        """
        # TODO(dan): HandlerError(BAD_REQUEST) on error while deserializing?
        if self.stream is None:
            return await self.serializer.deserialize(Content(headers=self.headers))

        return await self.serializer.deserialize(
            Content(
                headers=self.headers,
                data=b"".join([c async for c in self.stream]),
            )
        )
