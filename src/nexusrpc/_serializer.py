from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Type,
    Union,
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

    def serialize(self, value: Any) -> Union[Content, Awaitable[Content]]:
        """Serialize encodes a value into a Content."""
        ...

    # TODO(prerelease): does None work as the sentinel type here, meaning do not attempt
    # type conversion, despite the fact that Python treats None as a valid type?
    def deserialize(
        self, content: Content, as_type: Optional[Type[Any]] = None
    ) -> Union[Any, Awaitable[Any]]:
        """Deserialize decodes a Content into a value.

        Args:
            content: The content to deserialize.
            as_type: The type to convert the result of deserialization into.
                     Do not attempt type conversion if this is None.
        """
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
        stream: Optional[Union[AsyncIterable[bytes], Iterable[bytes]]] = None,
    ) -> None:
        """
        Args:
            serializer: The serializer to use for consuming the value.
            headers: Headers that include information on how to process the stream's content.
                     Headers constructed by the framework always have lower case keys.
                     User provided keys are treated case-insensitively.
            stream:  Iterable that contains request or response data. None means empty data.
        """
        self.serializer = serializer
        self.headers = headers
        self.stream = stream

    async def consume(self, as_type: Optional[Type[Any]] = None) -> Any:
        """
        Consume the underlying reader stream, deserializing via the embedded serializer.
        """
        # TODO(prerelease): HandlerError(BAD_REQUEST) on error while deserializing?
        if self.stream is None:
            return await self.serializer.deserialize(
                Content(headers=self.headers), as_type=as_type
            )
        elif not isinstance(self.stream, AsyncIterable):
            raise ValueError("When using consume, stream must be an AsyncIterable")

        return await self.serializer.deserialize(
            Content(
                headers=self.headers,
                data=b"".join([c async for c in self.stream]),
            ),
            as_type=as_type,
        )

    def consume_sync(self, as_type: Optional[Type[Any]] = None) -> Any:
        """
        Consume the underlying reader stream, deserializing via the embedded serializer.
        """
        # TODO(prerelease): HandlerError(BAD_REQUEST) on error while deserializing?
        if self.stream is None:
            return self.serializer.deserialize(
                Content(headers=self.headers), as_type=as_type
            )
        elif not isinstance(self.stream, Iterable):
            raise ValueError("When using consume_sync, stream must be an Iterable")

        return self.serializer.deserialize(
            Content(
                headers=self.headers,
                data=b"".join([c for c in self.stream]),
            ),
            as_type=as_type,
        )
