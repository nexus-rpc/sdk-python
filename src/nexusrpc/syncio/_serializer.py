from __future__ import annotations

from typing import (
    Any,
    Iterable,
    Mapping,
    Optional,
    Type,
)

from nexusrpc._serializer import Content, LazyValueT, Serializer


class LazyValue(LazyValueT):
    """
    A container for a value encoded in an underlying stream.

    It is used to stream inputs and outputs in the various client and server APIs.

    For the `async def` version of this class, see :py:class:`nexusrpc.LazyValue`.
    """

    def __init__(
        self,
        serializer: Serializer,
        headers: Mapping[str, str],
        stream: Optional[Iterable[bytes]] = None,
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

    def consume(self, as_type: Optional[Type[Any]] = None) -> Any:
        """
        Consume the underlying reader stream, deserializing via the embedded serializer.
        """
        # TODO(prerelease): HandlerError(BAD_REQUEST) on error while deserializing?
        if self.stream is None:
            return self.serializer.deserialize(
                Content(headers=self.headers, data=None), as_type=as_type
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
