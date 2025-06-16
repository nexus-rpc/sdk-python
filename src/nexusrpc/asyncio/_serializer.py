from typing import Any, AsyncIterable, Optional, Type

import nexusrpc.handler
from nexusrpc.handler._serializer import Content


class LazyValue(nexusrpc.handler.LazyValue):
    __doc__ = nexusrpc.handler.LazyValue.__doc__
    stream: Optional[AsyncIterable[bytes]]

    async def consume(self, as_type: Optional[Type[Any]] = None) -> Any:
        """
        Consume the underlying reader stream, deserializing via the embedded serializer.
        """
        # TODO(prerelease): HandlerError(BAD_REQUEST) on error while deserializing?
        if self.stream is None:
            return await self.serializer.deserialize(
                Content(headers=self.headers), as_type=as_type
            )

        return await self.serializer.deserialize(
            Content(
                headers=self.headers,
                data=b"".join([c async for c in self.stream]),
            ),
            as_type=as_type,
        )
