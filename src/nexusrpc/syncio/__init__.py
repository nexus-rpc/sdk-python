from __future__ import annotations

from typing import (
    Any,
    Iterable,
    Optional,
    Type,
)

import nexusrpc._handler
from nexusrpc._handler import Content


class LazyValue(nexusrpc._handler.LazyValue):
    __doc__ = nexusrpc._handler.LazyValue.__doc__
    stream: Optional[Iterable[bytes]]

    def consume(self, as_type: Optional[Type[Any]] = None) -> Any:
        """
        Consume the underlying reader stream, deserializing via the embedded serializer.
        """
        # TODO(prerelease): HandlerError(BAD_REQUEST) on error while deserializing?
        if self.stream is None:
            return self.serializer.deserialize(
                Content(headers=self.headers), as_type=as_type
            )

        return self.serializer.deserialize(
            Content(
                headers=self.headers,
                data=b"".join([c for c in self.stream]),
            ),
            as_type=as_type,
        )
