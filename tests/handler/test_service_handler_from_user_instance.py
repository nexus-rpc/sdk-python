from __future__ import annotations

import pytest

from nexusrpc.handler import StartOperationContext, service_handler
from nexusrpc.syncio import handler as syncio_handler

if False:

    @service_handler
    class MyServiceHandlerWithCallableInstance:
        class SyncOperationWithCallableInstance:
            def __call__(
                self,
                _handler: MyServiceHandlerWithCallableInstance,
                ctx: StartOperationContext,
                input: int,
            ) -> int:
                return input

        sync_operation_with_callable_instance = syncio_handler.sync_operation(
            name="sync_operation_with_callable_instance",
        )(
            SyncOperationWithCallableInstance(),
        )


@pytest.mark.skip(reason="TODO(preview): support callable instance")
def test_service_handler_from_user_instance():
    # service_handler = MyServiceHandlerWithCallableInstance()
    # ServiceHandler.from_user_instance(service_handler)
    pass
