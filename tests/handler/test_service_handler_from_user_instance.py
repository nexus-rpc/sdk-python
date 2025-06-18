from __future__ import annotations

import nexusrpc.handler
from nexusrpc.handler._core import ServiceHandler

# TODO(preview): test operation_handler version of this


@nexusrpc.handler.service_handler
class MyServiceHandlerWithCallableInstance:
    class SyncOperationWithCallableInstance:
        def __call__(
            self,
            _handler: MyServiceHandlerWithCallableInstance,
            ctx: nexusrpc.handler.StartOperationContext,
            input: int,
        ) -> int:
            return input

    sync_operation_with_callable_instance = nexusrpc.handler.sync_operation_handler(
        name="sync_operation_with_callable_instance",
    )(
        SyncOperationWithCallableInstance(),
    )


def test_service_handler_from_user_instance():
    service_handler = MyServiceHandlerWithCallableInstance()
    ServiceHandler.from_user_instance(service_handler)
