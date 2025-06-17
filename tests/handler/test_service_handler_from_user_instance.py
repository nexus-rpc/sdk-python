from __future__ import annotations

import nexusrpc._handler
from nexusrpc._handler._core import ServiceHandler

# TODO(preview): test operation_handler version of this


@nexusrpc._handler.service_handler
class MyServiceHandlerWithCallableInstance:
    class SyncOperationWithCallableInstance:
        def __call__(
            self,
            _handler: MyServiceHandlerWithCallableInstance,
            ctx: nexusrpc._handler.StartOperationContext,
            input: int,
        ) -> int:
            return input

    sync_operation_with_callable_instance = nexusrpc._handler.sync_operation_handler(
        name="sync_operation_with_callable_instance",
    )(
        SyncOperationWithCallableInstance(),
    )


def test_service_handler_from_user_instance():
    service_handler = MyServiceHandlerWithCallableInstance()
    ServiceHandler.from_user_instance(service_handler)
