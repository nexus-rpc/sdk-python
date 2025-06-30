from __future__ import annotations

from nexusrpc.handler import StartOperationContext, service_handler
from nexusrpc.handler._core import ServiceHandler
from nexusrpc.syncio import handler as syncio_handler


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


def test_service_handler_from_user_instance():
    service_handler = MyServiceHandlerWithCallableInstance()
    ServiceHandler.from_user_instance(service_handler)
