from __future__ import annotations

import pytest

import nexusrpc.handler
from nexusrpc.handler._core import ServiceHandler

# TODO(preview): test operation_handler version of this


if False:

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

        sync_operation_with_callable_instance = nexusrpc.handler.operation_handler(
            name="sync_operation_with_callable_instance",
        )(
            SyncOperationWithCallableInstance(),  # type: ignore
        )


@pytest.mark.skip(reason="TODO(prerelease): update this test after decorator change")
def test_service_handler_from_user_instance():
    service_handler = MyServiceHandlerWithCallableInstance()  # type: ignore
    ServiceHandler.from_user_instance(service_handler)
