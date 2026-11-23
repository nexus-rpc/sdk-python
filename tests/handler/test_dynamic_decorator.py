import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

import nexusrpc
from nexusrpc._util import get_operation_factory, set_operation_factory
from nexusrpc.handler import (
    CancelOperationContext,
    Handler,
    MiddlewareSafeOperationHandler,
    OperationContext,
    OperationHandler,
    OperationHandlerMiddleware,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
    operation_handler,
    service_handler,
    sync_operation,
)
from nexusrpc.handler._core import ServiceHandler
from tests.helpers import DummySerializer, TestOperationTaskCancellation


def start_context(name: str, service: str = "tools") -> StartOperationContext:
    return StartOperationContext(
        service=service,
        operation=name,
        headers={},
        request_id="req",
        task_cancellation=TestOperationTaskCancellation(),
    )


def cancel_context(name: str, service: str = "tools") -> CancelOperationContext:
    return CancelOperationContext(
        service=service,
        operation=name,
        headers={},
        task_cancellation=TestOperationTaskCancellation(),
    )


class Dynamic(OperationHandler[nexusrpc.LazyValue, Any]):
    def __init__(self) -> None:
        self.tools = {"echo"}
        self.cancelled: list[tuple[str, str]] = []

    async def start(
        self, ctx: StartOperationContext, input: nexusrpc.LazyValue
    ) -> StartOperationResultSync[Any] | StartOperationResultAsync:
        assert isinstance(input, nexusrpc.LazyValue)
        value = await input.consume(as_type=dict)
        if ctx.operation not in self.tools:
            raise nexusrpc.HandlerError(
                "Unknown tool", type=nexusrpc.HandlerErrorType.NOT_FOUND
            )
        if value.get("async"):
            return StartOperationResultAsync(f"{ctx.operation}:{value['id']}")
        return StartOperationResultSync((ctx.operation, ctx.request_id, value))

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        self.cancelled.append((ctx.operation, token))


class Named(OperationHandler[None, str]):
    def __init__(self) -> None:
        self.cancelled: list[tuple[str, str]] = []

    async def start(
        self, ctx: StartOperationContext, input: None
    ) -> StartOperationResultSync[str]:
        assert input is None
        return StartOperationResultSync("manifest")

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        self.cancelled.append((ctx.operation, token))


@service_handler(name="tools")
class Tools:
    def __init__(self) -> None:
        self.dynamic = Dynamic()
        self.named = Named()

    @operation_handler
    def list_tools(self) -> OperationHandler[None, str]:
        return self.named

    @operation_handler(dynamic=True)
    def forward(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
        return self.dynamic


# Named handlers keep precedence, while the fallback receives raw input and can return either result kind.
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ["operation", "value", "expected"],
    [
        pytest.param("list_tools", None, "manifest", id="named"),
        pytest.param("echo", {"id": 5}, ("echo", "req", {"id": 5}), id="dynamic-sync"),
        pytest.param("echo", {"id": 5, "async": True}, "echo:5", id="dynamic-async"),
    ],
)
async def test_dynamic_decorator_routes_raw_input_and_preserves_named_handlers(
    operation: str, value: Any, expected: Any
):
    tools = Tools()
    handler = Handler([tools])
    result = await handler.start_operation(
        start_context(operation), nexusrpc.LazyValue(DummySerializer(value), {})
    )
    if operation == "echo" and value.get("async"):
        assert isinstance(result, StartOperationResultAsync)
        assert result.token == expected
    else:
        assert isinstance(result, StartOperationResultSync)
        assert result.value == expected
    service = handler.service_handlers["tools"]
    assert service.get_operation_handler(operation) is (
        tools.named if operation == "list_tools" else tools.dynamic
    )
    assert set(service.operation_handlers) == {"list_tools"}
    assert nexusrpc.get_operation(Tools.forward) is None
    assert get_operation_factory(Tools.forward) is Tools.forward


# Removing a tool prevents new starts but does not prevent cancellation of its existing token.
@pytest.mark.asyncio
async def test_dynamic_cancel_after_tool_removal():
    tools = Tools()
    handler = Handler([tools])
    result = await handler.start_operation(
        start_context("echo"),
        nexusrpc.LazyValue(DummySerializer({"id": 5, "async": True}), {}),
    )
    assert isinstance(result, StartOperationResultAsync)
    tools.dynamic.tools.remove("echo")
    with pytest.raises(nexusrpc.HandlerError) as error:
        _ = await handler.start_operation(
            start_context("echo"), nexusrpc.LazyValue(DummySerializer({}), {})
        )
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    await handler.cancel_operation(cancel_context("echo"), result.token)
    assert tools.dynamic.cancelled == [("echo", "echo:5")]


# Cancellation selects the named handler before falling back for an unmatched name.
@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["list_tools", "removed"])
async def test_named_cancel_takes_precedence(operation: str):
    tools = Tools()
    handler = Handler([tools])
    await handler.cancel_operation(cancel_context(operation), "token")
    expected = [(operation, "token")]
    assert tools.named.cancelled == (expected if operation == "list_tools" else [])
    assert tools.dynamic.cancelled == (expected if operation == "removed" else [])


# A fallback never receives start or cancel requests for an unknown service.
@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["start", "cancel"])
async def test_dynamic_handler_does_not_handle_unknown_services(method: str):
    tools = Tools()
    handler = Handler([tools])
    with pytest.raises(nexusrpc.HandlerError) as error:
        if method == "start":
            _ = await handler.start_operation(
                start_context("echo", "missing"),
                nexusrpc.LazyValue(DummySerializer({}), {}),
            )
        else:
            await handler.cancel_operation(cancel_context("echo", "missing"), "token")
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    assert tools.dynamic.cancelled == []


# A fallback cannot specify a name, including an empty string.
@pytest.mark.parametrize("name", ["named", ""])
def test_dynamic_handler_rejects_name(name: str):
    with pytest.raises(ValueError, match="cannot specify name"):
        _ = operation_handler(name=name, dynamic=True)


# An inherited fallback cannot be combined with a second fallback.
def test_dynamic_handler_rejects_duplicates():
    with pytest.raises(RuntimeError, match="more than one dynamic"):

        @service_handler(name="duplicate")
        class _Duplicate(Tools):
            @operation_handler(dynamic=True)
            def other(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
                return Dynamic()

        _ = _Duplicate


# Two dynamic handlers declared on the same service are rejected.
def test_dynamic_handler_rejects_duplicates_on_same_class():
    with pytest.raises(RuntimeError, match="more than one dynamic"):

        @service_handler(name="duplicate")
        class _Duplicate:
            @operation_handler(dynamic=True)
            def first(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
                return Dynamic()

            @operation_handler(dynamic=True)
            def second(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
                return Dynamic()

        _ = _Duplicate


# A fallback must accept raw input rather than a decoded value.
def test_dynamic_handler_requires_raw_input():
    with pytest.raises(TypeError, match="LazyValue"):

        @service_handler(name="decoded")
        class _Decoded:
            @operation_handler(dynamic=True)
            def forward(self) -> OperationHandler[dict[str, Any], Any]:
                raise AssertionError("Reject decoded input before creating the handler")

        _ = _Decoded


# An explicit service definition can have a fallback without declaring any named operations.
@pytest.mark.asyncio
async def test_dynamic_handler_with_explicit_service_and_no_named_operations():
    @nexusrpc.service(name="empty")
    class Empty:
        pass

    @service_handler(service=Empty)
    class CatchAll:
        @operation_handler(dynamic=True)
        def forward(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
            return Dynamic()

    instance = CatchAll()
    service = ServiceHandler.from_user_instance(instance)
    assert not service.operation_handlers
    handler = Handler([instance])
    result = await handler.start_operation(
        start_context("echo", "empty"), nexusrpc.LazyValue(DummySerializer({}), {})
    )
    assert isinstance(result, StartOperationResultSync) and result.value[0] == "echo"


# Custom start-method decorators can attach a dynamic factory without an Operation.
@pytest.mark.asyncio
async def test_dynamic_factory_attached_to_start_method():
    @operation_handler(dynamic=True)
    def factory(_self: Any) -> OperationHandler[nexusrpc.LazyValue, Any]:
        return Dynamic()

    @service_handler(name="tools")
    class CustomService:
        async def forward(self) -> None:
            raise AssertionError("The start method must not be called as a factory")

        set_operation_factory(forward, factory)

    handler = Handler([CustomService()])
    assert not handler.service_handlers["tools"].operation_handlers
    assert nexusrpc.get_operation(CustomService.forward) is None
    assert get_operation_factory(CustomService.forward) is factory
    result = await handler.start_operation(
        start_context("echo"), nexusrpc.LazyValue(DummySerializer({"id": 5}), {})
    )
    assert isinstance(result, StartOperationResultSync)
    assert result.value == ("echo", "req", {"id": 5})


# A fallback can also accompany named operations from an explicit service definition.
@pytest.mark.asyncio
async def test_dynamic_handler_with_explicit_named_service():
    @nexusrpc.service(name="explicit")
    class Definition:
        list_tools: nexusrpc.Operation[None, str]

    @service_handler(service=Definition)
    class ExplicitTools(Tools):
        pass

    handler = Handler([ExplicitTools()])
    named = await handler.start_operation(
        start_context("list_tools", "explicit"),
        nexusrpc.LazyValue(DummySerializer(None), {}),
    )
    dynamic = await handler.start_operation(
        start_context("echo", "explicit"),
        nexusrpc.LazyValue(DummySerializer({}), {}),
    )
    assert isinstance(named, StartOperationResultSync) and named.value == "manifest"
    assert isinstance(dynamic, StartOperationResultSync) and dynamic.value[0] == "echo"


# Blocking fallback methods require an executor and still receive raw input when one is supplied.
@pytest.mark.asyncio
async def test_blocking_dynamic_handler_requires_an_executor():
    class Blocking(OperationHandler[nexusrpc.LazyValue, Any]):
        def start(
            self, ctx: StartOperationContext, input: nexusrpc.LazyValue
        ) -> StartOperationResultSync[Any]:  # type: ignore[override]
            return StartOperationResultSync(asyncio.run(input.consume(as_type=dict)))

        async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
            pass

    @service_handler(name="tools")
    class BlockingService:
        @operation_handler(dynamic=True)
        def forward(self) -> OperationHandler[nexusrpc.LazyValue, Any]:
            return Blocking()

    with pytest.raises(RuntimeError, match="executor"):
        _ = Handler([BlockingService()])
    with ThreadPoolExecutor() as executor:
        handler = Handler([BlockingService()], executor=executor)
        result = await handler.start_operation(
            start_context("echo"), nexusrpc.LazyValue(DummySerializer({"id": 5}), {})
        )
        assert isinstance(result, StartOperationResultSync) and result.value == {
            "id": 5
        }


class RecordingMiddleware(OperationHandlerMiddleware):
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []

    def intercept(
        self, ctx: OperationContext, next: MiddlewareSafeOperationHandler
    ) -> MiddlewareSafeOperationHandler:
        return RecordingOperationHandler(next, self)


class RecordingOperationHandler(MiddlewareSafeOperationHandler):
    def __init__(
        self, next: MiddlewareSafeOperationHandler, middleware: RecordingMiddleware
    ) -> None:
        self._next = next
        self._middleware = middleware

    async def start(
        self, ctx: StartOperationContext, input: Any
    ) -> StartOperationResultSync[Any] | StartOperationResultAsync:
        self._middleware.calls.append(("start", ctx.operation, input))
        return await self._next.start(ctx, input)

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        self._middleware.calls.append(("cancel", ctx.operation, token))
        return await self._next.cancel(ctx, token)


# Middleware sees the original name, raw start input, and cancellation token for the fallback.
@pytest.mark.asyncio
async def test_middleware_runs_for_dynamic_handler():
    middleware = RecordingMiddleware()
    tools = Tools()
    handler = Handler([tools], middleware=[middleware])
    raw = nexusrpc.LazyValue(DummySerializer({"id": 1, "async": True}), {})
    result = await handler.start_operation(start_context("echo"), raw)
    assert isinstance(result, StartOperationResultAsync)
    await handler.cancel_operation(cancel_context("echo"), result.token)
    assert middleware.calls == [
        ("start", "echo", raw),
        ("cancel", "echo", "echo:1"),
    ]
    assert tools.dynamic.cancelled == [("echo", "echo:1")]


# A NOT_FOUND raised by the fallback is returned unchanged for start and cancel.
@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["start", "cancel"])
async def test_not_found_from_dynamic_handler_is_final(method: str):
    class RejectingDynamic(Dynamic):
        async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
            raise nexusrpc.HandlerError(
                "Unknown tool", type=nexusrpc.HandlerErrorType.NOT_FOUND
            )

    tools = Tools()
    tools.dynamic = RejectingDynamic()
    tools.dynamic.tools.clear()
    handler = Handler([tools])
    with pytest.raises(nexusrpc.HandlerError) as error:
        if method == "start":
            _ = await handler.start_operation(
                start_context("echo"), nexusrpc.LazyValue(DummySerializer({}), {})
            )
        else:
            await handler.cancel_operation(cancel_context("echo"), "token")
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    assert str(error.value) == "Unknown tool"


# A named handler's NOT_FOUND must not trigger a second attempt through the fallback.
@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["start", "cancel"])
async def test_named_not_found_does_not_use_fallback(method: str):
    rejection = nexusrpc.HandlerError(
        "Named rejection", type=nexusrpc.HandlerErrorType.NOT_FOUND
    )

    class RejectingNamed(Named):
        async def start(
            self, ctx: StartOperationContext, input: None
        ) -> StartOperationResultSync[str]:
            raise rejection

        async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
            raise rejection

    tools = Tools()
    tools.named = RejectingNamed()
    tools.dynamic.tools.add("list_tools")
    handler = Handler([tools])
    with pytest.raises(nexusrpc.HandlerError) as error:
        if method == "start":
            _ = await handler.start_operation(
                start_context("list_tools"),
                nexusrpc.LazyValue(DummySerializer(None), {}),
            )
        else:
            await handler.cancel_operation(cancel_context("list_tools"), "token")
    assert error.value is rejection
    assert tools.dynamic.cancelled == []


# Without a fallback, unmatched start and cancel requests still return NOT_FOUND.
@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["start", "cancel"])
async def test_unknown_operation_without_dynamic_handler_is_not_found(method: str):
    @service_handler(name="named")
    class NamedOnly:
        @sync_operation
        async def list_tools(self, _ctx: StartOperationContext, _input: None) -> str:
            return "manifest"

    handler = Handler([NamedOnly()])
    with pytest.raises(nexusrpc.HandlerError) as error:
        if method == "start":
            _ = await handler.start_operation(
                start_context("echo", "named"),
                nexusrpc.LazyValue(DummySerializer({}), {}),
            )
        else:
            await handler.cancel_operation(cancel_context("echo", "named"), "token")
    assert error.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
