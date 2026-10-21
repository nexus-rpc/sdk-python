"""Tests that operations report the service that they belong to."""

from __future__ import annotations

import inspect
from typing import Any

import pytest

import nexusrpc
from nexusrpc import LazyValue, Operation
from nexusrpc.handler import (
    Handler,
    OperationHandler,
    StartOperationContext,
    StartOperationResultSync,
    operation_handler,
    service_handler,
    sync_operation,
)
from nexusrpc.handler._operation_handler import SyncOperationHandler
from tests.helpers import DummySerializer, TestOperationTaskCancellation


def _service_name(obj: Any) -> str | None:
    op = obj if isinstance(obj, Operation) else nexusrpc.get_operation(obj)
    assert op is not None
    return op.service.name if op.service else None


def _operation(obj: Any) -> Operation[Any, Any]:
    op = nexusrpc.get_operation(obj)
    assert op is not None
    return op


@nexusrpc.service(name="base-definition")
class BaseDefinition:
    annotated: Operation[int, int]
    renamed: Operation[int, int] = Operation(name="Renamed")


@nexusrpc.service(name="sub-definition")
class SubDefinition(BaseDefinition):
    added: Operation[int, int]


class UndecoratedSubDefinition(BaseDefinition):
    pass


def test_definition_operations_report_their_service():
    assert _service_name(SubDefinition.annotated) == "sub-definition"
    assert _service_name(SubDefinition.renamed) == "sub-definition"
    assert _service_name(SubDefinition.added) == "sub-definition"
    assert SubDefinition.renamed.name == "Renamed"

    # The parent keeps its own operations
    assert SubDefinition.annotated is not BaseDefinition.annotated
    assert _service_name(BaseDefinition.annotated) == "base-definition"
    assert _service_name(BaseDefinition.renamed) == "base-definition"
    # Equality ignores the service
    assert SubDefinition.annotated == BaseDefinition.annotated


def test_inherited_renamed_operation_is_set_under_its_method_name():
    assert "renamed" in SubDefinition.__dict__
    assert "Renamed" not in SubDefinition.__dict__


def test_undecorated_subclass_reports_the_parent_service():
    assert _service_name(UndecoratedSubDefinition.annotated) == "base-definition"


def test_operation_outside_a_service_has_no_service():
    assert Operation[int, int](name="free").service is None


@service_handler(service=BaseDefinition)
class BaseHandler:
    @sync_operation
    async def annotated(self, _ctx: StartOperationContext, input: int) -> int:
        return input + 1

    @sync_operation
    async def renamed(self, _ctx: StartOperationContext, input: int) -> int:
        return input + 2


@service_handler(name="sub-handler")
class SubHandler(BaseHandler):
    pass


def test_handler_methods_report_their_service():
    assert _service_name(SubHandler.annotated) == "sub-handler"
    assert _service_name(SubHandler.renamed) == "sub-handler"
    assert _operation(SubHandler.renamed).name == "Renamed"

    # The parent keeps its own methods and operations
    assert SubHandler.annotated is not BaseHandler.annotated
    assert _service_name(BaseHandler.annotated) == "base-definition"
    assert _service_name(BaseHandler.renamed) == "base-definition"
    assert inspect.iscoroutinefunction(SubHandler.annotated)


def test_handler_method_operations_use_the_definition_name_and_types():
    op = _operation(BaseHandler.renamed)
    assert op.name == "Renamed"
    assert op.method_name == "renamed"
    assert (op.input_type, op.output_type) == (int, int)


@pytest.mark.asyncio
async def test_inherited_handler_methods_still_handle_requests():
    assert await SubHandler().annotated(None, 1) == 2  # type: ignore[arg-type]

    handler = Handler(user_service_handlers=[SubHandler()])
    result = await handler.start_operation(
        StartOperationContext(
            service="sub-handler",
            operation="Renamed",
            headers={},
            request_id="request_id",
            task_cancellation=TestOperationTaskCancellation(),
        ),
        LazyValue(serializer=DummySerializer(1), headers={}),
    )
    assert result == StartOperationResultSync(3)


class UndecoratedSyncBase:
    @sync_operation
    def sync_op(self, _ctx: StartOperationContext, input: int) -> int:
        return input


@service_handler(name="sync-handler")
class SyncHandler(UndecoratedSyncBase):
    pass


def test_inherited_sync_handler_method_stays_sync():
    assert _service_name(SyncHandler.sync_op) == "sync-handler"
    assert not inspect.iscoroutinefunction(SyncHandler.sync_op)
    assert SyncHandler().sync_op(None, 5) == 5  # type: ignore[arg-type]
    # The undecorated parent is not a service
    assert _service_name(UndecoratedSyncBase.sync_op) is None


@service_handler(name="factory-handler")
class FactoryHandler:
    @operation_handler(name="Factory")
    def factory(self) -> OperationHandler[int, int]:
        async def start(_ctx: StartOperationContext, input: int) -> int:
            return input

        return SyncOperationHandler(start)


@service_handler(name="sub-factory-handler")
class SubFactoryHandler(FactoryHandler):
    pass


def test_operation_handler_factories_report_their_service():
    assert _service_name(FactoryHandler.factory) == "factory-handler"
    assert _service_name(SubFactoryHandler.factory) == "sub-factory-handler"
    assert _operation(SubFactoryHandler.factory).name == "Factory"
    assert isinstance(SubFactoryHandler().factory(), SyncOperationHandler)


def test_local_handler_class_reports_its_service():
    @service_handler(name="local-handler")
    class LocalHandler:
        @sync_operation
        async def op(self, _ctx: StartOperationContext, input: int) -> int:
            return input

    assert _service_name(LocalHandler.op) == "local-handler"
