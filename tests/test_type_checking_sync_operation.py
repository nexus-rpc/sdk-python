from nexusrpc.handler import StartOperationContext, service_handler, sync_operation


# This file is for type-level testing only
def _():
    @service_handler
    class TestServiceHandler:
        @sync_operation
        def good_syncio(self, ctx: StartOperationContext, input: int) -> str:
            return str(input)

        @sync_operation
        async def good_asyncio(self, ctx: StartOperationContext, input: int) -> str:
            return str(input)

        @sync_operation
        async def another_asyncio_1(
            self, ctx: StartOperationContext, input: int
        ) -> str:
            return await self.good_asyncio(ctx, input)

        @sync_operation(name="custom_name")
        def good_syncio_with_name(self, ctx: StartOperationContext, input: str) -> int:
            return len(input)

        @sync_operation(name="async_custom")
        async def good_asyncio_with_name(
            self, ctx: StartOperationContext, input: str
        ) -> int:
            return len(input)

        @sync_operation
        async def another_asyncio_2(
            self, ctx: StartOperationContext, input: str
        ) -> int:
            return await self.good_asyncio_with_name(ctx, input)

        # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter'
        # assert-type-error-mypy: "has incompatible type"
        @sync_operation  # type: ignore
        def syncio_bad_signature_1(self, x: int) -> str:
            return str(x)

        # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter'
        # assert-type-error-mypy: "has incompatible type"
        @sync_operation  # type: ignore
        def syncio_bad_signature_2(self, x: int, y: str, z: float) -> str:
            return str(x)

        # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter'
        # assert-type-error-mypy: "has incompatible type"
        @sync_operation  # type: ignore
        def syncio_bad_signature_3(self) -> str:
            return "test"

        # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter'
        # assert-type-error-mypy: "has incompatible type"
        @sync_operation  # type: ignore
        async def asyncio_bad_signature_1(self, x: int) -> str:
            return str(x)

        # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter'
        # assert-type-error-mypy: "has incompatible type"
        @sync_operation(name="bad")  # type: ignore
        def syncio_bad_signature_with_name(self, x: int) -> str:
            return str(x)

        # assert-type-error-pyright: 'Argument of type .+ cannot be assigned to parameter'
        # assert-type-error-mypy: "has incompatible type"
        @sync_operation(name="bad")  # type: ignore
        async def asyncio_bad_signature_with_name(self, x: int) -> str:
            return str(x)
