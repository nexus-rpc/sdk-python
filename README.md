
```python
@dataclass
class Handler:
    service_handlers: dict[str, ServiceHandler]

    def get_service_handler(self, ctx: OperationContext) -> ServiceHandler: ...


@dataclass
class ServiceHandler:
    name: str
    operation_handlers: dict[str, OperationHandler[Any, Any]]

    def get_operation_handler(self, ctx: OperationContext) -> OperationHandler: ...


class OperationHandler(Generic[I, O]):
    def start(
        self, ctx: StartOperationContext, input: I
    ) -> Union[
        StartOperationResultSync[O],
        Awaitable[StartOperationResultSync[O]],
        Awaitable[StartOperationResultAsync],
    ]: ...

    def fetch_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]: ...

    def fetch_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Union[O, Awaitable[O]]: ...

    def cancel(
        self, ctx: CancelOperationContext, token: str
    ) -> Union[None, Awaitable[None]]: ...
```