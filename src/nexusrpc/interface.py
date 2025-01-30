from dataclasses import dataclass
from typing import Generic, Type, TypeVar

I = TypeVar("I", contravariant=True)
O = TypeVar("O", covariant=True)
T = TypeVar("T")


class NexusService:
    pass


@dataclass
class NexusOperation(Generic[I, O]):
    name: str


# TODO(dan): support assigning invalid Python identifier as interface name
# The name of the interface is the name used to refer to the service in Nexus requests.
def nexus_service(interface: Type[T]) -> Type[T]:
    """
    Decorator that creates a Nexus service interface from an interface protocol type.

    Example:
        ```python
        @nexus_service
        class MyService:
            ...
        ```
    """
    # TODO
    data = {name: NexusOperation(name) for name in interface.__annotations__}

    return type(f"{interface.__name__}", (interface,), data)  # type: ignore
