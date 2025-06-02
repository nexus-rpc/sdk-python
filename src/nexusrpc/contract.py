"""
A Nexus service contract is a class with class attributes of type Operation.
It must be decorated with @service.
The decorator validates the Operation attributes, creating instances if no instance was supplied.
"""

from __future__ import annotations

import dataclasses
import typing
from dataclasses import dataclass
from typing import (
    Callable,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
    overload,
)

I = TypeVar("I", contravariant=True)
O = TypeVar("O", covariant=True)
T = TypeVar("T")


# TODO(dan): support inheritance in service contracts
@dataclass
class Service:
    name: str
    operations: dict[str, Operation] = dataclasses.field(default_factory=dict)


@dataclass
class Operation(Generic[I, O]):
    """
    Used to define a Nexus operation in a Nexus service contract definition.

    Example:
    ```python
    @nexusrpc.contract.service
    class MyNexusService:
        echo: nexusrpc.contract.Operation[EchoInput, EchoOutput]
        hello: nexusrpc.contract.Operation[HelloInput, HelloOutput]
    ```
    """

    name: str
    input_type: Type[I] = dataclasses.field(init=False)
    output_type: Type[O] = dataclasses.field(init=False)

    @classmethod
    def _create(cls, name: str, input_type: Type, output_type: Type) -> Operation:
        op = cls(name)
        op.input_type = input_type
        op.output_type = output_type
        return op


@overload
def service(cls: Type[T]) -> Type[T]: ...


@overload
def service(*, name: Optional[str] = None) -> Callable[[Type[T]], Type[T]]: ...


def service(
    cls: Optional[Type[T]] = None,
    *,
    name: Optional[str] = None,
) -> Union[Type[T], Callable[[Type[T]], Type[T]]]:
    """
    Decorator marking a class as a Nexus service definition.

    Example:
    ```python
    @nexusrpc.contract.service
    class MyNexusService:
        operation: nexusrpc.contract.Operation[EchoInput, EchoOutput]
        operation_B: nexusrpc.contract.Operation[HelloInput, HelloOutput] = nexusrpc.contract.Operation(name="operation-B")
    ```
    """

    def decorator(cls: Type[T]) -> Type[T]:
        if name is not None and not name:
            raise ValueError("Service name must not be empty.")
        # TODO(dan): check that op names are unique
        # TODO(dan): error on attempt foo = Operation[int, str](name="bar")
        #            The input and output types are not accessible on the instance.
        # TODO(dan): Support foo = Operation[int, str]? E.g. via
        # ops = {name: nexusrpc.contract.Operation[int, int] for name in op_names}
        # service_cls = nexusrpc.contract.service(type("ServiceContract", (), ops))
        # This will require forming a union of operations disovered via __annotations__
        # and __dict__

        operations = {}
        annotations = getattr(cls, "__annotations__", {})
        for op_name, op in annotations.items():
            if typing.get_origin(op) == Operation:
                args = typing.get_args(op)
                if len(args) != 2:
                    raise TypeError(
                        f"Each operation in the service contract should look like  "
                        f"nexusrpc.contract.Operation[MyInputType, MyOutputType]. "
                        f"However, '{op_name}' in '{cls}' has {len(args)} type parameters."
                    )
                input_type, output_type = args
                op = getattr(cls, op_name, None)
                if not op:
                    op = Operation(op_name)
                    setattr(cls, op_name, op)
                else:
                    if not isinstance(op, Operation):
                        raise TypeError(
                            f"Operation {op_name} must be an instance of nexusrpc.contract.Operation, "
                            f"but it is a {type(op)}"
                        )
                op.input_type = input_type
                op.output_type = output_type
                operations[op.name] = op

        cls.__nexus_service__ = Service(  # type: ignore
            name=name or cls.__name__,
            operations=operations,
        )

        return cls

    if cls is None:
        return decorator
    else:
        return decorator(cls)
