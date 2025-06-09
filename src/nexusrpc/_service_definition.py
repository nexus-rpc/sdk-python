"""
A Nexus service definition is a class with class attributes of type Operation. It must
be be decorated with @nexusrpc.service. The decorator validates the Operation
attributes.
"""

from __future__ import annotations

import dataclasses
import typing
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Generic,
    Optional,
    Type,
    Union,
    overload,
)

from nexusrpc.types import (
    InputT,
    OutputT,
    ServiceDefinitionT,
)


# TODO(prerelease): support inheritance in service definitions
@dataclass
class ServiceDefinition:
    name: str
    operations: dict[str, Operation[Any, Any]]


@dataclass
class Operation(Generic[InputT, OutputT]):
    """
    Used to define a Nexus operation in a Nexus service definition.

    To implement an operation handler, see `:py:meth:nexusrpc.handler.operation_handler`.

    Example:

    .. code-block:: python

        @nexusrpc.service
        class MyNexusService:
            my_operation: nexusrpc.Operation[MyInput, MyOutput]
    """

    name: Optional[str] = None
    method_name: str = dataclasses.field(init=False)
    input_type: Type[InputT] = dataclasses.field(init=False)
    output_type: Type[OutputT] = dataclasses.field(init=False)

    @property
    def key(self) -> str:
        return self.name or self.method_name

    @classmethod
    def _create(
        cls,
        *,
        name: Optional[str] = None,
        method_name: str,
        input_type: Type,
        output_type: Type,
    ) -> Operation:
        op = cls(name)
        op.method_name = method_name
        op.input_type = input_type
        op.output_type = output_type
        return op


@overload
def service(cls: Type[ServiceDefinitionT]) -> Type[ServiceDefinitionT]: ...


@overload
def service(
    *, name: Optional[str] = None
) -> Callable[[Type[ServiceDefinitionT]], Type[ServiceDefinitionT]]: ...


def service(
    cls: Optional[Type[ServiceDefinitionT]] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    Type[ServiceDefinitionT],
    Callable[[Type[ServiceDefinitionT]], Type[ServiceDefinitionT]],
]:
    """
    Decorator marking a class as a Nexus service definition.

    The decorator validates the operation definitions in the service definition: that they
    have the correct type, and that there are no duplicate operation names. The decorator
    also creates instances of the Operation class for each operation definition.


    Example:

    .. code-block:: python

        @nexusrpc.service
        class MyNexusService:
            my_operation: nexusrpc.Operation[MyInput, MyOutput]
    """

    def decorator(cls: Type[ServiceDefinitionT]) -> Type[ServiceDefinitionT]:
        if name is not None and not name:
            raise ValueError("Service name must not be empty.")
        service_name = name or cls.__name__
        # TODO(preview): error on attempt foo = Operation[int, str](name="bar")
        #            The input and output types are not accessible on the instance.
        # TODO(preview): Support foo = Operation[int, str]? E.g. via
        # ops = {name: nexusrpc.Operation[int, int] for name in op_names}
        # service_cls = nexusrpc.service(type("ServiceContract", (), ops))
        # This will require forming a union of operations disovered via __annotations__
        # and __dict__

        operations: dict[str, Operation] = {}
        annotations: dict[str, Any] = getattr(cls, "__annotations__", {})
        for annot_name, op in annotations.items():
            if typing.get_origin(op) == Operation:
                args = typing.get_args(op)
                if len(args) != 2:
                    raise TypeError(
                        f"Each operation in the service definition should look like  "
                        f"nexusrpc.Operation[MyInputType, MyOutputType]. "
                        f"However, '{annot_name}' in '{cls}' has {len(args)} type parameters."
                    )
                input_type, output_type = args
                op = getattr(cls, annot_name, None)
                if not op:
                    op = Operation._create(
                        method_name=annot_name,
                        input_type=input_type,
                        output_type=output_type,
                    )
                    setattr(cls, annot_name, op)
                else:
                    if not isinstance(op, Operation):
                        raise TypeError(
                            f"Operation {annot_name} must be an instance of nexusrpc.Operation, "
                            f"but it is a {type(op)}"
                        )
                    op.method_name = annot_name
                    op.input_type = input_type
                    op.output_type = output_type

                if op.key in operations:
                    raise ValueError(
                        f"Operation {op.key} in service {service_name} is defined multiple times"
                    )
                operations[op.key] = op

        cls.__nexus_service__ = ServiceDefinition(  # type: ignore
            name=service_name,
            operations=operations,
        )

        return cls

    if cls is None:
        return decorator
    else:
        return decorator(cls)
