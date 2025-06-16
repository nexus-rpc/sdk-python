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
    Mapping,
    Optional,
    Type,
    Union,
    overload,
)

from nexusrpc._util import get_annotations
from nexusrpc.types import (
    InputT,
    OutputT,
    ServiceDefinitionT,
)


@dataclass
class Operation(Generic[InputT, OutputT]):
    """Defines a Nexus operation in a Nexus service definition.

    This class is for definition of operation name and input/output types only; to
    implement an operation, see `:py:meth:nexusrpc.handler.operation_handler`.

    Example:

    .. code-block:: python

        @nexusrpc.service
        class MyNexusService:
            my_operation: nexusrpc.Operation[MyInput, MyOutput]
    """

    name: str
    method_name: Optional[str] = dataclasses.field(default=None)
    input_type: Optional[Type[InputT]] = dataclasses.field(default=None)
    output_type: Optional[Type[OutputT]] = dataclasses.field(default=None)


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

    # TODO(preview): error on attempt foo = Operation[int, str](name="bar")
    #            The input and output types are not accessible on the instance.
    # TODO(preview): Support foo = Operation[int, str]? E.g. via
    # ops = {name: nexusrpc.Operation[int, int] for name in op_names}
    # service_cls = nexusrpc.service(type("ServiceContract", (), ops))
    # This will require forming a union of operations disovered via __annotations__
    # and __dict__

    def decorator(cls: Type[ServiceDefinitionT]) -> Type[ServiceDefinitionT]:
        if name is not None and not name:
            raise ValueError("Service name must not be empty.")
        defn = ServiceDefinition.from_user_class(cls, name or cls.__name__)
        setattr(cls, "__nexus_service__", defn)

        # A decorated user service class must have a class attribute for each operation,
        # the value of which is the operation instance; it is not sufficient for the
        # operation to be represented by a type annotation alone.
        for op_name, op in defn.operations.items():
            setattr(cls, op_name, op)

        return cls

    if cls is None:
        return decorator
    else:
        return decorator(cls)


@dataclass
class ServiceDefinition:
    name: str
    operations: Mapping[str, Operation[Any, Any]]

    @classmethod
    def from_user_class(
        cls, user_class: Type[ServiceDefinitionT], name: str
    ) -> ServiceDefinition:
        """Create a ServiceDefinition from a user service definition class.

        All parent classes contribute operations to the ServiceDefinition, whether or not
        they are decorated with @nexusrpc.service.
        """
        # Recursively walk mro collecting operations not previously seen, stopping at an
        # already-decorated service definition.

        # If this class is decorated then return the already-computed ServiceDefinition.
        # (getattr would be affected by decorated parents)
        if defn := user_class.__dict__.get("__nexus_service__"):
            if isinstance(defn, ServiceDefinition):
                return defn

        if user_class is object:
            return ServiceDefinition(name=user_class.__name__, operations={})

        parent = user_class.mro()[1]
        parent_defn = ServiceDefinition.from_user_class(parent, parent.__name__)
        defn = ServiceDefinition(
            name=name,
            operations=(
                dict(parent_defn.operations) | cls._collect_operations(user_class)
            ),
        )

    @staticmethod
    def _collect_operations(
        user_class: Type[ServiceDefinitionT],
    ) -> dict[str, Operation[Any, Any]]:
        """Collect operations from a user service definition class.

        Does not visit parent classes.
        """
        operations: dict[str, Operation[Any, Any]] = {}
        print(f"🟠 user_class: {user_class.__name__}")
        annotations: dict[str, Any] = get_annotations(user_class)
        for annot_name, op in annotations.items():
            print(f"🟡 annot_name: {annot_name}")
            if typing.get_origin(op) != Operation:
                continue
            args = typing.get_args(op)
            if len(args) != 2:
                raise TypeError(
                    f"Each operation in the service definition should look like  "
                    f"nexusrpc.Operation[MyInputType, MyOutputType]. "
                    f"However, '{annot_name}' in '{user_class}' has {len(args)} type parameters."
                )
            input_type, output_type = args
            op = getattr(user_class, annot_name, None)
            if not op:
                op = Operation(
                    name=annot_name,
                    method_name=annot_name,
                    input_type=input_type,
                    output_type=output_type,
                )
            else:
                if not isinstance(op, Operation):
                    raise TypeError(
                        f"Operation {annot_name} must be an instance of nexusrpc.Operation, "
                        f"but it is a {type(op)}"
                    )
                op.method_name = annot_name
                op.input_type = input_type
                op.output_type = output_type

            if op.name in operations:
                raise ValueError(
                    f"Operation '{op.name}' in class '{user_class}' is defined multiple times"
                )
            operations[op.name] = op
        return operations
