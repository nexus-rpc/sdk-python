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
    # TODO(preview): they should not be able to set method_name in constructor
    method_name: Optional[str] = dataclasses.field(default=None)
    input_type: Optional[Type[InputT]] = dataclasses.field(default=None)
    output_type: Optional[Type[OutputT]] = dataclasses.field(default=None)

    def __post_init__(self):
        if not self.name:
            raise ValueError("Operation name cannot be empty")

    def _validation_errors(self) -> list[str]:
        errors = []
        if not self.name:
            errors.append(
                f"Operation has no name (method_name is '{self.method_name}')"
            )
        if not self.method_name:
            errors.append(f"Operation '{self.name}' has no method name")
        if not self.input_type:
            errors.append(f"Operation '{self.name}' has no input type")
        if not self.output_type:
            errors.append(f"Operation '{self.name}' has no output type")
        return errors


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

        # In order for callers to refer to operations at run-time, a decorated user
        # service class must itself have a class attribute for every operation, even if
        # declared only via a type annotation, and whether inherited from a parent class
        # or not.
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

    @staticmethod
    def from_user_class(
        user_class: Type[ServiceDefinitionT], name: str
    ) -> ServiceDefinition:
        """Create a ServiceDefinition from a user service definition class.

        All parent classes contribute operations to the ServiceDefinition, whether or not
        they are decorated with @nexusrpc.service.
        """
        # Recursively walk mro collecting operations not previously seen, stopping at an
        # already-decorated service definition.

        # If this class is decorated then return the already-computed ServiceDefinition.
        # Do not use getattr since it would retrieve a value from a decorated parent class.
        if defn := user_class.__dict__.get("__nexus_service__"):
            if isinstance(defn, ServiceDefinition):
                return defn

        if user_class is object:
            return ServiceDefinition(name=user_class.__name__, operations={})

        parent = user_class.mro()[1]
        parent_defn = ServiceDefinition.from_user_class(parent, parent.__name__)

        # Update the inherited operations with those collected at this level.
        defn = ServiceDefinition(
            name=name,
            operations=ServiceDefinition._merge_operations(
                parent_defn.operations, user_class
            ),
        )
        if errors := defn._validation_errors():
            raise ValueError(
                f"Service definition {name} has validation errors: {', '.join(errors)}"
            )
        return defn

    def _validation_errors(self) -> list[str]:
        errors = []
        if not self.name:
            errors.append("Service has no name")
        for op in self.operations.values():
            errors.extend(op._validation_errors())
        return errors

    @staticmethod
    def _merge_operations(
        parent_operations: Mapping[str, Operation[Any, Any]],
        user_class: Type[ServiceDefinitionT],
    ) -> dict[str, Operation[Any, Any]]:
        merged = dict(parent_operations)
        parent_ops_by_method_name = {op.method_name: op for op in merged.values()}
        for op_name, op in ServiceDefinition._collect_operations(user_class).items():
            # If the operation at this level derives from an annotation alone (no
            # accompanying instance), then merge information from the inherited
            # operation, as long as it doesn't conflict. We look up by method name; if
            # the op at this level derives from an annotation alone then it has not
            # overridden its name.
            if parent_op := parent_ops_by_method_name.get(op_name):
                if op_name not in user_class.__dict__:
                    # TODO(prerelease): what about if they are both type annotations? Then the later one should win.
                    if op.input_type != parent_op.input_type:
                        raise TypeError(
                            f"Operation '{op_name}' in class '{user_class}' has input_type "
                            f"({op.input_type}). This does not match the type of the same "
                            f"operation in a parent class: ({parent_op.input_type})."
                        )
                    if op.output_type != parent_op.output_type:
                        raise TypeError(
                            f"Operation '{op_name}' in class '{user_class}' has output_type ({op.output_type}). "
                            f"This does not match the type of the same operation in a parent class: ({parent_op.output_type})."
                        )
                else:
                    merged[op_name] = parent_op
            else:
                merged[op_name] = op
        return merged

    @staticmethod
    def _collect_operations(
        user_class: Type[ServiceDefinitionT],
    ) -> dict[str, Operation[Any, Any]]:
        """Collect operations from a user service definition class.

        Does not visit parent classes.
        """

        # Form the union of all class attribute names that are either an Operation
        # instance or have an Operation type annotation, or both.
        operations: dict[str, Operation[Any, Any]] = {}
        for k, v in user_class.__dict__.items():
            if isinstance(v, Operation):
                operations[k] = v
            elif typing.get_origin(v) is Operation:
                raise TypeError(
                    "Operation definitions in the service definition should look like  "
                    "my_op: nexusrpc.Operation[InputType, OutputType]. Did you accidentally "
                    "use '=' instead of ':'?"
                )

        annotations = {
            k: v
            for k, v in get_annotations(user_class).items()
            if typing.get_origin(v) == Operation
        }
        for key in operations.keys() | annotations.keys():
            # If the name has a type annotation, then add the input and output types to
            # the operation instance, or create the instance if there was only an
            # annotation.
            if op_type := annotations.get(key):
                args = typing.get_args(op_type)
                if len(args) != 2:
                    raise TypeError(
                        f"Operation types in the service definition should look like  "
                        f"nexusrpc.Operation[InputType, OutputType], but '{key}' in "
                        f"'{user_class}' has {len(args)} type parameters."
                    )
                input_type, output_type = args
                if key not in operations:
                    # It looked like
                    # my_op: Operation[I, O]
                    op = operations[key] = Operation(
                        name=key,
                        method_name=key,
                        input_type=input_type,
                        output_type=output_type,
                    )
                else:
                    op = operations[key]
                    # It looked like
                    # my_op: Operation[I, O] = Operation(...)
                    if not op.input_type:
                        op.input_type = input_type
                    elif op.input_type != input_type:
                        raise ValueError(
                            f"Operation {key} input_type ({op.input_type}) must match type parameter {input_type}"
                        )
                    if not op.output_type:
                        op.output_type = output_type
                    elif op.output_type != output_type:
                        raise ValueError(
                            f"Operation {key} output_type ({op.output_type}) must match type parameter {output_type}"
                        )
            else:
                # It looked like
                # my_op = Operation(...)
                op = operations[key]
                if not op.method_name:
                    op.method_name = key
                elif op.method_name != key:
                    raise ValueError(
                        f"Operation {key} method_name ({op.method_name}) must match attribute name {key}"
                    )

            if op.method_name is None:
                op.method_name = key

        operations_by_name = {}
        for op in operations.values():
            if op.name in operations_by_name:
                raise ValueError(
                    f"Operation '{op.name}' in class '{user_class}' is defined multiple times"
                )
            operations_by_name[op.name] = op
        return operations_by_name
