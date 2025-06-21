from __future__ import annotations

import nexusrpc
import nexusrpc.handler


@nexusrpc.service
class ContractA:
    base_op: nexusrpc.Operation[int, str]


def test_service_definition_decorator_collects_operations_from_annotations():
    service_defn = getattr(ContractA, "__nexus_service__", None)
    assert isinstance(service_defn, nexusrpc.ServiceDefinition)
    assert service_defn.operations.keys() == {"base_op"}
