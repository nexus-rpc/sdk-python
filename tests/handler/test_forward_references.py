# TODO(prerelease) This test fails with this import line
from __future__ import annotations

import pytest

import nexusrpc
import nexusrpc._handler


@nexusrpc.service
class ContractA:
    base_op: nexusrpc.Operation[int, str]


@pytest.mark.skip(
    reason="TODO(prerelease): The service contract decorator does not support forward type reference"
)
def test_service_definition_decorator_collects_operations_from_annotations():
    user_service_defn_cls = nexusrpc.service(ContractA)
    service_defn = getattr(user_service_defn_cls, "__nexus_service__", None)
    assert isinstance(service_defn, nexusrpc.ServiceDefinition)
    assert service_defn.operations.keys() == {"base_op"}
