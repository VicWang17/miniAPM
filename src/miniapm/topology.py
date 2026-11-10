from datetime import datetime
from typing import Literal
from uuid import UUID, uuid5

from pydantic import Field

from miniapm.models import ContractModel, ServiceName


class InstanceSpec(ContractModel):
    instance: str = Field(min_length=1)
    version: str = Field(min_length=1)


class ServiceSpec(ContractModel):
    service: ServiceName
    instances: tuple[InstanceSpec, ...] = Field(min_length=1)


class Topology(ContractModel):
    environment: str = "local"
    dataset_id: UUID
    route: str = "POST /orders"
    services: tuple[ServiceSpec, ...]

    def service(self, name: ServiceName) -> ServiceSpec:
        return next(spec for spec in self.services if spec.service == name)


class SpanPlan(ContractModel):
    event_id: UUID
    span_id: str = Field(pattern=r"^[0-9a-f]{16}$")
    parent_span_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{16}$")
    service: ServiceName
    instance: str
    version: str
    kind: Literal["server", "client"]


class RequestPlan(ContractModel):
    request_id: UUID
    trace_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    event_time: datetime
    environment: str
    dataset_id: UUID
    route: str
    source_kind: Literal["simulated"] = "simulated"
    spans: tuple[SpanPlan, ...]


def default_topology(dataset_id: UUID) -> Topology:
    return Topology(
        dataset_id=dataset_id,
        services=(
            ServiceSpec(
                service="order",
                instances=(
                    InstanceSpec(instance="order-1", version="v1"),
                    InstanceSpec(instance="order-2", version="v2"),
                ),
            ),
            ServiceSpec(
                service="inventory",
                instances=(
                    InstanceSpec(instance="inventory-1", version="v1"),
                    InstanceSpec(instance="inventory-2", version="v2"),
                ),
            ),
        ),
    )


def _stable_uuid(dataset_id: UUID, request_number: int, part: str) -> UUID:
    return uuid5(dataset_id, f"request:{request_number}:{part}")


def build_request_plan(
    topology: Topology,
    request_number: int,
    event_time: datetime,
) -> RequestPlan:
    if request_number < 0:
        raise ValueError("request_number must be non-negative")
    order = topology.service("order")
    inventory = topology.service("inventory")
    order_instance = order.instances[request_number % len(order.instances)]
    inventory_instance = inventory.instances[request_number % len(inventory.instances)]

    trace_id = _stable_uuid(topology.dataset_id, request_number, "trace").hex
    order_server_id = _stable_uuid(topology.dataset_id, request_number, "order-server").hex[:16]
    order_client_id = _stable_uuid(topology.dataset_id, request_number, "order-client").hex[:16]
    inventory_server_id = _stable_uuid(
        topology.dataset_id, request_number, "inventory-server"
    ).hex[:16]

    spans = (
        SpanPlan(
            event_id=_stable_uuid(topology.dataset_id, request_number, "event-order-server"),
            span_id=order_server_id,
            service="order",
            instance=order_instance.instance,
            version=order_instance.version,
            kind="server",
        ),
        SpanPlan(
            event_id=_stable_uuid(topology.dataset_id, request_number, "event-order-client"),
            span_id=order_client_id,
            parent_span_id=order_server_id,
            service="order",
            instance=order_instance.instance,
            version=order_instance.version,
            kind="client",
        ),
        SpanPlan(
            event_id=_stable_uuid(topology.dataset_id, request_number, "event-inventory-server"),
            span_id=inventory_server_id,
            parent_span_id=order_client_id,
            service="inventory",
            instance=inventory_instance.instance,
            version=inventory_instance.version,
            kind="server",
        ),
    )
    return RequestPlan(
        request_id=_stable_uuid(topology.dataset_id, request_number, "request"),
        trace_id=trace_id,
        event_time=event_time,
        environment=topology.environment,
        dataset_id=topology.dataset_id,
        route=topology.route,
        spans=spans,
    )
