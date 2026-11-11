import logging
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import format_trace_id
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from miniapm.instrumented_telemetry import InstrumentedMetrics, create_tracer_provider


class FaultInput(BaseModel):
    enabled: bool


def create_instrumented_app() -> FastAPI:
    service = os.environ["SERVICE_NAME"]
    resource = Resource.create({
        "service.name": service,
        "service.instance.id": os.environ["INSTANCE"],
        "service.version": os.environ["VERSION"],
        "deployment.environment.name": "linux-container",
    })
    provider, publisher = create_tracer_provider(resource)
    trace.set_tracer_provider(provider)
    state = {"cpu_fault": False}
    metrics = InstrumentedMetrics()
    logger = logging.getLogger("miniapm.service")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        provider.shutdown()
        publisher.close()

    app = FastAPI(title=f"miniAPM {service}", lifespan=lifespan)
    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()

    @app.middleware("http")
    async def observe_request(request: Request, call_next) -> Response:
        if request.url.path not in {"/orders", "/inventory/reserve"}:
            return await call_next(request)
        labels = metrics.started(request.url.path)
        started = time.monotonic()
        status = "ok"
        try:
            response = await call_next(request)
            status = "ok" if response.status_code < 500 else "error"
            return response
        except Exception:
            status = "error"
            raise
        finally:
            metrics.finished(labels, status, started)

    @app.get("/metrics", include_in_schema=False)
    def prometheus_metrics() -> Response:
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    @app.get("/healthz")
    def health() -> dict:
        return {"status": "ok", "service": service}

    @app.post("/fault")
    def set_fault(request: FaultInput) -> dict:
        state["cpu_fault"] = request.enabled
        logger.info("cpu fault changed to %s", request.enabled, extra={"route": "/fault"})
        return {"service": service, **state}

    @app.post("/inventory/reserve")
    async def reserve() -> dict:
        logger.info("inventory reservation completed", extra={"route": "/inventory/reserve"})
        return {
            "service": "inventory",
            "result": "reserved",
            "trace_id": format_trace_id(trace.get_current_span().get_span_context().trace_id),
        }

    @app.post("/orders")
    async def create_order() -> dict:
        if service != "order":
            return {"error": "unsupported service"}
        async with httpx.AsyncClient(timeout=5) as client:
            inventory = await client.post(f"{os.environ['INVENTORY_URL']}/inventory/reserve")
            inventory.raise_for_status()
        logger.info("order created after inventory reservation", extra={"route": "/orders"})
        return {
            "service": "order",
            "result": "created",
            "trace_id": format_trace_id(trace.get_current_span().get_span_context().trace_id),
            "inventory": inventory.json(),
        }

    return app
