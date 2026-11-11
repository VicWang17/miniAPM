import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import format_trace_id
from pydantic import BaseModel


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
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    state = {"cpu_fault": False}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        provider.shutdown()

    app = FastAPI(title=f"miniAPM {service}", lifespan=lifespan)
    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()

    @app.get("/healthz")
    def health() -> dict:
        return {"status": "ok", "service": service}

    @app.post("/fault")
    def set_fault(request: FaultInput) -> dict:
        state["cpu_fault"] = request.enabled
        return {"service": service, **state}

    @app.post("/inventory/reserve")
    async def reserve() -> dict:
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
        return {
            "service": "order",
            "result": "created",
            "trace_id": format_trace_id(trace.get_current_span().get_span_context().trace_id),
            "inventory": inventory.json(),
        }

    return app
