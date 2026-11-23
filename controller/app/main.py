"""
controller/app/main.py
FastAPI application entry-point for the Controller node.
"""
import asyncio

from fastapi import FastAPI, HTTPException, Request

from controller.app.api import internal_workers, metadata, objects
from controller.app.cluster import health_monitor
from controller.app.cluster.registry import health_snapshot
from controller.app.core.responses import PrettyJSONResponse
from controller.app.scheduler.replication import restore_replication

app = FastAPI(
    title="Cloud Object Store — Controller",
    default_response_class=PrettyJSONResponse,
)

# ── Pretty-print ALL HTTPException responses ──────────────────────────────────

@app.exception_handler(HTTPException)
async def pretty_http_exception_handler(request: Request, exc: HTTPException):
    return PrettyJSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail},
    )

# ── Routers ───────────────────────────────────────────────────────────────────

app.include_router(internal_workers.router)
app.include_router(objects.router)
app.include_router(metadata.router)

# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "workers": health_snapshot(),
        "object_count": len(objects.objects),
    }

# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    # Bind restore_replication to the shared objects dict
    async def _repair():
        await restore_replication(objects.objects)

    asyncio.create_task(health_monitor.replication_health_loop(_repair))
    import logging
    logging.getLogger("controller").info("Controller started.")
