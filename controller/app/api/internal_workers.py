"""
controller/app/api/internal_workers.py
Worker registration and heartbeat endpoints.
"""
from fastapi import APIRouter, HTTPException, Request

from controller.app.cluster import registry
from controller.app.core.responses import PrettyJSONResponse

router = APIRouter(prefix="/internal/workers", tags=["internal"])


@router.post("/register", status_code=200)
async def register_worker(request: Request):
    body = await request.json()
    worker_id = body.get("worker_id")
    url = body.get("url")
    if not worker_id or not url:
        raise HTTPException(400, "worker_id and url required")
    registry.register(worker_id, url)
    import logging
    logging.getLogger("controller").info("Worker registered: %s @ %s", worker_id, url)
    return {"status": "registered", "worker_id": worker_id}


@router.post("/heartbeat", status_code=200)
async def heartbeat(request: Request):
    body = await request.json()
    worker_id = body.get("worker_id")
    if not worker_id or not registry.beat(worker_id, body.get("chunk_count", 0)):
        raise HTTPException(404, "Unknown worker")
    return {"status": "ok"}
