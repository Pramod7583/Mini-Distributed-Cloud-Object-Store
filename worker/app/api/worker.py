"""
worker/app/api/worker.py
Worker info and health endpoints.
"""
from fastapi import APIRouter, HTTPException

from worker.app.api.chunks import get_store
from worker.app.core.config import STORAGE_DIR, WORKER_ID

router = APIRouter(tags=["worker"])


@router.get("/internal/workers/{worker_id}/chunks")
async def list_chunks(worker_id: str):
    if worker_id != WORKER_ID:
        raise HTTPException(404, "Worker ID mismatch")
    chunks = get_store().list_chunk_ids()
    return {"worker_id": WORKER_ID, "chunk_count": len(chunks), "chunks": chunks}


@router.get("/health")
async def health():
    chunks = get_store().list_chunk_ids()
    return {
        "worker_id": WORKER_ID,
        "status": "ok",
        "chunk_count": len(chunks),
        "storage_dir": str(STORAGE_DIR),
    }
