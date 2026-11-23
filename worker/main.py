"""
Worker Node - Mini Distributed Cloud Object Store
Stores chunks on disk and serves them via HTTP.
Sends heartbeats to the controller.
"""

import asyncio
import hashlib
import logging
import os
import time
from pathlib import Path

import aiofiles
import httpx
from fastapi import FastAPI, HTTPException, Request, Response

# ──────────────────────────────────────────────────────────────────────────────
# Config from environment
# ──────────────────────────────────────────────────────────────────────────────
WORKER_ID = os.environ.get("WORKER_ID", "worker-unknown")
WORKER_PORT = int(os.environ.get("WORKER_PORT", "8001"))
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://controller:8000")
STORAGE_DIR = Path(os.environ.get("STORAGE_DIR", "/data/chunks"))
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "5"))

# The URL this worker advertises to the controller (other services reach it here)
WORKER_ADVERTISE_URL = os.environ.get(
    "WORKER_ADVERTISE_URL", f"http://{WORKER_ID}:{WORKER_PORT}"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(WORKER_ID)

STORAGE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title=f"Cloud Object Store - Worker [{WORKER_ID}]")


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def chunk_path(chunk_id: str) -> Path:
    return STORAGE_DIR / f"{chunk_id}.chunk"


def stored_chunks() -> list:
    return [p.stem for p in STORAGE_DIR.glob("*.chunk")]


# ──────────────────────────────────────────────────────────────────────────────
# Internal chunk APIs
# ──────────────────────────────────────────────────────────────────────────────

@app.put("/internal/chunks/{chunk_id}", status_code=200)
async def store_chunk(chunk_id: str, request: Request):
    data = await request.body()
    expected_checksum = request.headers.get("X-Chunk-Checksum", "")

    # Verify checksum if provided
    if expected_checksum:
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected_checksum:
            raise HTTPException(400, f"Checksum mismatch: expected {expected_checksum}, got {actual}")

    path = chunk_path(chunk_id)
    async with aiofiles.open(path, "wb") as f:
        await f.write(data)

    logger.info("Stored chunk %s (%d bytes)", chunk_id, len(data))
    return {"chunk_id": chunk_id, "size": len(data), "status": "stored"}


@app.get("/internal/chunks/{chunk_id}")
async def retrieve_chunk(chunk_id: str):
    path = chunk_path(chunk_id)
    if not path.exists():
        raise HTTPException(404, f"Chunk {chunk_id} not found")

    async with aiofiles.open(path, "rb") as f:
        data = await f.read()

    return Response(content=data, media_type="application/octet-stream")


@app.delete("/internal/chunks/{chunk_id}", status_code=200)
async def delete_chunk(chunk_id: str):
    path = chunk_path(chunk_id)
    if path.exists():
        path.unlink()
        logger.info("Deleted chunk %s", chunk_id)
        return {"chunk_id": chunk_id, "status": "deleted"}
    raise HTTPException(404, f"Chunk {chunk_id} not found")


@app.get("/internal/workers/{worker_id}/chunks")
async def list_chunks(worker_id: str):
    if worker_id != WORKER_ID:
        raise HTTPException(404, "Worker ID mismatch")
    chunks = stored_chunks()
    return {"worker_id": WORKER_ID, "chunk_count": len(chunks), "chunks": chunks}


# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    chunks = stored_chunks()
    return {
        "worker_id": WORKER_ID,
        "status": "ok",
        "chunk_count": len(chunks),
        "storage_dir": str(STORAGE_DIR),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Startup: register + heartbeat loop
# ──────────────────────────────────────────────────────────────────────────────

async def register_with_controller():
    """Retry registration until the controller is reachable."""
    for attempt in range(20):
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.post(
                    f"{CONTROLLER_URL}/internal/workers/register",
                    json={"worker_id": WORKER_ID, "url": WORKER_ADVERTISE_URL},
                )
                if resp.status_code == 200:
                    logger.info("Registered with controller as %s", WORKER_ID)
                    return
                logger.warning("Registration returned %d", resp.status_code)
        except Exception as exc:
            logger.warning("Registration attempt %d failed: %s", attempt + 1, exc)
        await asyncio.sleep(3)
    logger.error("Could not register with controller after 20 attempts.")


async def heartbeat_loop():
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        chunks = stored_chunks()
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(
                    f"{CONTROLLER_URL}/internal/workers/heartbeat",
                    json={"worker_id": WORKER_ID, "chunk_count": len(chunks)},
                )
        except Exception as exc:
            logger.warning("Heartbeat failed: %s", exc)


@app.on_event("startup")
async def startup():
    asyncio.create_task(register_with_controller())
    asyncio.create_task(heartbeat_loop())
    logger.info("Worker %s started on port %d", WORKER_ID, WORKER_PORT)
