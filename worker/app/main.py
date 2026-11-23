"""
worker/app/main.py
FastAPI application entry-point for a Worker node.
"""
import asyncio
import logging

from fastapi import FastAPI

from worker.app.api import chunks as chunks_api
from worker.app.api import worker as worker_api
from worker.app.core.config import STORAGE_DIR, WORKER_ID, WORKER_PORT
from worker.app.heartbeat.reporter import heartbeat_loop, register_with_controller
from worker.app.storage.chunk_store import ChunkStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(WORKER_ID)

app = FastAPI(title=f"Cloud Object Store — Worker [{WORKER_ID}]")

app.include_router(chunks_api.router)
app.include_router(worker_api.router)


@app.on_event("startup")
async def startup():
    store = ChunkStore(STORAGE_DIR)
    chunks_api.set_store(store)
    asyncio.create_task(register_with_controller())
    asyncio.create_task(heartbeat_loop(store))
    logger.info("Worker %s started on port %d", WORKER_ID, WORKER_PORT)
