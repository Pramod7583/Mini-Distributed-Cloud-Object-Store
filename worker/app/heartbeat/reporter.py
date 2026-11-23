"""
worker/app/heartbeat/reporter.py
Handles registration with the controller and periodic heartbeat loop.
"""
import asyncio
import logging

import httpx

from worker.app.core.config import CONTROLLER_URL, WORKER_ADVERTISE_URL, WORKER_ID
from worker.app.storage.chunk_store import ChunkStore
from shared.constants import (
    HEARTBEAT_INTERVAL,
    WORKER_REGISTER_RETRIES,
    WORKER_REGISTER_RETRY_DELAY,
)

logger = logging.getLogger(WORKER_ID)


async def register_with_controller() -> None:
    for attempt in range(WORKER_REGISTER_RETRIES):
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
        await asyncio.sleep(WORKER_REGISTER_RETRY_DELAY)
    logger.error("Could not register with controller after %d attempts.", WORKER_REGISTER_RETRIES)


async def heartbeat_loop(store: ChunkStore) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        chunks = store.list_chunk_ids()
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                await client.post(
                    f"{CONTROLLER_URL}/internal/workers/heartbeat",
                    json={"worker_id": WORKER_ID, "chunk_count": len(chunks)},
                )
        except Exception as exc:
            logger.warning("Heartbeat failed: %s", exc)
