"""
controller/app/services/worker_client.py
HTTP helpers for talking to worker nodes.
"""
import logging
from typing import Optional

import httpx

logger = logging.getLogger("controller.worker_client")


async def put_chunk(worker_url: str, chunk_id: str, data: bytes, checksum: str) -> bool:
    """PUT a chunk to a worker. Returns True on success."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.put(
                f"{worker_url}/internal/chunks/{chunk_id}",
                content=data,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Chunk-Checksum": checksum,
                },
            )
            return resp.status_code == 200
    except Exception as exc:
        logger.warning("PUT chunk %s to %s failed: %s", chunk_id, worker_url, exc)
        return False


async def get_chunk(worker_url: str, chunk_id: str) -> Optional[bytes]:
    """GET a chunk from a worker. Returns bytes or None on failure."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(f"{worker_url}/internal/chunks/{chunk_id}")
            if resp.status_code == 200:
                return resp.content
            return None
    except Exception as exc:
        logger.warning("GET chunk %s from %s failed: %s", chunk_id, worker_url, exc)
        return None


async def delete_chunk(worker_url: str, chunk_id: str) -> bool:
    """DELETE a chunk from a worker."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.delete(f"{worker_url}/internal/chunks/{chunk_id}")
            return resp.status_code in (200, 204, 404)
    except Exception as exc:
        logger.warning("DELETE chunk %s from %s failed: %s", chunk_id, worker_url, exc)
        return False
