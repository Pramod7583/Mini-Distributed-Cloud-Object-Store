"""
controller/app/integrity/repair.py
Self-healing: detect corrupt chunk replicas and re-write from a healthy copy.
"""
import logging
from typing import Dict, List, Optional

from controller.app.integrity.checksum import sha256
from controller.app.services.worker_client import get_chunk, put_chunk
from shared.constants import HEARTBEAT_TIMEOUT
from controller.app.cluster import registry

logger = logging.getLogger("controller.integrity")


async def get_chunk_with_integrity(chunk_info: Dict) -> Optional[bytes]:
    """
    Retrieve a chunk, verifying SHA-256.
    - Skips dead workers.
    - Collects corrupted workers.
    - After finding a good copy, self-heals corrupted replicas.
    Returns the good data, or None if all replicas fail.
    """
    import time

    chunk_id = chunk_info["chunk_id"]
    expected = chunk_info["checksum"]
    locations = chunk_info.get("locations", [])

    corrupted: List[Dict] = []
    good_data: Optional[bytes] = None

    for loc in locations:
        worker_id = loc["worker_id"]
        worker = registry.workers.get(worker_id)
        if not worker:
            continue
        if time.time() - worker["last_heartbeat"] > HEARTBEAT_TIMEOUT:
            continue

        data = await get_chunk(worker["url"], chunk_id)
        if data is None:
            continue

        if sha256(data) == expected:
            good_data = data
            break
        else:
            logger.warning(
                "Chunk %s on worker %s is CORRUPTED (expected=%s got=%s)",
                chunk_id, worker_id, expected[:8], sha256(data)[:8],
            )
            corrupted.append({"worker_id": worker_id, "url": worker["url"]})

    # Self-heal: rewrite each corrupted copy from the good one
    if good_data is not None and corrupted:
        for cw in corrupted:
            ok = await put_chunk(cw["url"], chunk_id, good_data, expected)
            if ok:
                logger.info("Repaired corrupted chunk %s on %s", chunk_id, cw["worker_id"])
            else:
                logger.warning("Failed to repair chunk %s on %s", chunk_id, cw["worker_id"])

    return good_data
