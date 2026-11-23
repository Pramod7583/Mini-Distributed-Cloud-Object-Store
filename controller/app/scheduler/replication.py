"""
controller/app/scheduler/replication.py
Replication repair logic — called by the health monitor background task.
"""
import logging
from typing import Any, Dict

from controller.app.cluster import registry
from controller.app.integrity.repair import get_chunk_with_integrity
from controller.app.services.worker_client import put_chunk
from shared.constants import REPLICATION_FACTOR

logger = logging.getLogger("controller.replication")


async def restore_replication(objects: Dict[str, Dict[str, Any]]) -> None:
    alive_now = {w["worker_id"] for w in registry.alive_workers()}
    if not alive_now:
        return

    for obj_id, obj in list(objects.items()):
        for chunk in obj.get("chunks", []):
            chunk_id = chunk["chunk_id"]
            existing_locs = chunk.get("locations", [])

            live_locs = [l for l in existing_locs if l["worker_id"] in alive_now]

            if len(live_locs) >= REPLICATION_FACTOR:
                continue

            if not live_locs:
                logger.error("Chunk %s has NO live replicas — data loss!", chunk_id)
                continue

            data = await get_chunk_with_integrity(chunk)
            if data is None:
                logger.error("Cannot restore chunk %s — all copies unreadable", chunk_id)
                continue

            existing_wids = {l["worker_id"] for l in live_locs}
            needed = REPLICATION_FACTOR - len(live_locs)
            candidates = [
                w for w in registry.alive_workers()
                if w["worker_id"] not in existing_wids
            ]

            new_locs = list(live_locs)
            for target in candidates[:needed]:
                ok = await put_chunk(target["url"], chunk_id, data, chunk["checksum"])
                if ok:
                    new_locs.append({"worker_id": target["worker_id"], "url": target["url"]})
                    logger.info("Restored chunk %s replica on worker %s", chunk_id, target["worker_id"])

            if len(new_locs) > len(live_locs):
                chunk["locations"] = new_locs
