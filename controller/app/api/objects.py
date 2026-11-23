"""
controller/app/api/objects.py
Client-facing object endpoints: upload, download, delete, list.
"""
import hashlib
import io
import json
import math
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, File, Form, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from controller.app.cluster import registry
from controller.app.integrity.checksum import sha256
from controller.app.integrity.repair import get_chunk_with_integrity
from controller.app.scheduler.placement import pick_workers_for_chunk
from controller.app.services.worker_client import delete_chunk, put_chunk
from shared.constants import CHUNK_SIZE, REPLICATION_FACTOR
from shared.models import make_chunk_record, make_object_record

router = APIRouter(prefix="/api/v1/objects", tags=["objects"])

# In-memory object store
objects = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Upload ────────────────────────────────────────────────────────────────────

@router.post("", status_code=201)
async def upload_object(
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(None),
):
    raw = await file.read()
    total_size = len(raw)
    content_type = file.content_type or "application/octet-stream"
    filename = file.filename or "unnamed"

    custom_meta = {}
    if metadata:
        try:
            custom_meta = json.loads(metadata)
            if not isinstance(custom_meta, dict):
                raise ValueError
        except (json.JSONDecodeError, ValueError):
            raise HTTPException(400, "metadata must be a JSON object")

    alive = registry.alive_workers()
    if len(alive) < REPLICATION_FACTOR:
        raise HTTPException(
            503,
            f"Need at least {REPLICATION_FACTOR} alive workers; only {len(alive)} available.",
        )

    num_chunks = max(1, math.ceil(total_size / CHUNK_SIZE))
    object_id = str(uuid.uuid4())
    chunk_list = []

    for idx in range(num_chunks):
        chunk_data = raw[idx * CHUNK_SIZE: (idx + 1) * CHUNK_SIZE]
        chunk_id = f"{object_id}_{idx}"
        checksum = sha256(chunk_data)

        selected = pick_workers_for_chunk()
        locations = []
        for w in selected:
            ok = await put_chunk(w["url"], chunk_id, chunk_data, checksum)
            if ok:
                locations.append({"worker_id": w["worker_id"], "url": w["url"]})

        if len(locations) < REPLICATION_FACTOR:
            raise HTTPException(503, f"Could not store chunk {chunk_id} with required replication factor.")

        chunk_list.append(make_chunk_record(chunk_id, idx, len(chunk_data), checksum, locations))

    overall = sha256(raw)
    ts = _now()
    obj = make_object_record(object_id, filename, total_size, content_type, ts, ts, overall, num_chunks, chunk_list, custom_meta)
    objects[object_id] = obj

    import logging
    logging.getLogger("controller").info("Stored object %s (%d bytes, %d chunks)", object_id, total_size, num_chunks)

    return {
        "object_id": object_id,
        "name": filename,
        "size": total_size,
        "content_type": content_type,
        "num_chunks": num_chunks,
        "checksum": overall,
        "created_at": ts,
    }


# ── Download ──────────────────────────────────────────────────────────────────

@router.get("/{object_id}")
async def download_object(object_id: str):
    obj = objects.get(object_id)
    if not obj:
        raise HTTPException(404, "Object not found")

    assembled = io.BytesIO()
    for chunk_info in sorted(obj["chunks"], key=lambda c: c["index"]):
        data = await get_chunk_with_integrity(chunk_info)
        if data is None:
            raise HTTPException(503, f"Chunk {chunk_info['chunk_id']} is unavailable or corrupted on all replicas")
        assembled.write(data)

    assembled.seek(0)
    return StreamingResponse(
        assembled,
        media_type=obj["content_type"],
        headers={"Content-Disposition": f'attachment; filename="{obj["name"]}"'},
    )


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete("/{object_id}", status_code=204)
async def delete_object(object_id: str, background_tasks: BackgroundTasks):
    obj = objects.pop(object_id, None)
    if not obj:
        raise HTTPException(404, "Object not found")

    async def _delete_chunks():
        for chunk_info in obj["chunks"]:
            for loc in chunk_info["locations"]:
                worker = registry.workers.get(loc["worker_id"])
                if worker:
                    await delete_chunk(worker["url"], chunk_info["chunk_id"])

    background_tasks.add_task(_delete_chunks)
    from fastapi import Response
    return Response(status_code=204)


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("")
async def list_objects(limit: int = 100, offset: int = 0):
    all_objs = list(objects.values())
    page = all_objs[offset: offset + limit]
    return {
        "total": len(all_objs),
        "limit": limit,
        "offset": offset,
        "objects": [
            {
                "object_id": o["object_id"],
                "name": o["name"],
                "size": o["size"],
                "content_type": o["content_type"],
                "created_at": o["created_at"],
                "last_modified": o["last_modified"],
            }
            for o in page
        ],
    }
