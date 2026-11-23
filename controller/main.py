"""
Controller Node - Mini Distributed Cloud Object Store
Handles all client-facing REST API and coordinates storage workers.
"""

import asyncio
import hashlib
import io
import json
import logging
import math
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import (
    BackgroundTasks,
    FastAPI,
    File,
    Form,
    HTTPException,
    Request,
    Response,
    UploadFile,
)
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import JSONResponse, StreamingResponse

# ──────────────────────────────────────────────────────────────────────────────
# Pretty JSON response (indent=2 for human-readable curl / browser output)
# ──────────────────────────────────────────────────────────────────────────────

class PrettyJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(content, indent=2, ensure_ascii=False).encode("utf-8")

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
CHUNK_SIZE = 1 * 1024 * 1024          # 1 MB
REPLICATION_FACTOR = 2
HEARTBEAT_TIMEOUT = 15                # seconds before a worker is considered dead
REPLICATION_CHECK_INTERVAL = 10      # seconds between replication health checks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("controller")

app = FastAPI(
    title="Cloud Object Store - Controller",
    default_response_class=PrettyJSONResponse,
)

# ──────────────────────────────────────────────────────────────────────────────
# Bug Fix 2: Pretty-print ALL HTTPException error responses (400/404/503 etc.)
# ──────────────────────────────────────────────────────────────────────────────

@app.exception_handler(HTTPException)
async def pretty_http_exception_handler(request: Request, exc: HTTPException):
    return PrettyJSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail},
    )

# ──────────────────────────────────────────────────────────────────────────────
# In-memory state
# ──────────────────────────────────────────────────────────────────────────────
# objects[object_id] = { ... metadata dict ... }
objects: Dict[str, Dict[str, Any]] = {}

# workers[worker_id] = { "url": str, "last_heartbeat": float, "chunk_count": int }
workers: Dict[str, Dict[str, Any]] = {}

# Round-robin counter — incremented per chunk placement to spread chunks evenly
_rr_counter: int = 0

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def alive_workers() -> List[Dict[str, Any]]:
    cutoff = time.time() - HEARTBEAT_TIMEOUT
    return [
        {"worker_id": wid, **w}
        for wid, w in workers.items()
        if w["last_heartbeat"] >= cutoff
    ]


def pick_workers_for_chunk(exclude: Optional[List[str]] = None) -> List[Dict]:
    """
    Choose REPLICATION_FACTOR distinct alive workers for a chunk using round-robin
    so that multi-chunk uploads distribute evenly across all healthy workers.
    """
    global _rr_counter
    alive = alive_workers()
    # Sort alphabetically for stable, deterministic ordering
    alive.sort(key=lambda w: w["worker_id"])
    if exclude:
        alive = [w for w in alive if w["worker_id"] not in exclude]
    if len(alive) < REPLICATION_FACTOR:
        raise HTTPException(
            status_code=503,
            detail=f"Need {REPLICATION_FACTOR} alive workers; only {len(alive)} available.",
        )
    # Pick REPLICATION_FACTOR consecutive workers starting at the current counter
    n = len(alive)
    selected = [alive[(_rr_counter + i) % n] for i in range(REPLICATION_FACTOR)]
    _rr_counter = (_rr_counter + REPLICATION_FACTOR) % n
    return selected


async def worker_put_chunk(worker_url: str, chunk_id: str, data: bytes, checksum: str) -> bool:
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


async def worker_get_chunk(worker_url: str, chunk_id: str) -> Optional[bytes]:
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


async def worker_delete_chunk(worker_url: str, chunk_id: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.delete(f"{worker_url}/internal/chunks/{chunk_id}")
            return resp.status_code in (200, 204, 404)
    except Exception as exc:
        logger.warning("DELETE chunk %s from %s failed: %s", chunk_id, worker_url, exc)
        return False


async def get_chunk_with_integrity(chunk_info: Dict) -> Optional[bytes]:
    """
    Try to retrieve a valid chunk from any of its replica locations.
    Verifies checksum; skips corrupted copies and self-heals them once a
    good copy is found (Bug Fix 1).
    """
    chunk_id = chunk_info["chunk_id"]
    expected_checksum = chunk_info["checksum"]
    locations = chunk_info.get("locations", [])

    corrupted_workers: List[Dict] = []   # workers whose copy failed SHA-256
    good_data: Optional[bytes] = None

    for loc in locations:
        worker_id = loc["worker_id"]
        worker = workers.get(worker_id)
        if not worker:
            continue
        # Skip dead workers
        if time.time() - worker["last_heartbeat"] > HEARTBEAT_TIMEOUT:
            continue

        data = await worker_get_chunk(worker["url"], chunk_id)
        if data is None:
            continue

        actual = hashlib.sha256(data).hexdigest()
        if actual == expected_checksum:
            good_data = data
            break                        # found a valid copy — stop searching
        else:
            logger.warning(
                "Chunk %s on worker %s is CORRUPTED (expected=%s got=%s)",
                chunk_id, worker_id, expected_checksum, actual,
            )
            corrupted_workers.append({"worker_id": worker_id, "url": worker["url"]})

    # Bug Fix 1: self-heal — rewrite the corrupted copy from the good one
    if good_data is not None and corrupted_workers:
        for cw in corrupted_workers:
            ok = await worker_put_chunk(cw["url"], chunk_id, good_data, expected_checksum)
            if ok:
                logger.info(
                    "Repaired corrupted chunk %s on %s", chunk_id, cw["worker_id"]
                )
            else:
                logger.warning(
                    "Failed to repair chunk %s on %s", chunk_id, cw["worker_id"]
                )

    return good_data


# ──────────────────────────────────────────────────────────────────────────────
# Background: replication health checker
# ──────────────────────────────────────────────────────────────────────────────

async def replication_health_loop():
    """
    Periodically checks all chunks and restores replication factor
    when workers come back or chunks fall below REPLICATION_FACTOR copies.
    """
    await asyncio.sleep(5)  # give workers time to register on startup
    while True:
        await asyncio.sleep(REPLICATION_CHECK_INTERVAL)
        try:
            await restore_replication()
        except Exception as exc:
            logger.error("Replication health check error: %s", exc)


async def restore_replication():
    alive_now = {w["worker_id"] for w in alive_workers()}
    if not alive_now:
        return

    for obj_id, obj in list(objects.items()):
        for chunk in obj.get("chunks", []):
            chunk_id = chunk["chunk_id"]
            existing_locs = chunk.get("locations", [])

            # Filter to live locations
            live_locs = [l for l in existing_locs if l["worker_id"] in alive_now]

            if len(live_locs) >= REPLICATION_FACTOR:
                continue  # already healthy

            # Need to replicate
            if not live_locs:
                logger.error("Chunk %s has NO live replicas — data loss!", chunk_id)
                continue

            # Fetch data from an existing good copy
            data = await get_chunk_with_integrity(chunk)
            if data is None:
                logger.error("Cannot restore chunk %s — all copies unreadable", chunk_id)
                continue

            existing_worker_ids = {l["worker_id"] for l in live_locs}
            needed = REPLICATION_FACTOR - len(live_locs)
            candidates = [
                w for w in alive_workers()
                if w["worker_id"] not in existing_worker_ids
            ]

            # Bug Fix 3: only update chunk["locations"] after a successful PUT;
            # keep a local accumulator so metadata is never truncated on failure.
            new_locs = list(live_locs)
            for target in candidates[:needed]:
                ok = await worker_put_chunk(target["url"], chunk_id, data, chunk["checksum"])
                if ok:
                    new_locs.append({"worker_id": target["worker_id"], "url": target["url"]})
                    logger.info(
                        "Restored chunk %s replica on worker %s",
                        chunk_id, target["worker_id"],
                    )

            # Only commit the update if we actually added at least one new replica
            if len(new_locs) > len(live_locs):
                chunk["locations"] = new_locs


# ──────────────────────────────────────────────────────────────────────────────
# Startup
# ──────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    asyncio.create_task(replication_health_loop())
    logger.info("Controller started.")


# ──────────────────────────────────────────────────────────────────────────────
# Internal Worker APIs
# ──────────────────────────────────────────────────────────────────────────────

@app.post("/internal/workers/register", status_code=200)
async def register_worker(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    worker_id = body.get("worker_id")
    url = body.get("url")
    if not worker_id or not url:
        raise HTTPException(400, "worker_id and url required")

    existing_chunks: List[str] = body.get("existing_chunks", [])

    workers[worker_id] = {
        "url": url,
        "last_heartbeat": time.time(),
        "chunk_count": len(existing_chunks),
    }
    logger.info("Worker registered: %s @ %s (reported %d chunks)", worker_id, url, len(existing_chunks))

    if existing_chunks:
        background_tasks.add_task(_reconcile_worker_chunks, worker_id, url, existing_chunks)

    return {"status": "registered", "worker_id": worker_id}


async def _reconcile_worker_chunks(worker_id: str, worker_url: str, reported_chunks: List[str]) -> None:
    """
    Called in the background after a worker re-registers.

    For every chunk_id the worker reports on disk:
      Case A — Unreferenced (no active object owns it):
              DELETE it from the worker (orphaned / deleted-object leftover).
      Case B — Referenced chunk, but worker_id is NOT in chunk_locations
               and the chunk already has >= REPLICATION_FACTOR healthy replicas:
              DELETE it from the worker (excess / stale replica from re-replication).
      Case C — Referenced chunk, worker_id NOT in chunk_locations,
               and the chunk has < REPLICATION_FACTOR healthy replicas:
              Re-attach worker_id to chunk_locations (avoid duplicate).
      Case D — Referenced chunk, worker_id already in chunk_locations:
              Keep — nothing to do.
    """
    # Build a fast lookup: chunk_id → (object record, chunk record)
    chunk_index: Dict[str, Any] = {}
    for obj in objects.values():
        for chunk in obj.get("chunks", []):
            chunk_index[chunk["chunk_id"]] = chunk

    alive_now = {w["worker_id"] for w in alive_workers()}

    for chunk_id in reported_chunks:
        chunk = chunk_index.get(chunk_id)

        # ── Case A: orphaned chunk (no active object references it) ──────────
        if chunk is None:
            logger.info(
                "Reconcile %s: chunk %s is orphaned (no active object) — deleting",
                worker_id, chunk_id,
            )
            await worker_delete_chunk(worker_url, chunk_id)
            continue

        # Collect currently live locations (excluding newly registered worker itself
        # since it may not be in locations list yet)
        live_locs = [
            loc for loc in chunk.get("locations", [])
            if loc["worker_id"] != worker_id and loc["worker_id"] in alive_now
        ]
        already_in_locs = any(loc["worker_id"] == worker_id for loc in chunk.get("locations", []))

        if already_in_locs:
            # ── Case D: worker is already the legitimate owner ────────────────
            logger.debug("Reconcile %s: chunk %s already in locations — keeping", worker_id, chunk_id)
            continue

        # Worker is NOT in locations (it was replaced during re-replication)
        healthy_replica_count = len(live_locs)

        if healthy_replica_count >= REPLICATION_FACTOR:
            # ── Case B: excess copy — delete the stale replica ────────────────
            logger.info(
                "Reconcile %s: chunk %s has %d healthy replicas (>= RF=%d) — deleting stale copy",
                worker_id, chunk_id, healthy_replica_count, REPLICATION_FACTOR,
            )
            await worker_delete_chunk(worker_url, chunk_id)
        else:
            # ── Case C: under-replicated — re-attach this worker ─────────────
            worker_info = workers.get(worker_id)
            if worker_info:
                chunk["locations"].append({"worker_id": worker_id, "url": worker_url})
                logger.info(
                    "Reconcile %s: chunk %s re-attached (had %d healthy replica(s))",
                    worker_id, chunk_id, healthy_replica_count,
                )


@app.post("/internal/workers/heartbeat", status_code=200)
async def heartbeat(request: Request):
    body = await request.json()
    worker_id = body.get("worker_id")
    if not worker_id or worker_id not in workers:
        raise HTTPException(404, "Unknown worker")

    workers[worker_id]["last_heartbeat"] = time.time()
    workers[worker_id]["chunk_count"] = body.get("chunk_count", 0)
    return {"status": "ok"}


# ──────────────────────────────────────────────────────────────────────────────
# Client REST API — Upload
# ──────────────────────────────────────────────────────────────────────────────

@app.post("/api/v1/objects", status_code=201)
async def upload_object(
    file: UploadFile = File(...),
    metadata: Optional[str] = Form(None),
):
    raw = await file.read()
    total_size = len(raw)
    content_type = file.content_type or "application/octet-stream"
    filename = file.filename or "unnamed"

    # Parse custom metadata
    custom_meta: Dict[str, str] = {}
    if metadata:
        try:
            custom_meta = json.loads(metadata)
            if not isinstance(custom_meta, dict):
                raise ValueError
        except (json.JSONDecodeError, ValueError):
            raise HTTPException(400, "metadata must be a JSON object")

    # Chunk the object
    num_chunks = max(1, math.ceil(total_size / CHUNK_SIZE))
    chunk_list = []

    alive = alive_workers()
    if len(alive) < REPLICATION_FACTOR:
        raise HTTPException(
            503,
            f"Need at least {REPLICATION_FACTOR} alive workers; only {len(alive)} available.",
        )

    object_id = str(uuid.uuid4())

    for idx in range(num_chunks):
        start = idx * CHUNK_SIZE
        end = start + CHUNK_SIZE
        chunk_data = raw[start:end]
        chunk_id = f"{object_id}_{idx}"
        checksum = hashlib.sha256(chunk_data).hexdigest()

        selected = pick_workers_for_chunk()

        locations = []
        for w in selected:
            ok = await worker_put_chunk(w["url"], chunk_id, chunk_data, checksum)
            if ok:
                locations.append({"worker_id": w["worker_id"], "url": w["url"]})

        if len(locations) < REPLICATION_FACTOR:
            raise HTTPException(
                503,
                f"Could not store chunk {chunk_id} with required replication factor.",
            )

        chunk_list.append(
            {
                "chunk_id": chunk_id,
                "index": idx,
                "size": len(chunk_data),
                "checksum": checksum,
                "locations": locations,
            }
        )

    overall_checksum = hashlib.sha256(raw).hexdigest()
    ts = now_iso()

    obj = {
        "object_id": object_id,
        "name": filename,
        "size": total_size,
        "content_type": content_type,
        "created_at": ts,
        "last_modified": ts,
        "checksum": overall_checksum,
        "num_chunks": num_chunks,
        "chunks": chunk_list,
        "custom_metadata": custom_meta,
    }
    objects[object_id] = obj

    logger.info("Stored object %s (%s bytes, %d chunks)", object_id, total_size, num_chunks)

    return {
        "object_id": object_id,
        "name": filename,
        "size": total_size,
        "content_type": content_type,
        "num_chunks": num_chunks,
        "checksum": overall_checksum,
        "created_at": ts,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Client REST API — Download
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/v1/objects/{object_id}")
async def download_object(object_id: str):
    obj = objects.get(object_id)
    if not obj:
        raise HTTPException(404, "Object not found")

    assembled = io.BytesIO()
    for chunk_info in sorted(obj["chunks"], key=lambda c: c["index"]):
        data = await get_chunk_with_integrity(chunk_info)
        if data is None:
            raise HTTPException(
                503,
                f"Chunk {chunk_info['chunk_id']} is unavailable or corrupted on all replicas",
            )
        assembled.write(data)

    assembled.seek(0)
    return StreamingResponse(
        assembled,
        media_type=obj["content_type"],
        headers={"Content-Disposition": f'attachment; filename="{obj["name"]}"'},
    )


# ──────────────────────────────────────────────────────────────────────────────
# Client REST API — Metadata
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/v1/objects/{object_id}/metadata")
async def get_metadata(object_id: str):
    obj = objects.get(object_id)
    if not obj:
        raise HTTPException(404, "Object not found")

    return {
        "object_id": obj["object_id"],
        "name": obj["name"],
        "size": obj["size"],
        "content_type": obj["content_type"],
        "created_at": obj["created_at"],
        "last_modified": obj["last_modified"],
        "checksum": obj["checksum"],
        "num_chunks": obj["num_chunks"],
        "chunk_locations": [
            {
                "chunk_id": c["chunk_id"],
                "index": c["index"],
                "size": c["size"],
                "checksum": c["checksum"],
                "workers": [l["worker_id"] for l in c["locations"]],
            }
            for c in obj["chunks"]
        ],
        "custom_metadata": obj["custom_metadata"],
    }


@app.patch("/api/v1/objects/{object_id}/metadata")
async def update_metadata(object_id: str, request: Request):
    obj = objects.get(object_id)
    if not obj:
        raise HTTPException(404, "Object not found")

    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(400, "Body must be a JSON object")

    obj["custom_metadata"].update(body)
    obj["last_modified"] = now_iso()

    return {
        "object_id": object_id,
        "custom_metadata": obj["custom_metadata"],
        "last_modified": obj["last_modified"],
    }


# ──────────────────────────────────────────────────────────────────────────────
# Client REST API — Delete
# ──────────────────────────────────────────────────────────────────────────────

@app.delete("/api/v1/objects/{object_id}", status_code=204)
async def delete_object(object_id: str, background_tasks: BackgroundTasks):
    obj = objects.pop(object_id, None)
    if not obj:
        raise HTTPException(404, "Object not found")

    async def _delete_chunks():
        for chunk_info in obj["chunks"]:
            chunk_id = chunk_info["chunk_id"]

            # Primary: delete from all recorded replica locations
            primary_worker_ids = {loc["worker_id"] for loc in chunk_info["locations"]}
            for loc in chunk_info["locations"]:
                worker = workers.get(loc["worker_id"])
                if worker:
                    await worker_delete_chunk(worker["url"], chunk_id)

            # Broadcast: also send best-effort DELETE to any OTHER alive worker
            # that is NOT already in chunk_locations, to sweep out stale copies
            # left behind from previous re-replication cycles.
            for wid, w in workers.items():
                if wid in primary_worker_ids:
                    continue  # already handled above
                if time.time() - w["last_heartbeat"] >= HEARTBEAT_TIMEOUT:
                    continue  # skip dead workers
                await worker_delete_chunk(w["url"], chunk_id)
                # 404 responses are already treated as success in worker_delete_chunk

    background_tasks.add_task(_delete_chunks)
    return Response(status_code=204)


# ──────────────────────────────────────────────────────────────────────────────
# Client REST API — List
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/v1/objects")
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


# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "workers": {
            wid: {
                "url": w["url"],
                "alive": time.time() - w["last_heartbeat"] < HEARTBEAT_TIMEOUT,
                "last_heartbeat_ago_s": round(time.time() - w["last_heartbeat"], 1),
                "chunk_count": w.get("chunk_count", 0),
            }
            for wid, w in sorted(workers.items())   # alphabetical: worker1→worker4
        },
        "object_count": len(objects),
    }
