"""
worker/app/api/chunks.py
Internal chunk CRUD endpoints.
"""
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from worker.app.storage.chunk_store import ChunkStore

router = APIRouter(prefix="/internal/chunks", tags=["chunks"])

# Store instance is injected via app.state in main.py
_store: ChunkStore | None = None


def set_store(store: ChunkStore) -> None:
    global _store
    _store = store


def get_store() -> ChunkStore:
    assert _store is not None, "ChunkStore not initialised"
    return _store


@router.put("/{chunk_id}", status_code=200)
async def store_chunk(chunk_id: str, request: Request):
    data = await request.body()
    expected = request.headers.get("X-Chunk-Checksum", "")
    try:
        await get_store().write(chunk_id, data, expected)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"chunk_id": chunk_id, "size": len(data), "status": "stored"}


@router.get("/{chunk_id}")
async def retrieve_chunk(chunk_id: str):
    data = await get_store().read(chunk_id)
    if data is None:
        raise HTTPException(404, f"Chunk {chunk_id} not found")
    return Response(content=data, media_type="application/octet-stream")


@router.delete("/{chunk_id}", status_code=200)
async def delete_chunk(chunk_id: str):
    deleted = get_store().delete(chunk_id)
    if deleted:
        return {"chunk_id": chunk_id, "status": "deleted"}
    raise HTTPException(404, f"Chunk {chunk_id} not found")
