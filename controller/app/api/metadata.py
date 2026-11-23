"""
controller/app/api/metadata.py
Metadata GET and PATCH endpoints.
"""
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request

from controller.app.api.objects import objects

router = APIRouter(prefix="/api/v1/objects", tags=["metadata"])


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.get("/{object_id}/metadata")
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


@router.patch("/{object_id}/metadata")
async def update_metadata(object_id: str, request: Request):
    obj = objects.get(object_id)
    if not obj:
        raise HTTPException(404, "Object not found")

    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(400, "Body must be a JSON object")

    obj["custom_metadata"].update(body)
    obj["last_modified"] = _now()

    return {
        "object_id": object_id,
        "custom_metadata": obj["custom_metadata"],
        "last_modified": obj["last_modified"],
    }
