"""
shared/models.py
Lightweight plain-dict schemas used across the system.
No ORM — keeping it simple per project requirements.
"""
from typing import Any, Dict, List, Optional


def make_chunk_record(
    chunk_id: str,
    index: int,
    size: int,
    checksum: str,
    locations: List[Dict[str, str]],
) -> Dict[str, Any]:
    return {
        "chunk_id": chunk_id,
        "index": index,
        "size": size,
        "checksum": checksum,
        "locations": locations,  # [{"worker_id": ..., "url": ...}]
    }


def make_object_record(
    object_id: str,
    name: str,
    size: int,
    content_type: str,
    created_at: str,
    last_modified: str,
    checksum: str,
    num_chunks: int,
    chunks: List[Dict[str, Any]],
    custom_metadata: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    return {
        "object_id": object_id,
        "name": name,
        "size": size,
        "content_type": content_type,
        "created_at": created_at,
        "last_modified": last_modified,
        "checksum": checksum,
        "num_chunks": num_chunks,
        "chunks": chunks,
        "custom_metadata": custom_metadata or {},
    }
