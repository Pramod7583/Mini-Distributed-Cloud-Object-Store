"""
worker/app/storage/chunk_store.py
Disk-backed chunk storage using aiofiles.
"""
import hashlib
import logging
from pathlib import Path
from typing import List, Optional

import aiofiles

logger = logging.getLogger("worker.storage")


class ChunkStore:
    def __init__(self, storage_dir: Path):
        self.dir = storage_dir
        self.dir.mkdir(parents=True, exist_ok=True)

    def path(self, chunk_id: str) -> Path:
        return self.dir / f"{chunk_id}.chunk"

    def list_chunk_ids(self) -> List[str]:
        return [p.stem for p in self.dir.glob("*.chunk")]

    async def write(self, chunk_id: str, data: bytes, expected_checksum: str = "") -> None:
        if expected_checksum:
            actual = hashlib.sha256(data).hexdigest()
            if actual != expected_checksum:
                raise ValueError(
                    f"Checksum mismatch: expected {expected_checksum}, got {actual}"
                )
        async with aiofiles.open(self.path(chunk_id), "wb") as f:
            await f.write(data)
        logger.info("Stored chunk %s (%d bytes)", chunk_id, len(data))

    async def read(self, chunk_id: str) -> Optional[bytes]:
        p = self.path(chunk_id)
        if not p.exists():
            return None
        async with aiofiles.open(p, "rb") as f:
            return await f.read()

    def delete(self, chunk_id: str) -> bool:
        p = self.path(chunk_id)
        if p.exists():
            p.unlink()
            logger.info("Deleted chunk %s", chunk_id)
            return True
        return False
