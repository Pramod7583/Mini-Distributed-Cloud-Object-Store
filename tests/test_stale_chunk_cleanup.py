"""
tests/test_stale_chunk_cleanup.py
==================================
Tests for orphaned/stale chunk cleanup during:
  1. Worker re-registration reconciliation (POST /internal/workers/register)
  2. Object deletion broadcast (DELETE /api/v1/objects/{id})

Run with:
    pytest tests/test_stale_chunk_cleanup.py -v
"""

import asyncio
import hashlib
import json
import math
import time
from typing import Dict

import pytest
from fastapi.testclient import TestClient

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "controller"))
import main as ctrl
from main import app, objects, workers, CHUNK_SIZE, REPLICATION_FACTOR

# ─── Global fake chunk store (shared across all workers in tests) ─────────────

class PerWorkerStore:
    """Simulates independent per-worker disk storage."""
    def __init__(self):
        # Maps worker_url -> {chunk_id: bytes}
        self.disks: Dict[str, Dict[str, bytes]] = {}

    def reset(self):
        self.disks.clear()

    def put(self, url: str, chunk_id: str, data: bytes) -> bool:
        self.disks.setdefault(url, {})[chunk_id] = data
        return True

    def get(self, url: str, chunk_id: str):
        return self.disks.get(url, {}).get(chunk_id)

    def delete(self, url: str, chunk_id: str) -> bool:
        self.disks.get(url, {}).pop(chunk_id, None)
        return True

    def chunks_on(self, url: str):
        return set(self.disks.get(url, {}).keys())

    def inject(self, url: str, chunk_id: str, data: bytes = b"stale"):
        """Directly inject a chunk onto a worker's disk (simulates leftover on restart)."""
        self.disks.setdefault(url, {})[chunk_id] = data


_store = PerWorkerStore()


async def _fake_put(url, chunk_id, data, checksum):
    return _store.put(url, chunk_id, data)

async def _fake_get(url, chunk_id):
    return _store.get(url, chunk_id)

async def _fake_delete(url, chunk_id):
    return _store.delete(url, chunk_id)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_state():
    objects.clear()
    workers.clear()
    ctrl._rr_counter = 0
    _store.reset()
    yield
    objects.clear()
    workers.clear()
    ctrl._rr_counter = 0
    _store.reset()


@pytest.fixture(autouse=True)
def mock_io(monkeypatch):
    monkeypatch.setattr(ctrl, "worker_put_chunk", _fake_put)
    monkeypatch.setattr(ctrl, "worker_get_chunk", _fake_get)
    monkeypatch.setattr(ctrl, "worker_delete_chunk", _fake_delete)
    yield


client = TestClient(app)


def _add_workers(n: int = 4):
    for i in range(1, n + 1):
        wid = f"worker{i}"
        workers[wid] = {
            "url": f"http://worker{i}:8001",
            "last_heartbeat": time.time(),
            "chunk_count": 0,
        }


def _make_file(size: int) -> bytes:
    unit = b"ABCDEFGH12345678" * 64
    return (unit * math.ceil(size / len(unit)))[:size]


def _upload(size: int = 200) -> str:
    _add_workers(4)
    data = _make_file(size)
    r = client.post("/api/v1/objects",
                    files={"file": ("f.bin", data, "application/octet-stream")})
    assert r.status_code == 201
    return r.json()["object_id"]


# ═════════════════════════════════════════════════════════════════════════════
# 11. Worker re-registration reconciliation
# ═════════════════════════════════════════════════════════════════════════════

class TestWorkerRegistrationReconciliation:

    @pytest.mark.asyncio
    async def test_orphaned_chunk_deleted_on_registration(self):
        """
        Case A: a chunk that belongs to no active object must be deleted
        from the registering worker's disk.
        """
        _add_workers(4)
        worker2_url = "http://worker2:8001"

        # Inject a chunk that never belonged to any object
        _store.inject(worker2_url, "ghost_chunk_id_000")

        # Simulate worker2 re-registering and reporting the ghost chunk
        r = client.post("/internal/workers/register", json={
            "worker_id": "worker2",
            "url": worker2_url,
            "existing_chunks": ["ghost_chunk_id_000"],
        })
        assert r.status_code == 200

        # Give background task time to run
        await asyncio.sleep(0.1)

        # Orphaned chunk must be gone from worker2's disk
        assert "ghost_chunk_id_000" not in _store.chunks_on(worker2_url), \
            "Orphaned chunk was NOT deleted from worker2 during re-registration"

    @pytest.mark.asyncio
    async def test_excess_replica_deleted_on_registration(self):
        """
        Case B: chunk belongs to an active object, worker NOT in chunk_locations,
        and chunk already has >= RF healthy replicas → delete the excess copy.
        """
        # Upload with 4 workers so each chunk has exactly 2 replicas
        obj_id = _upload(200)
        chunk = objects[obj_id]["chunks"][0]
        chunk_id = chunk["chunk_id"]

        # chunk_locations has e.g. worker1 + worker2
        loc_worker_ids = {loc["worker_id"] for loc in chunk["locations"]}

        # Find a worker NOT in chunk_locations (e.g. worker3 or worker4)
        spare_id = next(wid for wid in ["worker1","worker2","worker3","worker4"]
                        if wid not in loc_worker_ids)
        spare_url = f"http://{spare_id}:8001"

        # Inject a stale copy onto the spare worker's disk (simulates leftover)
        _store.inject(spare_url, chunk_id, b"stale_copy_data")
        assert chunk_id in _store.chunks_on(spare_url)

        # Spare worker re-registers, reporting the stale chunk
        r = client.post("/internal/workers/register", json={
            "worker_id": spare_id,
            "url": spare_url,
            "existing_chunks": [chunk_id],
        })
        assert r.status_code == 200

        # Background reconcile runs
        await asyncio.sleep(0.1)

        # Stale excess copy must be purged
        assert chunk_id not in _store.chunks_on(spare_url), \
            f"Excess stale chunk was NOT deleted from {spare_id} during re-registration"

    @pytest.mark.asyncio
    async def test_under_replicated_chunk_reattached_on_registration(self):
        """
        Case C: chunk belongs to active object, worker NOT in chunk_locations,
        and chunk has < RF healthy replicas → re-attach worker to chunk_locations.
        """
        _add_workers(4)
        data = _make_file(200)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        chunk = objects[obj_id]["chunks"][0]
        chunk_id = chunk["chunk_id"]

        # Remove one location so chunk only has 1 healthy replica in metadata
        removed_loc = chunk["locations"].pop()
        removed_id = removed_loc["worker_id"]
        removed_url = removed_loc["url"]

        # Mark the removed worker dead so it doesn't count
        workers[removed_id]["last_heartbeat"] = time.time() - 9999

        assert len(chunk["locations"]) == 1

        # Another worker (the removed one) comes back and reports the chunk
        workers[removed_id]["last_heartbeat"] = time.time()  # it's alive again
        _store.inject(removed_url, chunk_id, b"valid_data")

        r2 = client.post("/internal/workers/register", json={
            "worker_id": removed_id,
            "url": removed_url,
            "existing_chunks": [chunk_id],
        })
        assert r2.status_code == 200
        await asyncio.sleep(0.1)

        # Chunk locations should have been re-extended to >= RF
        location_worker_ids = [loc["worker_id"] for loc in chunk["locations"]]
        assert removed_id in location_worker_ids, \
            f"{removed_id} was NOT re-attached to chunk_locations (Case C)"

    @pytest.mark.asyncio
    async def test_legitimate_chunk_untouched_on_registration(self):
        """
        Case D: chunk belongs to active object and worker IS already in
        chunk_locations → keep it, do NOT delete.
        """
        obj_id = _upload(200)
        chunk = objects[obj_id]["chunks"][0]
        chunk_id = chunk["chunk_id"]

        # Pick a worker already in locations
        legit_loc = chunk["locations"][0]
        legit_id = legit_loc["worker_id"]
        legit_url = legit_loc["url"]

        # Worker re-registers reporting its legitimate chunk
        r = client.post("/internal/workers/register", json={
            "worker_id": legit_id,
            "url": legit_url,
            "existing_chunks": [chunk_id],
        })
        assert r.status_code == 200
        await asyncio.sleep(0.1)

        # Chunk must still exist on disk
        assert chunk_id in _store.chunks_on(legit_url), \
            f"Legitimate chunk was wrongly deleted from {legit_id}"

    @pytest.mark.asyncio
    async def test_registration_without_existing_chunks_field(self):
        """
        Workers that don't send existing_chunks (e.g. fresh start)
        should still register successfully without errors.
        """
        r = client.post("/internal/workers/register", json={
            "worker_id": "worker1",
            "url": "http://worker1:8001",
            # No existing_chunks field
        })
        assert r.status_code == 200
        assert r.json()["status"] == "registered"
        assert "worker1" in workers

    @pytest.mark.asyncio
    async def test_multiple_orphaned_chunks_all_deleted(self):
        """All orphaned chunks in one registration call must all be deleted."""
        _add_workers(4)
        worker3_url = "http://worker3:8001"

        # Inject several orphaned chunks
        orphans = [f"orphan_{i}" for i in range(5)]
        for cid in orphans:
            _store.inject(worker3_url, cid)

        r = client.post("/internal/workers/register", json={
            "worker_id": "worker3",
            "url": worker3_url,
            "existing_chunks": orphans,
        })
        assert r.status_code == 200
        await asyncio.sleep(0.1)

        for cid in orphans:
            assert cid not in _store.chunks_on(worker3_url), \
                f"Orphan {cid} was not deleted from worker3"


# ═════════════════════════════════════════════════════════════════════════════
# 12. Object deletion broadcast
# ═════════════════════════════════════════════════════════════════════════════

class TestDeletionBroadcast:

    @pytest.mark.asyncio
    async def test_delete_removes_chunks_from_all_alive_workers(self):
        """
        After DELETE /api/v1/objects/{id}, chunks must be absent on ALL alive
        workers — not just those listed in chunk_locations.
        """
        obj_id = _upload(200)
        chunk = objects[obj_id]["chunks"][0]
        chunk_id = chunk["chunk_id"]

        # Determine which workers are NOT in chunk_locations
        loc_ids = {loc["worker_id"] for loc in chunk["locations"]}
        extra_id = next(wid for wid in ["worker1","worker2","worker3","worker4"]
                        if wid not in loc_ids)
        extra_url = f"http://{extra_id}:8001"

        # Inject a stale copy onto a worker that's NOT in chunk_locations
        _store.inject(extra_url, chunk_id, b"stale_from_rereplica")
        assert chunk_id in _store.chunks_on(extra_url)

        # Delete the object
        r = client.delete(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 204

        # Give background task time to broadcast deletes
        await asyncio.sleep(0.1)

        # The stale copy on the extra worker must also be gone
        assert chunk_id not in _store.chunks_on(extra_url), \
            f"Stale chunk was NOT deleted from {extra_id} during object deletion"

    @pytest.mark.asyncio
    async def test_delete_cleans_primary_locations(self):
        """
        Standard case: chunks in chunk_locations are deleted from their workers.
        """
        obj_id = _upload(200)
        chunk_infos = objects[obj_id]["chunks"]
        chunk_ids_and_urls = [
            (c["chunk_id"], loc["url"])
            for c in chunk_infos
            for loc in c["locations"]
        ]

        r = client.delete(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 204
        await asyncio.sleep(0.1)

        for cid, url in chunk_ids_and_urls:
            assert cid not in _store.chunks_on(url), \
                f"Primary chunk {cid} was not deleted from {url}"

    @pytest.mark.asyncio
    async def test_delete_does_not_error_on_404_from_worker(self):
        """
        Broadcast DELETE to workers that don't hold the chunk must not raise
        (worker_delete_chunk already treats 404 as success).
        """
        obj_id = _upload(200)
        # Don't inject any extra chunks — workers will return 404 for broadcast
        r = client.delete(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 204  # must not blow up

    @pytest.mark.asyncio
    async def test_delete_skips_dead_workers_in_broadcast(self):
        """
        Dead workers (heartbeat timed out) must NOT be contacted during broadcast.
        """
        obj_id = _upload(200)

        # Mark worker4 as dead
        workers["worker4"]["last_heartbeat"] = time.time() - 9999

        delete_calls = []
        orig_delete = ctrl.worker_delete_chunk

        async def tracking_delete(url, chunk_id):
            delete_calls.append(url)
            return await orig_delete(url, chunk_id)

        import main as ctrl_mod
        ctrl_mod.worker_delete_chunk = tracking_delete
        try:
            r = client.delete(f"/api/v1/objects/{obj_id}")
            assert r.status_code == 204
            await asyncio.sleep(0.1)
        finally:
            ctrl_mod.worker_delete_chunk = orig_delete

        # worker4 URL must NOT have been contacted
        assert "http://worker4:8001" not in delete_calls, \
            "Dead worker4 was incorrectly contacted during deletion broadcast"

    @pytest.mark.asyncio
    async def test_delete_multi_chunk_object_broadcasts_all_chunks(self):
        """
        For a multi-chunk object, every chunk must be broadcast-deleted
        from all alive workers.
        """
        obj_id = _upload(size=2 * CHUNK_SIZE + 100)
        all_chunk_ids = [c["chunk_id"] for c in objects[obj_id]["chunks"]]

        # Inject all chunks onto all 4 workers' disks as extra stale copies
        for wid in ["worker1", "worker2", "worker3", "worker4"]:
            w_url = f"http://{wid}:8001"
            for cid in all_chunk_ids:
                _store.inject(w_url, cid, b"stale")

        r = client.delete(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 204
        await asyncio.sleep(0.1)

        for wid in ["worker1", "worker2", "worker3", "worker4"]:
            w_url = f"http://{wid}:8001"
            remaining = _store.chunks_on(w_url) & set(all_chunk_ids)
            assert not remaining, \
                f"Worker {wid} still has chunks after deletion: {remaining}"
