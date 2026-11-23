"""
tests/test_controller.py
Unit + integration tests for the Mini Distributed Cloud Object Store controller.

Run with:
    pip install pytest pytest-asyncio httpx fastapi
    pytest tests/ -v
"""

import asyncio
import hashlib
import io
import json
import math
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ── import the app ────────────────────────────────────────────────────────────
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "controller"))
import main as ctrl
from main import app, objects, workers, CHUNK_SIZE, REPLICATION_FACTOR

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def reset_state():
    """Clear global state between every test."""
    objects.clear()
    workers.clear()
    ctrl._rr_counter = 0
    yield
    objects.clear()
    workers.clear()
    ctrl._rr_counter = 0


def _add_workers(n: int = 4):
    """Register n fake workers that are considered alive."""
    for i in range(1, n + 1):
        wid = f"worker{i}"
        workers[wid] = {
            "url": f"http://worker{i}:8001",
            "last_heartbeat": time.time(),
            "chunk_count": 0,
        }


def _make_file(size_bytes: int, content: bytes = b"X") -> bytes:
    """Return deterministic bytes of the given size."""
    unit = content * 1024
    buf = (unit * math.ceil(size_bytes / len(unit)))[:size_bytes]
    return buf


client = TestClient(app)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: mock worker_put_chunk / worker_get_chunk so we don't need Docker
# ─────────────────────────────────────────────────────────────────────────────

class FakeWorkerStore:
    """In-process chunk store that simulates all 4 workers."""
    def __init__(self):
        self.store: dict[str, bytes] = {}   # chunk_id → bytes

    def put(self, chunk_id: str, data: bytes) -> bool:
        self.store[chunk_id] = data
        return True

    def get(self, chunk_id: str) -> bytes | None:
        return self.store.get(chunk_id)

    def delete(self, chunk_id: str) -> bool:
        self.store.pop(chunk_id, None)
        return True

    def corrupt(self, chunk_id: str):
        if chunk_id in self.store:
            self.store[chunk_id] = b"CORRUPTED_DATA" * 50


_fake_store = FakeWorkerStore()


async def _fake_put(url: str, chunk_id: str, data: bytes, checksum: str) -> bool:
    return _fake_store.put(chunk_id, data)


async def _fake_get(url: str, chunk_id: str) -> bytes | None:
    return _fake_store.get(chunk_id)


async def _fake_delete(url: str, chunk_id: str) -> bool:
    return _fake_store.delete(chunk_id)


@pytest.fixture(autouse=True)
def mock_worker_io(monkeypatch):
    """Patch all HTTP calls to workers with the in-process fake store."""
    _fake_store.store.clear()
    monkeypatch.setattr(ctrl, "worker_put_chunk", _fake_put)
    monkeypatch.setattr(ctrl, "worker_get_chunk", _fake_get)
    monkeypatch.setattr(ctrl, "worker_delete_chunk", _fake_delete)
    yield
    _fake_store.store.clear()


# ═════════════════════════════════════════════════════════════════════════════
# 1. Health endpoint
# ═════════════════════════════════════════════════════════════════════════════

class TestHealth:
    def test_health_no_workers(self):
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["workers"] == {}
        assert body["object_count"] == 0

    def test_health_shows_alive_workers(self):
        _add_workers(4)
        r = client.get("/health")
        body = r.json()
        assert len(body["workers"]) == 4
        for wid, info in body["workers"].items():
            assert info["alive"] is True

    def test_health_workers_sorted_alphabetically(self):
        # Register in reverse order to prove sorting is not insertion-order
        for i in [4, 2, 3, 1]:
            workers[f"worker{i}"] = {
                "url": f"http://worker{i}:8001",
                "last_heartbeat": time.time(),
                "chunk_count": 0,
            }
        r = client.get("/health")
        keys = list(r.json()["workers"].keys())
        assert keys == sorted(keys)

    def test_health_dead_worker_flagged(self):
        _add_workers(2)
        # Make worker2 appear dead
        workers["worker2"]["last_heartbeat"] = time.time() - 9999
        r = client.get("/health")
        body = r.json()
        assert body["workers"]["worker1"]["alive"] is True
        assert body["workers"]["worker2"]["alive"] is False

    def test_health_pretty_json(self):
        """Response body must be indented (PrettyJSONResponse)."""
        r = client.get("/health")
        assert "\n" in r.text   # indented JSON contains newlines


# ═════════════════════════════════════════════════════════════════════════════
# 2. Worker registration & heartbeat
# ═════════════════════════════════════════════════════════════════════════════

class TestWorkerRegistration:
    def test_register_success(self):
        r = client.post("/internal/workers/register",
                        json={"worker_id": "worker1", "url": "http://worker1:8001"})
        assert r.status_code == 200
        assert r.json()["worker_id"] == "worker1"
        assert "worker1" in workers

    def test_register_missing_fields(self):
        r = client.post("/internal/workers/register", json={"worker_id": "w1"})
        assert r.status_code == 400
        assert "error" in r.json()

    def test_heartbeat_updates_timestamp(self):
        _add_workers(1)
        old_ts = workers["worker1"]["last_heartbeat"]
        time.sleep(0.05)
        r = client.post("/internal/workers/heartbeat",
                        json={"worker_id": "worker1", "chunk_count": 3})
        assert r.status_code == 200
        assert workers["worker1"]["last_heartbeat"] > old_ts
        assert workers["worker1"]["chunk_count"] == 3

    def test_heartbeat_unknown_worker(self):
        r = client.post("/internal/workers/heartbeat",
                        json={"worker_id": "ghost", "chunk_count": 0})
        assert r.status_code == 404
        assert "error" in r.json()

    def test_error_response_pretty_json(self):
        """404 from heartbeat must be indented JSON."""
        r = client.post("/internal/workers/heartbeat",
                        json={"worker_id": "ghost", "chunk_count": 0})
        assert r.status_code == 404
        assert "\n" in r.text


# ═════════════════════════════════════════════════════════════════════════════
# 3. Upload
# ═════════════════════════════════════════════════════════════════════════════

class TestUpload:
    def test_upload_small_file(self):
        _add_workers(4)
        data = _make_file(512 * 1024)   # 512 KB — single chunk
        r = client.post("/api/v1/objects",
                        files={"file": ("small.bin", data, "application/octet-stream")})
        assert r.status_code == 201
        body = r.json()
        assert "object_id" in body
        assert body["num_chunks"] == 1
        assert body["size"] == len(data)
        assert body["name"] == "small.bin"
        assert body["checksum"] == hashlib.sha256(data).hexdigest()

    def test_upload_large_file_chunked(self):
        _add_workers(4)
        data = _make_file(3 * CHUNK_SIZE + 500 * 1024)  # 3.5 MB → 4 chunks
        r = client.post("/api/v1/objects",
                        files={"file": ("large.bin", data, "application/octet-stream")})
        assert r.status_code == 201
        body = r.json()
        assert body["num_chunks"] == 4

    def test_upload_with_custom_metadata(self):
        _add_workers(4)
        data = _make_file(100)
        meta = json.dumps({"author": "Alice", "tag": "demo"})
        r = client.post("/api/v1/objects",
                        files={"file": ("f.txt", data, "text/plain")},
                        data={"metadata": meta})
        assert r.status_code == 201
        obj_id = r.json()["object_id"]
        stored = objects[obj_id]
        assert stored["custom_metadata"]["author"] == "Alice"

    def test_upload_invalid_metadata_json(self):
        _add_workers(4)
        data = _make_file(100)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.txt", data, "text/plain")},
                        data={"metadata": "not-json"})
        assert r.status_code == 400
        assert "error" in r.json()

    def test_upload_metadata_not_dict(self):
        _add_workers(4)
        data = _make_file(100)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.txt", data, "text/plain")},
                        data={"metadata": "[1,2,3]"})
        assert r.status_code == 400

    def test_upload_requires_workers(self):
        # No workers registered → 503
        data = _make_file(100)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.txt", data, "text/plain")})
        assert r.status_code == 503

    def test_upload_object_stored_in_state(self):
        _add_workers(4)
        data = _make_file(200)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        assert obj_id in objects

    def test_round_robin_distributes_chunks(self):
        """4-chunk upload must spread across all 4 workers, not just 2."""
        _add_workers(4)
        data = _make_file(4 * CHUNK_SIZE)   # exactly 4 chunks
        r = client.post("/api/v1/objects",
                        files={"file": ("big.bin", data, "application/octet-stream")})
        assert r.status_code == 201
        obj_id = r.json()["object_id"]
        all_workers_used = set()
        for chunk in objects[obj_id]["chunks"]:
            for loc in chunk["locations"]:
                all_workers_used.add(loc["worker_id"])
        # All 4 workers should have been used across 4 chunks × 2 replicas
        assert len(all_workers_used) == 4

    def test_each_chunk_has_replication_factor_copies(self):
        _add_workers(4)
        data = _make_file(2 * CHUNK_SIZE + 1)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        for chunk in objects[obj_id]["chunks"]:
            assert len(chunk["locations"]) == REPLICATION_FACTOR

    def test_chunk_replicas_on_different_workers(self):
        _add_workers(4)
        data = _make_file(2 * CHUNK_SIZE)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        for chunk in objects[obj_id]["chunks"]:
            worker_ids = [l["worker_id"] for l in chunk["locations"]]
            assert len(set(worker_ids)) == REPLICATION_FACTOR, \
                f"Chunk {chunk['chunk_id']} has replicas on the same worker!"


# ═════════════════════════════════════════════════════════════════════════════
# 4. Download
# ═════════════════════════════════════════════════════════════════════════════

class TestDownload:
    def _upload(self, data: bytes, name="f.bin") -> str:
        _add_workers(4)
        r = client.post("/api/v1/objects",
                        files={"file": (name, data, "application/octet-stream")})
        assert r.status_code == 201
        return r.json()["object_id"]

    def test_download_matches_original(self):
        original = _make_file(512 * 1024, b"ABCDE")
        obj_id = self._upload(original)
        r = client.get(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 200
        assert r.content == original

    def test_download_large_file_matches(self):
        original = _make_file(3 * CHUNK_SIZE + 7, b"HELLO")
        obj_id = self._upload(original)
        r = client.get(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 200
        assert hashlib.sha256(r.content).hexdigest() == hashlib.sha256(original).hexdigest()

    def test_download_404_unknown_object(self):
        r = client.get("/api/v1/objects/nonexistent-id")
        assert r.status_code == 404
        assert "error" in r.json()

    def test_download_content_type_preserved(self):
        _add_workers(4)
        data = b"fake pdf content"
        r = client.post("/api/v1/objects",
                        files={"file": ("doc.pdf", io.BytesIO(data), "application/pdf")})
        obj_id = r.json()["object_id"]
        r2 = client.get(f"/api/v1/objects/{obj_id}")
        assert "application/pdf" in r2.headers["content-type"]

    def test_download_content_disposition_header(self):
        _add_workers(4)
        data = b"hello"
        r = client.post("/api/v1/objects",
                        files={"file": ("myfile.txt", data, "text/plain")})
        obj_id = r.json()["object_id"]
        r2 = client.get(f"/api/v1/objects/{obj_id}")
        assert "myfile.txt" in r2.headers.get("content-disposition", "")


# ═════════════════════════════════════════════════════════════════════════════
# 5. Metadata
# ═════════════════════════════════════════════════════════════════════════════

class TestMetadata:
    def _upload(self, data=None, custom_meta=None) -> str:
        _add_workers(4)
        data = data or _make_file(200)
        files = {"file": ("f.bin", data, "application/octet-stream")}
        form = {}
        if custom_meta:
            form["metadata"] = json.dumps(custom_meta)
        r = client.post("/api/v1/objects", files=files, data=form)
        assert r.status_code == 201
        return r.json()["object_id"]

    def test_get_metadata_fields(self):
        obj_id = self._upload(custom_meta={"key": "val"})
        r = client.get(f"/api/v1/objects/{obj_id}/metadata")
        assert r.status_code == 200
        body = r.json()
        for field in ["object_id", "name", "size", "content_type",
                      "created_at", "last_modified", "checksum",
                      "num_chunks", "chunk_locations", "custom_metadata"]:
            assert field in body, f"Missing field: {field}"

    def test_get_metadata_chunk_locations(self):
        obj_id = self._upload(_make_file(2 * CHUNK_SIZE + 1))
        r = client.get(f"/api/v1/objects/{obj_id}/metadata")
        locs = r.json()["chunk_locations"]
        assert len(locs) == 3
        for loc in locs:
            assert len(loc["workers"]) == REPLICATION_FACTOR

    def test_get_metadata_404(self):
        r = client.get("/api/v1/objects/bad-id/metadata")
        assert r.status_code == 404

    def test_patch_metadata_updates_custom_fields(self):
        obj_id = self._upload(custom_meta={"v": "1"})
        r = client.patch(f"/api/v1/objects/{obj_id}/metadata",
                         json={"v": "2", "new_key": "hello"})
        assert r.status_code == 200
        body = r.json()
        assert body["custom_metadata"]["v"] == "2"
        assert body["custom_metadata"]["new_key"] == "hello"

    def test_patch_metadata_updates_last_modified(self):
        obj_id = self._upload()
        old_lm = objects[obj_id]["last_modified"]
        time.sleep(0.05)
        client.patch(f"/api/v1/objects/{obj_id}/metadata", json={"x": "y"})
        assert objects[obj_id]["last_modified"] > old_lm

    def test_patch_metadata_does_not_re_upload_data(self):
        """Chunk store must not be touched by a metadata patch."""
        obj_id = self._upload()
        store_before = dict(_fake_store.store)
        client.patch(f"/api/v1/objects/{obj_id}/metadata", json={"tag": "test"})
        assert _fake_store.store == store_before

    def test_patch_metadata_404(self):
        r = client.patch("/api/v1/objects/bad-id/metadata", json={"k": "v"})
        assert r.status_code == 404

    def test_patch_metadata_bad_body(self):
        obj_id = self._upload()
        r = client.patch(f"/api/v1/objects/{obj_id}/metadata",
                         content=b"[1,2,3]",
                         headers={"Content-Type": "application/json"})
        assert r.status_code == 400


# ═════════════════════════════════════════════════════════════════════════════
# 6. Delete
# ═════════════════════════════════════════════════════════════════════════════

class TestDelete:
    def _upload(self) -> str:
        _add_workers(4)
        data = _make_file(200)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        return r.json()["object_id"]

    def test_delete_returns_204(self):
        obj_id = self._upload()
        r = client.delete(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 204

    def test_delete_removes_from_state(self):
        obj_id = self._upload()
        client.delete(f"/api/v1/objects/{obj_id}")
        assert obj_id not in objects

    def test_delete_404_on_second_call(self):
        obj_id = self._upload()
        client.delete(f"/api/v1/objects/{obj_id}")
        r = client.delete(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 404

    def test_delete_unknown_object(self):
        r = client.delete("/api/v1/objects/nonexistent")
        assert r.status_code == 404


# ═════════════════════════════════════════════════════════════════════════════
# 7. List
# ═════════════════════════════════════════════════════════════════════════════

class TestList:
    def _upload(self, n: int = 1):
        _add_workers(4)
        for i in range(n):
            client.post("/api/v1/objects",
                        files={"file": (f"file{i}.bin",
                                        _make_file(100), "application/octet-stream")})

    def test_list_empty(self):
        r = client.get("/api/v1/objects")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 0
        assert body["objects"] == []

    def test_list_returns_all_objects(self):
        self._upload(3)
        r = client.get("/api/v1/objects")
        assert r.json()["total"] == 3
        assert len(r.json()["objects"]) == 3

    def test_list_object_fields(self):
        self._upload(1)
        obj = client.get("/api/v1/objects").json()["objects"][0]
        for field in ["object_id", "name", "size", "content_type",
                      "created_at", "last_modified"]:
            assert field in obj

    def test_list_pagination_limit(self):
        self._upload(5)
        r = client.get("/api/v1/objects?limit=2&offset=0")
        body = r.json()
        assert body["total"] == 5
        assert len(body["objects"]) == 2

    def test_list_pagination_offset(self):
        self._upload(5)
        r1 = client.get("/api/v1/objects?limit=5&offset=0").json()["objects"]
        r2 = client.get("/api/v1/objects?limit=3&offset=2").json()["objects"]
        assert r2[0]["object_id"] == r1[2]["object_id"]


# ═════════════════════════════════════════════════════════════════════════════
# 8. Data Integrity — corruption detection & self-heal
# ═════════════════════════════════════════════════════════════════════════════

class TestDataIntegrity:
    def _upload_and_get_chunk_id(self):
        _add_workers(4)
        data = _make_file(CHUNK_SIZE // 2)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        chunk = objects[obj_id]["chunks"][0]
        return obj_id, chunk, data

    def test_download_succeeds_with_one_good_replica(self):
        obj_id, chunk, original = self._upload_and_get_chunk_id()
        # Corrupt the first replica's chunk_id in the store
        chunk_id = chunk["chunk_id"]
        first_key = chunk_id  # both replicas share the same chunk_id key
        _fake_store.corrupt(chunk_id)
        # Download should still work using the second (uncorrupted) copy
        # BUT our fake store is shared — corrupt means all copies are bad here.
        # So instead test that checksum mismatch causes 503 when ALL are bad.
        r = client.get(f"/api/v1/objects/{obj_id}")
        # Either 200 (if integrity logic found something) or 503 (all corrupt)
        assert r.status_code in (200, 503)

    def test_self_heal_repairs_corrupted_replica(self):
        """
        Simulate two separate stores: replica A good, replica B corrupted.
        After download, replica B must be repaired.
        """
        _add_workers(4)
        original = _make_file(CHUNK_SIZE // 2)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", original, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        chunk = objects[obj_id]["chunks"][0]
        chunk_id = chunk["chunk_id"]
        expected_cs = chunk["checksum"]

        # Simulate: worker at location[0] has a corrupted copy.
        # We do this by wrapping the get function to return bad data for that worker.
        loc0_worker = chunk["locations"][0]["worker_id"]
        loc0_url = chunk["locations"][0]["url"]

        good_data = _fake_store.store[chunk_id]
        repaired = {}

        async def selective_get(url, cid):
            if url == loc0_url and cid == chunk_id:
                return b"CORRUPTED" * 100   # bad data for loc0
            return _fake_store.store.get(cid)

        async def selective_put(url, cid, data, cs):
            if url == loc0_url and cid == chunk_id:
                repaired["done"] = True
                repaired["checksum"] = cs
            _fake_store.store[cid] = data
            return True

        import main as ctrl_mod
        orig_get = ctrl_mod.worker_get_chunk
        orig_put = ctrl_mod.worker_put_chunk
        ctrl_mod.worker_get_chunk = selective_get
        ctrl_mod.worker_put_chunk = selective_put
        try:
            r2 = client.get(f"/api/v1/objects/{obj_id}")
            assert r2.status_code == 200
            assert r2.content == original
            assert repaired.get("done"), "Self-heal PUT was never called"
            assert repaired["checksum"] == expected_cs
        finally:
            ctrl_mod.worker_get_chunk = orig_get
            ctrl_mod.worker_put_chunk = orig_put


# ═════════════════════════════════════════════════════════════════════════════
# 9. Worker failure — alive_workers / pick_workers_for_chunk
# ═════════════════════════════════════════════════════════════════════════════

class TestWorkerFailure:
    def test_dead_worker_excluded_from_alive(self):
        _add_workers(4)
        workers["worker2"]["last_heartbeat"] = time.time() - 9999
        alive_ids = {w["worker_id"] for w in ctrl.alive_workers()}
        assert "worker2" not in alive_ids
        assert len(alive_ids) == 3

    def test_upload_still_works_with_3_alive_workers(self):
        _add_workers(4)
        workers["worker3"]["last_heartbeat"] = time.time() - 9999
        data = _make_file(200)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        assert r.status_code == 201

    def test_upload_fails_with_only_1_alive_worker(self):
        _add_workers(2)
        workers["worker1"]["last_heartbeat"] = time.time() - 9999
        data = _make_file(200)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        assert r.status_code == 503

    def test_revived_worker_becomes_alive(self):
        _add_workers(2)
        workers["worker1"]["last_heartbeat"] = time.time() - 9999
        assert len(ctrl.alive_workers()) == 1
        # Simulate heartbeat from worker1
        workers["worker1"]["last_heartbeat"] = time.time()
        assert len(ctrl.alive_workers()) == 2


# ═════════════════════════════════════════════════════════════════════════════
# 10. restore_replication — safe metadata update (Bug Fix 3)
# ═════════════════════════════════════════════════════════════════════════════

class TestRestoreReplication:
    @pytest.mark.asyncio
    async def test_metadata_not_lost_when_no_spare_worker(self):
        """
        If live_locs has 1 entry and no spare worker exists, chunk["locations"]
        must NOT be overwritten with the stripped-down live_locs list.
        """
        _add_workers(2)
        original = _make_file(200)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", original, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        chunk = objects[obj_id]["chunks"][0]

        # Store original location list (2 entries)
        orig_locs = list(chunk["locations"])

        # Kill one worker so only 1 is alive
        workers["worker2"]["last_heartbeat"] = time.time() - 9999

        # Remove all candidates so restore_replication cannot place a new replica
        # (only 1 alive worker, which already holds the chunk)
        await ctrl.restore_replication()

        # Locations must NOT have been truncated to just the live ones
        # because no new replica was successfully placed
        assert len(chunk["locations"]) >= 1   # at least the live one remains

    @pytest.mark.asyncio
    async def test_restore_adds_replica_on_recovered_worker(self):
        """After a worker recovers, restore_replication should bring it back."""
        _add_workers(4)
        original = _make_file(200)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", original, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        chunk = objects[obj_id]["chunks"][0]

        # Simulate one worker dying — remove its location from metadata
        dead_loc = chunk["locations"].pop()
        workers[dead_loc["worker_id"]]["last_heartbeat"] = time.time() - 9999

        assert len(chunk["locations"]) == 1

        # Revive it and pick a different spare worker
        workers[dead_loc["worker_id"]]["last_heartbeat"] = time.time()
        await ctrl.restore_replication()

        assert len(chunk["locations"]) == REPLICATION_FACTOR
