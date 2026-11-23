"""
tests/data_integrity/test_integrity.py
Tests for SHA-256 corruption detection and self-healing.
"""
import pytest

from shared.constants import CHUNK_SIZE
from controller.app.api.objects import objects
from tests.conftest import add_workers, make_bytes, fake_store
import controller.app.services.worker_client as wc_module
import controller.app.integrity.repair as repair_module


class TestDataIntegrity:
    def test_corruption_detected_returns_503_when_all_bad(self, client):
        add_workers(4)
        data = make_bytes(CHUNK_SIZE // 2)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        chunk_id = objects[obj_id]["chunks"][0]["chunk_id"]
        fake_store().corrupt(chunk_id)
        r2 = client.get(f"/api/v1/objects/{obj_id}")
        assert r2.status_code in (200, 503)

    def test_self_heal_repairs_one_corrupted_replica(self, client, monkeypatch):
        add_workers(4)
        original = make_bytes(CHUNK_SIZE // 2)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", original, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        chunk = objects[obj_id]["chunks"][0]
        chunk_id = chunk["chunk_id"]
        expected_cs = chunk["checksum"]

        loc0_url = chunk["locations"][0]["url"]
        good = fake_store().store[chunk_id]
        repaired = {}

        async def selective_get(url, cid):
            if url == loc0_url and cid == chunk_id:
                return b"CORRUPTED" * 100
            return fake_store().store.get(cid)

        async def selective_put(url, cid, data, cs):
            if url == loc0_url and cid == chunk_id:
                repaired["done"] = True
                repaired["checksum"] = cs
            fake_store().store[cid] = data
            return True

        monkeypatch.setattr(repair_module, "get_chunk", selective_get)
        monkeypatch.setattr(repair_module, "put_chunk", selective_put)
        monkeypatch.setattr(wc_module, "get_chunk", selective_get)

        r2 = client.get(f"/api/v1/objects/{obj_id}")
        assert r2.status_code == 200
        assert r2.content == original
        assert repaired.get("done"), "Self-heal PUT was never called"
        assert repaired["checksum"] == expected_cs
