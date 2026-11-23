"""
tests/integration/test_api.py
Full API integration tests: health, registration, metadata, delete, list.
"""
import json
import time

import pytest

from controller.app.api.objects import objects
from controller.app.cluster import registry
from tests.conftest import add_workers, make_bytes


class TestHealth:
    def test_health_no_workers(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["workers"] == {}

    def test_health_shows_all_workers(self, client):
        add_workers(4)
        r = client.get("/health")
        assert len(r.json()["workers"]) == 4

    def test_health_workers_sorted_alphabetically(self, client):
        for i in [4, 2, 3, 1]:
            registry.workers[f"worker{i}"] = {
                "url": f"http://worker{i}:8001",
                "last_heartbeat": time.time(),
                "chunk_count": 0,
            }
        keys = list(client.get("/health").json()["workers"].keys())
        assert keys == sorted(keys)

    def test_health_dead_worker_flagged(self, client):
        add_workers(2)
        registry.workers["worker2"]["last_heartbeat"] = time.time() - 9999
        body = client.get("/health").json()
        assert body["workers"]["worker1"]["alive"] is True
        assert body["workers"]["worker2"]["alive"] is False

    def test_responses_are_pretty_json(self, client):
        r = client.get("/health")
        assert "\n" in r.text


class TestWorkerRegistration:
    def test_register_success(self, client):
        r = client.post("/internal/workers/register",
                        json={"worker_id": "worker1", "url": "http://worker1:8001"})
        assert r.status_code == 200
        assert "worker1" in registry.workers

    def test_register_missing_fields(self, client):
        r = client.post("/internal/workers/register", json={"worker_id": "w1"})
        assert r.status_code == 400
        assert "error" in r.json()

    def test_heartbeat_updates_timestamp(self, client):
        add_workers(1)
        old_ts = registry.workers["worker1"]["last_heartbeat"]
        time.sleep(0.05)
        r = client.post("/internal/workers/heartbeat",
                        json={"worker_id": "worker1", "chunk_count": 3})
        assert r.status_code == 200
        assert registry.workers["worker1"]["last_heartbeat"] > old_ts
        assert registry.workers["worker1"]["chunk_count"] == 3

    def test_heartbeat_unknown_worker_404(self, client):
        r = client.post("/internal/workers/heartbeat",
                        json={"worker_id": "ghost", "chunk_count": 0})
        assert r.status_code == 404
        assert "\n" in r.text  # pretty JSON


class TestMetadata:
    def _upload(self, client, custom_meta=None):
        add_workers(4)
        data = make_bytes(200)
        form = {}
        if custom_meta:
            form["metadata"] = json.dumps(custom_meta)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")},
                        data=form)
        assert r.status_code == 201
        return r.json()["object_id"]

    def test_get_metadata_has_all_fields(self, client):
        obj_id = self._upload(client, custom_meta={"k": "v"})
        body = client.get(f"/api/v1/objects/{obj_id}/metadata").json()
        for f in ["object_id", "name", "size", "content_type", "created_at",
                  "last_modified", "checksum", "num_chunks", "chunk_locations", "custom_metadata"]:
            assert f in body

    def test_patch_metadata_merges_keys(self, client):
        obj_id = self._upload(client, custom_meta={"v": "1"})
        r = client.patch(f"/api/v1/objects/{obj_id}/metadata", json={"v": "2", "new": "hi"})
        assert r.status_code == 200
        cm = r.json()["custom_metadata"]
        assert cm["v"] == "2"
        assert cm["new"] == "hi"

    def test_patch_does_not_touch_chunk_store(self, client, monkeypatch):
        from tests.conftest import fake_store
        obj_id = self._upload(client)
        before = dict(fake_store().store)
        client.patch(f"/api/v1/objects/{obj_id}/metadata", json={"x": "y"})
        assert fake_store().store == before

    def test_patch_404_unknown(self, client):
        r = client.patch("/api/v1/objects/bad-id/metadata", json={"k": "v"})
        assert r.status_code == 404


class TestDeleteList:
    def test_delete_returns_204(self, client):
        add_workers(4)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", make_bytes(100), "application/octet-stream")})
        obj_id = r.json()["object_id"]
        assert client.delete(f"/api/v1/objects/{obj_id}").status_code == 204
        assert obj_id not in objects

    def test_double_delete_returns_404(self, client):
        add_workers(4)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", make_bytes(100), "application/octet-stream")})
        obj_id = r.json()["object_id"]
        client.delete(f"/api/v1/objects/{obj_id}")
        assert client.delete(f"/api/v1/objects/{obj_id}").status_code == 404

    def test_list_pagination(self, client):
        add_workers(4)
        for i in range(5):
            client.post("/api/v1/objects",
                        files={"file": (f"f{i}.bin", make_bytes(50), "application/octet-stream")})
        body = client.get("/api/v1/objects?limit=2&offset=0").json()
        assert body["total"] == 5
        assert len(body["objects"]) == 2
