"""
tests/upload/test_alternate_placement.py
Tests for upload: chunking, round-robin placement, replication factor.
"""
import hashlib
import json

import pytest

from shared.constants import CHUNK_SIZE, REPLICATION_FACTOR
from controller.app.api.objects import objects
from controller.app.cluster import registry
from tests.conftest import add_workers, make_bytes


class TestUpload:
    def test_upload_small_file(self, client):
        add_workers(4)
        data = make_bytes(512 * 1024)
        r = client.post("/api/v1/objects",
                        files={"file": ("small.bin", data, "application/octet-stream")})
        assert r.status_code == 201
        body = r.json()
        assert "object_id" in body
        assert body["num_chunks"] == 1
        assert body["size"] == len(data)
        assert body["checksum"] == hashlib.sha256(data).hexdigest()

    def test_upload_large_file_produces_multiple_chunks(self, client):
        add_workers(4)
        data = make_bytes(3 * CHUNK_SIZE + 500 * 1024)
        r = client.post("/api/v1/objects",
                        files={"file": ("large.bin", data, "application/octet-stream")})
        assert r.status_code == 201
        assert r.json()["num_chunks"] == 4

    def test_upload_with_custom_metadata(self, client):
        add_workers(4)
        data = make_bytes(100)
        meta = json.dumps({"author": "Alice", "tag": "demo"})
        r = client.post("/api/v1/objects",
                        files={"file": ("f.txt", data, "text/plain")},
                        data={"metadata": meta})
        assert r.status_code == 201
        stored = objects[r.json()["object_id"]]
        assert stored["custom_metadata"]["author"] == "Alice"

    def test_upload_invalid_metadata(self, client):
        add_workers(4)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.txt", make_bytes(10), "text/plain")},
                        data={"metadata": "not-json"})
        assert r.status_code == 400

    def test_upload_requires_alive_workers(self, client):
        r = client.post("/api/v1/objects",
                        files={"file": ("f.txt", make_bytes(10), "text/plain")})
        assert r.status_code == 503

    def test_round_robin_distributes_across_all_workers(self, client):
        add_workers(4)
        data = make_bytes(4 * CHUNK_SIZE)
        r = client.post("/api/v1/objects",
                        files={"file": ("big.bin", data, "application/octet-stream")})
        assert r.status_code == 201
        used = set()
        for chunk in objects[r.json()["object_id"]]["chunks"]:
            for loc in chunk["locations"]:
                used.add(loc["worker_id"])
        assert len(used) == 4

    def test_each_chunk_has_replication_factor_copies(self, client):
        add_workers(4)
        data = make_bytes(2 * CHUNK_SIZE + 1)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        for chunk in objects[r.json()["object_id"]]["chunks"]:
            assert len(chunk["locations"]) == REPLICATION_FACTOR

    def test_chunk_replicas_on_different_workers(self, client):
        add_workers(4)
        data = make_bytes(2 * CHUNK_SIZE)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        for chunk in objects[r.json()["object_id"]]["chunks"]:
            wids = [l["worker_id"] for l in chunk["locations"]]
            assert len(set(wids)) == REPLICATION_FACTOR
