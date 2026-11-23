"""
tests/failure_recovery/test_failure.py
Tests for worker failure detection and upload behaviour.
"""
import time

import pytest

from controller.app.cluster import registry
from tests.conftest import add_workers, make_bytes


class TestWorkerFailure:
    def test_dead_worker_excluded_from_alive(self):
        add_workers(4)
        registry.workers["worker2"]["last_heartbeat"] = time.time() - 9999
        alive_ids = {w["worker_id"] for w in registry.alive_workers()}
        assert "worker2" not in alive_ids
        assert len(alive_ids) == 3

    def test_upload_still_works_with_3_alive_workers(self, client):
        add_workers(4)
        registry.workers["worker3"]["last_heartbeat"] = time.time() - 9999
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", make_bytes(200), "application/octet-stream")})
        assert r.status_code == 201

    def test_upload_fails_with_only_1_alive_worker(self, client):
        add_workers(2)
        registry.workers["worker1"]["last_heartbeat"] = time.time() - 9999
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", make_bytes(200), "application/octet-stream")})
        assert r.status_code == 503

    def test_revived_worker_rejoins_alive_pool(self):
        add_workers(2)
        registry.workers["worker1"]["last_heartbeat"] = time.time() - 9999
        assert len(registry.alive_workers()) == 1
        registry.workers["worker1"]["last_heartbeat"] = time.time()
        assert len(registry.alive_workers()) == 2
