"""
tests/replication/test_reconciliation.py
Tests for replication restoration after worker failure/recovery.
"""
import time

import pytest

from shared.constants import REPLICATION_FACTOR
from controller.app.api.objects import objects
from controller.app.cluster import registry
from controller.app.scheduler import replication
from tests.conftest import add_workers, make_bytes


class TestReplication:
    @pytest.mark.asyncio
    async def test_metadata_not_lost_when_no_spare_worker(self, client):
        add_workers(2)
        data = make_bytes(200)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        chunk = objects[obj_id]["chunks"][0]

        # Kill one worker — only 1 alive
        registry.workers["worker2"]["last_heartbeat"] = time.time() - 9999

        await replication.restore_replication(objects)

        # Locations must NOT be truncated
        assert len(chunk["locations"]) >= 1

    @pytest.mark.asyncio
    async def test_restore_adds_replica_after_worker_recovery(self, client):
        add_workers(4)
        data = make_bytes(200)
        r = client.post("/api/v1/objects",
                        files={"file": ("f.bin", data, "application/octet-stream")})
        obj_id = r.json()["object_id"]
        chunk = objects[obj_id]["chunks"][0]

        dead_loc = chunk["locations"].pop()
        registry.workers[dead_loc["worker_id"]]["last_heartbeat"] = time.time() - 9999
        assert len(chunk["locations"]) == 1

        # Revive
        registry.workers[dead_loc["worker_id"]]["last_heartbeat"] = time.time()
        await replication.restore_replication(objects)

        assert len(chunk["locations"]) == REPLICATION_FACTOR
