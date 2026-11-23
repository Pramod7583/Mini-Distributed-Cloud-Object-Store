"""
tests/conftest.py
Shared fixtures: app TestClient, in-memory fake worker store, state reset.

Monkeypatching strategy
-----------------------
Python's `from module import name` creates a local binding in the importing
module at import time.  Patching only the *source* module (worker_client) has
no effect on those already-bound names.  We must therefore patch the name
**where it is used** (the "where it's looked up" rule):

  Source module                            Local bindings to patch
  ────────────────────────────────────────────────────────────────
  controller.app.services.worker_client    put_chunk / get_chunk / delete_chunk
  controller.app.api.objects               put_chunk  /             delete_chunk
  controller.app.integrity.repair          put_chunk  / get_chunk
  controller.app.scheduler.replication     put_chunk

All four locations are patched in mock_worker_io so that every code path that
issues real HTTP calls to worker nodes is intercepted by the in-memory store,
regardless of which test module is being executed.
"""
import math
import time

import pytest
from fastapi.testclient import TestClient

# Add project root to sys.path so packages resolve
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from controller.app.main import app
from controller.app.api.objects import objects
from controller.app.cluster import registry

# Import every module that holds a local binding to a worker-HTTP function.
import controller.app.services.worker_client as wc_module
import controller.app.api.objects as objects_module
import controller.app.integrity.repair as repair_module
import controller.app.scheduler.replication as replication_module


# ── Fake in-process worker store ──────────────────────────────────────────────

class FakeWorkerStore:
    def __init__(self):
        self.store: dict = {}

    def put(self, chunk_id, data):
        self.store[chunk_id] = data
        return True

    def get(self, chunk_id):
        return self.store.get(chunk_id)

    def delete(self, chunk_id):
        self.store.pop(chunk_id, None)
        return True

    def corrupt(self, chunk_id):
        if chunk_id in self.store:
            self.store[chunk_id] = b"CORRUPTED_DATA" * 50


_fake_store = FakeWorkerStore()


async def _fake_put(url, chunk_id, data, checksum):
    return _fake_store.put(chunk_id, data)


async def _fake_get(url, chunk_id):
    return _fake_store.get(chunk_id)


async def _fake_delete(url, chunk_id):
    return _fake_store.delete(chunk_id)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_state():
    objects.clear()
    registry.workers.clear()
    registry._rr_counter = 0
    _fake_store.store.clear()
    yield
    objects.clear()
    registry.workers.clear()
    registry._rr_counter = 0
    _fake_store.store.clear()


@pytest.fixture(autouse=True)
def mock_worker_io(monkeypatch):
    """
    Patch worker HTTP calls at every location where the name is bound.

    Rule: patch the module that *uses* the function (where the name is looked
    up at call time), not just the module that *defines* it.
    """
    # 1. Source module — covers any call that goes through the full dotted path
    monkeypatch.setattr(wc_module, "put_chunk",    _fake_put)
    monkeypatch.setattr(wc_module, "get_chunk",    _fake_get)
    monkeypatch.setattr(wc_module, "delete_chunk", _fake_delete)

    # 2. controller.app.api.objects
    #    `from controller.app.services.worker_client import delete_chunk, put_chunk`
    monkeypatch.setattr(objects_module, "put_chunk",    _fake_put)
    monkeypatch.setattr(objects_module, "delete_chunk", _fake_delete)

    # 3. controller.app.integrity.repair
    #    `from controller.app.services.worker_client import get_chunk, put_chunk`
    monkeypatch.setattr(repair_module, "get_chunk", _fake_get)
    monkeypatch.setattr(repair_module, "put_chunk", _fake_put)

    # 4. controller.app.scheduler.replication
    #    `from controller.app.services.worker_client import put_chunk`
    monkeypatch.setattr(replication_module, "put_chunk", _fake_put)

    yield


# ── Helpers ───────────────────────────────────────────────────────────────────

def add_workers(n: int = 4):
    for i in range(1, n + 1):
        wid = f"worker{i}"
        registry.workers[wid] = {
            "url": f"http://worker{i}:8001",
            "last_heartbeat": time.time(),
            "chunk_count": 0,
        }


def make_bytes(size: int, content: bytes = b"X") -> bytes:
    unit = content * 1024
    return (unit * math.ceil(size / len(unit)))[:size]


def fake_store() -> FakeWorkerStore:
    return _fake_store
