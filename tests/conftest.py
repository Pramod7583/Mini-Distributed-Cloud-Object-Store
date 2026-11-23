"""
tests/conftest.py
Shared fixtures: app TestClient, in-memory fake worker store, state reset.
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
import controller.app.services.worker_client as wc_module
import controller.app.integrity.repair as repair_module


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
    monkeypatch.setattr(wc_module, "put_chunk", _fake_put)
    monkeypatch.setattr(wc_module, "get_chunk", _fake_get)
    monkeypatch.setattr(wc_module, "delete_chunk", _fake_delete)
    # Also patch inside repair module (it imports worker_client functions)
    monkeypatch.setattr(repair_module, "put_chunk", _fake_put)
    monkeypatch.setattr(repair_module, "get_chunk", _fake_get)
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
