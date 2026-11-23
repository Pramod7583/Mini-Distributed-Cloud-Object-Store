"""
tests/download/test_download.py
Tests for download endpoint.
"""
import hashlib
import io

import pytest

from shared.constants import CHUNK_SIZE
from tests.conftest import add_workers, make_bytes


def _upload(client, data: bytes, name="f.bin", ctype="application/octet-stream") -> str:
    add_workers(4)
    r = client.post("/api/v1/objects", files={"file": (name, data, ctype)})
    assert r.status_code == 201
    return r.json()["object_id"]


class TestDownload:
    def test_download_matches_original(self, client):
        original = make_bytes(512 * 1024, b"ABCDE")
        obj_id = _upload(client, original)
        r = client.get(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 200
        assert r.content == original

    def test_download_large_file_matches_sha256(self, client):
        original = make_bytes(3 * CHUNK_SIZE + 7, b"HELLO")
        obj_id = _upload(client, original)
        r = client.get(f"/api/v1/objects/{obj_id}")
        assert r.status_code == 200
        assert hashlib.sha256(r.content).hexdigest() == hashlib.sha256(original).hexdigest()

    def test_download_404_for_unknown_object(self, client):
        r = client.get("/api/v1/objects/nonexistent")
        assert r.status_code == 404
        assert "error" in r.json()

    def test_content_type_preserved(self, client):
        data = b"fake pdf"
        obj_id = _upload(client, data, "doc.pdf", "application/pdf")
        r = client.get(f"/api/v1/objects/{obj_id}")
        assert "application/pdf" in r.headers["content-type"]

    def test_content_disposition_contains_filename(self, client):
        obj_id = _upload(client, b"hello", "myfile.txt", "text/plain")
        r = client.get(f"/api/v1/objects/{obj_id}")
        assert "myfile.txt" in r.headers.get("content-disposition", "")
