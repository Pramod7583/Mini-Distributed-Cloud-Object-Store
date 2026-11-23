#!/usr/bin/env python3
"""
scripts/demo.py — Full 6-step demonstration of the Mini Distributed Cloud Object Store.

Run from the project root:
    python scripts/demo.py

Requirements (host machine):
    pip install requests
"""
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time

import requests

CONTROLLER = os.environ.get("CONTROLLER_URL", "http://localhost:8000")
STEP = 0


def step(title: str):
    global STEP
    STEP += 1
    print(f"\n{'='*70}")
    print(f"  STEP {STEP}: {title}")
    print("=" * 70)


def wait_for_controller(timeout=60):
    print(f"Waiting for controller at {CONTROLLER} ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{CONTROLLER}/health", timeout=3)
            if r.status_code == 200:
                h = r.json()
                alive = sum(1 for w in h["workers"].values() if w["alive"])
                if alive >= 2:
                    print(f"✅ Controller up, {alive} workers alive.\n")
                    return
                print(f"  ... {alive} workers alive, waiting for at least 2 ...")
        except Exception:
            pass
        time.sleep(2)
    print("❌ Timeout waiting for controller/workers.")
    sys.exit(1)


def show_health():
    r = requests.get(f"{CONTROLLER}/health")
    h = r.json()
    print(f"  Object count : {h['object_count']}")
    for wid, w in h["workers"].items():
        status = "🟢 ALIVE" if w["alive"] else "🔴 DEAD"
        print(f"  Worker {wid}: {status} | chunks={w['chunk_count']} | last_seen={w['last_heartbeat_ago_s']}s ago")


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def make_file(size_bytes: int, suffix=".bin") -> str:
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "wb") as f:
        written = 0
        chunk = b"ABCDEFGH12345678" * 64
        while written < size_bytes:
            to_write = min(len(chunk), size_bytes - written)
            f.write(chunk[:to_write])
            written += to_write
    return path


# ── Step 1: Upload + Download + Verify ───────────────────────────────────────

def demo_basic_storage():
    step("Object Storage — Upload, Download, Verify")
    src = make_file(512 * 1024, ".txt")
    orig_hash = sha256_file(src)
    print(f"  Created test file: {src} (512 KB), SHA256={orig_hash[:16]}...")

    with open(src, "rb") as f:
        resp = requests.post(f"{CONTROLLER}/api/v1/objects",
                             files={"file": ("test-512kb.txt", f, "text/plain")})
    assert resp.status_code == 201, f"Upload failed: {resp.text}"
    result = resp.json()
    obj_id = result["object_id"]
    print(f"  ✅ Uploaded. object_id = {obj_id}")
    print(f"     Size={result['size']} bytes, chunks={result['num_chunks']}")

    dst = src + ".downloaded"
    r = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}", stream=True)
    assert r.status_code == 200
    with open(dst, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

    dl_hash = sha256_file(dst)
    match = "✅ MATCH" if dl_hash == orig_hash else "❌ MISMATCH"
    print(f"  Original  SHA256: {orig_hash}")
    print(f"  Retrieved SHA256: {dl_hash}")
    print(f"  Integrity check: {match}")
    assert dl_hash == orig_hash
    os.unlink(src); os.unlink(dst)
    return obj_id


# ── Step 2: Chunking ──────────────────────────────────────────────────────────

def demo_chunking():
    step("Chunking — Upload >1 MB file, Inspect Chunk Distribution")
    src = make_file(3 * 1024 * 1024 + 500 * 1024, ".bin")
    print(f"  Created test file: {src} (3.5 MB)")
    with open(src, "rb") as f:
        resp = requests.post(f"{CONTROLLER}/api/v1/objects",
                             files={"file": ("large-file.bin", f, "application/octet-stream")})
    assert resp.status_code == 201
    result = resp.json()
    obj_id = result["object_id"]
    print(f"  ✅ Uploaded. object_id = {obj_id}  |  Chunks: {result['num_chunks']}")

    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    print(f"\n  Chunk distribution:")
    print(f"  {'Chunk':5} {'Size':>10}  Workers")
    for c in meta["chunk_locations"]:
        print(f"  [{c['index']:3}] {c['size']:>10,} B  → {', '.join(c['workers'])}")

    os.unlink(src)
    return obj_id


# ── Step 3: Replication ───────────────────────────────────────────────────────

def demo_replication(obj_id: str):
    step(f"Replication — Verify 2 Copies Per Chunk (object_id={obj_id[:8]}...)")
    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    all_ok = True
    for c in meta["chunk_locations"]:
        unique = len(set(c["workers"]))
        status = "✅" if unique >= 2 else "❌"
        if unique < 2: all_ok = False
        print(f"  Chunk [{c['index']}]: replicas={unique}  workers={c['workers']}  {status}")
    print("\n  ✅ All chunks replicated on distinct workers." if all_ok else "\n  ❌ Replication factor not met!")


# ── Step 4: Metadata ──────────────────────────────────────────────────────────

def demo_metadata():
    step("Metadata — Upload with Custom Metadata, Retrieve, Modify")
    src = make_file(100 * 1024, ".jpg")
    custom = json.dumps({"project": "cloud-demo", "author": "student", "version": "1"})
    with open(src, "rb") as f:
        resp = requests.post(f"{CONTROLLER}/api/v1/objects",
                             files={"file": ("photo.jpg", f, "image/jpeg")},
                             data={"metadata": custom})
    assert resp.status_code == 201
    obj_id = resp.json()["object_id"]
    print(f"  ✅ Uploaded with custom metadata. object_id={obj_id}")

    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    print(f"    custom: {meta['custom_metadata']}")

    update = {"version": "2", "reviewed": "true", "rating": "5"}
    resp2 = requests.patch(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata", json=update)
    assert resp2.status_code == 200
    updated = resp2.json()
    print(f"  After PATCH: {updated['custom_metadata']}")
    print(f"  ✅ Metadata updated without re-uploading data.")
    os.unlink(src)
    return obj_id


# ── Step 5: Worker Failure ────────────────────────────────────────────────────

def demo_worker_failure(obj_id: str):
    step("Worker Failure — Stop worker2, Retrieve Object, Restart, Check Replication")
    print("  [Before failure] System health:")
    show_health()

    print("\n  → Stopping worker2 container ...")
    result = subprocess.run(["docker", "stop", "worker2"], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ⚠️  Could not stop worker2: {result.stderr.strip()}")
        return
    print("  ✅ worker2 stopped. Waiting 20s ...")
    time.sleep(20)

    print("\n  [During failure] System health:")
    show_health()

    r = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}", stream=True)
    if r.status_code == 200:
        total = sum(len(c) for c in r.iter_content(8192))
        print(f"  ✅ Retrieved {total:,} bytes using surviving replicas.")
    else:
        print(f"  ❌ Retrieval failed [{r.status_code}]: {r.text}")

    subprocess.run(["docker", "start", "worker2"], capture_output=True)
    print("  ✅ worker2 restarted. Waiting 20s for re-replication ...")
    time.sleep(20)
    print("\n  [After restart] System health:")
    show_health()

    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    for c in meta["chunk_locations"]:
        status = "✅" if len(c["workers"]) >= 2 else "⚠️ Under-replicated"
        print(f"    Chunk [{c['index']}]: {c['workers']}  {status}")


# ── Step 6: Data Integrity ────────────────────────────────────────────────────

def demo_data_integrity(obj_id: str):
    step("Data Integrity — Corrupt a Chunk, Detect, Recover from Replica")
    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    if not meta["chunk_locations"]:
        print("  No chunks found."); return

    chunk = meta["chunk_locations"][0]
    chunk_id = chunk["chunk_id"]
    worker_id = chunk["workers"][0]
    worker_ports = {
        "worker1": "http://localhost:8001",
        "worker2": "http://localhost:8002",
        "worker3": "http://localhost:8003",
        "worker4": "http://localhost:8004",
    }
    worker_url = worker_ports.get(worker_id, "http://localhost:8001")

    print(f"  Corrupting chunk {chunk_id} on {worker_id} ({worker_url})")
    r = requests.get(f"{worker_url}/internal/chunks/{chunk_id}")
    if r.status_code != 200:
        print(f"  ❌ Could not fetch chunk: {r.status_code}"); return

    orig_cs = hashlib.sha256(r.content).hexdigest()
    corrupted = b"CORRUPTED_DATA_" * 100 + r.content[1500:]
    requests.put(f"{worker_url}/internal/chunks/{chunk_id}", data=corrupted,
                 headers={"Content-Type": "application/octet-stream",
                          "X-Chunk-Checksum": hashlib.sha256(corrupted).hexdigest()})
    print(f"  Original  checksum: {orig_cs[:16]}...")
    print(f"  Corrupted checksum: {hashlib.sha256(corrupted).hexdigest()[:16]}...")

    r2 = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}", stream=True)
    if r2.status_code == 200:
        retrieved = b"".join(r2.iter_content(8192))
        ok = hashlib.sha256(retrieved).hexdigest() == meta["checksum"]
        print(f"  ✅ Data integrity preserved! Used healthy replica." if ok else "  ❌ Mismatch!")
    else:
        print(f"  ❌ Download failed [{r2.status_code}]: {r2.text}")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║   Mini Distributed Cloud Object Store — Full Demonstration      ║")
    print("╚══════════════════════════════════════════════════════════════════╝")

    wait_for_controller()

    obj_id_small = demo_basic_storage()
    obj_id_large = demo_chunking()
    demo_replication(obj_id_large)
    demo_metadata()
    demo_worker_failure(obj_id_large)
    demo_data_integrity(obj_id_small)

    print(f"\n{'='*70}")
    print("  ALL DEMONSTRATION STEPS COMPLETED")
    print("=" * 70)
