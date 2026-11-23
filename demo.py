#!/usr/bin/env python3
"""
demo.py — Full demonstration script for the Mini Distributed Cloud Object Store.

Covers all required demonstration scenarios:
  1. Object Storage:   Upload + download, verify match
  2. Chunking:         Upload a >1 MB file, show chunk distribution
  3. Replication:      Show each chunk has 2 copies on different workers
  4. Metadata:         Upload with custom metadata, retrieve, modify
  5. Worker Failure:   Stop worker2, show retrieval still works, restart, check re-replication
  6. Data Integrity:   Corrupt a chunk file, show detection + recovery

Requirements:
    pip install requests
    docker (and docker compose) must be running with the system already up.
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
                else:
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
    """Create a temp file of given size with deterministic content."""
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "wb") as f:
        written = 0
        chunk = b"ABCDEFGH12345678" * 64  # 1 KB block
        while written < size_bytes:
            to_write = min(len(chunk), size_bytes - written)
            f.write(chunk[:to_write])
            written += to_write
    return path


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — Object Storage: upload, download, verify
# ═══════════════════════════════════════════════════════════════════════════════

def demo_basic_storage():
    step("Object Storage — Upload, Download, Verify")

    src = make_file(512 * 1024, ".txt")  # 512 KB text file
    orig_hash = sha256_file(src)
    print(f"  Created test file: {src} (512 KB), SHA256={orig_hash[:16]}...")

    # Upload
    with open(src, "rb") as f:
        resp = requests.post(
            f"{CONTROLLER}/api/v1/objects",
            files={"file": ("test-512kb.txt", f, "text/plain")},
        )
    assert resp.status_code == 201, f"Upload failed: {resp.text}"
    result = resp.json()
    obj_id = result["object_id"]
    print(f"  ✅ Uploaded. object_id = {obj_id}")
    print(f"     Size={result['size']} bytes, chunks={result['num_chunks']}, checksum={result['checksum'][:16]}...")

    # Download
    dst = src + ".downloaded"
    r = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}", stream=True)
    assert r.status_code == 200, f"Download failed: {r.text}"
    with open(dst, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

    dl_hash = sha256_file(dst)
    match = "✅ MATCH" if dl_hash == orig_hash else "❌ MISMATCH"
    print(f"  Downloaded to {dst}")
    print(f"  Original  SHA256: {orig_hash}")
    print(f"  Retrieved SHA256: {dl_hash}")
    print(f"  Integrity check: {match}")
    assert dl_hash == orig_hash

    os.unlink(src)
    os.unlink(dst)
    return obj_id


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 — Chunking: upload >1 MB file, show chunk distribution
# ═══════════════════════════════════════════════════════════════════════════════

def demo_chunking():
    step("Chunking — Upload >1 MB file, Inspect Chunk Distribution")

    src = make_file(3 * 1024 * 1024 + 500 * 1024, ".bin")  # 3.5 MB
    print(f"  Created test file: {src} (3.5 MB)")

    with open(src, "rb") as f:
        resp = requests.post(
            f"{CONTROLLER}/api/v1/objects",
            files={"file": ("large-file.bin", f, "application/octet-stream")},
        )
    assert resp.status_code == 201, f"Upload failed: {resp.text}"
    result = resp.json()
    obj_id = result["object_id"]
    print(f"  ✅ Uploaded. object_id = {obj_id}")
    print(f"     Total size: {result['size']:,} bytes")
    print(f"     Chunks: {result['num_chunks']}")

    # Fetch and show chunk details
    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    print(f"\n  Chunk distribution across workers:")
    print(f"  {'Chunk':5} {'Size':>10}  Workers")
    print(f"  {'-'*5} {'-'*10}  {'-'*30}")
    for c in meta["chunk_locations"]:
        workers = ", ".join(c["workers"])
        print(f"  [{c['index']:3}] {c['size']:>10,} B  → {workers}")

    os.unlink(src)
    return obj_id


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3 — Replication: verify 2 copies per chunk on different workers
# ═══════════════════════════════════════════════════════════════════════════════

def demo_replication(obj_id: str):
    step(f"Replication — Verify 2 Copies Per Chunk (object_id={obj_id[:8]}...)")

    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    all_ok = True
    for c in meta["chunk_locations"]:
        workers = c["workers"]
        unique = len(set(workers))
        status = "✅" if unique >= 2 else "❌"
        if unique < 2:
            all_ok = False
        print(f"  Chunk [{c['index']}]: replicas={unique}  workers={workers}  {status}")

    if all_ok:
        print("\n  ✅ All chunks have >= 2 replicas on distinct workers.")
    else:
        print("\n  ❌ Some chunks do not meet the replication factor!")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4 — Metadata: upload with custom metadata, retrieve, modify
# ═══════════════════════════════════════════════════════════════════════════════

def demo_metadata():
    step("Metadata — Upload with Custom Metadata, Retrieve, Modify")

    src = make_file(100 * 1024, ".jpg")
    custom = json.dumps({"project": "cloud-demo", "author": "student", "version": "1"})

    with open(src, "rb") as f:
        resp = requests.post(
            f"{CONTROLLER}/api/v1/objects",
            files={"file": ("photo.jpg", f, "image/jpeg")},
            data={"metadata": custom},
        )
    assert resp.status_code == 201
    obj_id = resp.json()["object_id"]
    print(f"  ✅ Uploaded with custom metadata. object_id={obj_id}")

    # Retrieve metadata
    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    print(f"\n  Retrieved metadata:")
    print(f"    name          : {meta['name']}")
    print(f"    size          : {meta['size']} bytes")
    print(f"    content_type  : {meta['content_type']}")
    print(f"    created_at    : {meta['created_at']}")
    print(f"    checksum      : {meta['checksum'][:16]}...")
    print(f"    custom        : {meta['custom_metadata']}")

    # Update metadata (no re-upload needed)
    update = {"version": "2", "reviewed": "true", "rating": "5"}
    resp2 = requests.patch(
        f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata",
        json=update,
        headers={"Content-Type": "application/json"},
    )
    assert resp2.status_code == 200
    updated = resp2.json()
    print(f"\n  After PATCH update:")
    print(f"    custom_metadata: {updated['custom_metadata']}")
    print(f"    last_modified  : {updated['last_modified']}")
    print(f"  ✅ Metadata updated without re-uploading data.")

    os.unlink(src)
    return obj_id


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5 — Worker Failure: stop worker2, test retrieval, restart, check re-replication
# ═══════════════════════════════════════════════════════════════════════════════

def demo_worker_failure(obj_id: str):
    step("Worker Failure — Stop worker2, Retrieve Object, Restart, Check Replication")

    print("  [Before failure] System health:")
    show_health()

    print("\n  → Stopping worker2 container ...")
    result = subprocess.run(["docker", "stop", "worker2"], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ⚠️  Could not stop worker2: {result.stderr.strip()}")
        print("     (Are you running this inside Docker? Try running from the host.)")
        return
    print("  ✅ worker2 stopped.")

    # Wait for controller to detect failure (> heartbeat timeout)
    print("  Waiting 20s for controller to detect worker2 failure ...")
    time.sleep(20)

    print("\n  [During failure] System health:")
    show_health()

    # Retrieve object — should still work if the other replica is available
    print(f"\n  Attempting to download object {obj_id[:8]}... while worker2 is down ...")
    r = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}", stream=True)
    if r.status_code == 200:
        total = sum(len(c) for c in r.iter_content(8192))
        print(f"  ✅ Retrieved {total:,} bytes successfully (using surviving replicas).")
    else:
        print(f"  ❌ Retrieval failed [{r.status_code}]: {r.text}")

    print("\n  → Restarting worker2 ...")
    subprocess.run(["docker", "start", "worker2"], capture_output=True)
    print("  ✅ worker2 restarted. Waiting 20s for re-registration and re-replication ...")
    time.sleep(20)

    print("\n  [After restart] System health:")
    show_health()

    # Check chunk replication was restored
    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    print(f"\n  Chunk replication status after restart:")
    for c in meta["chunk_locations"]:
        workers = c["workers"]
        status = "✅" if len(workers) >= 2 else "⚠️ Under-replicated"
        print(f"    Chunk [{c['index']}]: {workers}  {status}")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6 — Data Integrity: corrupt a chunk, detect it, recover
# ═══════════════════════════════════════════════════════════════════════════════

def demo_data_integrity(obj_id: str):
    step("Data Integrity — Corrupt a Chunk, Detect, Recover from Replica")

    # Get metadata to find chunk locations
    meta = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}/metadata").json()
    if not meta["chunk_locations"]:
        print("  No chunks found.")
        return

    chunk = meta["chunk_locations"][0]
    chunk_id = chunk["chunk_id"]
    worker_id = chunk["workers"][0]  # corrupt the FIRST replica

    # Find the worker's exposed port
    worker_ports = {
        "worker1": "http://localhost:8001",
        "worker2": "http://localhost:8002",
        "worker3": "http://localhost:8003",
        "worker4": "http://localhost:8004",
    }
    worker_url = worker_ports.get(worker_id, f"http://localhost:8001")

    print(f"  Target chunk : {chunk_id}")
    print(f"  Corrupting on: {worker_id} ({worker_url})")

    # Fetch original chunk
    r = requests.get(f"{worker_url}/internal/chunks/{chunk_id}")
    if r.status_code != 200:
        print(f"  ❌ Could not retrieve chunk from {worker_id}: {r.status_code}")
        return

    original_data = r.content
    original_checksum = hashlib.sha256(original_data).hexdigest()
    print(f"  Original checksum: {original_checksum[:16]}...")

    # Write corrupted data
    corrupted = b"CORRUPTED_DATA_" * 100 + original_data[1500:]
    put_resp = requests.put(
        f"{worker_url}/internal/chunks/{chunk_id}",
        data=corrupted,
        headers={
            "Content-Type": "application/octet-stream",
            "X-Chunk-Checksum": hashlib.sha256(corrupted).hexdigest(),  # bypass worker check
        },
    )
    print(f"  Wrote corrupted data → {put_resp.status_code}")
    corrupted_checksum = hashlib.sha256(corrupted).hexdigest()
    print(f"  Corrupted checksum : {corrupted_checksum[:16]}... (different!)")

    # Now try to download — controller should detect corruption and use the other replica
    print(f"\n  Downloading object {obj_id[:8]}... (controller should detect + skip corrupt copy) ...")
    r2 = requests.get(f"{CONTROLLER}/api/v1/objects/{obj_id}", stream=True)
    if r2.status_code == 200:
        retrieved = b"".join(r2.iter_content(8192))
        retrieved_checksum = hashlib.sha256(retrieved).hexdigest()
        expected = meta["checksum"]
        match = retrieved_checksum == expected
        print(f"  Retrieved checksum : {retrieved_checksum[:16]}...")
        print(f"  Expected checksum  : {expected[:16]}...")
        if match:
            print(f"  ✅ Data integrity preserved! Controller used the healthy replica.")
        else:
            print(f"  ❌ Returned data does not match expected checksum.")
    else:
        print(f"  ❌ Download failed [{r2.status_code}]: {r2.text}")


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║   Mini Distributed Cloud Object Store — Full Demonstration      ║")
    print("╚══════════════════════════════════════════════════════════════════╝")

    wait_for_controller()

    # 1. Basic storage
    obj_id_small = demo_basic_storage()

    # 2. Chunking (3.5 MB file)
    obj_id_large = demo_chunking()

    # 3. Replication verification
    demo_replication(obj_id_large)

    # 4. Metadata
    obj_id_meta = demo_metadata()

    # 5. Worker failure (uses the large object)
    demo_worker_failure(obj_id_large)

    # 6. Data integrity (uses the small object)
    demo_data_integrity(obj_id_small)

    print(f"\n{'='*70}")
    print("  ALL DEMONSTRATION STEPS COMPLETED")
    print("=" * 70)
