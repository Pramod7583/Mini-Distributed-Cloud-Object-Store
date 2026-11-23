"""
controller/app/cluster/registry.py
In-memory worker registry — registration, heartbeat, alive-worker list.
"""
import time
from typing import Any, Dict, List

from shared.constants import HEARTBEAT_TIMEOUT

# workers[worker_id] = {"url": str, "last_heartbeat": float, "chunk_count": int}
workers: Dict[str, Dict[str, Any]] = {}

# Round-robin counter for even chunk placement
_rr_counter: int = 0


def register(worker_id: str, url: str) -> None:
    workers[worker_id] = {
        "url": url,
        "last_heartbeat": time.time(),
        "chunk_count": 0,
    }


def beat(worker_id: str, chunk_count: int) -> bool:
    """Update heartbeat timestamp. Returns False if worker unknown."""
    if worker_id not in workers:
        return False
    workers[worker_id]["last_heartbeat"] = time.time()
    workers[worker_id]["chunk_count"] = chunk_count
    return True


def alive_workers() -> List[Dict[str, Any]]:
    cutoff = time.time() - HEARTBEAT_TIMEOUT
    return [
        {"worker_id": wid, **w}
        for wid, w in workers.items()
        if w["last_heartbeat"] >= cutoff
    ]


def pick_workers_for_chunk(
    exclude: List[str] | None = None,
    replication_factor: int = 2,
) -> List[Dict[str, Any]]:
    """
    Round-robin selection of `replication_factor` distinct alive workers.
    Raises ValueError if not enough workers are available.
    """
    global _rr_counter
    alive = alive_workers()
    alive.sort(key=lambda w: w["worker_id"])  # stable alphabetical order
    if exclude:
        alive = [w for w in alive if w["worker_id"] not in exclude]
    if len(alive) < replication_factor:
        raise ValueError(
            f"Need {replication_factor} alive workers; only {len(alive)} available."
        )
    n = len(alive)
    selected = [alive[(_rr_counter + i) % n] for i in range(replication_factor)]
    _rr_counter = (_rr_counter + replication_factor) % n
    return selected


def health_snapshot() -> Dict[str, Any]:
    """Return a sorted health dict for the /health endpoint."""
    now = time.time()
    return {
        wid: {
            "url": w["url"],
            "alive": now - w["last_heartbeat"] < HEARTBEAT_TIMEOUT,
            "last_heartbeat_ago_s": round(now - w["last_heartbeat"], 1),
            "chunk_count": w.get("chunk_count", 0),
        }
        for wid, w in sorted(workers.items())
    }
