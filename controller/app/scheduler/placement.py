"""
controller/app/scheduler/placement.py
Chunk placement strategy (round-robin across alive workers).
Re-exports pick_workers_for_chunk from the registry.
"""
from controller.app.cluster.registry import pick_workers_for_chunk  # noqa: F401
