"""
shared/constants.py
System-wide constants shared between controller and worker.
"""

CHUNK_SIZE = 1 * 1024 * 1024       # 1 MB
REPLICATION_FACTOR = 2
HEARTBEAT_TIMEOUT = 15             # seconds — worker considered dead after this
REPLICATION_CHECK_INTERVAL = 10   # seconds between repair sweeps
HEARTBEAT_INTERVAL = 5            # seconds between worker heartbeats
WORKER_REGISTER_RETRIES = 20
WORKER_REGISTER_RETRY_DELAY = 3   # seconds
