"""
worker/app/core/config.py
Worker configuration from environment variables.
"""
import os
from pathlib import Path

from shared.constants import HEARTBEAT_INTERVAL, WORKER_REGISTER_RETRIES, WORKER_REGISTER_RETRY_DELAY

WORKER_ID = os.environ.get("WORKER_ID", "worker-unknown")
WORKER_PORT = int(os.environ.get("WORKER_PORT", "8001"))
CONTROLLER_URL = os.environ.get("CONTROLLER_URL", "http://controller:8000")
STORAGE_DIR = Path(os.environ.get("STORAGE_DIR", "/data/chunks"))
WORKER_ADVERTISE_URL = os.environ.get(
    "WORKER_ADVERTISE_URL", f"http://{WORKER_ID}:{WORKER_PORT}"
)
