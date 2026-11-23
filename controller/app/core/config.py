"""
controller/app/core/config.py
Central configuration — reads from environment variables.
"""
import logging
import os

from shared.constants import (
    CHUNK_SIZE,
    HEARTBEAT_TIMEOUT,
    REPLICATION_CHECK_INTERVAL,
    REPLICATION_FACTOR,
)

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("controller")
