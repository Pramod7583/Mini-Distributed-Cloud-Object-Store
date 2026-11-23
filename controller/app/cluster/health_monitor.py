"""
controller/app/cluster/health_monitor.py
Background task: periodically restores replication factor when workers fail/recover.
"""
import asyncio
import logging

from shared.constants import REPLICATION_CHECK_INTERVAL, REPLICATION_FACTOR

logger = logging.getLogger("controller.health_monitor")


async def replication_health_loop(restore_fn):
    """
    Calls restore_fn() every REPLICATION_CHECK_INTERVAL seconds.
    restore_fn is controller/app/scheduler/replication.restore_replication.
    """
    await asyncio.sleep(5)  # give workers time to register on startup
    while True:
        await asyncio.sleep(REPLICATION_CHECK_INTERVAL)
        try:
            await restore_fn()
        except Exception as exc:
            logger.error("Replication health check error: %s", exc)
