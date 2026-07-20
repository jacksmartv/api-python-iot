"""
Retention cleanup service — batched deletes for telemetry tables.

Runs hourly as a background asyncio task. Uses batches of 10k rows to avoid
lock pressure and large WAL segments. Autovacuum handles bloat reclamation.

Tables covered:
  - telemetry.measurement      (TELEMETRY_RETENTION_DAYS, default 90)
  - raw.telemetry_payload      (RAW_RETENTION_DAYS, default 14)
  - monitoring.device_status   (MONITORING_RETENTION_DAYS, default 90)
  - monitoring.gateway_status  (MONITORING_RETENTION_DAYS, default 90)
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from ..config import settings
from ..database import async_session
from ..metrics import RETENTION_CLEANUP_ERRORS, RETENTION_LAST_SUCCESS

logger = logging.getLogger(__name__)

_BATCH_SIZE = 10_000
_RUN_INTERVAL_SECONDS = 3600  # hourly


async def _delete_batch(session, table: str, ts_column: str, cutoff: datetime) -> int:
    """Deletes one batch of rows older than cutoff. Returns row count deleted."""
    result = await session.execute(
        text(f"""
            DELETE FROM {table}
            WHERE ({ts_column}) IN (
                SELECT {ts_column} FROM {table}
                WHERE {ts_column} < :cutoff
                LIMIT :batch_size
            )
        """),
        {"cutoff": cutoff, "batch_size": _BATCH_SIZE},
    )
    await session.commit()
    return result.rowcount


async def _run_table_cleanup(table: str, ts_column: str, cutoff: datetime) -> None:
    deleted_total = 0
    while True:
        async with async_session() as session:
            deleted = await _delete_batch(session, table, ts_column, cutoff)
        deleted_total += deleted
        if deleted < _BATCH_SIZE:
            break
        await asyncio.sleep(0.1)  # yield between batches — don't starve other coroutines
    if deleted_total > 0:
        logger.info(f"Retention: deleted {deleted_total} rows from {table}")


async def run_retention_cleanup() -> None:
    """Background task: runs retention cleanup every hour."""
    while True:
        await asyncio.sleep(_RUN_INTERVAL_SECONDS)
        now = datetime.now(timezone.utc)

        telemetry_cutoff = now - timedelta(days=settings.telemetry_retention_days)
        raw_cutoff = now - timedelta(days=settings.raw_retention_days)
        monitoring_cutoff = now - timedelta(days=settings.monitoring_retention_days)

        try:
            await _run_table_cleanup("telemetry.measurement", "ts", telemetry_cutoff)
            await _run_table_cleanup("raw.telemetry_payload", "received_at", raw_cutoff)
            await _run_table_cleanup("monitoring.device_status", "ts", monitoring_cutoff)
            await _run_table_cleanup("monitoring.gateway_status", "ts", monitoring_cutoff)

            RETENTION_LAST_SUCCESS.set(time.time())
            logger.info("Retention cleanup completed successfully")
        except Exception as e:
            RETENTION_CLEANUP_ERRORS.inc()
            logger.error(f"Retention cleanup failed: {e}")
            # Do not re-raise — keep the background task alive
