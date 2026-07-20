"""
Spatial telemetry snapshot refresh (Sprint 3).

Periodic (pull) job that recomputes spatial.asset_telemetry_snapshot from the
latest measurement of each device. Does NOT touch the MQTT ingestion hot path. The
GET /floors/{id}/telemetry endpoint reads the snapshot by PK; the heavy JOIN lives here.

Background task pattern same as retention.py (loop + sleep + metrics).
"""

import asyncio
import logging
import time

from sqlalchemy import text

from ..config import settings
from ..database import async_session
from ..metrics import (
    SPATIAL_SNAPSHOT_DURATION,
    SPATIAL_SNAPSHOT_LAST_STARTED,
    SPATIAL_SNAPSHOT_LAST_SUCCESS,
    SPATIAL_SNAPSHOT_REFRESH_ERRORS,
    SPATIAL_SNAPSHOT_ROWS,
)

logger = logging.getLogger(__name__)

_refresh_lock = asyncio.Lock()

# Orphan cleanup: assets that lost their device_id or were deleted.
_CLEANUP_SQL = text("""
    DELETE FROM spatial.asset_telemetry_snapshot s
    USING spatial.asset a
    WHERE s.asset_id = a.id
      AND (a.device_id IS NULL OR a.deleted_at IS NOT NULL)
""")

# Batched recalculation. CTEs precompute the latest value per device ONCE
# (DISTINCT ON), instead of a per-asset LATERAL. status derived from a last_ts threshold.
_REFRESH_SQL = text("""
    WITH latest_m AS (
        SELECT DISTINCT ON (s.device_id)
            s.device_id, m.ts AS last_ts, m.temperature_c, m.humidity_pct, m.supply_mv
        FROM core.sensor s
        JOIN telemetry.measurement m ON m.sensor_id = s.id
        ORDER BY s.device_id, m.ts DESC
    ),
    latest_ds AS (
        SELECT DISTINCT ON (device_id)
            device_id, rssi_dbm, ts AS rssi_ts
        FROM monitoring.device_status
        ORDER BY device_id, ts DESC
    )
    INSERT INTO spatial.asset_telemetry_snapshot
        (asset_id, org_id, device_id, last_ts, temperature_c, humidity_pct,
         supply_mv, rssi_dbm, rssi_ts, status, updated_at)
    SELECT
        a.id, a.org_id, a.device_id,
        lm.last_ts, lm.temperature_c, lm.humidity_pct, lm.supply_mv,
        lds.rssi_dbm, lds.rssi_ts,
        CASE
            WHEN lm.last_ts IS NULL THEN 'unknown'
            WHEN lm.last_ts >= NOW() - make_interval(mins => :online_threshold_min) THEN 'online'
            ELSE 'offline'
        END,
        NOW()
    FROM spatial.asset a
    LEFT JOIN latest_m  lm  ON lm.device_id  = a.device_id
    LEFT JOIN latest_ds lds ON lds.device_id = a.device_id
    WHERE a.device_id IS NOT NULL AND a.deleted_at IS NULL
    ON CONFLICT (asset_id) DO UPDATE SET
        device_id = EXCLUDED.device_id,
        last_ts = EXCLUDED.last_ts,
        temperature_c = EXCLUDED.temperature_c,
        humidity_pct = EXCLUDED.humidity_pct,
        supply_mv = EXCLUDED.supply_mv,
        rssi_dbm = EXCLUDED.rssi_dbm,
        rssi_ts = EXCLUDED.rssi_ts,
        status = EXCLUDED.status,
        updated_at = NOW()
""")


async def refresh_snapshot_once() -> int:
    """One refresh run. Returns rows materialized. Anti-overlap lock."""
    if _refresh_lock.locked():
        logger.warning("snapshot refresh already in progress, skipping this cycle")
        return 0
    async with _refresh_lock:
        SPATIAL_SNAPSHOT_LAST_STARTED.set(time.time())
        start = time.perf_counter()
        async with async_session() as session:
            await session.execute(_CLEANUP_SQL)
            result = await session.execute(
                _REFRESH_SQL,
                {"online_threshold_min": settings.spatial_online_threshold_min},
            )
            await session.commit()
            # rowcount lives on CursorResult; the base Result type doesn't expose it for mypy.
            rows = result.rowcount  # type: ignore[attr-defined]
        SPATIAL_SNAPSHOT_DURATION.observe(time.perf_counter() - start)
        SPATIAL_SNAPSHOT_ROWS.set(rows)
        SPATIAL_SNAPSHOT_LAST_SUCCESS.set(time.time())
        return rows


async def run_snapshot_refresh() -> None:
    """Background task: refreshes the snapshot every SPATIAL_SNAPSHOT_REFRESH_S.
    Sequential (sleep after the refresh) → no overlap by design; the lock is
    extra defense. Does not re-raise exceptions, to keep the task alive."""
    while True:
        try:
            rows = await refresh_snapshot_once()
            logger.info("spatial snapshot refreshed: %d rows", rows)
        except Exception as e:  # noqa: BLE001 — keep the task alive
            SPATIAL_SNAPSHOT_REFRESH_ERRORS.inc()
            logger.error("spatial snapshot refresh failed: %s", e)
        await asyncio.sleep(settings.spatial_snapshot_refresh_s)
