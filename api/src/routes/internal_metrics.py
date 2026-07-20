"""
SQL-backed Prometheus metrics endpoint for IoT last-seen data.

Exposes:
  iot_device_last_seen_seconds{serial_number, gateway_serial}  — UNIX timestamp of last measurement
  iot_gateway_last_seen_seconds{serial_number}                 — UNIX timestamp of last heartbeat
  iot_sensor_temperature_celsius{...}                          — last temperature per sensor
  iot_device_supply_mv{serial_number, gateway_serial}          — last supply voltage
  iot_device_alarm{serial_number, gateway_serial}              — node humidity alarm flag (0|1)
  iot_device_low_batt{serial_number, gateway_serial}           — node low-battery flag (0|1)
  iot_device_seq_completeness_pct{serial_number, gateway_serial} — node→gateway % (RF, 6h)
  iot_gateway_uplink_completeness_pct{serial_number}             — gateway→backend % (uplink, 6h)

These feed the recording rules that derive iot_device_seconds_since_seen and
iot_gateway_seconds_since_seen, which in turn drive the GatewayOffline and
SensorMissingTelemetry alert rules.

The endpoint is scraped by Prometheus at a 30s interval (sensor-state job).
It returns raw Prometheus text format (text/plain).

Query strategy:
  - Device last_seen: MAX(ts) from telemetry.measurement JOIN core.sensor JOIN core.device
    LEFT JOIN core.gateway via core.device.gateway_id.
    This avoids the gaps in monitoring.device_status (which is only written when
    rssi/buffer fields are present in the payload).
  - Gateway last_seen: MAX(ts) from monitoring.gateway_status (narrow table, cheap).
"""

import logging
import time

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import async_session

logger = logging.getLogger(__name__)

router = APIRouter()

# Simple in-process cache: refreshed at most once per 25s (just under the 30s scrape interval)
_cache_body: str = ""
_cache_ts: float = 0.0
_CACHE_TTL = 25.0


async def _build_metrics(session: AsyncSession) -> str:
    lines: list[str] = []

    # ── Device last_seen ──────────────────────────────────────────────────
    # Join telemetry.measurement → core.sensor → core.device → core.gateway
    # GROUP BY device gives MAX(ts) = last measurement received from that device.
    # gateway_serial is empty-string when device.gateway_id IS NULL (device not yet
    # assigned to a gateway). Inhibition rule only fires on equal non-empty values.
    device_query = text("""
        SELECT
            d.serial_number                          AS device_serial,
            COALESCE(g.serial_number, '')            AS gateway_serial,
            EXTRACT(EPOCH FROM MAX(m.ts))::BIGINT    AS last_seen_epoch
        FROM telemetry.measurement m
        JOIN core.sensor   s ON s.id        = m.sensor_id
        JOIN core.device   d ON d.id        = s.device_id
        LEFT JOIN core.gateway g ON g.id    = d.gateway_id
        GROUP BY d.serial_number, g.serial_number
    """)

    lines.append(
        "# HELP iot_device_last_seen_seconds Unix timestamp of last measurement from device"
    )
    lines.append("# TYPE iot_device_last_seen_seconds gauge")
    result = await session.execute(device_query)
    for row in result:
        labels = f'serial_number="{row.device_serial}",gateway_serial="{row.gateway_serial}"'
        lines.append(f"iot_device_last_seen_seconds{{{labels}}} {row.last_seen_epoch}")

    # ── Sensor current temperature ────────────────────────────────────────
    # Last temperature_c value per (device, node_id, sensor_key).
    # Used by SensorTemperatureHigh alert rule.
    temp_query = text("""
        SELECT
            d.serial_number                          AS device_serial,
            COALESCE(g.serial_number, '')            AS gateway_serial,
            s.node_id,
            s.sensor_key,
            m.temperature_c
        FROM telemetry.measurement m
        JOIN core.sensor   s ON s.id     = m.sensor_id
        JOIN core.device   d ON d.id     = s.device_id
        LEFT JOIN core.gateway g ON g.id = d.gateway_id
        WHERE s.sensor_key IN ('ti', 'ts', 't')
          AND m.temperature_c IS NOT NULL
          AND m.ts = (
              SELECT MAX(m2.ts)
              FROM telemetry.measurement m2
              WHERE m2.sensor_id = m.sensor_id
          )
    """)

    lines.append("# HELP iot_sensor_temperature_celsius Last reported temperature in Celsius")
    lines.append("# TYPE iot_sensor_temperature_celsius gauge")
    result = await session.execute(temp_query)
    for row in result:
        labels = (
            f'serial_number="{row.device_serial}",'
            f'gateway_serial="{row.gateway_serial}",'
            f'node_id="{row.node_id}",'
            f'sensor_key="{row.sensor_key}"'
        )
        lines.append(f"iot_sensor_temperature_celsius{{{labels}}} {float(row.temperature_c):.2f}")

    # ── Device supply voltage (battery) ──────────────────────────────────
    # Last supply_mv value per device (any sensor — takes the MAX ts across all sensors).
    # Used by SensorBatteryLow alert rule.
    supply_query = text("""
        SELECT
            d.serial_number                          AS device_serial,
            COALESCE(g.serial_number, '')            AS gateway_serial,
            m.supply_mv
        FROM telemetry.measurement m
        JOIN core.sensor   s ON s.id     = m.sensor_id
        JOIN core.device   d ON d.id     = s.device_id
        LEFT JOIN core.gateway g ON g.id = d.gateway_id
        WHERE m.supply_mv IS NOT NULL
          AND m.ts = (
              SELECT MAX(m2.ts)
              FROM telemetry.measurement m2
              JOIN core.sensor s2 ON s2.id = m2.sensor_id
              WHERE s2.device_id = d.id
                AND m2.supply_mv IS NOT NULL
          )
    """)

    lines.append("# HELP iot_device_supply_mv Last reported supply voltage in millivolts")
    lines.append("# TYPE iot_device_supply_mv gauge")
    result = await session.execute(supply_query)
    for row in result:
        labels = (
            f'serial_number="{row.device_serial}",'
            f'gateway_serial="{row.gateway_serial}"'
        )
        lines.append(f"iot_device_supply_mv{{{labels}}} {row.supply_mv}")

    # ── Node alarm / low battery state (from monitoring.device_runtime) ───
    # Live node alarm state (flags from firmware HM v3.6). 0|1.
    # Feeds the NodeHumidityAlarm / NodeLowBattery alerts.
    runtime_query = text("""
        SELECT
            d.serial_number                          AS device_serial,
            COALESCE(g.serial_number, '')            AS gateway_serial,
            COALESCE(dr.alarm, false)                AS alarm,
            COALESCE(dr.low_batt, false)             AS low_batt
        FROM monitoring.device_runtime dr
        JOIN core.device   d ON d.id     = dr.device_id
        LEFT JOIN core.gateway g ON g.id = d.gateway_id
    """)

    result = await session.execute(runtime_query)
    runtime_rows = result.all()

    lines.append("# HELP iot_device_alarm Node humidity alarm state (1=alarm, firmware flag)")
    lines.append("# TYPE iot_device_alarm gauge")
    for row in runtime_rows:
        labels = f'serial_number="{row.device_serial}",gateway_serial="{row.gateway_serial}"'
        lines.append(f"iot_device_alarm{{{labels}}} {1 if row.alarm else 0}")

    lines.append("# HELP iot_device_low_batt Node low-battery state (1=low, firmware flag)")
    lines.append("# TYPE iot_device_low_batt gauge")
    for row in runtime_rows:
        labels = f'serial_number="{row.device_serial}",gateway_serial="{row.gateway_serial}"'
        lines.append(f"iot_device_low_batt{{{labels}}} {1 if row.low_batt else 0}")

    # ── Packet completeness (node_seq continuity, 6h window) ──────────────
    # % = received / (received + missing). Missing = sum of gaps in the seq ordered by
    # VALUE (FIFO resends out of order), discarding jumps >= 100 (node reset/wraparound).
    # A single aggregated query for all devices (not one per device): the LAG runs inside
    # the CTE and is grouped. Bounded window (6h) to bound the cost of the 30s scrape.
    completeness_query = text("""
        WITH seqs AS (
            SELECT s.device_id, m.msg_counter AS seq,
                   LAG(m.msg_counter) OVER (PARTITION BY s.device_id ORDER BY m.msg_counter) AS prev
            FROM telemetry.measurement m
            JOIN core.sensor s ON s.id = m.sensor_id
            WHERE s.sensor_index = 0
              AND m.msg_counter IS NOT NULL
              AND m.ts > now() - interval '6 hours'
        ),
        gaps AS (
            SELECT device_id,
                   count(*) AS received,
                   COALESCE(SUM(CASE WHEN seq - prev > 1 AND seq - prev < 100
                                     THEN seq - prev - 1 ELSE 0 END), 0) AS missing
            FROM seqs
            GROUP BY device_id
        )
        SELECT d.serial_number AS device_serial,
               COALESCE(g.serial_number, '') AS gateway_serial,
               gp.received, gp.missing
        FROM gaps gp
        JOIN core.device d ON d.id = gp.device_id
        LEFT JOIN core.gateway g ON g.id = d.gateway_id
        WHERE gp.received > 0
    """)

    lines.append("# HELP iot_device_seq_completeness_pct Packet completeness % over last 6h")
    lines.append("# TYPE iot_device_seq_completeness_pct gauge")
    result = await session.execute(completeness_query)
    for row in result:
        total = row.received + row.missing
        pct = round(row.received / total * 100, 2) if total else 100.0
        labels = f'serial_number="{row.device_serial}",gateway_serial="{row.gateway_serial}"'
        lines.append(f"iot_device_seq_completeness_pct{{{labels}}} {pct}")

    # ── Gateway last_seen ─────────────────────────────────────────────────
    gateway_query = text("""
        SELECT
            serial_number,
            EXTRACT(EPOCH FROM MAX(ts))::BIGINT AS last_seen_epoch
        FROM monitoring.gateway_status
        GROUP BY serial_number
    """)

    lines.append("# HELP iot_gateway_last_seen_seconds Unix timestamp of last gateway heartbeat")
    lines.append("# TYPE iot_gateway_last_seen_seconds gauge")
    result = await session.execute(gateway_query)
    for row in result:
        labels = f'serial_number="{row.serial_number}"'
        lines.append(f"iot_gateway_last_seen_seconds{{{labels}}} {row.last_seen_epoch}")

    # ── Gateway uplink completeness (gw_seq, 6h window) ───────────────────
    # % of frames the gateway delivered to the backend without gaps in the gw_seq (the 'seq'
    # field of the /rx wrapper, already persisted in raw.telemetry_payload). DIFFERENT from
    # iot_device_seq_completeness_pct (node_seq = RF node->gateway). Grouped by gateway;
    # discards jumps >=1000 (gateway reset).
    uplink_query = text("""
        WITH seqs AS (
            SELECT payload->>'gw' AS gw, (payload->>'seq')::bigint AS seq,
                   LAG((payload->>'seq')::bigint)
                       OVER (PARTITION BY payload->>'gw' ORDER BY (payload->>'seq')::bigint) AS prev
            FROM raw.telemetry_payload
            WHERE payload ? 'raw' AND payload ? 'seq' AND payload ? 'gw'
              AND received_at > now() - interval '6 hours'
        )
        SELECT gw,
               count(*) AS received,
               COALESCE(SUM(CASE WHEN seq - prev > 1 AND seq - prev < 1000
                                 THEN seq - prev - 1 ELSE 0 END), 0) AS missing
        FROM seqs
        WHERE gw IS NOT NULL
        GROUP BY gw
    """)

    lines.append("# HELP iot_gateway_uplink_completeness_pct Gateway uplink completeness % (6h)")
    lines.append("# TYPE iot_gateway_uplink_completeness_pct gauge")
    result = await session.execute(uplink_query)
    for row in result:
        total = row.received + row.missing
        pct = round(row.received / total * 100, 2) if total else 100.0
        lines.append(f'iot_gateway_uplink_completeness_pct{{serial_number="{row.gw}"}} {pct}')

    return "\n".join(lines) + "\n"


@router.get("/internal/metrics/sensors", response_class=PlainTextResponse)
async def sensor_metrics() -> PlainTextResponse:
    """Prometheus text-format metrics for IoT device and gateway last-seen timestamps."""
    global _cache_body, _cache_ts

    now = time.monotonic()
    if now - _cache_ts < _CACHE_TTL and _cache_body:
        return PlainTextResponse(_cache_body, media_type="text/plain; version=0.0.4")

    try:
        async with async_session() as session:
            body = await _build_metrics(session)
        _cache_body = body
        _cache_ts = now
        return PlainTextResponse(body, media_type="text/plain; version=0.0.4")
    except Exception as e:
        logger.error(f"Failed to build sensor metrics: {e}")
        # Return stale cache if available, otherwise 500
        if _cache_body:
            logger.warning("Returning stale sensor metrics cache due to DB error")
            return PlainTextResponse(_cache_body, media_type="text/plain; version=0.0.4")
        raise
