"""
Telemetry ingestion service with buffering for near real-time.

Implements the flow described in the architecture:
1. Receives JSON (from HTTP or MQTT)
2. Parses the payload according to its format
3. Saves the payload to raw.telemetry_payload
4. Iterates over each sensor {id}
5. Inserts measurements into telemetry.measurement
6. Records state in monitoring.device_status
"""

import asyncio
import hashlib
import json
import logging
import uuid as uuid_module
from contextlib import suppress
from datetime import datetime, timezone
from enum import StrEnum
from typing import cast
from uuid import UUID

from sqlalchemy import CursorResult, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import settings
from ..database import async_session, engine
from ..metrics import (
    BUFFER_FLUSHES,
    BUFFER_OVERFLOW_DROPS,
    BUFFER_SIZE,
    FLUSH_DURATION,
    FLUSH_RETRY_COUNT,
    MEASUREMENTS_INSERTED,
    PAYLOADS_FAILED,
    PAYLOADS_PROCESSED,
    PAYLOADS_RECEIVED,
    PROCESSING_TIME,
)
from ..models import (
    Device,
    DeviceRuntime,
    DeviceStatus,
    FleetEvent,
    Gateway,
    GatewayConfig,
    GatewayStatus,
    Measurement,
    Sensor,
)
from .payload_parser import ParsedGatewayStatus, ParsedPayload, ParsedSensorData, parse_payload

logger = logging.getLogger(__name__)


class IngestResult(StrEnum):
    """Logical result of ingesting a recovered frame (did the data end up available?).

    Used by the recovery path (gap_recovery). The caller distinguishes "available" (INSERTED |
    ALREADY_PRESENT) from FAILED; the domain event is only emitted on INSERTED (real recovery).
    """

    INSERTED = "inserted"            # persisted just now -> real recovery
    # dedup discarded it: it was already there (race with normal /rx)
    ALREADY_PRESENT = "already_present"
    FAILED = "failed"                # parser/DB failed


class IngestionService:
    """
    Ingestion service with buffering to optimize inserts.

    Accumulates messages and flushes when either:
    - buffer_max_size messages is reached
    - buffer_max_seconds seconds have passed

    Supports multiple payload formats:
    - v1 format: TS1, t1, vc1, etc. keys
    - legacy format: sensor_0, sensor_1, etc.
    """

    MAX_BUFFER_OVERFLOW = 10_000

    def __init__(self):
        self._buffer: list[tuple[ParsedPayload, datetime, int]] = []
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._running = False
        self._gateway_semaphore = asyncio.Semaphore(10)

    async def start(self):
        """Starts the periodic flush service."""
        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info("Ingestion service started")

    async def stop(self):
        """Stops the service and performs a final flush."""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._flush_task

        # Dispose pool — cancellation may have left connections in bad state
        await engine.dispose()

        for attempt in range(2):
            try:
                await asyncio.wait_for(self._flush(), timeout=10.0)
                break
            except asyncio.TimeoutError:
                logger.error(f"Drain flush timed out (attempt {attempt + 1})")
                if attempt == 0:
                    await engine.dispose()
            except Exception as e:
                logger.error(f"Drain flush failed ({e}, attempt {attempt + 1})")
                if attempt == 0:
                    await engine.dispose()
                    await asyncio.sleep(1.0)
                else:
                    logger.error(f"Final drain failed, {len(self._buffer)} payloads lost")

        logger.info("Ingestion service stopped")

    async def _periodic_flush(self):
        """Task that periodically flushes the buffer."""
        while self._running:
            await asyncio.sleep(settings.buffer_max_seconds)
            if self._buffer:
                BUFFER_FLUSHES.labels(trigger="time").inc()
            await self._flush()

    async def ingest(self, payload: dict, serial_number: str | None = None) -> UUID:
        """
        Adds a payload to the ingestion buffer.

        Args:
            payload: The received JSON payload
            serial_number: Optional serial number (useful for MQTT where it comes from the topic)

        Returns:
            Device UUID
        """
        received_at = datetime.now(timezone.utc)

        # Parse the payload
        parsed = parse_payload(payload, serial_number)

        if not parsed.serial_number:
            raise ValueError("Missing serial number in payload")

        PAYLOADS_RECEIVED.labels(device_serial=parsed.serial_number).inc()

        if not parsed.sensors:
            logger.warning(
                f"Payload from {parsed.serial_number} produced no sensors — possible unknown format"
            )
            PAYLOADS_FAILED.labels(
                device_serial=parsed.serial_number, error_type="NoSensorsDiscovered"
            ).inc()

        async with self._lock:
            self._buffer.append((parsed, received_at, 0))
            BUFFER_SIZE.set(len(self._buffer))

            if len(self._buffer) >= settings.buffer_max_size:
                BUFFER_FLUSHES.labels(trigger="size").inc()
                await self._flush_unlocked()

        async with async_session() as session:
            device = await self._get_or_create_device(session, parsed.serial_number)
            await session.commit()
            return device.id

    async def ingest_parsed(self, parsed: ParsedPayload) -> UUID:
        """
        Ingests an already-parsed payload.

        Useful when parsing is done externally (e.g. MQTT consumer).
        """
        received_at = datetime.now(timezone.utc)

        if not parsed.serial_number:
            raise ValueError("Missing serial number in payload")

        PAYLOADS_RECEIVED.labels(device_serial=parsed.serial_number).inc()

        if not parsed.sensors:
            logger.warning(
                f"Payload from {parsed.serial_number} produced no sensors — possible unknown format"
            )
            PAYLOADS_FAILED.labels(
                device_serial=parsed.serial_number, error_type="NoSensorsDiscovered"
            ).inc()

        async with self._lock:
            self._buffer.append((parsed, received_at, 0))
            BUFFER_SIZE.set(len(self._buffer))

            if len(self._buffer) >= settings.buffer_max_size:
                BUFFER_FLUSHES.labels(trigger="size").inc()
                await self._flush_unlocked()

        async with async_session() as session:
            device = await self._get_or_create_device(session, parsed.serial_number)
            await session.commit()
            return device.id

    async def ingest_recovered_frame(
        self, parsed: ParsedPayload, source: str = "storage_scan"
    ) -> IngestResult:
        """SYNCHRONOUS ingestion of a frame recovered from the gateway's log (gap_recovery V2).

        Unlike ingest_parsed (buffered), it persists immediately in its own session and
        returns whether the frame was inserted or dedup discarded it. An INSERTED via this path is,
        by construction, a real recovery from the log scan (this method is used ONLY by the
        gap_recovery job, not normal /rx) -> the caller counts INSERTED as gaps closed by the
        scan. `source` is for logging/traceability. Reuses _process_parsed_payload (same dedup,
        same raw/sensors path).
        """
        received_at = datetime.now(timezone.utc)
        async with async_session() as session:
            try:
                inserted = await self._process_parsed_payload(session, parsed, received_at)
                await session.commit()
            except Exception:
                await session.rollback()
                logger.exception(
                    f"recovered-frame ingest failed for {parsed.serial_number} (source={source})"
                )
                return IngestResult.FAILED
        return IngestResult.INSERTED if inserted else IngestResult.ALREADY_PRESENT

    async def _flush(self):
        """Flushes the buffer with the lock held."""
        async with self._lock:
            await self._flush_unlocked()

    async def _flush_unlocked(self):
        """Flushes the buffer without acquiring the lock (must already be held)."""
        if not self._buffer:
            return

        buffer = self._buffer
        self._buffer = []
        BUFFER_SIZE.set(0)

        logger.info(f"Flushing {len(buffer)} payloads")

        requeue: list[tuple[ParsedPayload, datetime, int]] = []

        with FLUSH_DURATION.time():
            for parsed, received_at, retry_count in buffer:
                async with async_session() as session:
                    try:
                        with PROCESSING_TIME.labels(operation="total").time():
                            await self._process_parsed_payload(session, parsed, received_at)
                        await session.commit()
                        PAYLOADS_PROCESSED.labels(device_serial=parsed.serial_number).inc()
                    except Exception as e:
                        await session.rollback()
                        if retry_count >= 3:
                            logger.error(
                                f"Payload from {parsed.serial_number} failed {retry_count} times "
                                f"({type(e).__name__}), discarding as poison pill",
                                extra={
                                    "device_serial": parsed.serial_number,
                                    "error_type": type(e).__name__,
                                },
                            )
                            PAYLOADS_FAILED.labels(
                                device_serial=parsed.serial_number,
                                error_type="PoisonPill",
                            ).inc()
                        else:
                            FLUSH_RETRY_COUNT.inc()
                            logger.warning(
                                f"Payload from {parsed.serial_number} failed ({type(e).__name__}), "
                                f"re-queuing (attempt {retry_count + 1}/3)",
                                extra={
                                    "device_serial": parsed.serial_number,
                                    "retry_count": retry_count + 1,
                                },
                            )
                            requeue.append((parsed, received_at, retry_count + 1))

        if requeue:
            self._buffer = requeue + self._buffer
            # Hard cap — drop oldest if buffer grows unbounded during sustained DB outage
            if len(self._buffer) > self.MAX_BUFFER_OVERFLOW:
                dropped_count = len(self._buffer) - self.MAX_BUFFER_OVERFLOW
                dropped = self._buffer[:dropped_count]
                self._buffer = self._buffer[dropped_count:]
                for p, _, _ in dropped:
                    PAYLOADS_FAILED.labels(
                        device_serial=p.serial_number, error_type="BufferOverflow"
                    ).inc()
                BUFFER_OVERFLOW_DROPS.inc(dropped_count)
                logger.warning(f"Buffer overflow: dropped {dropped_count} oldest payloads")

        BUFFER_SIZE.set(len(self._buffer))
        succeeded = len(buffer) - len(requeue)
        logger.info(f"Flush complete: {succeeded} succeeded, {len(requeue)} re-queued")

    async def _process_parsed_payload(
        self, session: AsyncSession, parsed: ParsedPayload, received_at: datetime
    ):
        """Processes an already-parsed payload."""
        # 1. Get or create device
        device = await self._get_or_create_device(session, parsed.serial_number)

        # 2. Associate gateway (last-seen-via semantics for HM format)
        if parsed.gateway_serial:
            gw = await self._get_or_create_gateway(session, parsed.gateway_serial)
            await session.execute(
                text("UPDATE core.device SET gateway_id = :gw WHERE id = :dev"),
                {"gw": str(gw.id), "dev": str(device.id)},
            )

        # 4. Save the raw payload. ON CONFLICT DO NOTHING on the unique index
        #    (device_id, payload_hash) to absorb QoS-1 duplicates and floods.
        #    payload_hash is a DB-generated column — not included in the INSERT.
        #    RETURNING id lets us know whether it inserted or dedup discarded it (recovery path).
        raw_res = await session.execute(
            text("""
                INSERT INTO raw.telemetry_payload
                    (id, device_id, received_at, schema_version, payload)
                VALUES (:id, :device_id, :received_at, :schema_version, cast(:payload as jsonb))
                ON CONFLICT (device_id, payload_hash) DO NOTHING
                RETURNING id
            """),
            {
                "id": str(uuid_module.uuid4()),
                "device_id": str(device.id),
                "received_at": received_at,
                "schema_version": parsed.schema_version,
                "payload": json.dumps(parsed.raw_payload),
            },
        )
        raw_inserted = raw_res.first() is not None

        # 5. Process each sensor
        for sensor_data in parsed.sensors:
            await self._process_sensor_data(session, device, sensor_data, parsed.serial_number)

        # 6. FRAME-level alarm state (once per frame, NOT per sensor): detects
        #    transitions and emits events. Only applies if the frame carries the flags (HM v3.6).
        if parsed.alarm is not None or parsed.low_batt is not None:
            await self._process_alarm_state(session, device, parsed)

        # raw_inserted: True if the raw row was inserted just now, False if dedup discarded it.
        # The normal /rx path ignores it; the recovery path uses it (INSERTED vs ALREADY_PRESENT).
        return raw_inserted

    async def _process_alarm_state(
        self, session: AsyncSession, device: Device, parsed: ParsedPayload
    ):
        """Detects alarm/battery transitions in the frame and emits events (once per frame).

        Live state in monitoring.device_runtime (NOT device_status, which is historical). Only
        emits on the edge (false->true / true->false), never one event per frame. Fails silently: an
        error here must NOT break ingestion (the measurement was already saved).
        """
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        try:
            now = datetime.now(timezone.utc)
            # ts of the frame that originated the event (logical reference for the UI)
            source_ts = parsed.sensors[0].timestamp if parsed.sensors else now

            # Previous state (None if this is the first time the device is seen)
            prev = (
                await session.execute(
                    select(DeviceRuntime.alarm, DeviceRuntime.low_batt).where(
                        DeviceRuntime.device_id == device.id
                    )
                )
            ).first()
            prev_alarm = prev.alarm if prev else None
            prev_low_batt = prev.low_batt if prev else None

            new_alarm = parsed.alarm
            new_low_batt = parsed.low_batt
            alarm_changed = new_alarm is not None and bool(new_alarm) != bool(prev_alarm)
            low_batt_changed = (
                new_low_batt is not None and bool(new_low_batt) != bool(prev_low_batt)
            )

            # Node's current OTA config (measure_cycles/send_cycles/hum_alarm): arrives in EVERY
            # HM v3.6 frame, it's reflected as-is (there's no "transition" to detect, unlike
            # alarm/low_batt). decoded is already available further below for the events payload;
            # it's pulled forward here to avoid reading it twice.
            decoded_for_runtime = parsed.raw_payload.get("decoded", {})

            # UPSERT of the live state; *_changed_at only when that flag changed
            values = {
                "device_id": device.id, "alarm": new_alarm, "low_batt": new_low_batt,
                "measure_cycles": decoded_for_runtime.get("measure_cycles"),
                "send_cycles": decoded_for_runtime.get("send_cycles"),
                "hum_alarm_threshold": decoded_for_runtime.get("hum_alarm"),
                "last_seen": now,
            }
            if alarm_changed:
                values["alarm_changed_at"] = now
            if low_batt_changed:
                values["low_batt_changed_at"] = now
            set_ = {k: v for k, v in values.items() if k != "device_id"}
            await session.execute(
                pg_insert(DeviceRuntime).values(**values).on_conflict_do_update(
                    index_elements=["device_id"], set_=set_
                )
            )

            decoded = decoded_for_runtime
            base_payload = {
                "gateway_serial": parsed.gateway_serial,
                "source_ts": source_ts.isoformat(),
            }

            def emit(event_type: str, severity: str, extra: dict):
                session.add(FleetEvent(
                    id=uuid_module.uuid4(),
                    occurred_at=now,
                    event_type=event_type,
                    entity_type="device",
                    entity_id=device.id,
                    serial_number=parsed.serial_number,
                    severity=severity,
                    payload={**base_payload, **extra},
                ))

            if alarm_changed:
                if new_alarm:
                    emit("sensor.humidity_alarm", "critical", {
                        "humidity_adc": decoded.get("humidity_adc"),
                        "humidity_threshold_adc": decoded.get("hum_alarm"),
                    })
                else:
                    emit("sensor.humidity_alarm_cleared", "info", {})
            if low_batt_changed:
                if new_low_batt:
                    emit("sensor.low_battery", "warning", {"vcc_mv": decoded.get("vcc_mv")})
                else:
                    emit("sensor.low_battery_cleared", "info", {})

        except Exception as e:  # noqa: BLE001 — events must not break ingestion
            logger.error(f"alarm-state processing failed for {parsed.serial_number}: {e}")

    async def _process_sensor_data(
        self,
        session: AsyncSession,
        device: Device,
        sensor_data: ParsedSensorData,
        serial_number: str,
    ):
        """Processes the data for an individual sensor."""
        # Get or create sensor
        sensor = await self._get_or_create_sensor(
            session,
            device.id,
            sensor_data.sensor_index,
            sensor_data.sensor_type,
            node_id=sensor_data.node_id,
            sensor_key=sensor_data.sensor_key,
        )

        # Insert measurement (ON CONFLICT DO NOTHING for duplicates)
        measurement_stmt = insert(Measurement).values(
            sensor_id=sensor.id,
            ts=sensor_data.timestamp,
            temperature_c=sensor_data.temperature_c,
            humidity_pct=sensor_data.humidity_pct,
            voltage_cond_v=sensor_data.voltage_cond_v,
            supply_mv=sensor_data.supply_mv,
            msg_counter=sensor_data.msg_counter,
        ).on_conflict_do_nothing()
        await session.execute(measurement_stmt)

        # Record device status if status data is present
        if any([
            sensor_data.rssi_dbm is not None,
            sensor_data.buffer_used is not None,
            sensor_data.buffer_total is not None,
        ]):
            status_stmt = insert(DeviceStatus).values(
                device_id=device.id,
                ts=sensor_data.timestamp,
                rssi_dbm=sensor_data.rssi_dbm,
                buffer_used=sensor_data.buffer_used,
                buffer_total=sensor_data.buffer_total,
                supply_mv=sensor_data.supply_mv,
            ).on_conflict_do_nothing()
            await session.execute(status_stmt)

        MEASUREMENTS_INSERTED.labels(
            device_serial=serial_number,
            sensor_index=str(sensor_data.sensor_index)
        ).inc()

    async def _get_or_create_device(
        self, session: AsyncSession, serial_number: str
    ) -> Device:
        """Gets an existing device or creates it (idempotent under concurrency)."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        new_id = uuid_module.uuid4()
        stmt = (
            pg_insert(Device)
            .values(id=new_id, serial_number=serial_number)
            .on_conflict_do_nothing(index_elements=["serial_number"])
            .returning(Device.id)
        )
        result = await session.execute(stmt)
        row = result.fetchone()
        if row:
            logger.info(f"New device registered: {serial_number}")
            session.add(FleetEvent(
                id=uuid_module.uuid4(),
                occurred_at=datetime.now(timezone.utc),
                event_type="device.registered",
                entity_type="device",
                entity_id=row[0],
                serial_number=serial_number,
            ))
            return Device(id=row[0], serial_number=serial_number)
        device_result = await session.execute(
            select(Device).where(Device.serial_number == serial_number)
        )
        return cast(Device, device_result.scalar_one())

    async def _get_or_create_sensor(
        self,
        session: AsyncSession,
        device_id: UUID,
        sensor_index: int,
        sensor_type: str | None = None,
        node_id: int | None = None,
        sensor_key: str | None = None,
    ) -> Sensor:
        """Gets an existing sensor or creates it (idempotent under concurrency)."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        stmt = (
            pg_insert(Sensor)
            .values(
                id=uuid_module.uuid4(),
                device_id=device_id,
                sensor_index=sensor_index,
                sensor_type=sensor_type,
                node_id=node_id,
                sensor_key=sensor_key,
            )
            .on_conflict_do_nothing(index_elements=["device_id", "sensor_index"])
            .returning(Sensor.id, Sensor.sensor_type)
        )
        result = await session.execute(stmt)
        row = result.fetchone()
        if row:
            return Sensor(
                id=row[0], device_id=device_id, sensor_index=sensor_index, sensor_type=row[1]
            )
        result = await session.execute(
            select(Sensor).where(Sensor.device_id == device_id, Sensor.sensor_index == sensor_index)
        )
        sensor = result.scalar_one()
        if sensor_type and sensor.sensor_type is None:
            sensor.sensor_type = sensor_type
            await session.flush()
        return sensor

    async def _get_or_create_gateway(
        self, session: AsyncSession, serial_number: str
    ) -> Gateway:
        """Gets an existing gateway or creates it in core.gateway (idempotent)."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        new_id = uuid_module.uuid4()
        stmt = (
            pg_insert(Gateway)
            .values(id=new_id, serial_number=serial_number)
            .on_conflict_do_nothing(index_elements=["serial_number"])
            .returning(Gateway.id)
        )
        result = await session.execute(stmt)
        row = result.fetchone()
        if row:
            logger.info(f"New gateway registered: {serial_number}")
            session.add(FleetEvent(
                id=uuid_module.uuid4(),
                occurred_at=datetime.now(timezone.utc),
                event_type="gateway.registered",
                entity_type="gateway",
                entity_id=row[0],
                serial_number=serial_number,
            ))
            return Gateway(id=row[0], serial_number=serial_number)
        gateway_result = await session.execute(
            select(Gateway).where(Gateway.serial_number == serial_number)
        )
        return cast(Gateway, gateway_result.scalar_one())

    async def ingest_gateway(self, parsed: ParsedGatewayStatus) -> None:
        """Persists a gateway heartbeat directly (unbuffered, idempotent)."""
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        async with self._gateway_semaphore:
            async with async_session() as session:
                try:
                    await self._get_or_create_gateway(session, parsed.serial_number)
                    stmt = (
                        pg_insert(GatewayStatus)
                        .values(
                            id=uuid_module.uuid4(),
                            serial_number=parsed.serial_number,
                            ts=parsed.timestamp,
                            csq=parsed.csq,
                            erf=parsed.erf,
                            rst=parsed.rst,
                            vgw=parsed.vgw,
                            raw_payload=parsed.raw_payload,
                        )
                        .on_conflict_do_nothing(index_elements=["serial_number", "ts"])
                    )
                    await session.execute(stmt)
                    await session.commit()
                    logger.debug(f"Gateway status persisted: {parsed.serial_number}")
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Error persisting gateway status: {e}")
                    raise

    async def ingest_gateway_config(
        self,
        serial_number: str,
        config_type: str,
        config: dict,
        raw: bytes,
    ) -> None:
        """Persists a gateway configuration snapshot (setConfig / activeConfig)."""
        async with self._gateway_semaphore:
            async with async_session() as session:
                try:
                    gw = await self._get_or_create_gateway(session, serial_number)
                    session.add(
                        GatewayConfig(
                            gateway_id=gw.id,
                            serial_number=serial_number,
                            config_type=config_type,
                            broker=config.get("broker"),
                            port=config.get("port"),
                            client_id=config.get("client_id"),
                            fw_type=config.get("fw_type"),
                            topic_prefix=config.get("topic_prefix"),
                            interval_s=config.get("interval_s"),
                            supply_mv=config.get("supply_mv"),
                            broker2=config.get("broker2"),
                            raw_bytes=raw,
                        )
                    )
                    await session.commit()
                    logger.debug(
                        f"Gateway config persisted: {serial_number} ({config_type})"
                    )
                except Exception as e:
                    await session.rollback()
                    logger.error(
                        f"Error persisting gateway config for {serial_number}: {e}"
                    )
                    raise

    _CONFIG_V3_SECTIONS = {"provision", "runtime", "lora", "connect", "system"}

    async def _merge_partial_config_v3(self, serial_number: str, partial: dict) -> dict:
        """Merges a partial ack (one or more sections) onto the gateway's existing v3 config.

        Shallow merge per section: each key in `partial` replaces that entire section in the
        existing config; sections not included in `partial` remain as they were. If there's no
        prior row (first contact over LTE, no full config yet), returns `partial` as-is —
        leaving an incomplete config until the remaining sections arrive.
        """
        async with async_session() as session:
            row = (
                await session.execute(
                    text(
                        "SELECT config FROM monitoring.gateway_config_v3 WHERE serial_number = :sn"
                    ),
                    {"sn": serial_number},
                )
            ).one_or_none()
        existing = row.config if row is not None else {}
        return {**existing, **partial}

    async def ingest_gateway_config_v3(self, serial_number: str, ack: dict) -> None:
        """Persists the v3 config from the gw_get ACK: "current" UPSERT (1 row/gw) +
        hash-based dedup.

        - Canonical config_hash (sort_keys + separators + ensure_ascii=False) -> deterministic.
        - Idempotent UPSERT in 2 statements:
          (1) ON CONFLICT DO UPDATE ... WHERE config_hash IS DISTINCT FROM EXCLUDED -> doesn't
              rewrite if unchanged; if changed, advances config_updated_at.
          (2) if (1) affected no rows (identical config) -> UPDATE only received_at/gateway_ts_ms.
        - Partial ack (requested with `section`, LTE): `config` carries only that section (e.g.
          {"lora": {...}}) instead of all 5. It's merged onto the existing row (shallow merge per
          section) so as not to overwrite/clear the sections not requested — see the gateway
          command docs ("On LTE ... request section separately") and GatewayCommandRequest.section.
        """
        config = ack.get("config")
        if not isinstance(config, dict):
            logger.warning(
                f"gw_get ack has no valid config from gw {serial_number}, ignoring",
                extra={"device_serial": serial_number},
            )
            return

        if set(config) and not self._CONFIG_V3_SECTIONS <= set(config):
            config = await self._merge_partial_config_v3(serial_number, config)

        canonical = json.dumps(
            config, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        config_hash = hashlib.sha256(canonical.encode()).digest()
        request_id = ack.get("id")
        gateway_ts_ms = ack.get("ts_ms")
        fw = (config.get("system") or {}).get("fw")
        net_iface = (config.get("connect") or {}).get("net_iface")

        async with self._gateway_semaphore:
            async with async_session() as session:
                try:
                    gw = await self._get_or_create_gateway(session, serial_number)
                    params = {
                        "gid": gw.id,
                        "serial": serial_number,
                        "config": json.dumps(config),
                        "raw": json.dumps(ack),
                        "hash": config_hash,
                        "rid": request_id,
                        "gts": gateway_ts_ms,
                        "fw": fw,
                        "iface": net_iface,
                    }
                    # (1) insert/update only if the config changed (different hash). The WHERE
                    #     avoids rewriting the row when nothing changed.
                    upd = await session.execute(
                        text("""
                            INSERT INTO monitoring.gateway_config_v3
                                (gateway_id, serial_number, config, raw_payload, config_hash,
                                 request_id, gateway_ts_ms, fw, net_iface)
                            VALUES (:gid, :serial, cast(:config as jsonb), cast(:raw as jsonb),
                                    :hash, :rid, :gts, :fw, :iface)
                            ON CONFLICT (gateway_id) DO UPDATE SET
                                serial_number = EXCLUDED.serial_number,
                                config        = EXCLUDED.config,
                                raw_payload   = EXCLUDED.raw_payload,
                                config_hash   = EXCLUDED.config_hash,
                                request_id    = EXCLUDED.request_id,
                                gateway_ts_ms = EXCLUDED.gateway_ts_ms,
                                fw            = EXCLUDED.fw,
                                net_iface     = EXCLUDED.net_iface,
                                config_updated_at = now(),
                                received_at   = now()
                            WHERE monitoring.gateway_config_v3.config_hash
                                  IS DISTINCT FROM EXCLUDED.config_hash
                        """),
                        params,
                    )
                    # (2) identical config (rowcount 0 and it already existed) -> just refresh
                    #     receipt.
                    upd = cast(CursorResult, upd)
                    if upd.rowcount == 0:
                        await session.execute(
                            text("""
                                UPDATE monitoring.gateway_config_v3
                                SET received_at = now(), gateway_ts_ms = :gts, request_id = :rid
                                WHERE gateway_id = :gid
                            """),
                            {"gid": gw.id, "gts": gateway_ts_ms, "rid": request_id},
                        )
                    await session.commit()
                    logger.info(
                        f"Gateway config v3 persisted: {serial_number} "
                        f"(changed={upd.rowcount > 0})",
                        extra={"device_serial": serial_number},
                    )
                except Exception as e:
                    await session.rollback()
                    logger.error(
                        f"Error persisting gateway config v3 for {serial_number}: {e}",
                        extra={"device_serial": serial_number},
                    )
                    raise

    # Severity by gateway event type (everything else -> info).
    _GW_EVENT_WARNING_TYPES = {"sd_fail", "sd_space_low"}

    async def ingest_gateway_event(self, serial_number: str, event: dict) -> None:
        """Persists a gateway event into monitoring.event (event_type = gateway.<type>).

        Stores the full raw event in payload (the firmware may add reason/boot_reason).
        Severity: sd_fail/sd_space_low -> warning; everything else -> info.
        """
        etype = event.get("type")
        if not etype:
            logger.warning(
                f"gateway event with no 'type' from gw {serial_number}, ignoring",
                extra={"device_serial": serial_number},
            )
            return
        severity = "warning" if etype in self._GW_EVENT_WARNING_TYPES else "info"
        async with self._gateway_semaphore:
            async with async_session() as session:
                try:
                    # ensure the gateway exists (FK / entity_id of the event)
                    await self._get_or_create_gateway(session, serial_number)
                    await session.execute(
                        text("""
                            INSERT INTO monitoring.event
                                (id, occurred_at, event_type, entity_type, entity_id,
                                 serial_number, severity, payload)
                            SELECT :id, now(), :event_type, 'gateway', g.id,
                                   :serial, :severity, cast(:payload as jsonb)
                            FROM core.gateway g WHERE g.serial_number = :serial
                        """),
                        {
                            "id": str(uuid_module.uuid4()),
                            "event_type": f"gateway.{etype}",
                            "serial": serial_number,
                            "severity": severity,
                            "payload": json.dumps(event),
                        },
                    )
                    await session.commit()
                    logger.info(
                        f"Gateway event persisted: {serial_number} gateway.{etype} ({severity})",
                        extra={"device_serial": serial_number},
                    )
                except Exception as e:
                    await session.rollback()
                    logger.error(
                        f"Error persisting gateway event for {serial_number}: {e}",
                        extra={"device_serial": serial_number},
                    )
                    raise


# Service singleton
ingestion_service = IngestionService()
