"""
CRUD endpoints for devices (dashboard).
"""

import csv
import io
from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..auth_jwt import require_admin, require_any_role
from ..database import get_db
from ..models import (
    Device,
    DeviceRuntime,
    DeviceStatus,
    Gateway,
    Measurement,
    Sensor,
    TelemetryPayloadRaw,
    User,
)
from ..schemas import (
    DeviceCreate,
    DeviceListResponse,
    DeviceResponse,
    DeviceStats,
    DeviceStatusResponse,
    DeviceUpdate,
    GroupedMeasurementResponse,
    MeasurementResponse,
    SensorMeasurement,
    SensorResponse,
    SensorUpdate,
    SeqGap,
    SeqGapsResponse,
)
from ..services.calibration import (
    apply_calibration,
    resolve_active_calibrations_for_sensors,
)

router = APIRouter(prefix="/devices", tags=["devices"])


@router.get("/stats", response_model=DeviceStats)
async def get_device_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_any_role),
):
    """Get overall device statistics."""
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    last_24h = now - timedelta(hours=24)

    # Total devices
    total_devices_result = await db.execute(select(func.count(Device.id)))
    total_devices = total_devices_result.scalar() or 0

    # Total sensors
    total_sensors_result = await db.execute(select(func.count(Sensor.id)))
    total_sensors = total_sensors_result.scalar() or 0

    # Total measurements
    total_measurements_result = await db.execute(select(func.count()).select_from(Measurement))
    total_measurements = total_measurements_result.scalar() or 0

    # Measurements today
    measurements_today_result = await db.execute(
        select(func.count()).select_from(Measurement).where(Measurement.ts >= today_start)
    )
    measurements_today = measurements_today_result.scalar() or 0

    # Active devices (with data in last 24h)
    active_devices_subq = (
        select(Sensor.device_id)
        .join(Measurement, Measurement.sensor_id == Sensor.id)
        .where(Measurement.ts >= last_24h)
        .distinct()
    )
    active_devices_result = await db.execute(
        select(func.count()).select_from(active_devices_subq.subquery())
    )
    active_devices = active_devices_result.scalar() or 0

    return DeviceStats(
        total_devices=total_devices,
        active_devices=active_devices,
        total_sensors=total_sensors,
        total_measurements=total_measurements,
        measurements_today=measurements_today,
    )


@router.get("", response_model=list[DeviceListResponse])
async def list_devices(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_any_role),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
):
    """List all devices with basic info."""
    result = await db.execute(
        select(Device, Gateway.serial_number.label("gateway_serial"))
        .outerjoin(Gateway, Gateway.id == Device.gateway_id)
        .options(selectinload(Device.sensors))
        .order_by(Device.created_at.desc())
        .offset(skip)
        .limit(limit)
    )
    rows = result.all()

    response = []
    for device, gateway_serial in rows:
        last_seen_result = await db.execute(
            select(func.max(Measurement.ts))
            .join(Sensor, Sensor.id == Measurement.sensor_id)
            .where(Sensor.device_id == device.id)
        )
        last_seen = last_seen_result.scalar()

        meta = device.metadata_ or {}
        response.append(
            DeviceListResponse(
                id=device.id,
                serial_number=device.serial_number,
                display_name=meta.get("display_name") or None,
                created_at=device.created_at,
                sensor_count=len(device.sensors),
                last_seen=last_seen,
                gateway_serial=gateway_serial,
            )
        )

    return response


@router.get("/{device_id}", response_model=DeviceResponse)
async def get_device(
    device_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_any_role),
):
    """Get device details."""
    result = await db.execute(
        select(
            Device,
            Gateway.serial_number.label("gateway_serial"),
            func.max(Measurement.ts).label("last_seen"),
        )
        .options(selectinload(Device.sensors))
        .outerjoin(Gateway, Gateway.id == Device.gateway_id)
        .outerjoin(Sensor, Sensor.device_id == Device.id)
        .outerjoin(Measurement, Measurement.sensor_id == Sensor.id)
        .where(Device.id == device_id)
        .group_by(Device.id, Gateway.serial_number)
    )
    row = result.one_or_none()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found",
        )

    device, gateway_serial, last_seen = row

    runtime = (
        await db.execute(
            select(
                DeviceRuntime.measure_cycles,
                DeviceRuntime.send_cycles,
                DeviceRuntime.hum_alarm_threshold,
            ).where(DeviceRuntime.device_id == device_id)
        )
    ).one_or_none()

    return DeviceResponse.from_orm_model(
        device,
        gateway_serial=gateway_serial,
        last_seen=last_seen,
        measure_cycles=runtime.measure_cycles if runtime else None,
        send_cycles=runtime.send_cycles if runtime else None,
        hum_alarm_threshold=runtime.hum_alarm_threshold if runtime else None,
    )


@router.post("", response_model=DeviceResponse, status_code=status.HTTP_201_CREATED)
async def create_device(
    device_data: DeviceCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    """Create a new device (admin only)."""
    # Check if serial number already exists
    result = await db.execute(
        select(Device).where(Device.serial_number == device_data.serial_number)
    )
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Serial number already exists",
        )

    device = Device(
        serial_number=device_data.serial_number,
        metadata_=device_data.metadata,
    )
    db.add(device)
    await db.flush()  # get device.id before inserting sensors

    if device_data.sensor_types:
        for index, sensor_type in enumerate(device_data.sensor_types):
            if sensor_type is not None:
                db.add(Sensor(
                    device_id=device.id,
                    sensor_index=index,
                    sensor_type=sensor_type,
                ))

    await db.commit()
    # Reload with sensors relationship
    result = await db.execute(
        select(Device)
        .options(selectinload(Device.sensors))
        .where(Device.id == device.id)
    )
    device = result.scalar_one()

    return DeviceResponse.from_orm_model(device)


@router.patch("/{device_id}", response_model=DeviceResponse)
async def update_device(
    device_id: UUID,
    device_data: DeviceUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    """Update a device (admin only)."""
    result = await db.execute(
        select(Device)
        .options(selectinload(Device.sensors))
        .where(Device.id == device_id)
    )
    device = result.scalar_one_or_none()

    if device is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found",
        )

    # Merge display_name into metadata JSONB without clobbering other keys
    if device_data.display_name is not None or device_data.metadata is not None:
        current = dict(device.metadata_ or {})
        if device_data.metadata is not None:
            current.update(device_data.metadata)
        if device_data.display_name is not None:
            current["display_name"] = device_data.display_name
        device.metadata_ = current

    await db.commit()
    await db.refresh(device)

    return DeviceResponse.from_orm_model(device)


@router.patch("/{device_id}/sensors/{sensor_id}", response_model=SensorResponse)
async def update_sensor(
    device_id: UUID,
    sensor_id: UUID,
    sensor_data: SensorUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    """Update a sensor's type mapping (admin only)."""
    result = await db.execute(
        select(Sensor).where(Sensor.id == sensor_id, Sensor.device_id == device_id)
    )
    sensor = result.scalar_one_or_none()

    if sensor is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Sensor not found",
        )

    sensor.sensor_type = sensor_data.sensor_type
    await db.commit()
    await db.refresh(sensor)

    return SensorResponse.model_validate(sensor)


@router.delete("/{device_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_device(
    device_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    """Delete a device and all its data (admin only)."""
    result = await db.execute(select(Device).where(Device.id == device_id))
    device = result.scalar_one_or_none()

    if device is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found",
        )

    # Delete measurements first (via sensors)
    sensors_result = await db.execute(
        select(Sensor.id).where(Sensor.device_id == device_id)
    )
    sensor_ids = [s[0] for s in sensors_result.all()]

    if sensor_ids:
        await db.execute(
            delete(Measurement).where(Measurement.sensor_id.in_(sensor_ids))
        )

    # Delete device status
    await db.execute(delete(DeviceStatus).where(DeviceStatus.device_id == device_id))

    # Delete sensors
    await db.execute(delete(Sensor).where(Sensor.device_id == device_id))

    # Delete raw telemetry payloads
    await db.execute(delete(TelemetryPayloadRaw).where(TelemetryPayloadRaw.device_id == device_id))

    # Delete device
    await db.delete(device)
    await db.commit()


@router.get("/{device_id}/export")
async def export_device(
    device_id: UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
):
    """Exports all of a device's data as a JSON backup."""
    result = await db.execute(
        select(Device).options(selectinload(Device.sensors)).where(Device.id == device_id)
    )
    device = result.scalar_one_or_none()
    if device is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Device not found")

    status_result = await db.execute(
        select(DeviceStatus)
        .where(DeviceStatus.device_id == device_id)
        .order_by(DeviceStatus.ts.asc())
    )
    status_rows = status_result.scalars().all()

    return {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "device": {
            "id": str(device.id),
            "serial_number": device.serial_number,
            "created_at": device.created_at.isoformat(),
            "metadata": device.metadata_,
        },
        "sensors": [
            {
                "id": str(s.id),
                "sensor_index": s.sensor_index,
                "sensor_type": s.sensor_type,
            }
            for s in device.sensors
        ],
        "status_history": [
            {
                "ts": s.ts.isoformat(),
                "rssi_dbm": s.rssi_dbm,
                "buffer_used": s.buffer_used,
                "buffer_total": s.buffer_total,
            }
            for s in status_rows
        ],
    }


@router.get("/{device_id}/measurements/export")
async def export_device_measurements(
    device_id: UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
    hours: int | None = Query(None, ge=1, le=8760, description="Hours of data (default: all)"),
):
    """Exports a device's raw measurements as a streaming CSV."""
    result = await db.execute(
        select(Device).where(Device.id == device_id)
    )
    device = result.scalar_one_or_none()
    if device is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Device not found")

    since_clause = ""
    params: dict = {"device_id": str(device_id)}
    if hours is not None:
        since_clause = "AND m.ts >= NOW() - INTERVAL ':hours hours'"
        params["hours"] = hours

    query = text(f"""
        SELECT
            m.ts,
            d.serial_number,
            s.id AS sensor_id,
            s.sensor_index,
            s.sensor_key,
            m.temperature_c,
            m.humidity_pct,
            m.voltage_cond_v,
            m.supply_mv,
            m.msg_counter
        FROM telemetry.measurement m
        JOIN core.sensor s ON s.id = m.sensor_id
        JOIN core.device d ON d.id = s.device_id
        WHERE d.id = :device_id::uuid
        {since_clause}
        ORDER BY m.ts ASC
    """)

    # Active calibrations for the device's sensors -> chp column.
    device_sensor_ids = (
        await db.execute(select(Sensor.id).where(Sensor.device_id == device_id))
    ).scalars().all()
    calibs = await resolve_active_calibrations_for_sensors(db, list(device_sensor_ids))

    async def generate():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "ts", "serial_number", "sensor_index", "sensor_key",
            "temperature_c", "humidity_pct", "chp", "voltage_cond_v", "supply_mv", "msg_counter",
        ])
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)

        rows = await db.execute(query, params)
        for row in rows:
            chp = None
            calib = calibs.get(row.sensor_id)
            if calib is not None and row.humidity_pct is not None:
                chp, _ = apply_calibration(
                    float(row.humidity_pct), calib.m, calib.c, calib.ch_min, calib.ch_max
                )
            writer.writerow([
                row.ts.isoformat(),
                row.serial_number,
                row.sensor_index,
                row.sensor_key,
                row.temperature_c,
                row.humidity_pct,
                chp,
                row.voltage_cond_v,
                row.supply_mv,
                row.msg_counter,
            ])
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    filename = f"{device.serial_number}_measurements.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/{device_id}/measurements", response_model=list[MeasurementResponse])
async def get_device_measurements(
    device_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_any_role),
    sensor_index: int | None = Query(None, description="Filter by sensor index"),
    hours: int | None = Query(
        None, ge=1, le=168, description="Hours of data to fetch (ignored if start_date provided)"
    ),
    start_date: datetime | None = Query(None, description="Start datetime (ISO format)"),
    end_date: datetime | None = Query(None, description="End datetime (ISO format)"),
    limit: int = Query(1000, ge=1, le=10000),
):
    """Get measurements for a device."""
    # Verify device exists
    device_result = await db.execute(select(Device).where(Device.id == device_id))
    if device_result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found",
        )

    # Determine time range
    if start_date is not None:
        since = start_date
        until = end_date
    else:
        effective_hours = hours if hours is not None else 24
        since = datetime.now(timezone.utc) - timedelta(hours=effective_hours)
        until = None

    query = (
        select(Measurement)
        .join(Sensor, Sensor.id == Measurement.sensor_id)
        .where(Sensor.device_id == device_id)
        .where(Measurement.ts >= since)
        .order_by(Measurement.ts.desc())
        .limit(limit)
    )
    if until is not None:
        query = query.where(Measurement.ts <= until)

    if sensor_index is not None:
        query = query.where(Sensor.sensor_index == sensor_index)

    result = await db.execute(query)
    measurements = result.scalars().all()

    # Active calibration per sensor -> computed chp (adc/humidity_pct is left untouched).
    calibs = await resolve_active_calibrations_for_sensors(
        db, list({m.sensor_id for m in measurements})
    )
    out: list[MeasurementResponse] = []
    for m in measurements:
        chp, extra = None, False
        calib = calibs.get(m.sensor_id)
        if calib is not None and m.humidity_pct is not None:
            chp, extra = apply_calibration(
                float(m.humidity_pct), calib.m, calib.c, calib.ch_min, calib.ch_max
            )
        out.append(
            MeasurementResponse(
                ts=m.ts,
                temperature_c=m.temperature_c,
                humidity_pct=m.humidity_pct,
                voltage_cond_v=m.voltage_cond_v,
                supply_mv=m.supply_mv,
                msg_counter=m.msg_counter,
                chp=chp,
                chp_extrapolated=extra,
            )
        )
    return out


@router.get("/{device_id}/status", response_model=list[DeviceStatusResponse])
async def get_device_status_history(
    device_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_any_role),
    hours: int | None = Query(None, ge=1, le=168),
    start_date: datetime | None = Query(None, description="Start datetime (ISO format)"),
    end_date: datetime | None = Query(None, description="End datetime (ISO format)"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get status history for a device."""
    # Verify device exists
    result = await db.execute(select(Device).where(Device.id == device_id))
    if result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found",
        )

    # Determine time range
    if start_date is not None:
        since = start_date
        until = end_date
    else:
        effective_hours = hours if hours is not None else 24
        since = datetime.now(timezone.utc) - timedelta(hours=effective_hours)
        until = None

    query = (
        select(DeviceStatus)
        .where(DeviceStatus.device_id == device_id)
        .where(DeviceStatus.ts >= since)
        .order_by(DeviceStatus.ts.desc())
        .limit(limit)
    )
    if until is not None:
        query = query.where(DeviceStatus.ts <= until)

    result = await db.execute(query)
    return result.scalars().all()


@router.get("/{device_id}/seq-gaps", response_model=SeqGapsResponse)
async def get_device_seq_gaps(
    device_id: UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_any_role),
    hours: int = Query(24, ge=1, le=168),
    max_gap: int = Query(
        100, ge=2, le=65535,
        description="Seq jumps >= max_gap are discarded as node reset/wraparound, not as "
                    "loss. Default 100: losing >100 packets in a row is usually a reset or "
                    "a long disconnection, not a one-off loss.",
    ),
):
    """Detects missing packets (gaps in the node_seq) for a device over a window.

    The node_seq (msg_counter) is ordered by VALUE, not by ts: the node resends from its FIFO
    out of order, so ordering by arrival time would produce false gaps. Negative jumps (node
    reset) and jumps >= max_gap (reset / uint16 wraparound) are discarded as noise, not as loss.
    """
    dev = (
        await db.execute(select(Device).where(Device.id == device_id))
    ).scalar_one_or_none()
    if dev is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Device not found")

    # LAG over msg_counter ordered by value; slot 0 (the seq belongs to the frame, one sensor
    # is enough).
    rows = (
        await db.execute(
            text("""
                WITH seqs AS (
                    SELECT m.msg_counter AS seq, m.ts,
                           LAG(m.msg_counter) OVER (ORDER BY m.msg_counter) AS prev_seq
                    FROM telemetry.measurement m
                    JOIN core.sensor s ON s.id = m.sensor_id
                    WHERE s.device_id = :dev
                      AND s.sensor_index = 0
                      AND m.msg_counter IS NOT NULL
                      AND m.ts > now() - make_interval(hours => :hours)
                )
                SELECT seq, prev_seq, ts FROM seqs ORDER BY seq
            """),
            {"dev": str(device_id), "hours": hours},
        )
    ).all()

    total = len(rows)
    first_seq = rows[0].seq if rows else None
    last_seq = rows[-1].seq if rows else None

    gaps: list[SeqGap] = []
    missing_total = 0
    for r in rows:
        if r.prev_seq is None:
            continue
        delta = r.seq - r.prev_seq
        if 1 < delta < max_gap:  # real gap (discards reset/wrap above the threshold)
            miss = delta - 1
            missing_total += miss
            gaps.append(SeqGap(after_seq=r.prev_seq, before_seq=r.seq, missing=miss, at=r.ts))

    # completeness: packets received / expected (received + missing), as a %.
    completeness = round(total / (total + missing_total) * 100, 2) if total else None
    gaps.sort(key=lambda g: g.at, reverse=True)

    return SeqGapsResponse(
        device_id=str(device_id),
        serial_number=dev.serial_number,
        hours=hours,
        total_packets=total,
        first_seq=first_seq,
        last_seq=last_seq,
        missing_total=missing_total,
        completeness_pct=completeness,
        gaps=gaps,
    )


@router.get("/{device_id}/measurements/grouped", response_model=list[GroupedMeasurementResponse])
async def get_device_measurements_grouped(
    device_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_any_role),
    hours: int | None = Query(None, ge=1, le=168, description="Hours of data to fetch"),
    start_date: datetime | None = Query(None, description="Start datetime (ISO format)"),
    end_date: datetime | None = Query(None, description="End datetime (ISO format)"),
    limit: int = Query(500, ge=1, le=5000),
):
    """
    Get measurements grouped by timestamp.

    Returns one entry per timestamp with all sensor values for that moment.
    """
    # Verify device exists
    result = await db.execute(select(Device).where(Device.id == device_id))
    if result.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Device not found",
        )

    # Determine time range
    if start_date is not None:
        since = start_date
        until = end_date
    else:
        effective_hours = hours if hours is not None else 24
        since = datetime.now(timezone.utc) - timedelta(hours=effective_hours)
        until = None

    # Get measurements with sensor info
    query = (
        select(Measurement, Sensor.sensor_index)
        .join(Sensor, Sensor.id == Measurement.sensor_id)
        .where(Sensor.device_id == device_id)
        .where(Measurement.ts >= since)
        .order_by(Measurement.ts.desc())
    )
    if until is not None:
        query = query.where(Measurement.ts <= until)

    result = await db.execute(query)
    rows = result.all()

    # Active calibration per sensor -> chp computed per measurement.
    calibs = await resolve_active_calibrations_for_sensors(
        db, list({m.sensor_id for m, _ in rows})
    )

    # Group by timestamp (truncated to second)
    from collections import defaultdict
    grouped: dict[datetime, list[SensorMeasurement]] = defaultdict(list)

    for measurement, sensor_index in rows:
        humidity = float(measurement.humidity_pct) if measurement.humidity_pct else None
        chp, extra = None, False
        calib = calibs.get(measurement.sensor_id)
        if calib is not None and humidity is not None:
            chp, extra = apply_calibration(
                humidity, calib.m, calib.c, calib.ch_min, calib.ch_max
            )
        # Truncate to second for grouping
        ts_key = measurement.ts.replace(microsecond=0)
        grouped[ts_key].append(SensorMeasurement(
            sensor_index=sensor_index,
            temperature_c=float(measurement.temperature_c) if measurement.temperature_c else None,
            humidity_pct=humidity,
            voltage_cond_v=float(measurement.voltage_cond_v)
            if measurement.voltage_cond_v
            else None,
            supply_mv=measurement.supply_mv,
            msg_counter=measurement.msg_counter,
            chp=chp,
            chp_extrapolated=extra,
        ))

    # Sort by timestamp desc and limit
    sorted_timestamps = sorted(grouped.keys(), reverse=True)[:limit]

    return [
        GroupedMeasurementResponse(
            ts=ts,
            sensors=sorted(grouped[ts], key=lambda s: s.sensor_index)
        )
        for ts in sorted_timestamps
    ]
