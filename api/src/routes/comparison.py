"""
CRUD endpoints for the Comparisons section: sensor groups (one tab per group),
their manual reference values, and the overlaid series per metric.

Mirrors the calibration pattern (entity + N:N relation to sensors). The series endpoint
(POST /comparisons/series) reuses the same %CH logic as
GET /devices/{id}/measurements/grouped (services.calibration).
"""

from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..auth_jwt import require_comparison_access
from ..database import get_db
from ..models import (
    ComparisonGroup,
    ComparisonGroupSensor,
    Device,
    Gateway,
    Measurement,
    Sensor,
    User,
)
from ..schemas import (
    ComparisonGroupCreate,
    ComparisonGroupResponse,
    LatestRequest,
    LatestResponse,
    LatestSensorValue,
    SeriesRequest,
    SeriesResponse,
    SeriesSensorMeta,
)
from ..schemas.comparison import SensorSeries
from ..services.calibration import (
    apply_calibration,
    resolve_active_calibrations_for_sensors,
)

router = APIRouter(prefix="/comparisons", tags=["comparisons"])


def _to_response(group: ComparisonGroup) -> ComparisonGroupResponse:
    return ComparisonGroupResponse(
        id=group.id,
        name=group.name,
        position=group.position,
        sensor_ids=[link.sensor_id for link in group.sensor_links],
        reference_points=group.reference_points,
        target_min=group.target_min,
        target_max=group.target_max,
        created_at=group.created_at,
    )


async def _load(db: AsyncSession, group_id: UUID) -> ComparisonGroup:
    result = await db.execute(
        select(ComparisonGroup)
        .options(selectinload(ComparisonGroup.sensor_links))
        .where(ComparisonGroup.id == group_id)
    )
    group = result.scalar_one_or_none()
    if group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Comparison group not found"
        )
    return group


async def _validate_sensors(db: AsyncSession, sensor_ids: list[UUID]) -> None:
    if not sensor_ids:
        return
    found = (
        await db.execute(select(Sensor.id).where(Sensor.id.in_(sensor_ids)))
    ).scalars().all()
    missing = set(sensor_ids) - set(found)
    if missing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Nonexistent sensors: {sorted(str(m) for m in missing)}",
        )


@router.get("", response_model=list[ComparisonGroupResponse])
async def list_groups(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_comparison_access),
):
    result = await db.execute(
        select(ComparisonGroup)
        .options(selectinload(ComparisonGroup.sensor_links))
        .order_by(ComparisonGroup.position, ComparisonGroup.created_at)
    )
    return [_to_response(g) for g in result.scalars().unique().all()]


@router.get("/sensors/available")
async def available_sensors(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_comparison_access),
):
    """All sensors (any type) for the group picker, grouped by gateway.

    Unlike the calibration picker (humidity only), here sensors of any metric are
    compared, so there's no filtering by sensor_type.
    """
    rows = (
        await db.execute(
            select(
                Sensor.id,
                Sensor.sensor_index,
                Sensor.sensor_type,
                Device.serial_number,
                Device.metadata_,
                Gateway.serial_number,
                Gateway.metadata_,
            )
            .join(Device, Device.id == Sensor.device_id)
            .outerjoin(Gateway, Gateway.id == Device.gateway_id)
            .order_by(Device.serial_number, Sensor.sensor_index)
        )
    ).all()
    return [
        {
            "sensor_id": str(sid),
            "sensor_index": idx,
            "sensor_type": stype,
            "device_serial": dev_serial,
            "device_name": (dev_meta or {}).get("display_name"),
            "gateway_serial": gw_serial,
            "gateway_name": (gw_meta or {}).get("display_name") if gw_meta else None,
        }
        for sid, idx, stype, dev_serial, dev_meta, gw_serial, gw_meta in rows
    ]


@router.post("/series", response_model=SeriesResponse)
async def get_series(
    data: SeriesRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_comparison_access),
):
    """Series per metric for a set of sensors (possibly from several devices).

    Returns, per sensor, [epoch_ms, value] arrays for temperature_c, humidity_pct (raw ADC)
    and chp (calibrated %CH). Serves both saved groups and selections being edited.
    """
    if not data.sensor_ids:
        return SeriesResponse(sensors=[], series={})

    # Sensor metadata. The label prioritizes the device name (we're comparing equipment);
    # it only appends #index if the device has more than one sensor of the same type in
    # the selection.
    meta_rows = (
        await db.execute(
            select(
                Sensor.id,
                Sensor.sensor_index,
                Sensor.sensor_type,
                Device.serial_number,
                Device.metadata_,
            )
            .join(Device, Device.id == Sensor.device_id)
            .where(Sensor.id.in_(data.sensor_ids))
            .order_by(Device.serial_number, Sensor.sensor_index)
        )
    ).all()
    # how many selected sensors are there per device? (to decide if the #index is needed)
    per_device: dict[str, int] = {}
    for _sid, _idx, _st, dev_serial, _meta in meta_rows:
        per_device[dev_serial] = per_device.get(dev_serial, 0) + 1
    sensors_meta = []
    for sid, idx, stype, dev_serial, dev_meta in meta_rows:
        name = (dev_meta or {}).get("display_name") or dev_serial
        label = name if per_device[dev_serial] == 1 else f"{name} · #{idx}"
        sensors_meta.append(
            SeriesSensorMeta(
                sensor_id=sid,
                sensor_index=idx,
                sensor_type=stype,
                device_serial=dev_serial,
                label=label,
            )
        )

    # Time range (same criteria as /measurements/grouped)
    if data.start_date is not None:
        since = data.start_date
        until = data.end_date
    else:
        effective_hours = data.hours if data.hours is not None else 24
        since = datetime.now(timezone.utc) - timedelta(hours=effective_hours)
        until = None

    query = (
        select(Measurement)
        .where(Measurement.sensor_id.in_(data.sensor_ids))
        .where(Measurement.ts >= since)
        .order_by(Measurement.ts.asc())
    )
    if until is not None:
        query = query.where(Measurement.ts <= until)

    rows = (await db.execute(query)).scalars().all()

    calibs = await resolve_active_calibrations_for_sensors(
        db, list({m.sensor_id for m in rows})
    )

    series: dict[UUID, SensorSeries] = {sid: SensorSeries() for sid in data.sensor_ids}
    for m in rows:
        s = series.get(m.sensor_id)
        if s is None:
            continue
        ms = int(m.ts.timestamp() * 1000)
        if m.temperature_c is not None:
            s.temperature_c.append((ms, float(m.temperature_c)))
        humidity = float(m.humidity_pct) if m.humidity_pct is not None else None
        if humidity is not None:
            s.humidity_pct.append((ms, humidity))
            calib = calibs.get(m.sensor_id)
            if calib is not None:
                chp, _ = apply_calibration(
                    humidity, calib.m, calib.c, calib.ch_min, calib.ch_max
                )
                if chp is not None:
                    s.chp.append((ms, chp))

    # Cap to the last `limit` samples per metric and per sensor (asc order -> tail).
    if data.limit and data.limit > 0:
        for s in series.values():
            s.temperature_c = s.temperature_c[-data.limit :]
            s.humidity_pct = s.humidity_pct[-data.limit :]
            s.chp = s.chp[-data.limit :]

    return SeriesResponse(sensors=sensors_meta, series=series)


@router.post("/latest", response_model=LatestResponse)
async def get_latest(
    data: LatestRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_comparison_access),
):
    """Latest reading for each sensor (independent of range), for the per-sensor gauge."""
    if not data.sensor_ids:
        return LatestResponse(latest={})

    # Metadata (label per equipment). Reuses the same criteria as /series.
    meta_rows = (
        await db.execute(
            select(
                Sensor.id,
                Sensor.sensor_index,
                Device.serial_number,
                Device.metadata_,
            )
            .join(Device, Device.id == Sensor.device_id)
            .where(Sensor.id.in_(data.sensor_ids))
        )
    ).all()
    meta = {
        sid: {
            "sensor_index": idx,
            "device_serial": dev_serial,
            "label": (dev_meta or {}).get("display_name") or dev_serial,
        }
        for sid, idx, dev_serial, dev_meta in meta_rows
    }

    # Latest measurement per sensor (DISTINCT ON (sensor_id) ORDER BY sensor_id, ts DESC).
    rows = (
        await db.execute(
            select(Measurement)
            .where(Measurement.sensor_id.in_(data.sensor_ids))
            .order_by(Measurement.sensor_id, Measurement.ts.desc())
            .distinct(Measurement.sensor_id)
        )
    ).scalars().all()

    calibs = await resolve_active_calibrations_for_sensors(
        db, list({m.sensor_id for m in rows})
    )

    latest: dict[UUID, LatestSensorValue] = {}
    for m in rows:
        info = meta.get(m.sensor_id)
        if info is None:
            continue
        humidity = float(m.humidity_pct) if m.humidity_pct is not None else None
        chp = None
        calib = calibs.get(m.sensor_id)
        if calib is not None and humidity is not None:
            chp, _ = apply_calibration(
                humidity, calib.m, calib.c, calib.ch_min, calib.ch_max
            )
        latest[m.sensor_id] = LatestSensorValue(
            sensor_id=m.sensor_id,
            label=info["label"],
            sensor_index=info["sensor_index"],
            device_serial=info["device_serial"],
            ts=m.ts,
            temperature_c=float(m.temperature_c) if m.temperature_c is not None else None,
            humidity_pct=humidity,
            chp=chp,
        )

    return LatestResponse(latest=latest)


@router.get("/{group_id}", response_model=ComparisonGroupResponse)
async def get_group(
    group_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_comparison_access),
):
    return _to_response(await _load(db, group_id))


@router.post("", response_model=ComparisonGroupResponse, status_code=status.HTTP_201_CREATED)
async def create_group(
    data: ComparisonGroupCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_comparison_access),
):
    await _validate_sensors(db, data.sensor_ids)

    if data.position is not None:
        position = data.position
    else:
        max_pos = (
            await db.execute(select(func.max(ComparisonGroup.position)))
        ).scalar()
        position = (max_pos + 1) if max_pos is not None else 0

    group = ComparisonGroup(
        name=data.name,
        position=position,
        target_min=data.target_min,
        target_max=data.target_max,
        reference_points=[rp.model_dump(mode="json") for rp in data.reference_points],
        created_by=user.id,
    )
    db.add(group)
    await db.flush()
    for sid in data.sensor_ids:
        db.add(ComparisonGroupSensor(group_id=group.id, sensor_id=sid))
    await db.commit()

    return _to_response(await _load(db, group.id))


@router.patch("/{group_id}", response_model=ComparisonGroupResponse)
async def update_group(
    group_id: UUID,
    data: ComparisonGroupCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_comparison_access),
):
    group = await _load(db, group_id)
    await _validate_sensors(db, data.sensor_ids)

    group.name = data.name
    group.target_min = data.target_min
    group.target_max = data.target_max
    group.reference_points = [rp.model_dump(mode="json") for rp in data.reference_points]
    if data.position is not None:
        group.position = data.position

    # Replace the sensor set by mutating the ORM collection (delete-orphan removes the old ones).
    # Don't use a bulk DELETE: with expire_on_commit=False it would leave the in-memory collection
    # stale, and the response would return the previous sensors even though the DB is correct.
    group.sensor_links.clear()
    await db.flush()  # apply the delete before reinserting (avoids PK clash with the same ids)
    for sid in data.sensor_ids:
        group.sensor_links.append(ComparisonGroupSensor(sensor_id=sid))

    await db.commit()
    return _to_response(await _load(db, group.id))


@router.delete("/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_group(
    group_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_comparison_access),
):
    group = await _load(db, group_id)
    await db.delete(group)  # cascade deletes comparison_group_sensor
    await db.commit()
