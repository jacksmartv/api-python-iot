"""
CRUD endpoints for humidity calibrations (ADC -> %CH).

One ACTIVE calibration per material (enforced by a partial unique index); previous
ones remain as history (is_active=false). The coefficients (m, c, R², validity
range) are computed by the backend when saving, over the valid points.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from ..auth_jwt import require_admin, require_calibration_read
from ..database import get_db
from ..models import Calibration, CalibrationSensor, Device, Gateway, Sensor, User
from ..schemas import CalibrationCreate, CalibrationResponse
from ..services.calibration import compute_regression

router = APIRouter(prefix="/calibrations", tags=["calibrations"])


def _to_response(calib: Calibration, warning: str | None = None) -> CalibrationResponse:
    return CalibrationResponse(
        id=calib.id,
        name=calib.name,
        material=calib.material,
        sensor_type=calib.sensor_type,
        sensor_ids=[link.sensor_id for link in calib.sensor_links],
        temperature_c=calib.temperature_c,
        reference=calib.reference,
        points=calib.points,
        m=calib.m,
        c=calib.c,
        r_squared=calib.r_squared,
        ch_min=calib.ch_min,
        ch_max=calib.ch_max,
        is_active=calib.is_active,
        created_at=calib.created_at,
        warning=warning,
    )


async def _load(db: AsyncSession, calibration_id: UUID) -> Calibration:
    result = await db.execute(
        select(Calibration)
        .options(selectinload(Calibration.sensor_links))
        .where(Calibration.id == calibration_id)
    )
    calib = result.scalar_one_or_none()
    if calib is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Calibration not found"
        )
    return calib


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


def _warning(low_r2: bool) -> str | None:
    return "R² < 0.98: the fit is poor, review the points." if low_r2 else None


@router.get("", response_model=list[CalibrationResponse])
async def list_calibrations(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_calibration_read),
    material: str | None = Query(None),
    active: bool | None = Query(None),
    sensor_id: UUID | None = Query(None),
):
    query = (
        select(Calibration)
        .options(selectinload(Calibration.sensor_links))
        .order_by(Calibration.created_at.desc())
    )
    if material is not None:
        query = query.where(Calibration.material == material)
    if active is not None:
        query = query.where(Calibration.is_active.is_(active))
    if sensor_id is not None:
        query = query.join(
            CalibrationSensor, CalibrationSensor.calibration_id == Calibration.id
        ).where(CalibrationSensor.sensor_id == sensor_id)

    result = await db.execute(query)
    return [_to_response(c) for c in result.scalars().unique().all()]


@router.get("/sensors/available")
async def available_sensors(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_calibration_read),
):
    """Wood moisture (humidity) sensors for the calibration form's picker.

    Includes the gateway the device is connected to (to filter/group by gateway).
    """
    rows = (
        await db.execute(
            select(
                Sensor.id,
                Sensor.sensor_index,
                Device.serial_number,
                Device.metadata_,
                Gateway.serial_number,
                Gateway.metadata_,
            )
            .join(Device, Device.id == Sensor.device_id)
            .outerjoin(Gateway, Gateway.id == Device.gateway_id)
            .where(Sensor.sensor_type.in_(["humidity", "h"]))
            .order_by(Device.serial_number, Sensor.sensor_index)
        )
    ).all()
    return [
        {
            "sensor_id": str(sid),
            "sensor_index": idx,
            "device_serial": dev_serial,
            "device_name": (dev_meta or {}).get("display_name"),
            "gateway_serial": gw_serial,
            "gateway_name": (gw_meta or {}).get("display_name") if gw_meta else None,
        }
        for sid, idx, dev_serial, dev_meta, gw_serial, gw_meta in rows
    ]


@router.get("/{calibration_id}", response_model=CalibrationResponse)
async def get_calibration(
    calibration_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_calibration_read),
):
    return _to_response(await _load(db, calibration_id))


@router.post("", response_model=CalibrationResponse, status_code=status.HTTP_201_CREATED)
async def create_calibration(
    data: CalibrationCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    try:
        reg = compute_regression([p.model_dump() for p in data.points])
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))

    await _validate_sensors(db, data.sensor_ids)

    # One active per material: deactivate the previous active one for this material.
    await db.execute(
        update(Calibration)
        .where(Calibration.material == data.material, Calibration.is_active.is_(True))
        .values(is_active=False)
    )

    calib = Calibration(
        name=data.name,
        material=data.material,
        sensor_type=data.sensor_type,
        temperature_c=data.temperature_c,
        reference=data.reference,
        points=reg.points,
        m=reg.m,
        c=reg.c,
        r_squared=reg.r_squared,
        ch_min=reg.ch_min,
        ch_max=reg.ch_max,
        is_active=True,
        created_by=user.id,
    )
    db.add(calib)
    await db.flush()
    for sid in data.sensor_ids:
        db.add(CalibrationSensor(calibration_id=calib.id, sensor_id=sid))
    await db.commit()

    return _to_response(await _load(db, calib.id), _warning(reg.low_r2))


@router.patch("/{calibration_id}", response_model=CalibrationResponse)
async def update_calibration(
    calibration_id: UUID,
    data: CalibrationCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    calib = await _load(db, calibration_id)

    try:
        reg = compute_regression([p.model_dump() for p in data.points])
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e))

    await _validate_sensors(db, data.sensor_ids)

    # If it's active and the material changes, deactivate the previous active one for the
    # new material.
    if calib.is_active:
        await db.execute(
            update(Calibration)
            .where(
                Calibration.material == data.material,
                Calibration.is_active.is_(True),
                Calibration.id != calib.id,
            )
            .values(is_active=False)
        )

    calib.name = data.name
    calib.material = data.material
    calib.sensor_type = data.sensor_type
    calib.temperature_c = data.temperature_c
    calib.reference = data.reference
    calib.points = reg.points
    calib.m = reg.m
    calib.c = reg.c
    calib.r_squared = reg.r_squared
    calib.ch_min = reg.ch_min
    calib.ch_max = reg.ch_max

    # Replace the sensor set
    await db.execute(
        delete(CalibrationSensor).where(CalibrationSensor.calibration_id == calib.id)
    )
    for sid in data.sensor_ids:
        db.add(CalibrationSensor(calibration_id=calib.id, sensor_id=sid))

    await db.commit()
    return _to_response(await _load(db, calib.id), _warning(reg.low_r2))


@router.post("/{calibration_id}/activate", response_model=CalibrationResponse)
async def activate_calibration(
    calibration_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    calib = await _load(db, calibration_id)
    # deactivate the other active ones for the same material
    await db.execute(
        update(Calibration)
        .where(
            Calibration.material == calib.material,
            Calibration.is_active.is_(True),
            Calibration.id != calib.id,
        )
        .values(is_active=False)
    )
    calib.is_active = True
    await db.commit()
    return _to_response(await _load(db, calib.id))


@router.delete("/{calibration_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_calibration(
    calibration_id: UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_admin),
):
    calib = await _load(db, calibration_id)
    await db.delete(calib)  # cascade deletes calibration_sensor
    await db.commit()
