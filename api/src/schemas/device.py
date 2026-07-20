"""
Pydantic schemas for devices and sensors (CRUD).
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, field_validator

SensorType = Literal[
    "temperature",
    "temperature_interior",
    "temperature_surface",
    "conductivity",
    "humidity",
]


class SensorResponse(BaseModel):
    id: UUID
    sensor_index: int
    sensor_type: str | None = None

    class Config:
        from_attributes = True


class DeviceBase(BaseModel):
    serial_number: str
    metadata: dict | None = None


class DeviceCreate(DeviceBase):
    sensor_types: list[SensorType | None] | None = None

    @field_validator("sensor_types")
    @classmethod
    def validate_sensors(cls, v: list | None) -> list | None:
        if v is None:
            return v
        if len(v) > 16:
            raise ValueError("sensor_types list cannot exceed 16 slots")
        return v


class DeviceUpdate(BaseModel):
    display_name: str | None = None
    metadata: dict | None = None


class SensorUpdate(BaseModel):
    sensor_type: SensorType | None


class DeviceResponse(BaseModel):
    id: UUID
    serial_number: str
    display_name: str | None = None
    metadata: dict | None = None
    created_at: datetime
    sensors: list[SensorResponse] = []
    gateway_serial: str | None = None
    last_seen: datetime | None = None
    # Current OTA config of the HM node (measure_cycles/send_cycles/hum_alarm_threshold), as it
    # came in its last frame — the node has no "read config" command like the gateway does, so
    # this reflects the last value received, not one queried on-demand.
    measure_cycles: int | None = None
    send_cycles: int | None = None
    hum_alarm_threshold: int | None = None

    class Config:
        from_attributes = True

    @classmethod
    def from_orm_model(
        cls,
        device,
        gateway_serial: str | None = None,
        last_seen: datetime | None = None,
        measure_cycles: int | None = None,
        send_cycles: int | None = None,
        hum_alarm_threshold: int | None = None,
    ):
        meta = device.metadata_ or {}
        return cls(
            id=device.id,
            serial_number=device.serial_number,
            display_name=meta.get("display_name") or None,
            metadata=device.metadata_,
            created_at=device.created_at,
            sensors=[SensorResponse.model_validate(s) for s in device.sensors],
            gateway_serial=gateway_serial,
            last_seen=last_seen,
            measure_cycles=measure_cycles,
            send_cycles=send_cycles,
            hum_alarm_threshold=hum_alarm_threshold,
        )


class DeviceListResponse(BaseModel):
    id: UUID
    serial_number: str
    display_name: str | None = None
    created_at: datetime
    sensor_count: int = 0
    last_seen: datetime | None = None
    gateway_serial: str | None = None

    class Config:
        from_attributes = True


class DeviceStats(BaseModel):
    total_devices: int
    active_devices: int  # with data in the last 24h
    total_sensors: int
    total_measurements: int
    measurements_today: int


class MeasurementResponse(BaseModel):
    ts: datetime
    temperature_c: float | None = None
    humidity_pct: float | None = None  # raw ADC (0-1023); unchanged
    voltage_cond_v: float | None = None
    supply_mv: int | None = None
    msg_counter: int | None = None
    # Moisture content (%) computed with the sensor's active calibration.
    # null if the sensor has no active calibration. adc (humidity_pct) is left untouched.
    chp: float | None = None
    chp_extrapolated: bool = False  # true if chp falls outside the valid range

    class Config:
        from_attributes = True


class SensorMeasurement(BaseModel):
    """Measurement from an individual sensor."""
    sensor_index: int
    temperature_c: float | None = None
    humidity_pct: float | None = None
    voltage_cond_v: float | None = None
    supply_mv: int | None = None
    msg_counter: int | None = None
    chp: float | None = None
    chp_extrapolated: bool = False


class GroupedMeasurementResponse(BaseModel):
    """Measurements grouped by timestamp with data from all sensors."""
    ts: datetime
    sensors: list[SensorMeasurement]


class DeviceStatusResponse(BaseModel):
    ts: datetime
    rssi_dbm: int | None = None
    buffer_used: int | None = None
    buffer_total: int | None = None
    supply_mv: int | None = None

    class Config:
        from_attributes = True


class SeqGap(BaseModel):
    """A gap in the node's packet sequence (N packets missing between after_seq and before_seq)."""
    after_seq: int
    before_seq: int
    missing: int
    at: datetime  # ts of the packet that arrived after the gap


class SeqGapsResponse(BaseModel):
    """Continuity analysis of a device's node_seq (missing-packet detection).

    The seq is sorted by value (not by ts) because the node resends from its FIFO out of order.
    Negative jumps (node reset) and absurd jumps (uint16 wraparound) are discarded as noise.
    """
    device_id: str
    serial_number: str
    hours: int
    total_packets: int
    first_seq: int | None = None
    last_seq: int | None = None
    missing_total: int
    completeness_pct: float | None = None
    gaps: list[SeqGap]
