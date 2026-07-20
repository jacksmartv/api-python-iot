"""
Pydantic models for validating the telemetry payload.

Based on JSON schema version 1 of the architecture document.
The timestamp comes in YYYYMMDD.HHMMSS format and is converted to datetime.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


def parse_numeric_timestamp(value: float) -> datetime:
    """
    Converts numeric timestamp YYYYMMDD.HHMMSS to datetime.

    Example: 20250205.143022 -> 2025-02-05 14:30:22
    """
    int_part = int(value)
    decimal_part = value - int_part

    year = int_part // 10000
    month = (int_part // 100) % 100
    day = int_part % 100

    time_val = round(decimal_part * 1000000)
    hour = time_val // 10000
    minute = (time_val // 100) % 100
    second = time_val % 100

    return datetime(year, month, day, hour, minute, second)


class SensorData(BaseModel):
    """Data for an individual sensor."""

    timestamp: datetime
    temperature_c: float = Field(alias="temp")
    voltage_cond_v: float = Field(alias="volt_cond")
    supply_mv: int = Field(alias="supply")
    msg_counter: int = Field(alias="msg_cnt")

    @field_validator("timestamp", mode="before")
    @classmethod
    def convert_timestamp(cls, v: Any) -> datetime:
        if isinstance(v, (int, float)):
            return parse_numeric_timestamp(v)
        if isinstance(v, datetime):
            return v
        raise ValueError(f"Invalid timestamp format: {v}")


class DeviceStatus(BaseModel):
    """Operational status of the device."""

    rssi_dbm: int = Field(alias="rssi")
    buffer_used: int = Field(alias="buf_used")
    buffer_total: int = Field(alias="buf_total")
    supply_mv: int = Field(alias="supply")


class TelemetryPayload(BaseModel):
    """
    Complete telemetry payload received from a device.

    The payload contains:
    - serial_number: unique identifier of the device
    - schema_version: schema version (for future compatibility)
    - status: operational status of the device
    - sensors: dictionary with data for each sensor (sensor_0, sensor_1, etc.)
    """

    serial_number: str = Field(alias="sn")
    schema_version: int = Field(default=1, alias="schema_v")
    status: DeviceStatus
    sensors: dict[str, SensorData]

    @field_validator("sensors", mode="before")
    @classmethod
    def extract_sensors(cls, v: Any, info: Any) -> dict[str, SensorData]:
        """
        Extracts the sensors from the payload.

        If it comes as a dict with keys sensor_0, sensor_1, etc., uses it directly.
        Otherwise, looks for keys starting with 'sensor_' in the full payload.
        """
        if isinstance(v, dict) and all(k.startswith("sensor_") for k in v.keys()):
            return v
        return v

    class Config:
        populate_by_name = True


class TelemetryPayloadRaw(BaseModel):
    """
    Wrapper to receive the raw payload and extract sensors dynamically.

    Useful when the sensors come as top-level keys in the JSON.
    """

    serial_number: str = Field(alias="sn")
    schema_version: int = Field(default=1, alias="schema_v")
    status: DeviceStatus

    class Config:
        extra = "allow"  # Allows additional fields (the sensors)
        populate_by_name = True

    def to_telemetry_payload(self) -> TelemetryPayload:
        """Converts to TelemetryPayload extracting sensors from extra fields."""
        sensors = {}
        for key, value in (self.__pydantic_extra__ or {}).items():
            if key.startswith("sensor_"):
                sensors[key] = SensorData.model_validate(value)

        return TelemetryPayload.model_validate({
            "sn": self.serial_number,
            "schema_v": self.schema_version,
            "status": self.status,
            "sensors": sensors,
        })
