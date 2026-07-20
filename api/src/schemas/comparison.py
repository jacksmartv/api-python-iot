"""
Pydantic schemas for the Comparisons section (sensor groups + series + references).
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

# Comparable metrics. 'temperature' -> temperature_c; 'humidity_pct' -> raw ADC;
# 'chp' -> calibrated %CH. Used to place each reference value on the correct chart.
Metric = Literal["temperature", "humidity_pct", "chp"]


class ReferencePoint(BaseModel):
    """Manual reference value at a given time (e.g.: 13:45 -> 20 °C)."""

    ts: datetime
    value: float
    metric: Metric
    label: str | None = None


class ComparisonGroupCreate(BaseModel):
    name: str
    sensor_ids: list[UUID] = Field(default_factory=list)
    reference_points: list[ReferencePoint] = Field(default_factory=list)
    # Expected target %CH humidity range (optional).
    target_min: float | None = None
    target_max: float | None = None
    position: int | None = None  # optional; the server appends at the end if not provided


# Editing replaces the entire definition (same payload as creation).
ComparisonGroupUpdate = ComparisonGroupCreate


class ComparisonGroupResponse(BaseModel):
    id: UUID
    name: str
    position: int
    sensor_ids: list[UUID]
    reference_points: list[ReferencePoint]
    target_min: float | None = None
    target_max: float | None = None
    created_at: datetime


class SeriesRequest(BaseModel):
    """Requests the series for a set of sensors (saved or being edited)."""

    sensor_ids: list[UUID] = Field(default_factory=list)
    hours: int | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    limit: int = 5000


class SeriesSensorMeta(BaseModel):
    sensor_id: UUID
    sensor_index: int
    sensor_type: str | None = None
    device_serial: str
    label: str


class SensorSeries(BaseModel):
    """Per-metric series for a sensor. Each point is [epoch_ms, value]."""

    temperature_c: list[tuple[int, float]] = Field(default_factory=list)
    humidity_pct: list[tuple[int, float]] = Field(default_factory=list)
    chp: list[tuple[int, float]] = Field(default_factory=list)


class SeriesResponse(BaseModel):
    sensors: list[SeriesSensorMeta]
    # { "<sensor_id>": SensorSeries }
    series: dict[UUID, SensorSeries]


class LatestRequest(BaseModel):
    sensor_ids: list[UUID] = Field(default_factory=list)


class LatestSensorValue(BaseModel):
    """Latest reading for a sensor (for the per-sensor gauge)."""

    sensor_id: UUID
    label: str
    sensor_index: int
    device_serial: str
    ts: datetime | None = None
    temperature_c: float | None = None
    humidity_pct: float | None = None
    chp: float | None = None


class LatestResponse(BaseModel):
    latest: dict[UUID, LatestSensorValue]
