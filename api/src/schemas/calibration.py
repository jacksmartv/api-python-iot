"""
Pydantic schemas for humidity calibrations (ADC -> %CH).
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class CalibrationPointIn(BaseModel):
    """Point submitted from the form (the `valid` flag is determined by the server)."""

    adc: int
    ch_percent: float


class CalibrationPoint(BaseModel):
    """Persisted / returned point, with its validity flag."""

    adc: int
    ch_percent: float
    valid: bool


class CalibrationCreate(BaseModel):
    name: str
    material: str
    sensor_type: str = "humidity"
    sensor_ids: list[UUID] = Field(default_factory=list)  # sensors this applies to
    temperature_c: float | None = None
    reference: str | None = None
    points: list[CalibrationPointIn]


# Editing replaces the entire definition (same payload as creation).
CalibrationUpdate = CalibrationCreate


class CalibrationResponse(BaseModel):
    id: UUID
    name: str
    material: str
    sensor_type: str
    sensor_ids: list[UUID]
    temperature_c: float | None = None
    reference: str | None = None
    points: list[CalibrationPoint]
    m: float
    c: float
    r_squared: float
    ch_min: float
    ch_max: float
    is_active: bool
    created_at: datetime
    warning: str | None = None  # e.g. "R² < 0.98" (does not block saving)
