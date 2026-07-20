"""
Models for the 'telemetry' schema: physical measurements (time-series).
"""

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Index, Integer, Numeric
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Measurement(Base):
    """Timestamped physical measurement."""

    __tablename__ = "measurement"
    __table_args__ = (
        Index("ix_measurement_sensor_ts", "sensor_id", "ts"),
        {"schema": "telemetry"},
    )

    sensor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.sensor.id"), primary_key=True
    )
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    temperature_c: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    humidity_pct: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    voltage_cond_v: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    supply_mv: Mapped[int | None] = mapped_column(Integer, nullable=True)
    msg_counter: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
