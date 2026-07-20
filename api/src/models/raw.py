"""
Models for the 'raw' schema: original JSON payload for auditing.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, Integer
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class TelemetryPayloadRaw(Base):
    """Original JSON payload received from the device."""

    __tablename__ = "telemetry_payload"
    __table_args__ = (
        Index("ix_raw_payload_device_received", "device_id", "received_at"),
        {"schema": "raw"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    device_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.device.id"), nullable=False
    )
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        # No server_default — always supplied explicitly; SQL DEFAULT NOW() stays in the migration
    )
    schema_version: Mapped[int] = mapped_column(Integer, default=1)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
