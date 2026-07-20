"""
Models for the 'monitoring' schema: gateway operational status.
"""

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class GatewayStatus(Base):
    """Periodic heartbeat from a gateway: signal, voltage, errors."""

    __tablename__ = "gateway_status"
    __table_args__ = (
        Index("ix_gateway_status_serial_ts", "serial_number", "ts"),
        {"schema": "monitoring"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    serial_number: Mapped[str] = mapped_column(Text, nullable=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    csq: Mapped[int | None] = mapped_column(Integer, nullable=True)
    erf: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rst: Mapped[int | None] = mapped_column(Integer, nullable=True)
    vgw: Mapped[int | None] = mapped_column(Integer, nullable=True)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
