"""
Models for the 'core' schema: devices and sensors.
"""

import uuid
from datetime import datetime
from typing import Literal

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class Device(Base):
    """Physical entity that sends telemetry."""

    __tablename__ = "device"
    __table_args__ = {"schema": "core"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    serial_number: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)
    # gateway_id added in migration 007 — device→gateway FK for gateway_serial inhibition label
    gateway_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.gateway.id"), nullable=True
    )

    # Relationships
    sensors: Mapped[list["Sensor"]] = relationship(back_populates="device")


class Gateway(Base):
    """IoT gateway — entity that aggregates data from sensor nodes."""

    __tablename__ = "gateway"
    __table_args__ = {"schema": "core"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    serial_number: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)


class Sensor(Base):
    """Logical sensor identified by the {id} suffix."""

    __tablename__ = "sensor"
    __table_args__ = (
        UniqueConstraint("device_id", "sensor_index", name="uq_sensor_device_index"),
        {"schema": "core"},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    device_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.device.id"), nullable=False
    )
    sensor_index: Mapped[int] = mapped_column(nullable=False)
    sensor_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Canonical identity added in migration 007 — stable across SENSOR_KEY_REGISTRY changes
    node_id: Mapped[int | None] = mapped_column(nullable=True)
    sensor_key: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relationships
    device: Mapped["Device"] = relationship(back_populates="sensors")


class Calibration(Base):
    """ADC→%CH calibration via regression: ch = m·log10((1023-adc)/adc) + c.

    One active per material (partial unique index in the DB). The coefficients
    and validity range are calculated/persisted by the backend over the valid points.
    """

    __tablename__ = "calibration"
    __table_args__ = {"schema": "core"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    material: Mapped[str] = mapped_column(Text, nullable=False)
    sensor_type: Mapped[str] = mapped_column(Text, nullable=False, default="humidity")
    temperature_c: Mapped[float | None] = mapped_column(
        Numeric(asdecimal=False), nullable=True
    )
    reference: Mapped[str | None] = mapped_column(Text, nullable=True)
    # [{ "adc": int, "ch_percent": float, "valid": bool }, ...]
    points: Mapped[list] = mapped_column(JSONB, nullable=False)
    m: Mapped[float] = mapped_column(Numeric(asdecimal=False), nullable=False)
    c: Mapped[float] = mapped_column(Numeric(asdecimal=False), nullable=False)
    r_squared: Mapped[float] = mapped_column(Numeric(asdecimal=False), nullable=False)
    ch_min: Mapped[float] = mapped_column(Numeric(asdecimal=False), nullable=False)
    ch_max: Mapped[float] = mapped_column(Numeric(asdecimal=False), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.user.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Sensors this applies to
    sensor_links: Mapped[list["CalibrationSensor"]] = relationship(
        back_populates="calibration", cascade="all, delete-orphan"
    )


class CalibrationSensor(Base):
    """Calibration ↔ sensor association (the chosen sensors it applies to)."""

    __tablename__ = "calibration_sensor"
    __table_args__ = {"schema": "core"}

    calibration_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("core.calibration.id", ondelete="CASCADE"),
        primary_key=True,
    )
    sensor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("core.sensor.id", ondelete="CASCADE"),
        primary_key=True,
    )

    calibration: Mapped["Calibration"] = relationship(back_populates="sensor_links")


class ComparisonGroup(Base):
    """Group of sensors for the Comparisons section (one tab per group).

    Overlays the same metric from multiple sensors on a chart and stores manual
    reference values (reference_points) to calculate the percentage error.
    """

    __tablename__ = "comparison_group"
    __table_args__ = {"schema": "core"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Expected target %CH humidity range for the group (used by the per-sensor gauge).
    target_min: Mapped[float | None] = mapped_column(Numeric(asdecimal=False), nullable=True)
    target_max: Mapped[float | None] = mapped_column(Numeric(asdecimal=False), nullable=True)
    # [{ "ts": ISO8601, "value": float, "metric": temperature|humidity_pct|chp, "label": str|None }]
    reference_points: Mapped[list] = mapped_column(
        JSONB, nullable=False, default=list, server_default="[]"
    )
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.user.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Sensors that make up the group
    sensor_links: Mapped[list["ComparisonGroupSensor"]] = relationship(
        back_populates="group", cascade="all, delete-orphan"
    )


class ComparisonGroupSensor(Base):
    """Comparison group ↔ sensor association."""

    __tablename__ = "comparison_group_sensor"
    __table_args__ = {"schema": "core"}

    group_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("core.comparison_group.id", ondelete="CASCADE"),
        primary_key=True,
    )
    sensor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("core.sensor.id", ondelete="CASCADE"),
        primary_key=True,
    )

    group: Mapped["ComparisonGroup"] = relationship(back_populates="sensor_links")


class FirmwareRelease(Base):
    """OTA firmware release (.bin) uploaded to S3. Deploying to gateways via the MQTT ota
    command is deferred, not implemented yet — the public_url is copied and used by other means."""

    __tablename__ = "firmware_release"
    __table_args__ = {"schema": "core"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    original_filename: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    target: Mapped[str] = mapped_column(Text, nullable=False, default="gateway")
    s3_key: Mapped[str] = mapped_column(Text, nullable=False)
    public_url: Mapped[str] = mapped_column(Text, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    checksum_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    etag: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.user.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


# Python-side type hints only — the source of truth for the enum is the CHECK constraint in
# the SQL migration; this does not generate or replace it. It keeps a typo ("succes", "Success")
# from passing the linter/type-checker even though the DB would reject it anyway at runtime. The
# Literal exists solely as a development aid (autocomplete, typos caught by the type-checker); the
# definitive validation of the value happens in the database via the CHECK constraint — these are
# two independent layers, neither generating the other.
FirmwareDeploymentStatus = Literal["pending", "command_sent", "success", "failed", "timeout"]


class FirmwareDeployment(Base):
    """Deployment of a version to one or more gateways — an event distinct from FirmwareRelease
    (the catalog)."""

    __tablename__ = "firmware_deployment"
    __table_args__ = {"schema": "core"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    firmware_release_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.firmware_release.id"), nullable=False
    )
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.user.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class FirmwareDeploymentGateway(Base):
    """Deployment status for ONE target gateway. gw_serial is a loose TEXT (no FK to core.gateway),
    same pattern as monitoring.gw_seq_recovery — an MQTT command log, not a domain relationship."""

    __tablename__ = "firmware_deployment_gateway"
    __table_args__ = (
        Index("ix_firmware_deployment_gateway_serial", "gw_serial", "created_at"),
        Index(
            "uq_firmware_deployment_gateway_request_id", "request_id",
            unique=True, postgresql_where=Column("request_id", UUID).isnot(None),
        ),
        {"schema": "core"},
    )

    deployment_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.firmware_deployment.id", ondelete="CASCADE"),
        primary_key=True,
    )
    gw_serial: Mapped[str] = mapped_column(Text, primary_key=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="pending")
    request_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    command_sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    acked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
