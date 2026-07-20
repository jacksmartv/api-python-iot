"""
Models for the 'spatial' schema: buildings, floors, layers, assets on floor plans.

Single-tenant in V1: org_id is present in all tables but hardcoded
(DEFAULT_ORG_ID) until V2. Asset != Device (device_id nullable).
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


class Building(Base):
    """Building: root of the spatial hierarchy."""

    __tablename__ = "building"
    __table_args__ = {"schema": "spatial"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    org_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    country: Mapped[str | None] = mapped_column(Text, nullable=True)
    lat: Mapped[float | None] = mapped_column(nullable=True)
    lng: Mapped[float | None] = mapped_column(nullable=True)
    timezone: Mapped[str] = mapped_column(Text, nullable=False, default="UTC")
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.user.id", ondelete="SET NULL"), nullable=True
    )

    floors: Mapped[list["Floor"]] = relationship(
        back_populates="building", cascade="all, delete-orphan"
    )


class Floor(Base):
    """Floor of a building. Contains the floor plan (normalized SVG) and the assets."""

    __tablename__ = "floor"
    __table_args__ = {"schema": "spatial"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    building_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("spatial.building.id", ondelete="CASCADE"),
        nullable=False,
    )
    org_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    level: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    plan_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    plan_viewbox: Mapped[str | None] = mapped_column(Text, nullable=True)
    plan_width_m: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    plan_height_m: Mapped[float | None] = mapped_column(Numeric(10, 2), nullable=True)
    plan_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    plan_status: Mapped[str] = mapped_column(Text, nullable=False, default="none")
    # Migration 011 — floor plan observability (Sprint 2)
    plan_node_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plan_svg_size_kb: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plan_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    plan_processing_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    building: Mapped["Building"] = relationship(back_populates="floors")
    layers: Mapped[list["Layer"]] = relationship(
        back_populates="floor", cascade="all, delete-orphan"
    )
    assets: Mapped[list["Asset"]] = relationship(
        back_populates="floor", cascade="all, delete-orphan"
    )


class Layer(Base):
    """Logical layer for grouping/filtering assets in the viewer."""

    __tablename__ = "layer"
    __table_args__ = {"schema": "spatial"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    floor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("spatial.floor.id", ondelete="CASCADE"),
        nullable=False,
    )
    org_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    color: Mapped[str] = mapped_column(Text, nullable=False, default="#3b82f6")
    icon: Mapped[str] = mapped_column(Text, nullable=False, default="circle")
    default_visible: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    floor: Mapped["Floor"] = relationship(back_populates="layers")


class Asset(Base):
    """Spatial entity positioned on a floor plan. device_id NULLABLE (Asset != Device)."""

    __tablename__ = "asset"
    __table_args__ = {"schema": "spatial"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    floor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("spatial.floor.id", ondelete="CASCADE"),
        nullable=False,
    )
    building_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("spatial.building.id", ondelete="CASCADE"),
        nullable=False,
    )
    org_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    layer_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("spatial.layer.id", ondelete="SET NULL"),
        nullable=True,
    )
    device_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("core.device.id", ondelete="SET NULL"),
        nullable=True,
    )
    asset_type: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    pos_x: Mapped[float | None] = mapped_column(Numeric(8, 6), nullable=True)
    pos_y: Mapped[float | None] = mapped_column(Numeric(8, 6), nullable=True)
    pos_z: Mapped[float | None] = mapped_column(Numeric(8, 6), nullable=True)
    rotation_deg: Mapped[float] = mapped_column(Numeric(6, 2), nullable=False, default=0)
    plan_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    properties: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    tags: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, default=list)
    pos_source: Mapped[str] = mapped_column(Text, nullable=False, default="manual")
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.user.id", ondelete="SET NULL"), nullable=True
    )

    floor: Mapped["Floor"] = relationship(back_populates="assets")


class AssetLayer(Base):
    """N:N asset↔layer. Additive, dormant until V3 (multi-layer per asset)."""

    __tablename__ = "asset_layer"
    __table_args__ = {"schema": "spatial"}

    asset_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("spatial.asset.id", ondelete="CASCADE"),
        primary_key=True,
    )
    layer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("spatial.layer.id", ondelete="CASCADE"),
        primary_key=True,
    )


class AssetPositionHistory(Base):
    """Append-only history of asset movements (audit + replay)."""

    __tablename__ = "asset_position_history"
    __table_args__ = {"schema": "spatial"}

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    asset_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("spatial.asset.id", ondelete="CASCADE"),
        nullable=False,
    )
    org_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    pos_x: Mapped[float | None] = mapped_column(Numeric(8, 6), nullable=True)
    pos_y: Mapped[float | None] = mapped_column(Numeric(8, 6), nullable=True)
    rotation_deg: Mapped[float | None] = mapped_column(Numeric(6, 2), nullable=True)
    plan_version: Mapped[int] = mapped_column(Integer, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False, default="api")
    session_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    moved_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.user.id", ondelete="SET NULL"), nullable=True
    )
    moved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class AssetTelemetrySnapshot(Base):
    """Latest telemetry state per asset. UPSERT in the ingest path → Sprint 3."""

    __tablename__ = "asset_telemetry_snapshot"
    __table_args__ = {"schema": "spatial"}

    asset_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("spatial.asset.id", ondelete="CASCADE"),
        primary_key=True,
    )
    org_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    device_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    last_ts: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    temperature_c: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    humidity_pct: Mapped[float | None] = mapped_column(Numeric, nullable=True)
    supply_mv: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rssi_dbm: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rssi_ts: Mapped[datetime | None] = mapped_column(  # migration 012
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
