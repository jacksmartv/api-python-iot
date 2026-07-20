"""
Models for the 'monitoring' schema: device operational status.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class FleetEvent(Base):
    """Append-only log of point-in-time fleet events (registrations, etc.)."""

    __tablename__ = "event"
    __table_args__ = (
        Index("ix_event_occurred_at", "occurred_at"),
        Index("ix_event_entity", "entity_type", "entity_id", "occurred_at"),
        {"schema": "monitoring"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    entity_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    serial_number: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str | None] = mapped_column(Text, nullable=True)  # info|warning|critical
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


class DeviceRuntime(Base):
    """Live device state (1 row/device, UPSERT). Detects alarm/battery transitions.

    Distinct from DeviceStatus (temporal history by ts): this holds the CURRENT state.
    """

    __tablename__ = "device_runtime"
    __table_args__ = ({"schema": "monitoring"},)

    device_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.device.id", ondelete="CASCADE"), primary_key=True
    )
    alarm: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    alarm_changed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    low_batt: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    low_batt_changed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # Current OTA config of the HM node, exactly as it arrives in each frame (measure_cycles/
    # send_cycles/hum_alarm) — the node has no "read config" command (unlike the gateway), so
    # this IS the only way to see its config: it's updated on every frame received, it is not
    # historical. See payload_parser.py::decode_hm_v36.
    measure_cycles: Mapped[int | None] = mapped_column(Integer, nullable=True)
    send_cycles: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hum_alarm_threshold: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class GwSeqRecovery(Base):
    """Recovery status for a missing gw_seq (gateway → backend uplink).

    1 row per (gateway, missing seq). The self-healing job (services/gap_recovery.py) requests it
    from the gateway's SD card via storage read and updates the status. Persistent: survives
    restarts and is auditable (how many frames were definitively lost per gateway).
    """

    __tablename__ = "gw_seq_recovery"
    __table_args__ = (
        Index("ix_gw_seq_recovery_status", "gw_serial", "status"),
        Index("ix_gw_seq_recovery_serial_seq", "gw_serial", "gw_seq"),
        {"schema": "monitoring"},
    )

    gw_serial: Mapped[str] = mapped_column(Text, primary_key=True)
    gw_seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    # V3: pending | inflight | recovered | not_found  (abandoned from V1 is no longer used)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="pending")
    # NOT_FOUND | SD_NOT_READY | TIMEOUT | PARSER | INGEST | MQTT_ERROR
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    first_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_response_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    recovered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # V3: in-flight batch (seqs inflight together share batch_id; inflight_at is for the timeout)
    inflight_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    batch_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class DeviceStatus(Base):
    """Device operational status: RSSI, buffer, voltage."""

    __tablename__ = "device_status"
    __table_args__ = (
        Index("ix_device_status_device_ts", "device_id", "ts"),
        {"schema": "monitoring"},
    )

    device_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.device.id"), primary_key=True
    )
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    rssi_dbm: Mapped[int | None] = mapped_column(Integer, nullable=True)
    buffer_used: Mapped[int | None] = mapped_column(Integer, nullable=True)
    buffer_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    supply_mv: Mapped[int | None] = mapped_column(Integer, nullable=True)


class GatewayConfig(Base):
    """Snapshot of gateway configuration received via MQTT (setConfig / activeConfig)."""

    __tablename__ = "gateway_config"
    __table_args__ = (
        Index("ix_gateway_config_gateway_type", "gateway_id", "config_type", "received_at"),
        Index("ix_gateway_config_serial", "serial_number", "config_type", "received_at"),
        {"schema": "monitoring"},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.gateway.id", ondelete="CASCADE"), nullable=False
    )
    serial_number: Mapped[str] = mapped_column(Text, nullable=False)
    config_type: Mapped[str] = mapped_column(Text, nullable=False)  # 'setConfig' or 'activeConfig'
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    broker: Mapped[str | None] = mapped_column(Text, nullable=True)
    port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    client_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    fw_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    topic_prefix: Mapped[str | None] = mapped_column(Text, nullable=True)
    interval_s: Mapped[int | None] = mapped_column(Integer, nullable=True)
    supply_mv: Mapped[int | None] = mapped_column(Integer, nullable=True)
    broker2: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_bytes: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)


class GatewayConfigV3(Base):
    """Gateway firmware v3 config (JSON from the cmd/ack to {"target":"gw","action":"get"}).

    "Current" model: 1 row per gateway (PK = gateway_id), idempotent UPSERT with dedupe by
    config_hash. Does NOT replace GatewayConfig (legacy 416-byte binary), it complements it
    (see migration 019).
    """

    __tablename__ = "gateway_config_v3"
    __table_args__ = (
        Index("ix_gateway_config_v3_serial", "serial_number"),
        {"schema": "monitoring"},
    )

    # PK = gateway_id (1 row per gateway, no autogenerated id)
    gateway_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("core.gateway.id", ondelete="CASCADE"), primary_key=True
    )
    # deliberate denormalization; source of truth = core.gateway
    serial_number: Mapped[str] = mapped_column(Text, nullable=False)
    schema_version: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default="3")
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    config_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)  # sha256 digest, dedupe
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)  # correlation, NOT unique
    gateway_ts_ms: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    fw: Mapped[str | None] = mapped_column(Text, nullable=True)
    net_iface: Mapped[str | None] = mapped_column(Text, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    config_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
