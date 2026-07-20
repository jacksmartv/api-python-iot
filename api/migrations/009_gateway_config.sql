-- Migration 009: monitoring.gateway_config
-- Stores each gateway config snapshot received via MQTT topics:
--   telemetry/{IMEI}/GW/setConfig   — config being written to gateway
--   telemetry/{IMEI}/GW/activeConfig — config currently active in firmware
-- Both use the same 416-byte binary struct (decoded fields stored as columns).
-- raw_bytes stores the original binary for future re-parsing.

CREATE TABLE IF NOT EXISTS monitoring.gateway_config (
    id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    gateway_id    UUID         NOT NULL REFERENCES core.gateway(id) ON DELETE CASCADE,
    serial_number TEXT         NOT NULL,
    config_type   TEXT         NOT NULL,   -- 'setConfig' or 'activeConfig'
    received_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    broker        TEXT,
    port          INTEGER,
    client_id     TEXT,
    fw_type       TEXT,
    topic_prefix  TEXT,
    interval_s    INTEGER,
    supply_mv     INTEGER,
    broker2       TEXT,
    raw_bytes     BYTEA        NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_gateway_config_gateway_type
    ON monitoring.gateway_config (gateway_id, config_type, received_at DESC);

CREATE INDEX IF NOT EXISTS ix_gateway_config_serial
    ON monitoring.gateway_config (serial_number, config_type, received_at DESC);
