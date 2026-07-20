-- Migration 019: monitoring.gateway_config_v3 — gateway firmware v3 config (JSON from cmd/ack).
-- Firmware v3 (gateway/{IMEI}/... scheme) does NOT emit config on its own; it returns it when
-- requested with {"target":"gw","action":"get"} → responds with JSON on gateway/{IMEI}/cmd/ack. The
-- legacy monitoring.gateway_config table (416b binary, set/activeConfig) is left intact and NOT touched.
-- "current" model: 1 row per gateway (PK = gateway_id), idempotent UPSERT with hash-based dedupe.
-- See SPATIAL_SPRINT_GATEWAY_CONFIG_V3.md.

CREATE TABLE IF NOT EXISTS monitoring.gateway_config_v3 (
    gateway_id        UUID PRIMARY KEY REFERENCES core.gateway(id) ON DELETE CASCADE,  -- 1 row/gw
    serial_number     TEXT NOT NULL,       -- deliberate DENORM: source of truth = core.gateway
    schema_version    SMALLINT NOT NULL DEFAULT 3,  -- version of the PARSER that produced the row (internal;
                                                    -- NOT the schema_version of the endpoint response)
    config            JSONB NOT NULL,      -- config object from the ACK (5 sections)
    raw_payload       JSONB NOT NULL,      -- FULL ACK (ok/id/action/target/section/config)
    config_hash       BYTEA NOT NULL,      -- sha256 DIGEST (32 raw bytes) of the canonical config, for dedupe
    request_id        TEXT,                -- firmware's "id" — correlation only, NOT unique
    gateway_ts_ms     BIGINT,              -- ts_ms of the ACK: when the firmware generated the config
    fw                TEXT,                -- indexed from config.system.fw
    net_iface         TEXT,                -- indexed from config.connect.net_iface
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),  -- 1st time we learned about this gw
    config_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),  -- last time config CHANGED (different hash)
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now()   -- last ACK received, even if identical
);

CREATE INDEX IF NOT EXISTS ix_gateway_config_v3_serial
    ON monitoring.gateway_config_v3 (serial_number);

-- Deferrable: CREATE INDEX ix_gateway_config_v3_fw ON monitoring.gateway_config_v3 (fw)
--            — only if filtering by firmware is ever needed.
-- NOTE: request_id deliberately has NO unique index — a retry or buggy firmware can repeat it.
--       It's correlation metadata, never a PK/unique.
-- NOTE: serial_number is a deliberate denormalization. The source of truth for the serial is core.gateway
--       (via the gateway_id FK). If a serial is fixed there, it refreshes here on the next ACK or via JOIN.
