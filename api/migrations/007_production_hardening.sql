-- Migration 007: Production hardening
-- Applies schema fixes identified in the production-readiness audit.
-- Must run AFTER migration 006 (core.gateway must exist).

-- ---------------------------------------------------------------------------
-- Step 1: gateway_id FK on monitoring.gateway_status
-- serial_number stays for denormalized lookups; gateway_id is the real FK.
-- ---------------------------------------------------------------------------
ALTER TABLE monitoring.gateway_status
    ADD COLUMN IF NOT EXISTS gateway_id UUID REFERENCES core.gateway(id) ON DELETE SET NULL;

-- Step 2: Unique constraint on gateway_status to absorb MQTT QoS-1 duplicates
-- Delete duplicate rows first (keep the one with the lowest id per serial_number+ts)
DELETE FROM monitoring.gateway_status
WHERE id NOT IN (
    SELECT DISTINCT ON (serial_number, ts) id
    FROM monitoring.gateway_status
    ORDER BY serial_number, ts, id
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_gateway_status_serial_ts
    ON monitoring.gateway_status (serial_number, ts);

-- Step 3: Backfill gateway_id for existing rows
UPDATE monitoring.gateway_status gs
SET gateway_id = g.id
FROM core.gateway g
WHERE g.serial_number = gs.serial_number
  AND gs.gateway_id IS NULL;

CREATE INDEX IF NOT EXISTS ix_gateway_status_gateway_id
    ON monitoring.gateway_status (gateway_id, ts DESC);

-- ---------------------------------------------------------------------------
-- Step 4: node_id + sensor_key on core.sensor (canonical sensor identity)
-- These replace sensor_index as the stable Prometheus label pair.
-- sensor_index is kept for backwards compat; drop in migration 008.
-- ---------------------------------------------------------------------------
ALTER TABLE core.sensor
    ADD COLUMN IF NOT EXISTS node_id INTEGER,
    ADD COLUMN IF NOT EXISTS sensor_key TEXT;

-- Partial unique index: only enforced when both columns are populated.
-- Old code writes NULLs → excluded from index → no conflicts during rollout.
CREATE UNIQUE INDEX IF NOT EXISTS uq_sensor_canonical
    ON core.sensor (device_id, node_id, sensor_key)
    WHERE node_id IS NOT NULL AND sensor_key IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Step 5: gateway_id FK on core.device (device → gateway relationship)
-- Required to populate gateway_serial label in the SQL metrics endpoint.
-- ---------------------------------------------------------------------------
ALTER TABLE core.device
    ADD COLUMN IF NOT EXISTS gateway_id UUID REFERENCES core.gateway(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS ix_device_gateway_id
    ON core.device (gateway_id);

-- ---------------------------------------------------------------------------
-- Step 6: monitoring.incident table (open/close lifecycle for business events)
-- Partial unique index prevents duplicate open incidents per entity+type.
-- NULL != NULL in PostgreSQL, so a table UNIQUE constraint would NOT work.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.incident (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    incident_type TEXT       NOT NULL,
    entity_type   TEXT       NOT NULL,
    entity_id     UUID       NOT NULL,
    serial_number TEXT       NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at   TIMESTAMPTZ,
    payload       JSONB
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_incident_open
    ON monitoring.incident (incident_type, entity_id)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_incident_entity
    ON monitoring.incident (entity_id, started_at DESC);

-- ---------------------------------------------------------------------------
-- Step 7: payload_hash on raw.telemetry_payload (dedup on HTTP retry)
-- Generated column requires PostgreSQL 12+.
-- ---------------------------------------------------------------------------
ALTER TABLE raw.telemetry_payload
    ADD COLUMN IF NOT EXISTS payload_hash TEXT
        GENERATED ALWAYS AS (encode(sha256(payload::text::bytea), 'hex')) STORED;

-- Delete duplicate raw payloads before creating unique index
DELETE FROM raw.telemetry_payload
WHERE id NOT IN (
    SELECT DISTINCT ON (device_id, payload_hash) id
    FROM raw.telemetry_payload
    ORDER BY device_id, payload_hash, received_at
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_payload_device_hash
    ON raw.telemetry_payload (device_id, payload_hash);
