-- Initial migration: Create base schemas and tables
-- Based on Arquitectura_Telemetria_PostgreSQL.pdf

-- =============================================================================
-- SCHEMAS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS telemetry;
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS raw;

-- =============================================================================
-- EXTENSIONS
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- CORE: Devices and Sensors
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.device (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    serial_number TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS core.sensor (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    device_id UUID NOT NULL REFERENCES core.device(id),
    sensor_index INTEGER NOT NULL,
    sensor_type TEXT,
    UNIQUE(device_id, sensor_index)
);

CREATE INDEX IF NOT EXISTS ix_sensor_device_id ON core.sensor(device_id);

-- =============================================================================
-- TELEMETRY: Physical measurements (time-series)
-- =============================================================================

CREATE TABLE IF NOT EXISTS telemetry.measurement (
    sensor_id UUID NOT NULL REFERENCES core.sensor(id),
    ts TIMESTAMPTZ NOT NULL,
    temperature_c NUMERIC,
    voltage_cond_v NUMERIC,
    supply_mv INTEGER,
    msg_counter BIGINT,
    PRIMARY KEY(sensor_id, ts)
);

CREATE INDEX IF NOT EXISTS ix_measurement_sensor_ts
    ON telemetry.measurement(sensor_id, ts DESC);

-- =============================================================================
-- MONITORING: Device operational state
-- =============================================================================

CREATE TABLE IF NOT EXISTS monitoring.device_status (
    device_id UUID NOT NULL REFERENCES core.device(id),
    ts TIMESTAMPTZ NOT NULL,
    rssi_dbm INTEGER,
    buffer_used INTEGER,
    buffer_total INTEGER,
    supply_mv INTEGER,
    PRIMARY KEY(device_id, ts)
);

CREATE INDEX IF NOT EXISTS ix_device_status_device_ts
    ON monitoring.device_status(device_id, ts DESC);

-- =============================================================================
-- RAW: Original JSON payload for auditing
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw.telemetry_payload (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    device_id UUID NOT NULL REFERENCES core.device(id),
    received_at TIMESTAMPTZ DEFAULT NOW(),
    schema_version INTEGER DEFAULT 1,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_payload_device_received
    ON raw.telemetry_payload(device_id, received_at DESC);

-- =============================================================================
-- COMMENTS (DB Documentation)
-- =============================================================================

COMMENT ON SCHEMA core IS 'Devices and sensors';
COMMENT ON SCHEMA telemetry IS 'Physical measurements (time-series)';
COMMENT ON SCHEMA monitoring IS 'Device operational state';
COMMENT ON SCHEMA raw IS 'Original JSON payload for auditing';

COMMENT ON TABLE core.device IS 'Physical entity that sends telemetry';
COMMENT ON TABLE core.sensor IS 'Logical sensor identified by the {id} suffix';
COMMENT ON TABLE telemetry.measurement IS 'Physical measurement with timestamp';
COMMENT ON TABLE monitoring.device_status IS 'RSSI, buffer, voltage';
COMMENT ON TABLE raw.telemetry_payload IS 'Original received JSON';
