-- Migration 021: wood moisture calibration (ADC → %CH).
-- 'humidity' sensors return raw ADC (0-1023). A regression calibration
--   ch = m*log10((1023-adc)/adc) + c
-- converts that ADC to moisture content (%). One ACTIVE calibration per material
-- (wood species, concrete type, etc.); applies to a chosen set of sensors
-- (core.calibration_sensor). The coefficients (m, c, R2, validity range) are computed and
-- persisted by the backend on save, over the valid points (trusted range 8-25%). The
-- raw ADC is NOT touched.

CREATE TABLE IF NOT EXISTS core.calibration (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name          TEXT NOT NULL,
    material      TEXT NOT NULL,                       -- material measured (e.g. wood species, concrete type)
    sensor_type   TEXT NOT NULL DEFAULT 'humidity',    -- type of sensor calibrated

    temperature_c NUMERIC,                             -- test context
    reference     TEXT,                                -- e.g. "gravimetry"

    -- Loaded points (includes invalid ones, with their flag) for editing/history:
    -- [{ "adc": 512, "ch_percent": 14.2, "valid": true }, ...]
    points        JSONB NOT NULL,

    -- Coefficients and fit (computed server-side over the VALID points)
    m             NUMERIC NOT NULL,
    c             NUMERIC NOT NULL,
    r_squared     NUMERIC NOT NULL,
    ch_min        NUMERIC NOT NULL,                    -- validity range = min/max valid ch
    ch_max        NUMERIC NOT NULL,

    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_by    UUID REFERENCES core."user"(id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Only one ACTIVE calibration per material (enforced by the DB).
CREATE UNIQUE INDEX IF NOT EXISTS uq_calibration_active_material
    ON core.calibration(material) WHERE is_active;

-- Sensors that each calibration applies to (the ones chosen in the form).
CREATE TABLE IF NOT EXISTS core.calibration_sensor (
    calibration_id UUID NOT NULL REFERENCES core.calibration(id) ON DELETE CASCADE,
    sensor_id      UUID NOT NULL REFERENCES core.sensor(id) ON DELETE CASCADE,
    PRIMARY KEY (calibration_id, sensor_id)
);
-- Fast lookup "which calibration applies to sensor X?"
CREATE INDEX IF NOT EXISTS ix_calibration_sensor_sensor ON core.calibration_sensor(sensor_id);

COMMENT ON TABLE core.calibration IS 'ADC->%CH calibrations (ch = m*log10((1023-adc)/adc) + c). One active per material.';
COMMENT ON TABLE core.calibration_sensor IS 'Sensors that each calibration applies to.';
