-- Migration: Add humidity column to measurements

ALTER TABLE telemetry.measurement ADD COLUMN IF NOT EXISTS humidity_pct NUMERIC;

COMMENT ON COLUMN telemetry.measurement.humidity_pct IS 'Relative humidity as a percentage';
