-- Migration 024: sensor comparison groups.
-- The "Comparisons" section lets you group sensors (possibly from different devices) and
-- view them overlaid on the same chart per metric (temperature, humidity ADC, calibrated %CH).
-- Each group = one tab. It also stores manual reference values (e.g. 13:45 -> 20 C) to
-- compute each sensor's percentage error relative to that reference. Mirrors the pattern of
-- core.calibration / core.calibration_sensor (021).

CREATE TABLE IF NOT EXISTS core.comparison_group (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name             TEXT NOT NULL,
    position         INTEGER NOT NULL DEFAULT 0,          -- tab order

    -- Manual reference values loaded per tab:
    -- [{ "ts": "2026-07-13T13:45:00Z", "value": 20.0, "metric": "temperature", "label": null }, ...]
    -- metric in {temperature, humidity_pct, chp} -> drawn on that metric's chart.
    reference_points JSONB NOT NULL DEFAULT '[]',

    created_by       UUID REFERENCES core."user"(id),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Sensors that make up each group (the ones chosen in the picker).
CREATE TABLE IF NOT EXISTS core.comparison_group_sensor (
    group_id  UUID NOT NULL REFERENCES core.comparison_group(id) ON DELETE CASCADE,
    sensor_id UUID NOT NULL REFERENCES core.sensor(id) ON DELETE CASCADE,
    PRIMARY KEY (group_id, sensor_id)
);
-- Fast lookup "which groups is sensor X in?"
CREATE INDEX IF NOT EXISTS ix_comparison_group_sensor_sensor
    ON core.comparison_group_sensor(sensor_id);

COMMENT ON TABLE core.comparison_group IS 'Sensor comparison groups (one tab per group) + manual reference values.';
COMMENT ON TABLE core.comparison_group_sensor IS 'Sensors that make up each comparison group.';
