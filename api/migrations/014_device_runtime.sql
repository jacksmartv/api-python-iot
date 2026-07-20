-- Migration 014: live device state + severity on events.
-- Enables alarm events for HM nodes (humidity_alarm, low_battery) on state TRANSITION.

-- Live device state (1 row/device, UPSERT on every frame). Does NOT touch monitoring.device_status,
-- which is a temporal history (snapshot per ts). This is where the CURRENT state lives to detect transitions.
CREATE TABLE IF NOT EXISTS monitoring.device_runtime (
    device_id           UUID PRIMARY KEY REFERENCES core.device(id) ON DELETE CASCADE,
    alarm               BOOLEAN,
    alarm_changed_at    TIMESTAMPTZ,                          -- when alarm last changed
    low_batt            BOOLEAN,
    low_batt_changed_at TIMESTAMPTZ,                          -- when low_batt last changed
    last_seen           TIMESTAMPTZ NOT NULL DEFAULT now()    -- last frame seen (always UPSERT)
);

-- severity as a first-class column of the event (not hidden in payload->>).
-- Allows WHERE severity='critical' + index, without JSON operators. CHECK avoids case variants.
ALTER TABLE monitoring.event ADD COLUMN IF NOT EXISTS severity TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'ck_event_severity'
    ) THEN
        ALTER TABLE monitoring.event ADD CONSTRAINT ck_event_severity
            CHECK (severity IS NULL OR severity IN ('info', 'warning', 'critical'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_event_severity ON monitoring.event (severity, occurred_at DESC);
