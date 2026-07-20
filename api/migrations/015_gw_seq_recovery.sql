-- Migration 015: recovery state for missing gw_seq (gateway → backend uplink).
-- Supports the self-healing job that requests the lost frames from the gateway's SD card (storage read).
-- Persistent table: survives restarts, auditable, avoids endlessly re-requesting an unrecoverable seq.
-- See SPATIAL_SPRINT_GAP_RECOVERY.md §3.4.

CREATE TABLE IF NOT EXISTS monitoring.gw_seq_recovery (
    gw_serial        TEXT NOT NULL,
    gw_seq           BIGINT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    reason           TEXT,                  -- why it wasn't recovered (see CHECK below)
    attempts         INTEGER NOT NULL DEFAULT 0,  -- storage read commands published successfully
    first_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempt_at  TIMESTAMPTZ,           -- last successful publish of the command
    last_response_at TIMESTAMPTZ,           -- last response received from the gateway (found:true/false)
    recovered_at     TIMESTAMPTZ,
    PRIMARY KEY (gw_serial, gw_seq),
    CONSTRAINT ck_gw_seq_recovery_status
        CHECK (status IN ('pending', 'recovered', 'not_found', 'abandoned')),
    CONSTRAINT ck_gw_seq_recovery_reason
        CHECK (reason IS NULL OR reason IN
               ('NOT_FOUND', 'SD_NOT_READY', 'TIMEOUT', 'PARSER', 'INGEST', 'MQTT_ERROR'))
);

-- the job filters pending rows by gateway+status; the gauge counts pending per gw
CREATE INDEX IF NOT EXISTS ix_gw_seq_recovery_status
    ON monitoring.gw_seq_recovery (gw_serial, status);

-- the /recovery endpoint does WHERE gw_serial = ... ORDER BY gw_seq DESC LIMIT/OFFSET
CREATE INDEX IF NOT EXISTS ix_gw_seq_recovery_serial_seq
    ON monitoring.gw_seq_recovery (gw_serial, gw_seq DESC);
