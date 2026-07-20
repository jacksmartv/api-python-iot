-- Migration 012: telemetry over the floorplan (Sprint 3)
-- (a) index for the EXACT predicate used by the snapshot refresh job.
-- (b) rssi_ts: RSSI freshness kept separate from last_ts (RSSI and temperature have different cycles).

-- (a) partial index that replicates the job's predicate: device_id IS NOT NULL AND deleted_at IS NULL
CREATE INDEX IF NOT EXISTS ix_asset_device_alive
    ON spatial.asset (device_id)
    WHERE device_id IS NOT NULL AND deleted_at IS NULL;

-- (b) RSSI freshness kept separate from last_ts
ALTER TABLE spatial.asset_telemetry_snapshot ADD COLUMN IF NOT EXISTS rssi_ts TIMESTAMPTZ;

COMMENT ON COLUMN spatial.asset_telemetry_snapshot.rssi_ts IS 'Timestamp of the last device_status (RSSI/supply); distinct from last_ts (measurement)';
