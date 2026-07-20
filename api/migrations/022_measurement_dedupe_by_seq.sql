-- Migration 022: dedupe measurements by (sensor_id, msg_counter), in addition to the PK by ts.
--
-- The PK (sensor_id, ts) is required for gap_recovery (correlates by the frame's ts_ms) and is not
-- touched. But an HM node can resend the SAME message (same node_seq, no new reading) due to an
-- RF-layer retry — the gateway receives it as a distinct uplink (its own gw_seq and reception
-- ts_ms), so dedup by (sensor_id, ts) doesn't catch it: it goes in as a legitimately "new" row
-- even though it's the same repeated reading. Confirmed live (2026-07-12): two frames with
-- consecutive gw_seq (57991/57992) and different ts_ms (2.5s apart) but the same `raw` hex and the
-- same node_seq. Found by exposing the Seq column in the webui (Recent Measurements).
--
-- Before adding the index, clean up existing duplicates: keep the oldest row
-- for each (sensor_id, msg_counter), delete the rest.
DELETE FROM telemetry.measurement m
USING (
    SELECT sensor_id, msg_counter, min(ts) AS keep_ts
    FROM telemetry.measurement
    WHERE msg_counter IS NOT NULL
    GROUP BY sensor_id, msg_counter
    HAVING count(*) > 1
) dupes
WHERE m.sensor_id = dupes.sensor_id
  AND m.msg_counter = dupes.msg_counter
  AND m.ts <> dupes.keep_ts;

-- Partial unique index (NULL does not participate — other sensor_types may not carry msg_counter).
-- `ON CONFLICT DO NOTHING` without explicit index_elements (see services/ingestion.py
-- _process_sensor_data) already covers this constraint in addition to the PK, with no code changes.
CREATE UNIQUE INDEX IF NOT EXISTS uq_measurement_sensor_msg_counter
    ON telemetry.measurement (sensor_id, msg_counter)
    WHERE msg_counter IS NOT NULL;
