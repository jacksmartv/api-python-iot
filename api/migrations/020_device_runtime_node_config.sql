-- Migration 020: current OTA config of the HM node in device_runtime.
--
-- The HM v3.6 frame carries measure_cycles/send_cycles/hum_alarm on every uplink (decode_hm_v36),
-- previously decoded and discarded. The node has no "read config" command (unlike the
-- gateway, see monitoring.gateway_config_v3) — this IS the only way to see its current config.
-- Same pattern as alarm/low_batt: 1 row/device, UPSERT on every frame, CURRENT state (not historical).

ALTER TABLE monitoring.device_runtime
    ADD COLUMN IF NOT EXISTS measure_cycles       INTEGER,
    ADD COLUMN IF NOT EXISTS send_cycles          INTEGER,
    ADD COLUMN IF NOT EXISTS hum_alarm_threshold  INTEGER;
