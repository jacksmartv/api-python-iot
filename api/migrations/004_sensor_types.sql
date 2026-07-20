-- Migration 004: No DDL required.
-- core.sensor.sensor_type TEXT already exists from migration 001.
-- UNIQUE(device_id, sensor_index) is the correct identity constraint.
-- Supported types validated at application layer: 'temperature', 'humidity'.
-- Max 3 sensors per device enforced at application layer.
SELECT 1;
