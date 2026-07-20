-- Migration 017: adapts monitoring.gw_seq_recovery to the V3 model (recovery by seq batch).
-- V3 reuses the per-seq table from V1 (015) but adds the 'inflight' state (seq included in a
-- requested batch, awaiting response) + inflight_at (for the timeout) + batch_id (batch correlation).
-- Does NOT touch gw_log_scan (016, from V2) — dropped in a later cleanup migration 018 once V3 is validated.
-- See SPATIAL_SPRINT_GAP_RECOVERY_V3.md §3.4.

-- 1. FIRST clean up states that V3 no longer uses (abandoned from V1) → pending. Must happen BEFORE
--    recreating the CHECK: if the new CHECK (without 'abandoned') is applied while abandoned rows exist,
--    Postgres rejects it (CheckViolation) and the migration fails. (V1's attempts/reason are left unused.)
UPDATE monitoring.gw_seq_recovery SET status = 'pending'
    WHERE status = 'abandoned';

-- 2. NOW recreate the CHECK with the 'inflight' state (no rows violate it anymore).
ALTER TABLE monitoring.gw_seq_recovery DROP CONSTRAINT IF EXISTS ck_gw_seq_recovery_status;
ALTER TABLE monitoring.gw_seq_recovery ADD CONSTRAINT ck_gw_seq_recovery_status
    CHECK (status IN ('pending', 'inflight', 'recovered', 'not_found'));

-- 3. new columns for the in-flight batch
ALTER TABLE monitoring.gw_seq_recovery
    ADD COLUMN IF NOT EXISTS inflight_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS batch_id    UUID;
