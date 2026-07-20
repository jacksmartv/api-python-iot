-- Migration 018: gap_recovery V2 cleanup. V3 (seq batching) replaced the offset/limit scan,
-- so the scan cursor table is no longer used. See SPATIAL_SPRINT_GAP_RECOVERY_V3.md §5.

DROP TABLE IF EXISTS monitoring.gw_log_scan;
