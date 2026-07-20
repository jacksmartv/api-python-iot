-- Migration 016: gateway log scan cursor (gap_recovery V2).
-- V2 paginates gwlog.txt with read offset/limit (sequential, cheap) instead of requesting
-- individual seqs (read seq:N → read_busy). This table stores the scan cursor per gateway, resumable across runs.
-- monitoring.gw_seq_recovery (V1) is NOT dropped: it's kept as history until V2 is validated in prod;
-- the V2 job simply stops using it. Dropped in a later migration. See SPATIAL_SPRINT_GAP_RECOVERY_V2.md.

CREATE TABLE IF NOT EXISTS monitoring.gw_log_scan (
    gw_serial               TEXT PRIMARY KEY,
    next_offset             BIGINT NOT NULL DEFAULT 0,   -- cursor: next page to request
    last_successful_offset  BIGINT,                      -- last confirmed offset persisted
    last_total_lines        BIGINT,                      -- number of log lines last time (info, UI)
    inflight_offset         BIGINT,                      -- page requested and not yet responded (NULL = idle)
    inflight_at             TIMESTAMPTZ,                 -- when it was requested (to expire the in-flight)
    last_page_at            TIMESTAMPTZ,                 -- last page requested
    last_response_at        TIMESTAMPTZ,                 -- last response received
    recovered_total         BIGINT NOT NULL DEFAULT 0,   -- frames INSERTED by the scan (cumulative)
    last_busy_at            TIMESTAMPTZ                  -- last read_busy (gateway health)
);
