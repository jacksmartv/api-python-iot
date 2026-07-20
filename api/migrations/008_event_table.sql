-- Migration 008: monitoring.event table for point-in-time fleet events.
-- Used to track gateway.registered, device.registered, and future lifecycle events.
-- Separate from monitoring.incident (open/close lifecycle) — this is append-only audit log.

CREATE TABLE IF NOT EXISTS monitoring.event (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type   TEXT        NOT NULL,   -- 'gateway.registered', 'device.registered'
    entity_type  TEXT        NOT NULL,   -- 'gateway', 'device'
    entity_id    UUID        NOT NULL,
    serial_number TEXT       NOT NULL,
    payload      JSONB
);

CREATE INDEX IF NOT EXISTS ix_event_occurred_at
    ON monitoring.event (occurred_at DESC);

CREATE INDEX IF NOT EXISTS ix_event_entity
    ON monitoring.event (entity_type, entity_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS ix_event_type
    ON monitoring.event (event_type, occurred_at DESC);
