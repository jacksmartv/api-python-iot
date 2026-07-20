-- Table of known gateways (equivalent to core.device for nodes)
CREATE TABLE IF NOT EXISTS core.gateway (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    serial_number TEXT        NOT NULL UNIQUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata      JSONB
);
