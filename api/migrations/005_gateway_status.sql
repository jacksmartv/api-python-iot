-- Gateway heartbeat table
CREATE TABLE IF NOT EXISTS monitoring.gateway_status (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    serial_number TEXT        NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    csq           INTEGER,
    erf           INTEGER,
    rst           INTEGER,
    vgw           INTEGER,
    raw_payload   JSONB       NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_gateway_status_serial_ts
    ON monitoring.gateway_status (serial_number, ts);
