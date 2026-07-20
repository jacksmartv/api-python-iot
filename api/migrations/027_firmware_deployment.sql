-- Migration 027: OTA firmware deploy to gateways via MQTT command (firmware OTA Phase 2).
-- See FIRMWARE_SPRINT_OTA_DEPLOY_MQTT.md — release (core.firmware_release, migration 026) and
-- deployment are different concepts: a release exists only once, a deploy is an event
-- (the same version can be deployed to one gateway today and others tomorrow).

-- Modeled on: monitoring.gw_seq_recovery (migrations 015/017) — same nature (outbound MQTT
-- command with state tracking). Plain gw_serial TEXT, no formal FK to core.gateway,
-- same pattern as that table.

-- A triggered deploy: one firmware version, to one or more gateways, started by an admin.
-- "which gateways" is not persisted here — that lives in firmware_deployment_gateway (1 row per
-- target gateway), allowing a deploy to 50 gateways to have 50 independent states.
CREATE TABLE IF NOT EXISTS core.firmware_deployment (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    firmware_release_id UUID NOT NULL REFERENCES core.firmware_release(id),
    created_by          UUID REFERENCES core."user"(id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Deploy state FOR ONE specific gateway. Plain gw_serial TEXT (no formal FK to
-- core.gateway), same pattern as monitoring.gw_seq_recovery — this table is an MQTT command
-- log, not a domain relationship.
CREATE TABLE IF NOT EXISTS core.firmware_deployment_gateway (
    deployment_id    UUID NOT NULL REFERENCES core.firmware_deployment(id) ON DELETE CASCADE,
    gw_serial        TEXT NOT NULL,
    -- pending: created, command not yet published (or publish failed, see command_sent)
    -- command_sent: send_command confirmed the publish (does not imply the gateway received it)
    -- success | failed | timeout: terminal states
    status           TEXT NOT NULL DEFAULT 'pending',
    -- MQTT command id (correlates with the ack on cmd/ack). Identifies ONE specific MQTT
    -- publish, NOT the whole logical deployment — a redeploy of the SAME version to the SAME
    -- gateway (a manual retry, for example) generates a new row with a new request_id, it does
    -- not reuse the old one. UNIQUE, not just an index: a request_id should never repeat across
    -- rows — if some day an UPDATE filters only by request_id (instead of
    -- deployment_id+gw_serial), a collision would be a silent bug (updates the wrong
    -- row). The UNIQUE constraint makes that impossible at the DB level.
    request_id       UUID,
    error_detail     TEXT,          -- error ack detail, if status='failed'
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    command_sent_at  TIMESTAMPTZ,
    acked_at         TIMESTAMPTZ,   -- when ANY ack arrived (success or failed)
    PRIMARY KEY (deployment_id, gw_serial),
    CONSTRAINT ck_firmware_deployment_gateway_status
        CHECK (status IN ('pending', 'command_sent', 'success', 'failed', 'timeout'))
);
CREATE INDEX IF NOT EXISTS ix_firmware_deployment_gateway_serial
    ON core.firmware_deployment_gateway (gw_serial, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_firmware_deployment_gateway_request_id
    ON core.firmware_deployment_gateway (request_id) WHERE request_id IS NOT NULL;

COMMENT ON TABLE core.firmware_deployment IS 'Deploy of a firmware version to one or more gateways — a distinct event from firmware_release (the catalog). See FIRMWARE_SPRINT_OTA_DEPLOY_MQTT.md.';
COMMENT ON TABLE core.firmware_deployment_gateway IS 'Deploy state per target gateway. request_id correlates with gateway/{serial}/cmd/ack.';
