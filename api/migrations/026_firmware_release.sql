-- Migration 026: OTA firmware releases for ESP32 gateways.
-- Uploads the .bin to S3 (dedicated bucket sensorhub-fw, prefix gateway/*, public read without listing)
-- and records the row here. The MQTT `ota` command that triggers the download from the gateway
-- is NOT implemented in this sprint (manual deploy: the public_url is copied and used through
-- another channel) — see FIRMWARE_SPRINT_OTA_S3_UPLOAD.md.
--
-- Immutable versions: once a version is published it cannot be replaced (UNIQUE on version).
-- The S3 object uses a fixed name (firmware.bin, namespaced by version) — original_filename
-- is preserved only as metadata for the UI, to avoid accumulating name variants in the bucket.

CREATE TABLE IF NOT EXISTS core.firmware_release (
    id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    original_filename TEXT NOT NULL,
    version           TEXT NOT NULL UNIQUE,
    target            TEXT NOT NULL DEFAULT 'gateway',   -- reserved: currently always 'gateway', no UI selector
    s3_key            TEXT NOT NULL,
    public_url        TEXT NOT NULL,
    size_bytes        BIGINT NOT NULL,
    checksum_sha256   TEXT NOT NULL,
    etag              TEXT,                              -- S3 identifier, not a cryptographic validation
    created_by        UUID REFERENCES core."user"(id),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE core.firmware_release IS 'OTA firmware releases (.bin) uploaded to S3 (bucket sensorhub-fw), pending MQTT ota command for automatic deploy to gateways.';
