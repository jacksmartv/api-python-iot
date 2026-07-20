-- Migration 010: Spatial Management module
-- Schema 'spatial' for buildings/floors/layers/assets over floorplans.
-- Single-tenant in V1: org_id present in every table but hardcoded
-- (DEFAULT_ORG_ID) until V2. See SPATIAL_SPRINT1_BACKEND.md.
-- Uses gen_random_uuid() (native PG13+, same as migration 007).

CREATE SCHEMA IF NOT EXISTS spatial;

-- ---------------------------------------------------------------------------
-- BUILDING
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spatial.building (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id      UUID NOT NULL,                       -- V2: currently = DEFAULT_ORG_ID
    name        TEXT NOT NULL,
    address     TEXT,
    city        TEXT,
    country     TEXT,
    lat         DOUBLE PRECISION,
    lng         DOUBLE PRECISION,
    timezone    TEXT NOT NULL DEFAULT 'UTC',
    metadata    JSONB,
    deleted_at  TIMESTAMPTZ,                         -- soft delete
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by  UUID REFERENCES core."user"(id) ON DELETE SET NULL
);

-- ---------------------------------------------------------------------------
-- FLOOR
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spatial.floor (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    building_id   UUID NOT NULL REFERENCES spatial.building(id) ON DELETE CASCADE,
    org_id        UUID NOT NULL,
    name          TEXT NOT NULL,
    level         INTEGER NOT NULL DEFAULT 0,
    sort_order    INTEGER NOT NULL DEFAULT 0,
    plan_url      TEXT,
    plan_viewbox  TEXT,
    plan_width_m  NUMERIC(10,2),
    plan_height_m NUMERIC(10,2),
    plan_version  INTEGER NOT NULL DEFAULT 0,
    plan_status   TEXT NOT NULL DEFAULT 'none',      -- none/processing/ready/failed (Sprint 2)
    deleted_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- LAYER
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spatial.layer (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    floor_id        UUID NOT NULL REFERENCES spatial.floor(id) ON DELETE CASCADE,
    org_id          UUID NOT NULL,
    name            TEXT NOT NULL,
    color           TEXT NOT NULL DEFAULT '#3b82f6',
    icon            TEXT NOT NULL DEFAULT 'circle',
    default_visible BOOLEAN NOT NULL DEFAULT true,
    sort_order      INTEGER NOT NULL DEFAULT 0,      -- render order (z)
    deleted_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- ASSET  (Asset != Device: device_id NULLABLE — fire extinguishers/exits are assets with no telemetry)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spatial.asset (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    floor_id     UUID NOT NULL REFERENCES spatial.floor(id) ON DELETE CASCADE,
    building_id  UUID NOT NULL REFERENCES spatial.building(id) ON DELETE CASCADE,
    org_id       UUID NOT NULL,
    layer_id     UUID REFERENCES spatial.layer(id) ON DELETE SET NULL,   -- primary scalar layer
    device_id    UUID REFERENCES core.device(id) ON DELETE SET NULL,     -- NULLABLE
    asset_type   TEXT NOT NULL,                       -- free TEXT, validated in app (no PG enum)
    name         TEXT NOT NULL,
    display_name TEXT,
    pos_x        NUMERIC(8,6),
    pos_y        NUMERIC(8,6),
    pos_z        NUMERIC(8,6),                         -- reserved for V4 (3D), NULL in V1
    rotation_deg NUMERIC(6,2) NOT NULL DEFAULT 0,
    plan_version INTEGER,
    version      INTEGER NOT NULL DEFAULT 0,           -- optimistic locking
    properties   JSONB,
    tags         TEXT[] NOT NULL DEFAULT '{}',
    pos_source   TEXT NOT NULL DEFAULT 'manual',       -- manual/import
    deleted_at   TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by   UUID REFERENCES core."user"(id) ON DELETE SET NULL
);

-- ---------------------------------------------------------------------------
-- ASSET_LAYER  (additive N:N, dormant until V3 — multi-layer per asset)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spatial.asset_layer (
    asset_id UUID NOT NULL REFERENCES spatial.asset(id) ON DELETE CASCADE,
    layer_id UUID NOT NULL REFERENCES spatial.layer(id) ON DELETE CASCADE,
    PRIMARY KEY (asset_id, layer_id)
);

-- ---------------------------------------------------------------------------
-- ASSET_POSITION_HISTORY  (append-only; audit + replay)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spatial.asset_position_history (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_id     UUID NOT NULL REFERENCES spatial.asset(id) ON DELETE CASCADE,
    org_id       UUID NOT NULL,
    pos_x        NUMERIC(8,6),
    pos_y        NUMERIC(8,6),
    rotation_deg NUMERIC(6,2),
    plan_version INTEGER NOT NULL,
    source       TEXT NOT NULL DEFAULT 'api',         -- api / mqtt / import
    session_id   UUID,                                -- batch movement tracking
    moved_by     UUID REFERENCES core."user"(id) ON DELETE SET NULL,
    moved_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- ASSET_TELEMETRY_SNAPSHOT  (table already created; UPSERT in ingest is Sprint 3)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spatial.asset_telemetry_snapshot (
    asset_id      UUID PRIMARY KEY REFERENCES spatial.asset(id) ON DELETE CASCADE,
    org_id        UUID NOT NULL,
    device_id     UUID,
    last_ts       TIMESTAMPTZ,
    temperature_c NUMERIC,
    humidity_pct  NUMERIC,
    supply_mv     INTEGER,
    rssi_dbm      INTEGER,
    status        TEXT,                                -- online/offline/unknown
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- INDEXES (partial: normal queries only see live rows)
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_building_org
    ON spatial.building (org_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_floor_building
    ON spatial.floor (building_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_layer_floor
    ON spatial.layer (floor_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_asset_floor_alive
    ON spatial.asset (floor_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_asset_floor_layer
    ON spatial.asset (floor_id, layer_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_asset_device
    ON spatial.asset (device_id) WHERE device_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_asset_pos_hist_asset
    ON spatial.asset_position_history (asset_id, moved_at DESC);

-- ---------------------------------------------------------------------------
-- TRIGGER updated_at  (verified: no reusable function exists in the project)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION spatial.touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_building_touch ON spatial.building;
CREATE TRIGGER trg_building_touch BEFORE UPDATE ON spatial.building
    FOR EACH ROW EXECUTE FUNCTION spatial.touch_updated_at();

DROP TRIGGER IF EXISTS trg_floor_touch ON spatial.floor;
CREATE TRIGGER trg_floor_touch BEFORE UPDATE ON spatial.floor
    FOR EACH ROW EXECUTE FUNCTION spatial.touch_updated_at();

DROP TRIGGER IF EXISTS trg_asset_touch ON spatial.asset;
CREATE TRIGGER trg_asset_touch BEFORE UPDATE ON spatial.asset
    FOR EACH ROW EXECUTE FUNCTION spatial.touch_updated_at();

-- ---------------------------------------------------------------------------
-- COMMENTS
-- ---------------------------------------------------------------------------
COMMENT ON SCHEMA spatial IS 'Spatial Management: buildings, floors, layers, assets over floorplans';
COMMENT ON COLUMN spatial.asset.device_id IS 'Optional link to core.device. NULL = physical asset with no telemetry (extinguisher, exit)';
COMMENT ON COLUMN spatial.asset.version IS 'Optimistic locking: UPDATE ... WHERE version = expected';
COMMENT ON COLUMN spatial.asset.pos_z IS 'Reserved for V4 (3D Digital Twin); NULL in V1';
