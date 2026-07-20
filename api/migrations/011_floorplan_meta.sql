-- Migration 011: floorplan observability fields (Sprint 2 — SVG Ingestion Pipeline)
-- Adds the fields that spatial.floor was missing to (a) unstick hung uploads,
-- (b) report actionable errors, (c) tune limits with real data.

ALTER TABLE spatial.floor ADD COLUMN IF NOT EXISTS plan_node_count INTEGER;
ALTER TABLE spatial.floor ADD COLUMN IF NOT EXISTS plan_svg_size_kb INTEGER;
ALTER TABLE spatial.floor ADD COLUMN IF NOT EXISTS plan_error TEXT;
ALTER TABLE spatial.floor ADD COLUMN IF NOT EXISTS plan_processing_started_at TIMESTAMPTZ;

COMMENT ON COLUMN spatial.floor.plan_node_count IS 'Nodes in the normalized SVG (guard against heavy rendering)';
COMMENT ON COLUMN spatial.floor.plan_svg_size_kb IS 'Size of the normalized SVG in KB (double guard with node_count)';
COMMENT ON COLUMN spatial.floor.plan_error IS 'Actionable reason when plan_status=failed';
COMMENT ON COLUMN spatial.floor.plan_processing_started_at IS 'Marks active processing; NULL if no job is in progress (unstuck via timeout)';
