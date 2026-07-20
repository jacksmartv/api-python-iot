-- Migration 025: target moisture range (%CH) per comparison group.
-- Each group defines an expected minimum and maximum %CH; in the UI, each sensor's
-- gauge shows whether its latest reading falls within that range.

ALTER TABLE core.comparison_group
    ADD COLUMN IF NOT EXISTS target_min NUMERIC,
    ADD COLUMN IF NOT EXISTS target_max NUMERIC;

COMMENT ON COLUMN core.comparison_group.target_min IS 'Minimum expected %CH moisture for the group (target range).';
COMMENT ON COLUMN core.comparison_group.target_max IS 'Maximum expected %CH moisture for the group (target range).';
