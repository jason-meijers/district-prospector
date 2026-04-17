-- Phase 3.5: per-district research mode override.
-- null (or missing) → use global Settings.contact_hunter_mode
-- 'pipeline'         → skip ContactHunter entirely
-- 'hybrid'           → run pipeline first, hunt on gaps
-- 'full_agent'       → skip pipeline; ContactHunter runs the whole research

ALTER TABLE public.districts
    ADD COLUMN IF NOT EXISTS research_mode text;

ALTER TABLE public.districts
    DROP CONSTRAINT IF EXISTS districts_research_mode_check;

ALTER TABLE public.districts
    ADD CONSTRAINT districts_research_mode_check
    CHECK (
        research_mode IS NULL
        OR research_mode = ANY (ARRAY['pipeline', 'hybrid', 'full_agent'])
    );

COMMENT ON COLUMN public.districts.research_mode IS
    'Per-district override for research strategy. null = fall back to Settings.contact_hunter_mode.';
