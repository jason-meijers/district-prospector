-- Phase 0: eval harness
-- Seed with 20-30 districts whose expected contacts are known-correct, then
-- run app.eval to capture precision/recall over time as the pipeline evolves.

CREATE TABLE IF NOT EXISTS public.eval_districts (
    district_id uuid PRIMARY KEY REFERENCES public.districts(id) ON DELETE CASCADE,
    expected_superintendent jsonb,
    expected_curriculum jsonb,
    expected_cte jsonb,
    notes text,
    last_verified timestamptz,
    created_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
    updated_at timestamptz NOT NULL DEFAULT timezone('utc', now())
);

COMMENT ON TABLE public.eval_districts IS
    'Ground-truth contacts per district for the eval harness. Each expected_* column holds a jsonb array of {name, job_title, email, phone}.';

CREATE TABLE IF NOT EXISTS public.eval_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_label text,
    research_mode text NOT NULL DEFAULT 'pipeline',
    started_at timestamptz NOT NULL DEFAULT timezone('utc', now()),
    completed_at timestamptz,
    district_count integer NOT NULL DEFAULT 0,
    overall_precision numeric,
    overall_recall numeric,
    per_district jsonb NOT NULL DEFAULT '[]'::jsonb,
    summary jsonb NOT NULL DEFAULT '{}'::jsonb
);

COMMENT ON TABLE public.eval_runs IS
    'One row per eval run; per_district contains {district_id, precision, recall, missing, extra, pipeline_usage}.';

CREATE INDEX IF NOT EXISTS eval_runs_started_at_idx
    ON public.eval_runs (started_at DESC);

ALTER TABLE public.eval_districts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.eval_runs ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS eval_districts_service_role_all ON public.eval_districts;
CREATE POLICY eval_districts_service_role_all
ON public.eval_districts
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);

DROP POLICY IF EXISTS eval_runs_service_role_all ON public.eval_runs;
CREATE POLICY eval_runs_service_role_all
ON public.eval_runs
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);
