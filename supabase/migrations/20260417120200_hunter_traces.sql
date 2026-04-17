-- Phase 3: ContactHunter per-step tracing for observability + debugging.

CREATE TABLE IF NOT EXISTS public.hunter_traces (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    hunt_id uuid NOT NULL,
    district_id uuid REFERENCES public.districts(id) ON DELETE SET NULL,
    district_name text,
    step integer NOT NULL,
    tool text NOT NULL,
    args jsonb NOT NULL DEFAULT '{}'::jsonb,
    result_summary jsonb NOT NULL DEFAULT '{}'::jsonb,
    tokens_input integer,
    tokens_output integer,
    duration_ms integer,
    error text,
    created_at timestamptz NOT NULL DEFAULT timezone('utc', now())
);

CREATE INDEX IF NOT EXISTS hunter_traces_hunt_id_idx
    ON public.hunter_traces (hunt_id, step);
CREATE INDEX IF NOT EXISTS hunter_traces_district_id_idx
    ON public.hunter_traces (district_id);

ALTER TABLE public.hunter_traces ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS hunter_traces_service_role_all ON public.hunter_traces;
CREATE POLICY hunter_traces_service_role_all
ON public.hunter_traces
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);
