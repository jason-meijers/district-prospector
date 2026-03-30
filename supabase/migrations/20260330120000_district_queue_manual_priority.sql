-- Queue: status 'manual' is claimed before 'pending'.
-- Set a row's status to 'manual' in Supabase to run it next (among manual rows,
-- most recently updated first). After a run it becomes 'done' or 'error' like other jobs.

ALTER TABLE public.districts DROP CONSTRAINT IF EXISTS districts_status_check;

ALTER TABLE public.districts ADD CONSTRAINT districts_status_check
  CHECK (status = ANY (ARRAY[
    'pending'::text,
    'manual'::text,
    'processing'::text,
    'done'::text,
    'error'::text
  ]));

CREATE OR REPLACE FUNCTION public.claim_next_district()
RETURNS SETOF districts
LANGUAGE sql
AS $function$
  UPDATE districts d
  SET
    status = 'processing',
    updated_at = timezone('utc', now())
  FROM (
    SELECT id
    FROM districts
    WHERE status IN ('manual', 'pending')
    ORDER BY
      CASE WHEN status = 'manual' THEN 0 ELSE 1 END,
      CASE WHEN status = 'manual' THEN updated_at END DESC NULLS LAST,
      CASE WHEN status = 'pending' THEN created_at END ASC NULLS LAST
    FOR UPDATE SKIP LOCKED
    LIMIT 1
  ) picked
  WHERE d.id = picked.id
  RETURNING d.*;
$function$;
