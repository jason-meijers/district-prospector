-- Durable "user skipped this Slack review" so future runs do not re-post Block Kit.

CREATE TABLE IF NOT EXISTS public.contact_review_skips (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    pipedrive_org_id bigint NOT NULL,
    pipedrive_person_id bigint,
    kind text NOT NULL CHECK (kind = ANY (ARRAY[
        'create_person',
        'update_person',
        'mark_former',
        'make_poc'
    ])),
    create_name_key text,
    skipped_by text,
    created_at timestamptz NOT NULL DEFAULT timezone('utc', now())
);

-- One skip per org + person + action kind (when we have a Pipedrive person id).
CREATE UNIQUE INDEX IF NOT EXISTS contact_review_skips_org_person_kind_uidx
    ON public.contact_review_skips (pipedrive_org_id, pipedrive_person_id, kind)
    WHERE pipedrive_person_id IS NOT NULL;

-- Proposed creates have no person id — key by normalized name|title within org.
CREATE UNIQUE INDEX IF NOT EXISTS contact_review_skips_org_create_key_uidx
    ON public.contact_review_skips (pipedrive_org_id, create_name_key)
    WHERE create_name_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS contact_review_skips_org_idx
    ON public.contact_review_skips (pipedrive_org_id);

ALTER TABLE public.contact_review_skips ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS contact_review_skips_service_role_all ON public.contact_review_skips;
CREATE POLICY contact_review_skips_service_role_all
ON public.contact_review_skips
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);
