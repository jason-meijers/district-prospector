-- Upgrade deployments that ran the original contact_review_skips migration
-- before new_contact_not_target_role and the create_key+kind unique index.

ALTER TABLE public.contact_review_skips DROP CONSTRAINT IF EXISTS contact_review_skips_kind_check;
ALTER TABLE public.contact_review_skips ADD CONSTRAINT contact_review_skips_kind_check CHECK (kind = ANY (ARRAY[
    'create_person',
    'update_person',
    'mark_former',
    'make_poc',
    'new_contact_not_target_role'
]));

DROP INDEX IF EXISTS contact_review_skips_org_create_key_uidx;
CREATE UNIQUE INDEX IF NOT EXISTS contact_review_skips_org_create_key_kind_uidx
    ON public.contact_review_skips (pipedrive_org_id, create_name_key, kind)
    WHERE create_name_key IS NOT NULL;
