-- Phase 4: pending_actions for Slack Block Kit interactive buttons.
-- The Slack message carries only the action id; the full Pipedrive API
-- payload lives server-side so the payload is never user-editable and
-- the Slack UI stays clean.

CREATE TABLE IF NOT EXISTS public.pending_actions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    kind text NOT NULL CHECK (kind = ANY (ARRAY[
        'create_person',
        'update_person',
        'mark_former',
        'add_note'
    ])),
    pipedrive_org_id bigint,
    pipedrive_person_id bigint,
    payload jsonb NOT NULL,
    note_payload jsonb,
    status text NOT NULL DEFAULT 'pending' CHECK (status = ANY (ARRAY[
        'pending',
        'executing',
        'executed',
        'failed',
        'cancelled'
    ])),
    slack_channel text,
    slack_message_ts text,
    slack_response_url text,
    executed_at timestamptz,
    executed_by text,
    result jsonb,
    error text,
    created_at timestamptz NOT NULL DEFAULT timezone('utc', now())
);

CREATE INDEX IF NOT EXISTS pending_actions_status_idx
    ON public.pending_actions (status);
CREATE INDEX IF NOT EXISTS pending_actions_created_at_idx
    ON public.pending_actions (created_at DESC);

ALTER TABLE public.pending_actions ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pending_actions_service_role_all ON public.pending_actions;
CREATE POLICY pending_actions_service_role_all
ON public.pending_actions
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);
