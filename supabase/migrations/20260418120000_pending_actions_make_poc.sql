-- Allow make_poc pending actions (assign contact as deal main contact when Former PoC).

ALTER TABLE public.pending_actions DROP CONSTRAINT IF EXISTS pending_actions_kind_check;

ALTER TABLE public.pending_actions ADD CONSTRAINT pending_actions_kind_check CHECK (
    kind = ANY (ARRAY[
        'create_person'::text,
        'update_person'::text,
        'mark_former'::text,
        'add_note'::text,
        'make_poc'::text
    ])
);
