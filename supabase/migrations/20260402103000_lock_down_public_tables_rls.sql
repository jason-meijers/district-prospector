-- Security hardening: lock down all District Prospector data tables.
-- This project uses server-side Supabase service role access only.

ALTER TABLE public.districts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.found_contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.new_contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pipedrive_contacts_snapshot ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS districts_service_role_all ON public.districts;
CREATE POLICY districts_service_role_all
ON public.districts
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);

DROP POLICY IF EXISTS found_contacts_service_role_all ON public.found_contacts;
CREATE POLICY found_contacts_service_role_all
ON public.found_contacts
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);

DROP POLICY IF EXISTS new_contacts_service_role_all ON public.new_contacts;
CREATE POLICY new_contacts_service_role_all
ON public.new_contacts
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);

DROP POLICY IF EXISTS pipedrive_contacts_snapshot_service_role_all ON public.pipedrive_contacts_snapshot;
CREATE POLICY pipedrive_contacts_snapshot_service_role_all
ON public.pipedrive_contacts_snapshot
FOR ALL
TO service_role
USING (true)
WITH CHECK (true);

-- Defensive hardening for queue RPC exposure.
REVOKE ALL ON FUNCTION public.claim_next_district() FROM PUBLIC;
REVOKE ALL ON FUNCTION public.claim_next_district() FROM anon;
REVOKE ALL ON FUNCTION public.claim_next_district() FROM authenticated;
GRANT EXECUTE ON FUNCTION public.claim_next_district() TO service_role;
