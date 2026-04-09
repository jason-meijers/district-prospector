from __future__ import annotations
import httpx
from app.config import (
    get_settings,
    PIPEDRIVE_WEBSITE_FIELD_KEY,
    PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY,
)


class PipedriveClient:
    """Thin wrapper around Pipedrive REST API v1."""

    def __init__(self):
        self.settings = get_settings()
        self.base_url = f"https://{self.settings.pipedrive_domain}/api/v1"
        self.token = self.settings.pipedrive_api_token

    def _params(self, extra: dict | None = None) -> dict:
        params = {"api_token": self.token}
        if extra:
            params.update(extra)
        return params

    async def get_organization(self, org_id: int) -> dict:
        """Fetch a single organization by ID."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{self.base_url}/organizations/{org_id}",
                params=self._params(),
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("data") or {}

    async def update_organization(self, org_id: int, fields: dict) -> dict:
        """Update fields on an organization record."""
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.put(
                f"{self.base_url}/organizations/{org_id}",
                params=self._params(),
                json=fields,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", {})

    async def update_org_website(self, org_id: int, url: str) -> dict:
        """Write the URL to the organization's built-in ``website`` field only."""
        return await self.update_organization(org_id, {"website": url})

    async def get_org_persons(self, org_id: int) -> list[dict]:
        """Fetch all persons linked to an organization."""
        persons = []
        start = 0
        async with httpx.AsyncClient(timeout=15) as client:
            while True:
                resp = await client.get(
                    f"{self.base_url}/organizations/{org_id}/persons",
                    params=self._params({"start": start, "limit": 100}),
                )
                resp.raise_for_status()
                body = resp.json()
                items = body.get("data") or []
                persons.extend(items)

                pagination = body.get("additional_data", {}).get("pagination", {})
                if pagination.get("more_items_in_collection"):
                    start = pagination.get("next_start", start + 100)
                else:
                    break
        return persons

    def format_persons_for_prompt(self, persons: list[dict]) -> list[dict]:
        """
        Convert raw Pipedrive person records into the simplified format
        the Claude prompt expects.
        """
        simplified: list[dict] = []

        # Only compare against active, non-merged contacts in our target
        # role categories. This keeps the prompt focused on the people
        # we actually care about and avoids ghost/merged records.
        allowed_role_ids = {467, 468, 470, 471, 623, 472, 482}

        for p in persons:
            # Skip soft-deleted / merged / inactive people
            if not p.get("active_flag", True):
                continue
            if p.get("deleted") or p.get("archived"):
                continue
            if p.get("merge_into_id"):
                continue

            # Pull role category from either custom_fields or top level
            custom_fields = p.get("custom_fields") or {}
            role_id = (
                custom_fields.get(PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY)
                or p.get(PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY)
            )
            # Pipedrive may return enum fields as {id, type} dicts in v2 responses
            if isinstance(role_id, dict):
                role_id = role_id.get("id")
            try:
                role_id_int = int(role_id) if role_id is not None else None
            except (TypeError, ValueError):
                role_id_int = None

            # Only include the role categories we care about
            if role_id_int not in allowed_role_ids:
                continue

            # Extract primary email
            email = None
            if p.get("email"):
                for e in p["email"]:
                    if e.get("primary") or not email:
                        email = e.get("value")

            # Extract primary phone
            phone = None
            if p.get("phone"):
                for ph in p["phone"]:
                    if ph.get("primary") or not phone:
                        phone = ph.get("value")

            # Skip empty phone values Pipedrive sometimes returns
            if phone and phone.strip() == "":
                phone = None

            simplified.append({
                "person_id": p["id"],
                "name": p.get("name", "") or "",
                "job_title": p.get("job_title") or "",
                "role_category_id": role_id_int,
                "email": email,
                "phone": phone,
            })
        return simplified

    async def get_org_deals(self, org_id: int, status: str = "open") -> list[dict]:
        """
        Fetch deals linked to an organisation.  Returns simplified dicts
        with id, title, person_id (main contact), and status.
        Only non-deleted, non-archived deals with the given status are kept.
        """
        deals: list[dict] = []
        start = 0
        async with httpx.AsyncClient(timeout=15) as client:
            while True:
                resp = await client.get(
                    f"{self.base_url}/organizations/{org_id}/deals",
                    params=self._params({"start": start, "limit": 100, "status": status}),
                )
                resp.raise_for_status()
                body = resp.json()
                items = body.get("data") or []
                for d in items:
                    person = d.get("person_id") or {}
                    deals.append({
                        "deal_id": d.get("id"),
                        "title": d.get("title") or "",
                        "person_id": person.get("value") if isinstance(person, dict) else person,
                        "person_name": person.get("name", "") if isinstance(person, dict) else "",
                        "status": d.get("status") or "open",
                    })
                pagination = body.get("additional_data", {}).get("pagination", {})
                if pagination.get("more_items_in_collection"):
                    start = pagination.get("next_start", start + 100)
                else:
                    break
        return deals

    def get_all_person_names(self, persons: list[dict]) -> dict[str, int]:
        """
        Return a mapping of normalised lowercase name → Pipedrive person ID
        for every active, non-deleted person in the org, regardless of role
        category.  Used as a safety net to prevent recommending contacts that
        already exist but weren't included in the role-filtered prompt context.
        """
        names: dict[str, int] = {}
        for p in persons:
            if not p.get("active_flag", True):
                continue
            if p.get("deleted") or p.get("archived") or p.get("merge_into_id"):
                continue
            name = (p.get("name") or "").strip()
            pid = p.get("id")
            if name and pid:
                names[name.lower()] = pid
        return names

    def get_org_website(self, org_data: dict) -> str | None:
        """
        Extract the district website URL from the org record.
        Checks the built-in Pipedrive 'website' field first, then falls back
        to the custom website field (PIPEDRIVE_WEBSITE_FIELD_KEY).  Some
        accounts may also use a legacy 'url' field, so we include that as a
        final fallback.
        """
        url = (
            org_data.get("website")
            or org_data.get("url")
            or org_data.get(PIPEDRIVE_WEBSITE_FIELD_KEY)
        )
        if url and not url.startswith("http"):
            url = f"https://{url}"
        return url or None
