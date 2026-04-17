from __future__ import annotations
from typing import Literal
from pydantic_settings import BaseSettings
from functools import lru_cache


# ─────────────────────────────────────────────────────────────
# Pipedrive Field Keys (hardcoded — these rarely change)
# ─────────────────────────────────────────────────────────────

# Organization: "Research Contacts" dropdown
PIPEDRIVE_TRIGGER_FIELD_KEY = "cf11c91a6c8f4a5224996873a268427012008ca5"
PIPEDRIVE_TRIGGER_OPTION_ID = 632       # "Trigger" option (incoming)
PIPEDRIVE_TRIGGER_DONE_ID = 633         # "Done" option (set on completion)

# Organization: Email domain field
PIPEDRIVE_EMAIL_DOMAIN_FIELD_KEY = "2ac5713c612dbdf7aa0a0ae58a984b6f625ab7a5"

# Organization: Last researched date field
PIPEDRIVE_LAST_RESEARCHED_FIELD_KEY = "07c187825ba2796affd2bbdaf34e34db5c616b35"

# Organization: Website URL field
PIPEDRIVE_WEBSITE_FIELD_KEY = "4a4f94dd89f0afbdf947300ce0972423b04afd74"

# Organization: District name field — used to identify school district orgs.
# Only orgs where this field is non-empty are synced into the batch pipeline.
PIPEDRIVE_DISTRICT_NAME_FIELD_KEY = "032ab30b355f790ba1fe3e8deb5413b73530b47b"

# Person: "Role Category" dropdown
PIPEDRIVE_ROLE_CATEGORY_FIELD_KEY = "7f22b8624616bef2d0adce26b28f8b3055dcbaae"

# Person: "Salutation / Title" dropdown
PIPEDRIVE_SALUTATION_FIELD_KEY = "daad0b7d71c528d6378701c5adb3d5c0d5254d33"

# Salutation options — label (with period) → option ID
SALUTATION_OPTIONS: dict[str, int] = {
    "Dr.": 139,
    "Mrs.": 169,
    "Mr.": 170,
    "Ms.": 171,
}

# All Role Category dropdown options — Claude will pick the best match
ROLE_CATEGORY_OPTIONS = {
    465: "Accounts Payable & Purchasing",
    498: "Administrative Assistant",
    466: "Advisor",
    467: "Assistant CTE Director",
    617: "Assistant Principal",
    468: "Assistant Superintendent",
    469: "Counselor",
    470: "CTE Coordinator",
    471: "CTE Director",
    623: "Curriculum Coordinator",
    472: "Curriculum Director",
    473: "Department Head",
    474: "Director",
    475: "Former",
    476: "GEAR UP Coordinator",
    477: "GEAR UP Director",
    607: "IT",
    478: "Other",
    479: "President",
    480: "Principal",
    481: "Program Manager",
    482: "Superintendent",
    483: "Teacher",
    484: "TRIO Assistant Director",
    485: "TRIO Coordinator",
    486: "TRIO Director",
}

# Reverse lookup: label → ID
ROLE_CATEGORY_BY_LABEL = {v: k for k, v in ROLE_CATEGORY_OPTIONS.items()}

# Role desirability ranking for deal contact replacement.
# Lower index = more desirable.  Same-role match is always preferred;
# this list is the tiebreaker when multiple candidates are available.
ROLE_PRIORITY: list[int] = [
    482,  # Superintendent
    468,  # Assistant Superintendent
    472,  # Curriculum Director
    623,  # Curriculum Coordinator
    471,  # CTE Director
    467,  # Assistant CTE Director
    470,  # CTE Coordinator
    474,  # Director
    481,  # Program Manager
    478,  # Other
]


class Settings(BaseSettings):
    # Anthropic
    anthropic_api_key: str

    # Pipedrive
    pipedrive_api_token: str
    pipedrive_domain: str

    # Slack
    slack_bot_token: str
    slack_channel_id: str

    # Agent (existing webhook flow)
    max_subpages: int = 7
    max_directory_pages: int = 2  # extra pagination pages to fetch per staff-directory-like URL
    claude_model: str = "claude-sonnet-4-20250514"
    pipedrive_use_batch_pipeline: bool = False

    # Optional: require this secret on /webhook/pipedrive (header X-Webhook-Secret or query ?secret=)
    webhook_secret: str | None = None

    # Batch pipeline (all optional so existing webhook flow works without them)
    supabase_url: str | None = None
    supabase_service_key: str | None = None
    firecrawl_api_key: str | None = None
    # Firecrawl /v2/scrape job timeout (ms). API default 30000; slow or JS-heavy pages often need 60–120s+.
    firecrawl_scrape_timeout_ms: int = 120_000
    # Extra delay before scrape (ms) for client-rendered content; 0 = off.
    firecrawl_scrape_wait_for_ms: int = 0

    # Optional: after scrape, use /v2/scrape/{id}/interact for JS-heavy / paginated directories
    firecrawl_interact_enabled: bool = True
    # Trigger interact only when markdown is shorter than this AND URL looks directory-like
    firecrawl_interact_min_markdown_chars: int = 400
    firecrawl_interact_max_pages: int = 15
    firecrawl_interact_max_seconds: int = 120
    firecrawl_interact_concurrency: int = 2

    # Directory-first pipeline: map candidate count, cheap LLM URL triage, post-triage scrape caps
    batch_map_candidate_limit: int = 50
    batch_triage_max_candidates: int = 45
    batch_url_triage_model: str = "claude-haiku-4-5"
    batch_max_scrape_urls: int = 8
    batch_enrichment_url_cap: int = 5

    # How many districts to process in parallel per batch run
    batch_concurrency: int = 10
    # Max distinct target URLs to scrape per district (not counting pagination within a directory)
    batch_max_target_pages: int = 15
    # Max characters to pass to Claude per page (controls token cost).
    # 12k avoids losing staff blocks on sites with heavy repeated nav before main content.
    batch_chars_per_page: int = 12000

    # Pipedrive-triggered max-lead profile (used when pipedrive_use_batch_pipeline=true)
    pipedrive_map_candidate_limit: int = 80
    pipedrive_triage_max_candidates: int = 60
    pipedrive_max_scrape_urls: int = 12
    pipedrive_enrichment_url_cap: int = 8
    pipedrive_chars_per_page: int = 16000

    # Google Sheets ID for EOD review export (optional)
    google_sheet_id: str | None = None

    # ── Contact Hunter (LLM tool-calling agent loop) ────────────
    # off         — skip agent entirely (legacy pipeline only)
    # gap_fill    — run pipeline first, invoke agent only when target roles are missing (default)
    # full        — skip pipeline, let the agent run the whole research end-to-end
    contact_hunter_mode: Literal["off", "gap_fill", "full"] = "gap_fill"
    hunter_max_tool_calls_gap_fill: int = 8
    hunter_max_tool_calls_full: int = 15
    hunter_max_output_tokens: int = 60_000
    hunter_max_seconds: int = 180
    hunter_model: str = "claude-sonnet-4-20250514"

    # ── Platform adapters ───────────────────────────────────────
    # When an adapter reports a detection score ≥ this threshold, its
    # contacts are used directly and Firecrawl/LLM extraction is skipped.
    platform_adapter_min_confidence: float = 0.75
    platform_adapters_enabled: bool = True

    # ── Slack interactivity (Block Kit action buttons) ──────────
    # Shared secret used to verify Slack request signatures on
    # POST /slack/interact. Required when slack_use_block_kit=true.
    slack_signing_secret: str | None = None
    # Feature flag: post Block Kit messages with interactive buttons
    # (stores full Pipedrive payloads in pending_actions) instead of the
    # legacy emoji + ---Payload--- text format parsed by Zapier.
    slack_use_block_kit: bool = False
    # Base URL this service is reachable at; used to log a reminder of
    # the Slack App "Interactivity request URL" that should be configured.
    public_base_url: str | None = None

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
