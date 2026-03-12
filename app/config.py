from __future__ import annotations
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

    # Agent
    max_subpages: int = 7
    max_directory_pages: int = 2  # extra pagination pages to fetch per staff-directory-like URL
    claude_model: str = "claude-sonnet-4-20250514"

    # Optional: require this secret on /webhook/pipedrive (header X-Webhook-Secret or query ?secret=)
    webhook_secret: str | None = None

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
