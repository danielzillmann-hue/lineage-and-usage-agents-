from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_", extra="ignore")

    gcp_project: str = Field(default="dan-sandpit")
    gcp_region: str = Field(default="australia-southeast1")
    firestore_database: str = Field(default="(default)")
    firestore_collection_runs: str = Field(default="lineage_runs")
    results_bucket: str = Field(default="dan-sandpit-lineage-results")

    anthropic_api_key: str = Field(default="")
    inventory_model: str = Field(default="claude-sonnet-4-6")
    lineage_model: str = Field(default="claude-sonnet-4-6")
    usage_model: str = Field(default="claude-sonnet-4-6")
    summary_model: str = Field(default="claude-opus-4-7")

    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])
    log_level: str = Field(default="INFO")


@lru_cache
def get_settings() -> Settings:
    return Settings()
