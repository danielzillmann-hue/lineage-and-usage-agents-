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

    # Vertex AI Gemini — uses ADC, no API key needed.
    vertex_location: str = Field(default="australia-southeast1")
    inventory_model: str = Field(default="gemini-2.5-flash")
    lineage_model: str = Field(default="gemini-2.5-flash")
    usage_model: str = Field(default="gemini-2.5-flash")
    summary_model: str = Field(default="gemini-2.5-pro")

    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])
    log_level: str = Field(default="INFO")


@lru_cache
def get_settings() -> Settings:
    return Settings()
