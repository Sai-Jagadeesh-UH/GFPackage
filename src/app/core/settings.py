"""Centralized settings loaded from .env via pydantic-settings.

All environment variables are validated and typed here.
Other modules import `settings` instead of calling os.getenv directly.

Usage:
    from app.core.settings import settings

    settings.prod_storage_constr  # Azure Blob connection string
    settings.access_key           # Delta Lake storage account key
    settings.root_dir             # Project root as Path
"""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Application settings loaded from .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Project root directory
    root_dir: Path = Field(default=Path("."))

    # Azure Blob Storage connection string (bronze/silver uploads + config tables)
    prod_storage_constr: str = Field(default="")

    # Azure Storage account key (Delta Lake gold layer writes)
    access_key: str = Field(default="")

    # Delta Lake configuration
    delta_storage_account: str = Field(default="gasfundiesdeltalake")
    delta_table_name: str = Field(default="GFundiesProd")
    delta_container: str = Field(default="goldlayer")

    # Azure Blob Storage account name (for constructing container URLs)
    blob_account_name: str = Field(default="stgfpipelineprod")

    # Azure Blob container names
    bronze_container: str = Field(default="bronze")
    silver_container: str = Field(default="silver")

    # Azure Table Storage table names
    pipe_configs_table: str = Field(default="PipeConfigs")
    Loc_configs_table: str = Field(default="LocMetadata")
    enbridge_metadata_table: str = Field(default="EnbridgeMetadata")


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return cached settings instance. Loaded once from .env."""
    return AppSettings()


# Module-level singleton for convenience
settings = get_settings()
