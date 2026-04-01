from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from .types import ScrapeResult


class BasePipeScraper(ABC):
    """Contract for pipeline data scraping.

    Implementations discover available pipes and datasets dynamically from
    the live UI rather than relying on pre-loaded config tables.
    """

    @property
    @abstractmethod
    def parent_pipe(self) -> str:
        """Parent company name, e.g. 'Enbridge'."""

    @property
    @abstractmethod
    def download_root(self) -> Path:
        """Root directory where raw downloads are saved."""

    @abstractmethod
    async def scrape_date(
        self,
        scrape_date: datetime,
        headless: bool = True,
    ) -> list[ScrapeResult]:
        """Scrape all available pipes and datasets for a single date.

        Discovers pipes and dataset availability live from the UI.
        Returns one ScrapeResult per (pipe, dataset_type) attempted.
        """

    @abstractmethod
    async def scrape_metadata(self) -> None:
        """Scrape metadata (location lists) for all pipes via browser."""
