from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from .types import PipeConfig, ScrapeResult


class BasePipeScraper(ABC):
    """Contract for pipeline data scraping.

    Each pipeline implementation handles its own browser automation,
    URL construction, and download logic using helpers from core.browser.
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
    async def scrape(
        self,
        pipe_config: PipeConfig,
        scrape_date: datetime,
        headless: bool = True,
    ) -> list[ScrapeResult]:
        """Scrape all available dataset types (OA, SG, ST, NN) for one pipe on one date.

        Returns a ScrapeResult per dataset type attempted.
        """

    @abstractmethod
    async def scrape_all(
        self,
        pipe_configs: list[PipeConfig],
        scrape_date: datetime,
        headless: bool = True,
    ) -> list[ScrapeResult]:
        """Scrape all configured pipes for a single date."""

    @abstractmethod
    async def scrape_metadata(self, pipe_configs: list[PipeConfig]) -> None:
        """Download metadata files (point lists, etc.) for all pipes."""
