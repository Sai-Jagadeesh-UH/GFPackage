"""Enbridge runner — orchestrates scrape → munge → push pipeline.

Provides scrapeToday, scrapeSomeday, scrapeHistoric, and scrapeFailedDates
entry points. Replaces the old Runner/ directory.
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from app.base.types import RunStats
from app.core.logging import logger
from app.core.paths import PipelinePaths
from app.core.settings import settings
from app.core.azure_tables import dump_pipe_configs

from .scraper import EnbridgeScraper
from .silver_munger import EnbridgeSilverMunger
from .pusher import EnbridgePusher
from . import config as cfg


class EnbridgeRunner:
    """Orchestrator for all Enbridge scraping operations."""

    def __init__(self, root_dir: Path | None = None, headless: bool = True):
        root = root_dir or settings.root_dir.resolve()
        self._headless = headless
        self._paths = PipelinePaths.create(root, "Enbridge")
        self._scraper = EnbridgeScraper(self._paths)
        self._silver_munger = EnbridgeSilverMunger(self._paths)
        self._pusher = EnbridgePusher(self._paths)

    @property
    def paths(self) -> PipelinePaths:
        return self._paths

    async def _load_pipe_configs_df(self) -> pd.DataFrame:
        """Load pipe configs DataFrame for silver/gold munging (still needed for munge logic)."""
        df = await asyncio.to_thread(dump_pipe_configs, self._paths.config_files)
        if df is None:
            raise RuntimeError("Failed to load PipeConfigs")
        return df

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def scrape_today(self) -> RunStats:
        """Scrape yesterday + today for all pipes, then push."""
        stats = RunStats(pipeline="Enbridge", start_time=datetime.now())
        pipe_df = await self._load_pipe_configs_df()

        yesterday = datetime.today() - timedelta(days=1)
        today = datetime.today()

        logger.info(f"scrapeToday - {yesterday=}")
        results, _ = await self._scraper.scrape_date(yesterday, self._headless)
        for r in results:
            stats.add(r)

        logger.info(f"scrapeToday - {today=}")
        results, _ = await self._scraper.scrape_date(today, self._headless)
        for r in results:
            stats.add(r)

        await self._pusher.push_all(self._silver_munger, pipe_df, stats=stats)

        stats.end_time = datetime.now()
        logger.info(f"{'*' * 15} completed in {stats.duration_s:.2f}s {'*' * 15}")
        return stats

    async def scrape_someday(self, scrape_day: datetime) -> RunStats:
        """Scrape a specific date for all pipes, then push."""
        if scrape_day > datetime.today():
            raise ValueError(f"Cannot scrape future date: {scrape_day}")

        stats = RunStats(pipeline="Enbridge", start_time=datetime.now())
        pipe_df = await self._load_pipe_configs_df()

        logger.info(f"scrapeSomeday - {scrape_day=}")
        results, _ = await self._scraper.scrape_date(scrape_day, self._headless)
        for r in results:
            stats.add(r)

        await self._pusher.push_all(self._silver_munger, pipe_df, stats=stats)

        stats.end_time = datetime.now()
        logger.info(f"{'*' * 15} completed in {stats.duration_s:.2f}s {'*' * 15}")
        return stats

    async def scrape_range(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> RunStats:
        """Scrape a date range day-by-day, munge, push, archive, and clean each day.

        Metadata is scraped only on the first day. Each day's files are archived
        to downloads/enbridge/archive/{YYYY-MM-DD}/ before cleaning.
        """
        if end_date > datetime.today():
            raise ValueError(f"Cannot scrape future date: {end_date}")

        stats = RunStats(pipeline="Enbridge", start_time=datetime.now())
        pipe_df = await self._load_pipe_configs_df()

        current = start_date
        is_first = True
        while current <= end_date:
            logger.info(f"scrapeRange - {current:%Y-%m-%d}")
            results, _ = await self._scraper.scrape_date(
                current, self._headless, skip_meta=not is_first
            )
            for r in results:
                stats.add(r)


            await self._pusher.push_all(
                self._silver_munger, pipe_df, stats=stats, target_day=current
            )
            # await self._archive_day(current)
            await self._clear_downloads()

            current += timedelta(days=1)
            is_first = False

        stats.end_time = datetime.now()
        logger.info(f"{'*' * 15} range complete in {stats.duration_s:.2f}s {'*' * 15}")
        return stats

    # async def scrape_historic(
    #     self,
    #     start_date: datetime | None = None,
    #     end_date: datetime | None = None,
    #     push: bool = True,
    # ) -> RunStats:
    #     """Backfill historical data day-by-day.

    #     Set push=False to scrape/download only without cloud pushes.
    #     """
    #     start = start_date or (datetime.today() - timedelta(days=365 * 3 + 1))
    #     end = end_date or datetime.today()

    #     stats = RunStats(pipeline="Enbridge", start_time=datetime.now())
    #     pipe_df = await self._load_pipe_configs_df()

    #     current = start
    #     while current <= end:
    #         logger.info(f"scrapeHistoric - {current:%Y-%m-%d}")
    #         results, changes = await self._scraper.scrape_date(current, self._headless)
    #         for r in results:
    #             stats.add(r)
    #         if current == start:
    #             stats.add_pipe_changes(changes)

    #         if push:
    #             await self._pusher.push_all(self._silver_munger, pipe_df, stats=stats, target_day=current)
    #             await self._clear_downloads()

    #         current += timedelta(days=1)

    #     stats.end_time = datetime.now()
    #     logger.info(f"{'*' * 15} historic completed in {stats.duration_s:.2f}s {'*' * 15}")
    #     return stats

    async def _archive_day(self, day: datetime) -> None:
        """Copy raw, silver, and gold files for a day into the archive folder."""
        import shutil
        archive_dir = self._paths.downloads / "FileBackUp" / day.strftime("%Y-%m-%d")
        dirs_to_archive = [
            self._paths.oa_raw, self._paths.sg_raw, self._paths.nn_raw,
            self._paths.meta_raw,
            self._paths.oa_silver, self._paths.sg_silver, self._paths.nn_silver,
            self._paths.gold_dir,
        ]

        def _do_archive():
            archive_dir.mkdir(parents=True, exist_ok=True)
            for d in dirs_to_archive:
                if not d.exists():
                    continue
                label = d.name
                dest = archive_dir / label
                dest.mkdir(parents=True, exist_ok=True)
                for f in d.iterdir():
                    if f.is_file():
                        try:
                            shutil.copy2(f, dest / f.name)
                        except OSError as e:
                            logger.error(f"Archive failed: {f.name} — {e}")
            logger.info(f"Archived day {day:%Y-%m-%d} → {archive_dir}")

        await asyncio.to_thread(_do_archive)

    async def _clear_downloads(self) -> None:
        """Delete all files in raw and silver directories between historic days."""
        dirs_to_clear = [
            self._paths.oa_raw, self._paths.sg_raw, self._paths.nn_raw,
            self._paths.oa_silver, self._paths.sg_silver, self._paths.nn_silver,
        ]

        def _do_clear():
            for d in dirs_to_clear:
                for f in d.iterdir():
                    if f.is_file():
                        try:
                            f.unlink()
                        except OSError as e:
                            logger.error(f"Clear failed: {f.name} — {e}")

        await asyncio.to_thread(_do_clear)

    async def scrape_failed_dates(self) -> RunStats:
        ...
    
        # """Re-scrape unique dates from the fails CSV, then push."""
        # stats = RunStats(pipeline="Enbridge", start_time=datetime.now())
        # pipe_df = await self._load_pipe_configs_df()

        # fail_file = self._paths.fail_file
        # fail_backup = fail_file.with_name(fail_file.stem + "_run1.csv")

        # try:
        #     fail_file.rename(fail_backup)
        #     df = await asyncio.to_thread(
        #         lambda: pd.read_csv(fail_backup, sep="|", header=None)
        #         .drop_duplicates()
        #         .rename(columns={0: "pipecode", 1: "type", 2: "scrape_date"})
        #     )
        #     df["scrape_date"] = pd.to_datetime(df["scrape_date"], format="%Y/%m/%d")

        #     for scrape_date in df["scrape_date"].unique():
        #         dt = scrape_date.to_pydatetime()
        #         logger.info(f"scraping failed date: {dt:%Y-%m-%d}")
        #         results, _ = await self._scraper.scrape_date(dt, self._headless)
        #         for r in results:
        #             stats.add(r)

        #     await self._pusher.push_all(self._silver_munger, pipe_df, stats=stats)
        #     fail_backup.unlink(missing_ok=True)

        # except Exception as e:
        #     logger.error(f"scrape_failed_dates failed: {e}")
        #     if fail_backup.exists():
        #         fail_backup.rename(fail_file)

        # stats.end_time = datetime.now()
        # return stats

