"""Enbridge runner — orchestrates scrape → munge → push pipeline.

Provides scrapeToday, scrapeSomeday, scrapeHistoric, and scrapeFailedDates
entry points. Replaces the old Runner/ directory.
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from app.base.types import PipeConfig, RunStats, ScrapeResult
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

    async def _load_pipe_configs(self) -> tuple[pd.DataFrame, list[PipeConfig]]:
        """Load pipe configs from Azure Table / cached parquet."""
        df = await asyncio.to_thread(dump_pipe_configs, self._paths.config_files)
        if df is None:
            raise RuntimeError("Failed to load PipeConfigs")

        def _nan_to_none(val):
            """Convert pandas NaN to None for pydantic validation."""
            if pd.isna(val):
                return None
            return val or None

        enb_df = df[df["ParentPipe"] == cfg.PARENT_PIPE]
        configs = []
        for _, row in enb_df.iterrows():
            configs.append(PipeConfig(
                pipe_code=row["PipeCode"],
                parent_pipe=row["ParentPipe"],
                pipe_name=row["PipeName"],
                gf_pipe_id=str(row["GFPipeID"]),
                oa_code=_nan_to_none(row.get("PointCapCode")),
                sg_code=_nan_to_none(row.get("SegmentCapCode")),
                st_code=_nan_to_none(row.get("StorageCapCode")),
                nn_code=_nan_to_none(row.get("NoNoticeCode")),
                meta_code=_nan_to_none(row.get("MetaCode")),
            ))
        return df, configs

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def scrape_today(self) -> RunStats:
        """Scrape yesterday + today's OA/SG/ST, today's NN, metadata, then push."""
        stats = RunStats(pipeline="Enbridge", start_time=datetime.now())
        pipe_df, configs = await self._load_pipe_configs()

        # Metadata download
        await self._scraper.scrape_metadata(configs)

        # Yesterday
        yesterday = datetime.today() - timedelta(days=1)
        logger.info(f"scrapeToday - {yesterday=}")
        results = await self._scraper.scrape_all(configs, yesterday, self._headless)
        for r in results:
            stats.add(r)

        # Today
        today = datetime.today()
        logger.info(f"scrapeToday - {today=}")
        results = await self._scraper.scrape_all(configs, today, self._headless)
        for r in results:
            stats.add(r)

        # NN (today, with lag applied internally)
        logger.info(f"scrapeToday - NN {today=}")
        nn_results = await self._scraper.scrape_nn(configs, today, self._headless)
        for r in nn_results:
            stats.add(r)

        # Push all (bronze → silver → gold → cleanup)
        await self._pusher.push_all(self._silver_munger, pipe_df, stats=stats)

        stats.end_time = datetime.now()
        logger.info(
            f"{'*' * 15} completed in {stats.duration_s:.2f}s {'*' * 15}"
        )
        return stats

    async def scrape_someday(self, scrape_day: datetime) -> RunStats:
        """Scrape a specific date for all pipes, then push."""
        if scrape_day > datetime.today():
            raise ValueError(f"Cannot scrape future date: {scrape_day}")

        stats = RunStats(pipeline="Enbridge", start_time=datetime.now())
        pipe_df, configs = await self._load_pipe_configs()

        logger.info(f"scrapeSomeday - {scrape_day=}")
        results = await self._scraper.scrape_all(configs, scrape_day, self._headless)
        for r in results:
            stats.add(r)

        nn_results = await self._scraper.scrape_nn(configs, scrape_day, self._headless)
        for r in nn_results:
            stats.add(r)

        await self._pusher.push_all(self._silver_munger, pipe_df, stats=stats)

        stats.end_time = datetime.now()
        logger.info(
            f"{'*' * 15} completed in {stats.duration_s:.2f}s {'*' * 15}"
        )
        return stats

    async def scrape_historic(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> RunStats:
        """Backfill historical data day-by-day: scrape → munge → push → clear → next.

        Sequential processing keeps VM load bounded and maximises success rate.
        """
        start = start_date or (datetime.today() - timedelta(days=365 * 3 + 1))
        end = end_date or datetime.today()

        stats = RunStats(pipeline="Enbridge", start_time=datetime.now())
        pipe_df, configs = await self._load_pipe_configs()

        # Metadata once at the start
        await self._scraper.scrape_metadata(configs)

        current = start
        while current <= end:
            logger.info(f"scrapeHistoric - {current:%Y-%m-%d}")

            results = await self._scraper.scrape_all(configs, current, self._headless)
            for r in results:
                stats.add(r)

            nn_results = await self._scraper.scrape_nn(configs, current, self._headless)
            for r in nn_results:
                stats.add(r)

            await self._pusher.push_all(self._silver_munger, pipe_df, stats=stats)
            await self._clear_downloads()

            current += timedelta(days=1)

        stats.end_time = datetime.now()
        logger.info(
            f"{'*' * 15} historic completed in {stats.duration_s:.2f}s {'*' * 15}"
        )
        return stats

    async def _clear_downloads(self) -> None:
        """Delete all files in raw and silver directories between historic days."""
        dirs_to_clear = [
            self._paths.oa_raw, self._paths.sg_raw,
            self._paths.st_raw, self._paths.nn_raw,
            self._paths.oa_silver, self._paths.sg_silver,
            self._paths.st_silver, self._paths.nn_silver,
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
        """Re-scrape dates from the fails CSV, then push."""
        stats = RunStats(pipeline="Enbridge", start_time=datetime.now())
        pipe_df, configs = await self._load_pipe_configs()

        fail_file = self._paths.fail_file
        fail_backup = fail_file.with_name(fail_file.stem + "_run1.csv")

        try:
            fail_file.rename(fail_backup)
            df = await asyncio.to_thread(
                lambda: pd.read_csv(fail_backup, sep="|", header=None)
                .drop_duplicates()
                .rename(columns={0: "pipecode", 1: "type", 2: "scrape_date"})[
                    ["pipecode", "scrape_date"]
                ]
            )
            df["scrape_date"] = df["scrape_date"].apply(
                lambda x: datetime.strptime(x, "%Y/%m/%d")
            )

            for record in df.to_dict(orient="records"):
                pipecode = record["pipecode"]
                scrape_date = record["scrape_date"]
                logger.info(f"scraping failed - {record}")

                # Find matching config
                matching = [c for c in configs if c.pipe_code == pipecode]
                if matching:
                    results = await self._scraper.scrape(
                        matching[0], scrape_date, self._headless
                    )
                    for r in results:
                        stats.add(r)

                nn_results = await self._scraper.scrape_nn(
                    configs, scrape_date, self._headless
                )
                for r in nn_results:
                    stats.add(r)

            await self._pusher.push_all(self._silver_munger, pipe_df, stats=stats)

            fail_backup.unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"scrape_failed_dates failed: {e}")
            if fail_backup.exists():
                fail_backup.rename(fail_file)

        stats.end_time = datetime.now()
        return stats

