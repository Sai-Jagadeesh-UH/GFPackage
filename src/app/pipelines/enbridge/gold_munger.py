"""Enbridge gold munger — implements BaseGoldMunger.

Merges silver OA, OC, and NN parquets into a single gold DataFrame,
discovers new OC locations, and updates segment configs.
"""

import asyncio
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl

from app.base.munger import BaseGoldMunger
from app.base.types import RowType
from app.core.logging import logger

from . import config as cfg


class EnbridgeGoldMunger(BaseGoldMunger):
    """Silver → gold transformation for Enbridge pipeline data."""

    def __init__(self, paths):
        """
        Args:
            paths: PipelinePaths instance.
        """
        self._paths = paths

    @property
    def parent_pipe(self) -> str:
        return cfg.PARENT_PIPE

    async def merge(self, silver_dirs: dict[RowType, Path]) -> pl.DataFrame:
        """Merge OA, OC, NN silver parquets into one gold DataFrame.

        Removes Timestamp (will be re-added fresh), deduplicates,
        and adds a new Timestamp column.
        """
        def _merge():
            lazy_frames: list[pl.LazyFrame] = []

            for row_type, silver_dir in silver_dirs.items():
                if not silver_dir.exists() or not any(silver_dir.iterdir()):
                    continue
                lf = pl.scan_parquet(silver_dir).select(pl.exclude("Timestamp")).unique()
                lazy_frames.append(lf)

            if not lazy_frames:
                raise ValueError("No silver data to merge into gold")

            return (
                pl.concat(lazy_frames)
                .with_columns(
                    pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp")
                )
                .collect()
            )

        return await asyncio.to_thread(_merge)

    def discover_locations(
        self,
        gold_df: pl.DataFrame,
        segment_configs_df: pd.DataFrame,
        pipe_configs_df: pd.DataFrame,
    ) -> pd.DataFrame | None:
        """Not used for gold layer — OC location discovery happens in silver_munger.

        Returns None as all location discovery is done during OC silver processing.
        """
        return None

    async def clean_directories(self) -> None:
        """Remove all files in download subdirectories for the next run.

        TODO: Re-enable cleanup once testing is complete.
        """
        logger.info("Cleanup skipped (disabled for testing)")
        # def _clean():
        #     for folder in self._paths.downloads.iterdir():
        #         if folder.is_dir():
        #             for file in folder.iterdir():
        #                 try:
        #                     file.unlink()
        #                 except OSError as e:
        #                     logger.error(f"Delete failed: {file.name} — {e}")
        #
        #     config_files_dir = self._paths.config_files
        #     if config_files_dir.exists():
        #         for file in config_files_dir.iterdir():
        #             if file.is_file():
        #                 try:
        #                     file.unlink()
        #                 except OSError as e:
        #                     logger.error(f"Delete failed: configFiles/{file.name} — {e}")
        #
        # await asyncio.to_thread(_clean)
