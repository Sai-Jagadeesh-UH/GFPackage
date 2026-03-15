from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
import polars as pl

from .types import RowType


class BaseSilverMunger(ABC):
    """Contract for raw → silver transformation.

    Reads raw CSVs/parquets downloaded by the scraper,
    normalizes column names and types, and writes silver parquets
    conforming to the gold schema (with Nones for missing fields).
    """

    @property
    @abstractmethod
    def parent_pipe(self) -> str:
        """Parent company name."""

    @abstractmethod
    async def process(
        self,
        raw_dir: Path,
        silver_dir: Path,
        dataset_type: RowType,
        pipe_configs_df: pd.DataFrame,
    ) -> list[str]:
        """Convert raw files to normalized silver parquets.

        Args:
            raw_dir: directory containing raw CSVs from scraper
            silver_dir: output directory for normalized parquets
            dataset_type: dataset type being processed (OA, SG, ST, NN)
            pipe_configs_df: pipeline configuration DataFrame

        Returns:
            List of filenames that were empty/skipped.
        """

    @abstractmethod
    async def cleanse(self, file_path: Path, dataset_type: RowType, **kwargs) -> None:
        """Transform a single raw parquet into normalized silver form.

        Each pipeline implements its own column mapping, date parsing,
        flow indicator mapping, and null-filling logic here.
        Uses shared helpers from core.transforms.
        """


class BaseGoldMunger(ABC):
    """Contract for silver → gold transformation.

    Merges all silver parquets across dataset types,
    fills missing fields, discovers new locations,
    and produces a single gold DataFrame for Delta Lake push.
    """

    @property
    @abstractmethod
    def parent_pipe(self) -> str:
        """Parent company name."""

    @abstractmethod
    def merge(self, silver_dirs: dict[RowType, Path]) -> pl.DataFrame:
        """Merge silver parquets across all dataset types.

        Args:
            silver_dirs: mapping of RowType → silver directory path

        Returns:
            Merged DataFrame ready for gold push.
        """

    @abstractmethod
    def discover_locations(
        self,
        gold_df: pl.DataFrame,
        segment_configs_df: pd.DataFrame,
        pipe_configs_df: pd.DataFrame,
    ) -> pd.DataFrame | None:
        """Find new locations in the data not yet in segment configs.

        Returns:
            DataFrame of new location records to upsert, or None.
        """
