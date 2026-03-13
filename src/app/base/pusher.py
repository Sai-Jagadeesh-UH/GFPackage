from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

from .types import RowType


class BasePusher(ABC):
    """Contract for medallion layer push (bronze → silver → gold).

    Each pipeline implements its own blob path templates and
    file name parsing logic, but uses shared upload helpers
    from core.cloud.
    """

    @property
    @abstractmethod
    def parent_pipe(self) -> str:
        """Parent company name."""

    @abstractmethod
    def bronze_blob_path(self, dataset_type: RowType, file_path: Path) -> str:
        """Construct blob path for a raw file in the bronze container.

        e.g. 'Enbridge/PointCapacity/202501/AG/AG_OA_20250115_INTRDY.csv'
        """

    @abstractmethod
    def silver_blob_path(self, dataset_type: RowType, file_path: Path) -> str:
        """Construct blob path for a normalized file in the silver container."""

    @abstractmethod
    async def push_bronze(self, raw_dir: Path, dataset_type: RowType) -> None:
        """Upload raw files to the bronze blob container."""

    @abstractmethod
    async def push_silver(self, silver_dir: Path, dataset_type: RowType) -> None:
        """Upload normalized parquets to the silver blob container."""

    @abstractmethod
    def push_gold(self, gold_df: pl.DataFrame) -> None:
        """Push merged gold DataFrame to Delta Lake."""
