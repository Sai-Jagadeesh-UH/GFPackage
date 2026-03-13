from datetime import date, timedelta
from functools import cached_property

import polars as pl
from deltalake import write_deltalake, DeltaTable
from pydantic import BaseModel, Field

from .logging import logger
from .settings import settings


class DeltaLakeConfig(BaseModel, frozen=True):
    """Validated configuration for Delta Lake connection."""
    storage_account: str = Field(default="gasfundiesdeltalake", min_length=1)
    access_key: str = Field(default="")
    table_name: str = Field(default="GFundiesProd", min_length=1)
    container: str = Field(default="goldlayer-test", min_length=1)
    log_retention: str = Field(default="interval 1 days")
    prune_threshold_days: int = Field(default=30, ge=0)

    @cached_property
    def table_uri(self) -> str:
        return f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{self.table_name}"

    @cached_property
    def storage_options(self) -> dict[str, str]:
        return {
            "AZURE_STORAGE_ACCOUNT_NAME": self.storage_account,
            "AZURE_STORAGE_ACCOUNT_KEY": self.access_key,
        }


class LakeMerge:
    """Push a DataFrame to a Delta Lake table using partition-level overwrites.

    For each unique RowType in the data, overwrites the matching
    (ParentPipe, EffGasDay, RowType) slice in the Delta table.
    """

    def __init__(
        self,
        df: pl.DataFrame,
        parent_pipe: str,
        config: DeltaLakeConfig | None = None,
        optimize: bool = False,
    ):
        self.config = config or DeltaLakeConfig(
            storage_account=settings.delta_storage_account,
            access_key=settings.access_key,
            table_name=settings.delta_table_name,
            container=settings.delta_container,
        )
        self.parent_pipe = parent_pipe
        self.df = df

        try:
            self.dt = DeltaTable(
                table_uri=self.config.table_uri,
                storage_options=self.config.storage_options,
            )
            self.dt.alter.set_table_properties(
                {"delta.logRetentionDuration": self.config.log_retention}
            )
        except Exception:
            # Table doesn't exist yet — create it with the first write
            logger.info("Delta table not found, creating on first write")
            self.dt = None

        self._push_by_row_type()

        if optimize and self.dt is not None:
            self._prune_table()

    def _push_by_row_type(self) -> None:
        """Write data partitioned by RowType with predicate-based overwrite."""
        for row_type in self.df.select("RowType").unique().to_series():
            chunk = self.df.filter(pl.col("RowType") == row_type)
            dates_csv = ", ".join(
                f"'{d}'" for d in chunk.select("EffGasDay").unique().to_series()
            )
            try:
                if self.dt is None:
                    # First run — create table
                    write_deltalake(
                        table_or_uri=self.config.table_uri,
                        data=chunk.to_arrow(),
                        mode="overwrite",
                        partition_by="EffGasMonth",
                        storage_options=self.config.storage_options,
                    )
                    logger.info(f"Delta table created with {row_type}")
                    # Load the newly created table for subsequent writes
                    self.dt = DeltaTable(
                        table_uri=self.config.table_uri,
                        storage_options=self.config.storage_options,
                    )
                else:
                    predicate = (
                        f"ParentPipe = '{self.parent_pipe}' "
                        f"AND EffGasDay IN ({dates_csv}) "
                        f"AND RowType = '{row_type}'"
                    )
                    write_deltalake(
                        table_or_uri=self.config.table_uri,
                        data=chunk.to_arrow(),
                        mode="overwrite",
                        partition_by="EffGasMonth",
                        storage_options=self.config.storage_options,
                        predicate=predicate,
                    )
            except Exception as e:
                logger.error(f"Delta push failed: {row_type} dates={dates_csv} — {e}")

    def _prune_table(self) -> None:
        """Z-order optimize and vacuum if data is recent."""
        if self.dt is None:
            return
        max_date = self.df.select("EffGasDay").max().item(0, 0)
        if max_date <= (date.today() - timedelta(days=self.config.prune_threshold_days)):
            return

        for month in self.df.select("EffGasMonth").unique().to_series():
            try:
                self.dt.optimize.z_order(
                    ["EffGasDay", "GFPipeID", "GFLocID"],
                    partition_filters=[("EffGasMonth", "=", month)],
                )
                self.dt.vacuum(
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
            except Exception as e:
                logger.error(f"Delta prune failed: {e}")
