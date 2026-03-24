import os
from datetime import datetime, timedelta, date
import polars as pl

from deltalake import write_deltalake, DeltaTable


from ..utils import error_detailed, logger


staging_storage_options = {
    "AZURE_STORAGE_ACCOUNT_NAME": r'gasfundiesdeltalake',
    "AZURE_STORAGE_ACCOUNT_KEY": os.getenv('ACCESS_KEY', '')
}


class LakeMerge():

    def __init__(self, df: pl.DataFrame | None = None, tablename: str = 'GFundiesProd', optimize: bool = False):

        self.table_uri = f"abfss://goldlayer@gasfundiesdeltalake.dfs.core.windows.net/{tablename}"

        self.dt = DeltaTable(table_uri=self.table_uri,
                             storage_options=staging_storage_options)
        self.dt.alter.set_table_properties(
            {"delta.logRetentionDuration": "interval 1 days"})

        if df is not None:
            self.df = df
            self.pushTypes()

        if (optimize):
            self.pruneTable()

    def pushTypes(self):
        for row_type in self.df.select('RowType').unique().to_series():
            chunk = self.df.filter(pl.col('RowType') == row_type)
            condition = ' , '.join(
                [f"'{i}'" for i in chunk.select('EffGasDay').unique().to_series()])

            try:
                write_deltalake(
                    table_or_uri=self.table_uri,
                    data=chunk.to_arrow(),
                    mode="overwrite",
                    partition_by='EffGasMonth',
                    storage_options=staging_storage_options,
                    predicate=f"ParentPipe = 'Enbridge' AND EffGasDay IN ({condition}) AND RowType = '{row_type}'"
                )
            except Exception as e:
                logger.critical(
                    f"failed delta push : {row_type} for dates {condition} - {error_detailed(e)}")

    def pruneTable(self):
        if self.df.select('EffGasDay').max().item(0, 0) <= (date.today() - timedelta(days=30)):
            return
        try:
            for month in self.df.select('EffGasMonth').unique().to_series():
                self.dt.optimize.z_order(["EffGasDay", "GFPipeID", "GFLocID"], partition_filters=[
                                        ("EffGasMonth", "=", month)])
                self.dt.vacuum(retention_hours=0, dry_run=False,
                               enforce_retention_duration=False, full=True)
        except Exception as e:
            logger.critical(
                f"failed pruning : {error_detailed(e)}")
