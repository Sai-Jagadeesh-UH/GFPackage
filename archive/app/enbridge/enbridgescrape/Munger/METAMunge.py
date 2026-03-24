import re
from datetime import datetime
import pandas as pd
import polars as pl
from ..utils import paths, logger
from ...artifacts.azureDump import getTable


metaPath = paths.downloads / 'MetaData'

# dbFile = paths.dbFile

ParentPipe = 'Enbridge'


def InitialPushCleanse():
    dfList = []
    for filePath in metaPath.iterdir():
        pipe_code = filePath.name.split('_', 2)[0]
        tempDf = pd.read_csv(filePath, dtype=str).assign(
            PipeCode=pipe_code)
        dfList.append(tempDf)

    df = pl.from_pandas(pd.concat(dfList)).unique()\
        .rename(lambda cloName: re.sub(r'[^a-zA-Z0-9]', '', cloName)).fill_null("")
    df = df.with_columns(
        *[pl.col(cloName).str.strip_chars() for cloName in df.columns],
        pl.lit("Enbridge").alias("PartitionKey"),
        pl.lit(datetime.now().strftime("%Y%m%d")).alias("ChangedOn"),
    )\
        .with_columns(
        pl.concat_str([pl.col("ChangedOn"),
                       pl.col("PartitionKey"),
                       pl.col("Loc")]
                      ).alias("RowKey")
    )

    updateMetaTable(df)


def updateMetaTable(df: pl.DataFrame):
    operations = [("upsert", row) for row in df.iter_rows(named=True)]

    with getTable(tableName='EnbridgeMetadata') as table_client:
        for i in range(0, len(operations), 90):
            try:
                table_client.submit_transaction(
                    operations[i:min(i+90, len(operations))])
            except Exception as e:
                logger.critical(
                    f"There was an error updating EnbridgeMetadata Table - {e}")
                # logger.critical(f"Error: {e}")


def metaMunge():
    dfList = []
    for filePath in metaPath.iterdir():
        pipe_code = filePath.name.split('_', 2)[0]

        tempDf = pd.read_csv(filePath, dtype=str).assign(
            PipeCode=pipe_code)
        dfList.append(tempDf)

    Curdf = pl.from_pandas(pd.concat(dfList)).unique()\
        .rename(lambda cloName: re.sub(r'[^a-zA-Z0-9]', '', cloName)).fill_null("")
    Curdf = Curdf.with_columns(
        *[pl.col(cloName).str.strip_chars() for cloName in Curdf.columns],
        pl.lit(ParentPipe).alias("PartitionKey"))

    with getTable('EnbridgeMetadata') as table_client:
        Metadf = pl.DataFrame(table_client.query_entities(
            f"PartitionKey eq '{ParentPipe}'")).select(pl.exclude(["RowKey", "ChangedOn"]))

    newRows = Curdf.join(Metadf, how='anti', on=Curdf.columns)\
        .with_columns(
            pl.concat_str([pl.lit(datetime.now().strftime("%Y%m%d")),
                           pl.col("PartitionKey"),
                           pl.col("Loc")]
                          ).alias("RowKey"),

            pl.lit(datetime.now().strftime("%Y%m%d")).alias("ChangedOn"),
    )

    if (not newRows.is_empty()):
        updateMetaTable(newRows)
