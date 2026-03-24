from datetime import datetime

import functools
import operator
import polars as pl
from pathlib import Path

from ..utils import paths, pipeConfigs_df, logger, error_detailed

ParentPipe = 'Enbridge'


selectedCols = ['Effective_From_DateTime', 'Loc_Prop', 'Loc_Name',
                'Allocated_Qty', 'Direction_Of_Flow', 'Accounting_Physincal_Indicator']


def saveFile(df: pl.DataFrame) -> None:
    df.write_csv(paths.processed /
                 f'OA_Processed{datetime.now().strftime("%d%m%Y%H%M%S")}.csv')


def batchDateParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: datetime.strptime(inString, "%m/%d/%Y %H:%M").date(), inSeries))


def batchYMonthParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inDate: int(f"{str(inDate.year)}{str(inDate.month).rjust(2, '0')}"), inSeries))


def paddedString(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: str(inString).rjust(6, '0'), inSeries))


def batchFloatParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: float(inString.replace(',', '')), inSeries))


def batchFIMapper(inSeries: pl.Series) -> pl.Series:
    flow_Map = {
        'D': 'D',
        'R': 'R',
        'B': 'B',
        'Delivery': 'D',
        'Receipt': 'R',
        'Storage Injection': 'D',
        'Storage Withdrawal': 'R',
    }
    return pl.Series(map(lambda inString: flow_Map.get(inString, 'D'), inSeries))


async def cleanseNN(filePath: Path):
    try:
        df = pl.scan_parquet(filePath)\
            .select([*selectedCols, 'PipeCode'])\
            .filter(
            ~functools.reduce(
                operator.and_,
                map(lambda col: pl.col(col).is_null(), selectedCols)
            ))

        df = df.with_columns(
            pl.col("Effective_From_DateTime")
            .map_batches(batchDateParse, return_dtype=pl.Date).alias('EffGasDay'),
            pl.col("Loc_Prop")
            .map_batches(paddedString, return_dtype=pl.String).alias('Loc'),
            pl.col("Loc_Name").cast(pl.String).alias('LocName'),
            pl.lit('Enbridge').cast(pl.String).alias('ParentPipe'),
            pl.col("Allocated_Qty")
            .map_batches(batchFloatParse, return_dtype=pl.Float64).alias('TotalScheduledQuantity'),
            pl.col('Direction_Of_Flow')
            .map_batches(batchFIMapper, return_dtype=pl.String).alias('FlowInd'),
            pl.lit(None).cast(pl.String).alias('CycleDesc'),
            pl.lit(None).cast(pl.String).alias('LocPurpDesc'),
            pl.lit(None).cast(pl.String).alias('LocZn'),
            pl.lit(None).cast(pl.String).alias('LocSegment'),
            pl.lit(None).alias('DesignCapacity'),
            pl.lit(None).alias('OperatingCapacity'),
            pl.lit(None).alias('OperationallyAvailableCapacity'),
            pl.lit(None).cast(pl.String).alias('IT'),
            pl.lit(None).cast(pl.String).alias('AllQtyAvail'),
            pl.lit(datetime.now()).cast(pl.Datetime).alias('Timestamp'),
            pl.col("Accounting_Physincal_Indicator").cast(
                pl.String).alias('QtyReason'),

        )

        df = df.join(pl.LazyFrame(pipeConfigs_df.loc[pipeConfigs_df['ParentPipe'] == ParentPipe, [
            'GFPipeID', 'PipeCode', 'PipeName']]), on='PipeCode', how='inner')\
            .with_columns(
                #  Add EffGasMonth, GFPipeID, GFLocID , RowType for data modeling purposes

                #  EffGasMonth
                pl.col("EffGasDay")
                .map_batches(batchYMonthParse, return_dtype=pl.Int64)
                .alias('EffGasMonth'),

                # GFPipeID
                pl.col('GFPipeID').cast(pl.Int64),

                # GFLocID
                pl.col('Loc')
                .map_batches(paddedString, return_dtype=pl.String)
                .alias('GFLocID'),

                # RowType
                pl.lit('NN').alias('RowType'),


                pl.col('Loc').map_batches(paddedString,
                                          return_dtype=pl.String),
                pl.col('PipeName').alias('PipelineName')

        )\
            .with_columns(
            pl.concat_str([pl.col('GFPipeID'),
                           pl.col("RowType"),
                           pl.col('Loc'),
                           pl.col('FlowInd')], separator='').alias('GFLOC'))\
            .select([
                'EffGasMonth', 'GFPipeID', 'GFLocID', 'RowType',  # modeling
                'ParentPipe',
                'PipelineName',
                'GFLOC',
                'EffGasDay',
                'CycleDesc',
                'LocPurpDesc',
                'Loc',
                'LocName',
                'LocZn',
                'LocSegment',
                'DesignCapacity',
                'OperatingCapacity',
                'TotalScheduledQuantity',
                'OperationallyAvailableCapacity',
                'IT',
                'FlowInd',
                'AllQtyAvail',
                'QtyReason',
                'Timestamp']).collect().write_parquet(filePath)

    except Exception as e:
        print(f"cleanseNN failed for {filePath} - {error_detailed(e)}")
        logger.critical(
            f"cleanseNN failed for {filePath} - {error_detailed(e)}")


def formatNN():
    return
