from datetime import datetime

import functools
import operator
import polars as pl
from pathlib import Path

from ..utils import paths, pipeConfigs_df, logger, error_detailed
# from .MungeAll import processOA
# from src.artifacts import getPipes, updatePipes

ParentPipe = 'Enbridge'

OA_path = paths.downloads / 'OA'

selectedCols = ['Cycle_Desc', 'Eff_Gas_Day', 'Loc', 'Loc_Name', 'Loc_Zn', 'Flow_Ind_Desc', 'Loc_Purp_Desc', 'IT', 'All_Qty_Avail',
                'Total_Design_Capacity', 'Operating_Capacity', 'Total_Scheduled_Quantity', 'Operationally_Available_Capacity']


def saveFile(df: pl.DataFrame) -> None:
    df.write_csv(paths.processed /
                 f'OA_Processed{datetime.now().strftime("%d%m%Y%H%M%S")}.csv')


def batchDateParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: datetime.strptime(inString, "%m-%d-%Y").date(), inSeries))


def batchYMonthParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inDate: int(f"{str(inDate.year)}{str(inDate.month).rjust(2, '0')}"), inSeries))


def paddedString(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: str(inString).rjust(6, '0'), inSeries))


def batchFloatParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: float(str(inString).replace(',', '')), inSeries))


def batchFIMapper(inSeries: pl.Series) -> pl.Series:
    flow_Map = {
        'Delivery': 'D',
        'Receipt': 'R',
        'Storage Injection': 'D',
        'Storage Withdrawal': 'R'
    }
    return pl.Series(map(lambda inString: flow_Map.get(inString, 'D'), inSeries))


async def cleanseOA(filePath: Path):
    '''
    delete empty rows, add new columns and normalize datatypes for gold push
    ['ParentPipe', 'PipelineName', 'GFLOC', 'EffGasDay', 'CycleDesc', 'LocPurpDesc', 'Loc', 'LocName', 'LocZn', 'LocSegment', 'DesignCapacity', 'OperatingCapacity', 'TotalScheduledQuantity', 'OperationallyAvailableCapacity', 'IT', 'FlowInd', 'AllQtyAvail', 'QtyReason', 'Timestamp']

    Add EffGasMonth, GFPipeID, GFLocID , RowType for data modeling purposes

    :param filePath: local file path of OA file
    :type filePath: Path
    '''
    # return
    try:
        df = pl.scan_parquet(filePath)\
            .select([*selectedCols, 'PipeCode'])\
            .filter(
            ~functools.reduce(
                operator.and_,
                map(lambda columnName: pl.col(columnName).is_null(), selectedCols)
            ))

        df = df.with_columns(
            pl.col("Eff_Gas_Day")
            .map_batches(batchDateParse, return_dtype=pl.Date)
            .alias('EffGasDay'),

            pl.col("Total_Design_Capacity")
            .map_batches(batchFloatParse, return_dtype=pl.Float64)
            .alias('DesignCapacity'),

            pl.col("Operating_Capacity")
            .map_batches(batchFloatParse, return_dtype=pl.Float64)
            .alias('OperatingCapacity'),

            pl.col("Total_Scheduled_Quantity")
            .map_batches(
                batchFloatParse, return_dtype=pl.Float64)
            .alias('TotalScheduledQuantity'),

            pl.col("Operationally_Available_Capacity")
            .map_batches(batchFloatParse, return_dtype=pl.Float64)
            .alias('OperationallyAvailableCapacity'),

            pl.col("Flow_Ind_Desc")
            .map_batches(batchFIMapper, return_dtype=pl.String)
            .alias('FlowInd'),

            pl.lit(datetime.now())
            .cast(pl.Datetime)
            .alias('Timestamp'),

            pl.col("Loc")
            .cast(pl.String)
            .map_batches(paddedString, return_dtype=pl.String),

            pl.lit('Enbridge').cast(pl.String).alias('ParentPipe'),
            pl.lit(None).cast(pl.String).alias('QtyReason'),
            pl.lit(None).cast(pl.String).alias('LocSegment'),)\
            .rename({
                'Cycle_Desc': 'CycleDesc',
                'Loc_Name': 'LocName',
                'Loc_Zn': 'LocZn',
                'Loc_Purp_Desc': 'LocPurpDesc',
                'All_Qty_Avail': 'AllQtyAvail',
                'IT': 'IT'
            })

        df.join(pl.LazyFrame(pipeConfigs_df.loc[pipeConfigs_df['ParentPipe'] == ParentPipe, ['GFPipeID', 'PipeCode', 'PipeName']]), on='PipeCode', how='inner')\
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
                pl.lit('OA').alias('RowType'),


                pl.col('LocZn').cast(pl.String),

                pl.col('PipeName').cast(pl.String).alias('PipelineName')

        )\
            .with_columns(
                pl.concat_str([pl.col('GFPipeID'),
                               pl.col('RowType'),
                               pl.col('GFLocID'),
                               pl.col('FlowInd')], separator='')
                .alias('GFLOC'))\
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
        print(f"cleanseOA failed for {filePath} - {error_detailed(e)}")
        logger.critical(
            f"cleanseOA failed for {filePath} - {error_detailed(e)}")
