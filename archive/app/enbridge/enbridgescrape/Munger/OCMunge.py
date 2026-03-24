from datetime import datetime

from pathlib import Path
import functools
import operator

import polars as pl
import pandas as pd

from src.artifacts import error_detailed
from ..utils import paths, logger, pipeConfigs_df
# from .MungeAll import processOC

OC_path = paths.downloads / 'OC'
ParentPipe = 'Enbridge'


# def parseDate(dateString: str) -> date:
#     return date(year=int(dateString[:4]), month=int(dateString[5:7]), day=int(dateString[8:]))


def batchDateParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: datetime.strptime(inString, "%Y%m%d").date(), inSeries))


def batchYMonthParse(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inDate: int(f"{str(inDate.year)}{str(inDate.month).rjust(2, '0')}"), inSeries))


def paddedString(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(lambda inString: str(inString).rjust(6, '0'), inSeries))


def batchTDMapper(inSeries: pl.Series) -> pl.Series:
    def check(val):
        if (val < 0):
            return 'TD2'
        if (val > 0):
            return 'TD1'
        return 'TD1'
    return pl.Series(map(check, inSeries))


def batchFIMapper(inSeries: pl.Series) -> pl.Series:
    flow_Map = {
        'TD1': 'F',
        'TD2': 'B',
    }
    return pl.Series(map(lambda inString: flow_Map.get(inString, 'B'), inSeries))


def batchAbsolute(inSeries: pl.Series) -> pl.Series:
    return pl.Series(map(float, map(abs, inSeries)))


# def getConcatDf() -> pd.DataFrame:

#     orderdList = []
#     for filename in glob.glob(str(OC_path / "*.csv")):
#         with open(filename) as file:
#             orderdList.append((filename, file.readline().split('  ')[0][10:]))

#     combined_df_concise = pd.concat([
#         pd.read_csv(item[0], usecols=['Station Name', 'Cap',
#                     'Nom', 'Cap2'], skiprows=1).assign(EffGasDay=item[1])
#         for item in orderdList], ignore_index=True).drop_duplicates(keep='first')

#     combined_df_concise['EffGasDay'] = combined_df_concise['EffGasDay'].apply(
#         parseDate)
#     # parseDate(temp[2])

#     # framesList = []
#     # for index, filePath in enumerate(ALL_OC_Files):
#     #     temp = ALL_OC_Files[0].name.split('_')
#     #     tempDf = pd.read_csv(filePath, header=1, usecols=[0, 1, 2, 3])
#     #     tempDf['PipeCode'] = temp[0]
#     #     tempDf['EffDate'] = parseDate(temp[2])

#     #     framesList.append(tempDf)

#     return combined_df_concise

async def cleanseOC(filePath: Path, segmentConfigs: pd.DataFrame):
    # segmentConfigs = pd.read_parquet(
    #     dirs.configFiles / 'SegmentConfigs.parquet').rename(columns={'StationName': 'Station Name'})
    # return
    try:
        lz = pl.scan_parquet(filePath)\
            .filter(
            ~functools.reduce(
                operator.and_,
                map(lambda column: pl.col(column).is_null(),
                    ['Station Name', 'Cap', 'Nom', 'Cap2'])
            ))\
            .with_columns(
            pl.col('Cap').cast(pl.Float64),
            pl.col('Cap2').cast(pl.Float64),
            pl.col('Nom').cast(pl.Float64),
            pl.col('CycleDesc').cast(pl.String),
        )

        lazyList: list[pl.LazyFrame] = []

        # records with no cap2, >=TD1 <TD2
        dfTD1 = lz.filter(pl.col('Cap2').is_null())\
            .with_columns(
                pl.col('Cap').alias('OpCap'),
                pl.col('Nom').map_batches(batchTDMapper,
                                          return_dtype=pl.String).alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

        lazyList.append(dfTD1)

        # row with both Cap & Cap2
        dfTD2 = lz.filter(~pl.col('Cap2').is_null())

        # cap always TD1 cap2 always TD2 if cap & nom<0 -> Nom==0
        dfTD2Cap1NomNeg = dfTD2.filter(pl.col('Nom') < 0)\
            .with_columns(
            pl.col('Cap').alias('OpCap'),
            pl.lit(float(0)).alias('Nom'),
            pl.lit('TD1').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

        lazyList.append(dfTD2Cap1NomNeg)

        # cap always TD1 cap2 always TD2 if cap2 & nom<0 -> Nom is unchanged
        dfTD2Cap2NomNeg = dfTD2.filter(pl.col('Nom') < 0)\
            .with_columns(
            pl.col('Cap2').alias('OpCap'),
            pl.lit('TD2').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

        lazyList.append(dfTD2Cap2NomNeg)

        # cap always TD1 cap2 always TD2 if cap & nom>0 -> Nom is unchanged
        dfTD2Cap1NomPos = dfTD2.filter(pl.col('Nom') > 0)\
            .with_columns(
            pl.col('Cap').alias('OpCap'),
            pl.lit('TD1').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

        lazyList.append(dfTD2Cap1NomPos)

        # cap always TD1 cap2 always TD2 if cap2 & nom>0 -> Nom==0
        dfTD2Cap2NomPos = dfTD2.filter(pl.col('Nom') > 0)\
            .with_columns(
            pl.col('Cap2').alias('OpCap'),
            pl.lit(float(0)).alias('Nom'),
            pl.lit('TD2').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

        lazyList.append(dfTD2Cap2NomPos)

        # cap always TD1 cap2 always TD2 if cap2 & nom==0 -> Nom is unchanged
        dfTD2Cap1NomZero = dfTD2.filter(pl.col('Nom') == 0)\
            .with_columns(
            pl.col('Cap').alias('OpCap'),
            pl.lit('TD1').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

        lazyList.append(dfTD2Cap1NomZero)

        # cap always TD1 cap2 always TD2 if cap2 & nom==0 -> Nom is unchanged
        dfTD2Cap2NomZero = dfTD2.filter(pl.col('Nom') == 0)\
            .with_columns(
            pl.col('Cap2').alias('OpCap'),
            pl.lit('TD2').alias('FlowInd'))\
            .select(['PipeCode', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

        lazyList.append(dfTD2Cap2NomZero)

        df = pl.concat(lazyList, how="vertical")\
            .with_columns(
            pl.col('OpCap').map_batches(
                batchAbsolute, return_dtype=pl.Float64),
            pl.col('Nom').map_batches(batchAbsolute, return_dtype=pl.Float64),
            pl.col('EffGasDate').map_batches(
                batchDateParse, return_dtype=pl.Date).alias('EffGasDay'),
            pl.col('FlowInd').map_batches(
                batchFIMapper, return_dtype=pl.String),
            pl.lit(None).cast(pl.String).alias('LocZn'),
        )


# ['GFPipeID', 'PipeCode', 'PipeName']

        df.join(pl.LazyFrame(pipeConfigs_df.loc[pipeConfigs_df['ParentPipe'] == ParentPipe, ['GFPipeID', 'PipeCode', 'PipeName']]), on='PipeCode', how='inner')\
            .join(pl.LazyFrame(segmentConfigs), on=['PipeCode', 'Station Name'], how='inner')\
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
                pl.lit('STA').alias('RowType'),

                (pl.col('OpCap') - pl.col('Nom')
                 ).alias('OperationallyAvailableCapacity')
        )\
            .with_columns(
                pl.concat_str([pl.col('GFPipeID'),
                               pl.col('RowType'),
                               #    pl.lit("SG"),
                               pl.col('Loc'),
                               pl.col('FlowInd')], separator='')
                .alias('GFLOC'),

                pl.lit('Enbridge').cast(pl.String).alias('ParentPipe'),
                pl.col('PipeName').alias('PipelineName'),
                # pl.col('EffGasDate').alias('EffGasDay'),
                # pl.lit(None).cast(pl.String).alias('CycleDesc'),
                pl.lit(None).cast(pl.String).alias('LocPurpDesc'),
                pl.col('Station Name').alias('LocName'),
                pl.lit(None).cast(pl.String).alias('LocZn'),
                pl.lit(None).cast(pl.String).alias('LocSegment'),
                pl.lit(None).alias('DesignCapacity'),
                pl.col('OpCap').alias('OperatingCapacity'),
                pl.col('Nom').alias('TotalScheduledQuantity'),
                pl.lit(None).cast(pl.String).alias('IT'),
                pl.lit(None).cast(pl.String).alias('AllQtyAvail'),
                pl.lit(None).cast(pl.String).alias('QtyReason'),
                pl.lit(datetime.now()).cast(pl.Datetime).alias('Timestamp'),
        )\
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
        print(f"cleanseOC failed for {filePath} - {error_detailed(e)}")
        logger.critical(
            f"cleanseOC failed for {filePath} - {error_detailed(e)}")
