from typing import Callable, Any


import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime, date

def getBatch(inputFunction : Callable) -> Callable:
    def innerWrap(inSeries : pl.Series):
        return pl.Series(map(inputFunction, inSeries))
    return innerWrap

@getBatch
def stripString(x: str | None) -> str | None:
    if x:
        return x.strip()
    return None


@getBatch
def parseOADate(x: str) -> date:
    return datetime.strptime(x,"%m-%d-%Y").date()

@getBatch
def parseSGDate(x: str) -> date:
    return datetime.strptime(x,"%Y%m%d").date()


@getBatch
def parseFloat(x:str) -> float:
    return float(x.replace(",",'')) 

@getBatch
def int2String(x:int | None) -> str | None:
    if x:
        return f"{x}"
    return None

@getBatch
def zeroPadded7(x:str | None) -> str | None:
    if x:
    # temp= 7 - len(x)
        return f"{x:0>7}"
    return None

@getBatch
def GFPadded7(x:str | None) -> str | None:
    if x:
    # temp= 7 - len(x)
        return f"GF{x:0>5}"
    return None

@getBatch
def batchTDMapper(val: int | float) -> str :
    if (val < 0):
        return 'TD2'
    if (val > 0):
        return 'TD1'
    return 'TD1'

@getBatch
def batchFIMapper(x : str) -> str :
    if (x == 'TD1'):
        return 'F'
    return 'B'

@getBatch
def batchAbsolute(x : int | float) -> int | float :
    return abs(x)



raw_SG_path = Path('.').absolute().parent / 'downloads/enbridge/SG_raw'

pipe_configs_path = Path('.').absolute().parent / 'src/artifacts/configFiles/PipeConfigs.parquet'


configdDf = pl.read_parquet(pipe_configs_path)

temp_list =[]
for i in raw_SG_path.iterdir():
    pipe_code = i.name.split('_',1)[0].strip()
    firstLine = open(i).readline()
    if firstLine:
        gdate, cylce = firstLine.split('  Cycle: ')
        gdate = datetime.strptime((gdate.split('Gas Date: ')[1]).strip(), "%Y-%m-%d")
        cylce = cylce.strip()
        temp = pd.read_csv(i, header=1,skiprows=0, usecols=['Station Name','Cap','Nom','Cap2'])
        if not temp.empty:
            temp_list.append(temp.assign(EffGasDate=gdate, CycleDesc =cylce, PipeCode=pipe_code ))
        
df_SG : pd.DataFrame = pd.concat(temp_list, ignore_index=True)

df =pl.from_dataframe(df=df_SG).unique().join(configdDf["GFPipeID","PipeCode"], how="inner", on="PipeCode").select(pl.exclude("PipeCode"))


lz = df.lazy().with_columns(
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
    .select(['GFPipeID', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

lazyList.append(dfTD1)
# dfTD1.collect()


# row with both Cap & Cap2
dfTD2 = lz.filter(~pl.col('Cap2').is_null())

# cap always TD1 cap2 always TD2 if cap & nom<0 -> Nom==0
dfTD2Cap1NomNeg = dfTD2.filter(pl.col('Nom') < 0)\
    .with_columns(
    pl.col('Cap').alias('OpCap'),
    pl.lit(float(0)).alias('Nom'),
    pl.lit('TD1').alias('FlowInd'))\
    .select(['GFPipeID', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

lazyList.append(dfTD2Cap1NomNeg)
# dfTD2Cap1NomNeg.collect()



# cap always TD1 cap2 always TD2 if cap2 & nom<0 -> Nom is unchanged
dfTD2Cap2NomNeg = dfTD2.filter(pl.col('Nom') < 0)\
    .with_columns(
    pl.col('Cap2').alias('OpCap'),
    pl.lit('TD2').alias('FlowInd'))\
    .select(['GFPipeID', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

lazyList.append(dfTD2Cap2NomNeg)

# dfTD2Cap2NomNeg.collect()


# cap always TD1 cap2 always TD2 if cap & nom>0 -> Nom is unchanged
dfTD2Cap1NomPos = dfTD2.filter(pl.col('Nom') > 0)\
    .with_columns(
    pl.col('Cap').alias('OpCap'),
    pl.lit('TD1').alias('FlowInd'))\
    .select(['GFPipeID', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

lazyList.append(dfTD2Cap1NomPos)

# dfTD2Cap1NomPos.collect()


# cap always TD1 cap2 always TD2 if cap2 & nom>0 -> Nom==0
dfTD2Cap2NomPos = dfTD2.filter(pl.col('Nom') > 0)\
    .with_columns(
    pl.col('Cap2').alias('OpCap'),
    pl.lit(float(0)).alias('Nom'),
    pl.lit('TD2').alias('FlowInd'))\
    .select(['GFPipeID', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

lazyList.append(dfTD2Cap2NomPos)
# dfTD2Cap2NomPos.collect()


# cap always TD1 cap2 always TD2 if cap2 & nom==0 -> Nom is unchanged
dfTD2Cap1NomZero = dfTD2.filter(pl.col('Nom') == 0)\
    .with_columns(
    pl.col('Cap').alias('OpCap'),
    pl.lit('TD1').alias('FlowInd'))\
    .select(['GFPipeID', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

lazyList.append(dfTD2Cap1NomZero)

# dfTD2Cap1NomZero.collect()


# cap always TD1 cap2 always TD2 if cap2 & nom==0 -> Nom is unchanged
dfTD2Cap2NomZero = dfTD2.filter(pl.col('Nom') == 0)\
    .with_columns(
    pl.col('Cap2').alias('OpCap'),
    pl.lit('TD2').alias('FlowInd'))\
    .select(['GFPipeID', 'Station Name', 'OpCap', 'FlowInd', 'Nom', 'EffGasDate', 'CycleDesc'])

lazyList.append(dfTD2Cap2NomZero)

# dfTD2Cap2NomZero.collect()
df_silver = pl.concat(lazyList, how="vertical")\
    .with_columns(
    pl.col('OpCap').map_batches(
        batchAbsolute, return_dtype=pl.Float64),
    pl.col('Nom').map_batches(batchAbsolute, return_dtype=pl.Float64)
    ).collect()
    
#  push silver into silver layer in parquet format 

