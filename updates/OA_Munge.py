
from typing import Callable

import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime, date


OA_columns ={
    'Eff_Gas_Day': 'GasDay',
    'Cycle_Desc' : 'CycleDesc',
    'Loc':'LocID',
    'Loc_Name':'LocName',
    'Flow_Ind_Desc': 'FlowDirection',
    'IT':'IT',
    'Total_Design_Capacity':'DesignCapacity',
    'Operating_Capacity':'OperatingCapacity',
    'Total_Scheduled_Quantity':'TotalScheduledQuantity',
    'Operationally_Available_Capacity':'OperationallyAvailableCapacity',
 }

raw_OA_path = Path('.').absolute().parent / 'downloads/enbridge/OA_raw'

pipe_configs_path = Path('.').absolute().parent / 'src/artifacts/configFiles/PipeConfigs.parquet'

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
def parseDate(x: str) -> date:
    return datetime.strptime(x,"%m-%d-%Y").date()


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

def mungeSilver():
    temp_list =[]
    for i in raw_OA_path.iterdir():
        pipe_code = i.name.split('_',1)[0].strip()
        temp = pd.read_csv(i)
        if not temp.empty:
            temp_list.append(temp.assign(PipeCode=pipe_code))
            
    df_OA : pd.DataFrame= pd.concat(temp_list)
    df = pl.from_dataframe(df=df_OA.astype(str))
    # detect the 
    


def mungeGold():
    temp_list =[]
    for i in raw_OA_path.iterdir():
        pipe_code = i.name.split('_',1)[0].strip()
        temp = pd.read_csv(i)
        if not temp.empty:
            temp_list.append(temp.assign(PipeCode=pipe_code))
            
    df_OA : pd.DataFrame= pd.concat(temp_list)
    df = pl.from_dataframe(df=df_OA.astype(str))
    
    configdDf = pl.read_parquet(pipe_configs_path)


    OA_df = df.join(configdDf,how="inner",on="PipeCode").rename(mapping=OA_columns)\
        .select([*list(OA_columns.values()),
                "GFPipeID"
            ])\
            .with_columns(
            
            pl.col('CycleDesc').map_batches(stripString,return_dtype=pl.String),
            
            pl.col('GasDay').map_batches(parseDate,return_dtype=pl.Date),
            
            pl.col('LocID').map_batches(stripString,return_dtype=pl.String),
            
            pl.lit("OA").cast(pl.String).alias('Dataset'),
            
            pl.concat_str([
                    pl.col('GFPipeID').map_batches(int2String,return_dtype=pl.String),
                    pl.col('LocID').map_batches(zeroPadded7,return_dtype=pl.String)
                ]).alias("GFLocID"),
            
            pl.concat_str([
                    pl.col('GFPipeID').map_batches(int2String,return_dtype=pl.String),
                    pl.lit("OA").cast(pl.String),
                    pl.col('LocID').map_batches(zeroPadded7,return_dtype=pl.String)
                ]).alias("GFLocDirID"),
            

            pl.col('LocName').map_batches(stripString,return_dtype=pl.String),
            
            pl.col('DesignCapacity').map_batches(parseFloat,return_dtype=pl.Float64),
            pl.col('OperatingCapacity').map_batches(parseFloat,return_dtype=pl.Float64),
            pl.col('TotalScheduledQuantity').map_batches(parseFloat,return_dtype=pl.Float64),
            pl.col('OperationallyAvailableCapacity').map_batches(parseFloat,return_dtype=pl.Float64),
            
            pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp")
        )\
            .select([
                'GasDay',
                'GFPipeID',
                'GFLocID',
                'Dataset',
                'FlowDirection',
                'LocID',
                'LocName',
                'IT',
                'DesignCapacity',
                'OperatingCapacity',
                'TotalScheduledQuantity',
                'OperationallyAvailableCapacity',
                'Timestamp',
        ])
            
    # invoke push to delta table
    