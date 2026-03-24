from typing import Callable

import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime, date

from Meta_Munge import getMetaDf

loc_cols = ['GFPipeID', 'PipeName', 'GFLocID', 'GF_LocID_Tag', 'LocID', 'Loc', 'GF_LocName', 'LocName', 'GF_FacilityType', 'GF_FacilityTypeGroup', 'ReportedFacilityType', 'LocSegment', 'LocZone', 'State', 'County', 'InterconnectingEntity', 'UpdatedTime']

fact_cols =['GasMonth','GasDay','Dataset','GFLocID','LocName','DesignCapacity','OperatingCapacity','TotalScheduledQuantity','OperationallyAvailableCapacity','IT','FlowDirection','Timestamp',]


NN_cols_selected = ['Effective_From_DateTime',
#  'Effective_End_DateTime',
 'Loc_Prop',
 'Loc_Name',
 'Allocated_Qty',
 'Direction_Of_Flow',
#  'Accounting_Physincal_Indicator',
#  'Posting_DateTime',
 'PipeCode']

NN_cols = ['Effective_From_DateTime',
 'Effective_End_DateTime',
 'Loc_Prop',
 'Loc_Name',
 'Allocated_Qty',
 'Direction_Of_Flow',
 'Accounting_Physincal_Indicator',
 'Posting_DateTime',
 'PipeCode']


NN_gold_Map ={
    'Effective_From_DateTime': 'GasMonth',
#  'Effective_End_DateTime',
 'Loc_Prop':'Loc',
 'Loc_Name': 'LocName',
 'Allocated_Qty':'',
 'Direction_Of_Flow':'FlowDirection',
#  'Accounting_Physincal_Indicator',
#  'Posting_DateTime',
 'PipeCode': 'PipeCode'
}

raw_NN_path = Path('.').absolute().parent / 'downloads/enbridge/NN_raw'

pipe_configs_path = Path('.').absolute().parent / 'src/artifacts/configFiles/PipeConfigs.parquet'

pipesDf = pl.read_parquet(pipe_configs_path)

async def getConcatNN()-> pl.DataFrame:
    temp_list =[]
    for i in raw_NN_path.iterdir():
        pipe_code = i.name.split('_',1)[0].strip()
        temp = pd.read_csv(i,dtype=str)
        if not temp.empty:
            temp_list.append(temp.assign(PipeCode=pipe_code))
        
    return pl.from_dataframe(df=pd.concat(temp_list,ignore_index=True))


async def getNNLocs(concatedDd : pl.DataFrame):
    metaDf = await getMetaDf()
    
    NNLocs=concatedDd["Loc_Prop", "Loc_Name","PipeCode"].unique().join(pipesDf["GFPipeID","PipeName","PipeCode"], on="PipeCode",how="left")\
        .with_columns(
            pl.col("Loc_Prop").str.strip_chars().alias("Loc"),
            pl.col("Loc_Name").alias("LocName"),
            pl.col("Loc_Prop").str.strip_chars().str.zfill(7).alias("LocID"),

            pl.concat_str(
                [
                    pl.col("GFPipeID"),
                    pl.col("Loc_Prop").str.strip_chars().str.zfill(7)

                ]
            ).alias("GFLocID"),
            
            pl.lit(datetime.now()).cast(pl.Datetime).alias("UpdatedTime"),

        )
        
    return NNLocs.join(metaDf,on="GFLocID", how="left")\
        .with_columns(
            *[pl.lit(None).cast(pl.String).alias(col_name) for col_name in loc_cols if col_name not in NNLocs.columns],
        ).select(loc_cols).fill_null("").unique()
