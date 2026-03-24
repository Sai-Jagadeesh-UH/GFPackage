from typing import Callable

import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime, date



loc_cols = ['GFPipeID', 'PipeName', 'GFLocID', 'GF_LocID_Tag', 'LocID', 'Loc', 'GF_LocName', 'LocName', 'GF_FacilityType', 'GF_FacilityTypeGroup', 'ReportedFacilityType', 'LocSegment', 'LocZone', 'State', 'County', 'InterconnectingEntity', 'UpdatedTime']

fact_cols =['GasDay','GFLocID','LocName','DesignCapacity','OperatingCapacity','TotalScheduledQuantity','OperationallyAvailableCapacity','IT','Direction','Timestamp']


meta_loc_columns =[
 'Loc',
 'Loc Name',
 'Loc St Abbrev',
 'Loc Cnty',
 'Loc Zone',
 'Dir Flo',
 'Loc Type Ind',
 'PipeCode'
]

meta_map = {
    'Loc St Abbrev':'State',
    'Loc Cnty': 'County',
    'Loc Zone':'LocZone',
    'Loc Type Ind':'ReportedFacilityType',
    }

Meta_path = Path('.').absolute().parent / 'downloads/enbridge/MetaData'

pipe_configs_path = Path('.').absolute().parent / 'src/artifacts/configFiles/PipeConfigs.parquet'

pipesDf = pl.read_parquet(pipe_configs_path)


async def getMetaDf() -> pl.DataFrame:
    
    temp_list =[]
    for i in Meta_path.iterdir():
        pipe_code = i.name.split('_',1)[0].strip()
        temp = pd.read_csv(i,dtype=str)
        if not temp.empty:
            temp_list.append(temp.assign(PipeCode=pipe_code))
        
    df_Meta : pd.DataFrame= pd.concat(temp_list,ignore_index=True)
    df = pl.from_dataframe(df=df_Meta)
    df= df.with_columns(*[pl.col(col_name).str.strip_chars() for col_name in df.columns]).fill_null("")

    
    await pushMeta(df)


    # Add GFLocID to use metadata to populte Loc Data in OA and NN
    return df.select(meta_loc_columns)\
        .join(pipesDf["PipeCode","GFPipeID","PipeName"], on="PipeCode").unique()\
        .with_columns(
            pl.concat_str(
                [
                    pl.col("GFPipeID"),
                    pl.col("Loc").str.strip_chars().str.zfill(7).cast(pl.String)
                    
                ]
            ).alias("GFLocID")
        ).unique().rename(mapping=meta_map)


async def pushMeta(metDf: pl.DataFrame | pd.DataFrame):
    # get the metaData from azure Table
    # compare the current metDf with the metaData
    # push the new rows in to Azure Table
    pass