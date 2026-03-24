from typing import Callable

import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime, date

from Meta_Munge import getMetaDf

loc_cols = ['GFPipeID', 'PipeName', 'GFLocID', 'GF_LocID_Tag', 'LocID', 'Loc', 'GF_LocName', 'LocName', 'GF_FacilityType', 'GF_FacilityTypeGroup', 'ReportedFacilityType', 'LocSegment', 'LocZone', 'State', 'County', 'InterconnectingEntity', 'UpdatedTime']

fact_cols =['GasMonth','GasDay','Dataset','GFLocID','LocName','DesignCapacity','OperatingCapacity','TotalScheduledQuantity','OperationallyAvailableCapacity','IT','FlowDirection','Timestamp',]

OA_cols_gold = [ 'Eff_Gas_Day', 'Loc', 'Loc_Name', 'Flow_Ind_Desc', 'IT', 'Total_Design_Capacity', 'Operating_Capacity', 'Total_Scheduled_Quantity', 'Operationally_Available_Capacity', 'PipeCode' ] 

OA_gold_map ={
    'Eff_Gas_Day': 'GasDay',
    # 'Cycle_Desc' : 'CycleDesc',
    # 'Loc':'LocID',
    'Loc_Name':'LocName',
    'Flow_Ind_Desc': 'FlowDirection',
    # 'IT':'IT',
    'Total_Design_Capacity':'DesignCapacity',
    'Operating_Capacity':'OperatingCapacity',
    'Total_Scheduled_Quantity':'TotalScheduledQuantity',
    'Operationally_Available_Capacity':'OperationallyAvailableCapacity',
 }

OA_flow_map =  {
        'Delivery': 'D',
        'Receipt': 'R',
        'Storage Injection': 'D',
        'Storage Withdrawal': 'R'
    }

raw_OA_path = Path('.').absolute().parent / 'downloads/enbridge/OA_raw'

pipe_configs_path = Path('.').absolute().parent / 'src/artifacts/configFiles/PipeConfigs.parquet'

pipesDf = pl.read_parquet(pipe_configs_path)

async def getConcatOA()-> pl.DataFrame:
    temp_list =[]
    for i in raw_OA_path.iterdir():
        pipe_code = i.name.split('_',1)[0].strip()
        temp = pd.read_csv(i,dtype=str)
        if not temp.empty:
            temp_list.append(temp.assign(PipeCode=pipe_code))

    return pl.from_dataframe(df=pd.concat(temp_list,ignore_index=True))


async def updateOALocs(concatedDd : pl.DataFrame):
    
    MetaDf = await getMetaDf()
    
    
    LocsDf = concatedDd.select(OA_cols_gold).rename(mapping=OA_gold_map)\
        .select(["Loc","LocName","PipeCode"])\
            .join(pipesDf["PipeCode","GFPipeID","PipeName"], on="PipeCode").unique()

    LocsDf = LocsDf.with_columns(
        pl.col("Loc").str.strip_chars(),
        pl.col("Loc").str.strip_chars().str.zfill(7).alias("LocID")
    ).with_columns(
        pl.concat_str([
            pl.col("GFPipeID"),
            pl.col("LocID")
        ]).alias("GFLocID"),
        pl.lit(datetime.now()).cast(pl.Datetime).alias("UpdatedTime")
    ).join(MetaDf,on="GFLocID", how="left")
    
    LocsDf =  LocsDf.with_columns(
            *[pl.lit(None).cast(pl.String).alias(col_name) for col_name in loc_cols if col_name not in LocsDf.columns]
        ).select(loc_cols).fill_null("")
    
    await pushNewOALocs(LocsDf)
    

async def pushNewOALocs(OALocsDf: pl.DataFrame):
    # dump the LocMetaConfigs from Azure Table 
    # check the GFLocID's missing in the LocMetaConfigs
    # push the new GFLocID's rows to the Azure Table
    pass



async def pushOASilver(concatedDd : pl.DataFrame):
    
    # cleanse the columns and add GFLocID
    silverOA = concatedDd.join(pipesDf["PipeCode","GFPipeID"], on="PipeCode", how="left").with_columns(
        pl.concat_str(
            [
                pl.col("GFPipeID"),
                pl.col("Loc").str.zfill(7)

            ]
        ).alias("GFLocID")
    ).select(pl.exclude(["PipeCode","GFPipeID"])).unique()
    
    
    # push the silverOA to silver Lake
    

async def pushOAGold(concatedDd : pl.DataFrame)->pl.LazyFrame:
    
    goldOADf=concatedDd.select(OA_cols_gold).rename(OA_gold_map)\
        .join(pipesDf["GFPipeID","PipeCode"],on="PipeCode",how="left").lazy()\
            .with_columns(

                pl.col("GasDay").str.to_date(format="%m-%d-%Y"),
                
                pl.col("FlowDirection").str.strip_chars().replace_strict(OA_flow_map, default="N"),
                
                pl.lit(datetime.now()).cast(pl.Datetime).alias("Timestamp"),

                pl.lit("OA").alias("Dataset"),

                pl.col("Loc").str.strip_chars(),

                pl.col("DesignCapacity").str.replace_all(",",""),
                pl.col("OperatingCapacity").str.replace_all(",",""),
                pl.col("TotalScheduledQuantity").str.replace_all(",",""),
                pl.col("OperationallyAvailableCapacity").str.replace_all(",",""),

            )\
            .with_columns(
                pl.col("Loc").str.zfill(7).alias("LocID"),

                pl.col("DesignCapacity").str.to_decimal(scale=2),
                pl.col("OperatingCapacity").str.to_decimal(scale=2),
                pl.col("TotalScheduledQuantity").str.to_decimal(scale=2),
                pl.col("OperationallyAvailableCapacity").str.to_decimal(scale=2),
            )\
            .with_columns(
                pl.concat_str([
                    pl.col("GFPipeID"),
                    pl.col("LocID")
                ]).alias("GFLocID"),
                
                pl.concat_str(
                    [
                        pl.col("GasDay").dt.year().cast(pl.String),
                        pl.col("GasDay").dt.month().cast(pl.String).str.zfill(2)
                    ]
                ).str.to_integer().alias("GasMonth")
            ).select(fact_cols)
            
    return goldOADf # this will be concatenated with others and pushed to Gold as a whole
            
    



# LocsDf.write_csv(pipe_configs_path.parent / "OALocMeta.csv")

