import os


import pandas as pd
from contextlib import contextmanager
from azure.data.tables import TableServiceClient

from .BaseLogWriters import baseLogger
from .detLog import error_detailed
from .dirsFile import dirs


@contextmanager
def getTable(tableName: str):
    '''
    context Manager for table, creates table if not exist and return Table client
    :type tableName: str
    '''
    deltaConstr = os.getenv('PROD_STORAGE_CONSTR', '')
    try:
        with TableServiceClient.from_connection_string(conn_str=deltaConstr) as client:
            yield client.create_table_if_not_exists(table_name=tableName)
    except Exception as e:
        baseLogger.critical(
            f"table creation/retrieval failed - {error_detailed(e)}")


def dumpPipeConfigs(forceDump: bool = False) -> pd.DataFrame | None:
    '''
    downloads the pipeconfigs from PipeConfigs Table if 'PipeConfigs.parquet' doesnt already exist
    columns = ['ParentPipe', 'PipeName', 'GFPipeID', 'PipeCode', 'MetaCode', 'PointCapCode', 'SegmentCapCode', 'NoNoticeCode']
    '''
    if (forceDump or (not (dirs.configFiles / 'PipeConfigs.parquet').exists())):
        baseLogger.error("Dumping PipeConfigs from Azure Table Storage")
        with getTable('PipeConfigs') as table_client:
            df = pd.DataFrame(table_client.query_entities("", select=[
                'ParentPipe', 'PipeName', 'GFPipeID', 'PipeCode', 'MetaCode', 'PointCapCode', 'SegmentCapCode', 'NoNoticeCode']))
            try:
                df['GFPipeID'] = df['GFPipeID'].apply(lambda x: x.value)
                df.to_parquet(dirs.configFiles /
                              'PipeConfigs.parquet', index=False)
                return df
            except Exception as e:
                baseLogger.critical(
                    f"There was an error dumping PipeConfigs - {error_detailed(e)}")
    else:
        return pd.read_parquet(dirs.configFiles / 'PipeConfigs.parquet')


def dumpSegmentConfigs(forceDump: bool = False) -> pd.DataFrame | None:
    '''
    downloads the segmentconfigs from SegmentConfigs Table if 'SegmentConfigs.parquet' doesnt already exist
    '''
    if (forceDump or (not (dirs.configFiles / 'SegmentConfigs.parquet').exists())):
        baseLogger.error("Dumping SegmentConfigs from Azure Table Storage")
        with getTable('SegmentConfigs') as table_client:
            df = pd.DataFrame(table_client.query_entities(""))

        df.to_parquet(dirs.configFiles / 'SegmentConfigs.parquet', index=False)

        return df

    else:
        return pd.read_parquet(dirs.configFiles / 'SegmentConfigs.parquet')


def updateSegmentConfigs(df: pd.DataFrame):
    '''
    upserts new rows into SegmentConfigs Table
    :type df: pd.DataFrame
    '''
    operations = [("upsert", row)
                  for row in df.to_dict(orient='records')]

    for i in range(0, len(operations), 90):
        with getTable(tableName='SegmentConfigs') as table_client:
            try:
                table_client.submit_transaction(
                    operations[i:min(i+90, len(operations))])  # type: ignore
            except Exception as e:
                print("There was an error updating SegmentConfigs")
                print(f"Error: {e}")


def updatePipeConfigs(parentPipe: str):
    pass
