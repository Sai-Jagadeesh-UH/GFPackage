import os
from typing import List
from .detLog import error_detailed

from contextlib import contextmanager

import pandas as pd

from azure.data.tables import TableServiceClient


@contextmanager
def getTable(tableName: str):
    deltaConstr = os.getenv('DELTA_STORAGE_CONSTR', '')
    try:
        with TableServiceClient.from_connection_string(conn_str=deltaConstr) as client:
            yield client.create_table_if_not_exists(table_name=tableName)
    except Exception as e:
        print(f"table creation/retrieval failed - {error_detailed(e)}")
    finally:
        pass


def dumpTable(tableName: str):
    return
    with getTable(tableName) as table_client:
        try:
            table_client.query_entities("")
            # .create_entity(entity=entity)
        except Exception as e:
            print("There was an error inserting the entity")
            print(f"Error: {e}")


def upsertTable(entityFrame: pd.DataFrame, tableName: str):
    return
    if tableName == 'GFPipes':
        entityFrame['PartitionKey'] = 'MetaData'
        entityFrame['RowKey'] = entityFrame['GFPipeID'].astype(
            str) + entityFrame['TSP'].astype(str).apply(lambda x: x.rjust(15, '0'))

        entityFrame = entityFrame[['PartitionKey',
                                   'RowKey', 'TSP', 'TSP_Name']]

        operations = [("upsert", row)
                      for row in entityFrame.to_dict(orient='records')]

        with getTable(tableName) as table_client:
            try:
                table_client.submit_transaction(operations)  # type: ignore
            except Exception as e:
                print("There was an error inserting the entity")
                print(f"Error: {e}")


def archiveFiles(fileList: List[str]):
    pass

    # def upsertEntities(entittyFrame: pd.DataFrame, tableName: str):
    #     entityList = entittyFrame.to_dict()

    #     with getTable(tableName) as table_client:

    #         operations = [
    #             ("create", entity1),
    #             # ("delete", entity2),
    #             ("upsert", entity3),
    #             # ("update", entity4, {"mode": "replace"}),
    #         ]
    #         try:
    #             table_client.submit_transaction(operations)
    #         except Exception as e:
    #             print("There was an error with the transaction operation")
    #             print(f"Error: {e}")
