import os
import asyncio

from pathlib import Path
import pandas as pd


from azure.storage.blob import StandardBlobTier
from azure.storage.blob.aio import BlobServiceClient


from contextlib import asynccontextmanager
from ..Munger import cleanseOA, cleanseOC, cleanseNN
from src.artifacts import error_detailed, dumpSegmentConfigs, updateSegmentConfigs, dirs


from ..utils import paths, logger, pipeConfigs_df, error_detailed

oa_path = paths.downloads / 'OA'
ocap_path = paths.downloads / 'OC'
nn_path = paths.downloads / 'NN'


oa_path.mkdir(exist_ok=True, parents=True)
ocap_path.mkdir(exist_ok=True, parents=True)
nn_path.mkdir(exist_ok=True, parents=True)


conn_string = os.environ["PROD_STORAGE_CONSTR"]


@asynccontextmanager
async def get_blob_service_client():
    client = BlobServiceClient.from_connection_string(conn_string)
    yield client
    await client.close()


async def runSilver(filePath: Path, blob_path: str, blob_service_client: BlobServiceClient):
    '''
    uploads the file into 'silver' container

    :param filePath: local file path
    :type filePath: Path
    :param blob_path: container blob path
    :type blob_path: str
    :param blob_service_client: blob service client in silver container
    :type blob_service_client: BlobServiceClient
    '''
    try:
        with open(filePath, mode="rb") as data:
            await blob_service_client.get_blob_client(blob=blob_path, container='silver')\
                .upload_blob(data=data,
                             overwrite=True,
                             standard_blob_tier=StandardBlobTier.COLD)
    except Exception as e:
        logger.critical(
            f"silver blob upload failed - {blob_path} - {error_detailed(e)}")


async def processOA():
    '''
    Converts Raw OA csv's into normalized parquet using 'cleanseOA'
    '''
    emptyList = []
    try:
        for filePath in (paths.downloads / 'OA_raw').iterdir():
            try:
                pipeCode = filePath.name.split('_', 2)[0]
                df = pd.read_csv(filePath, header=0)
                if (len(df) < 1):
                    emptyList.append(filePath.name.replace('.csv', '.parquet'))
                df.assign(PipeCode=pipeCode)\
                    .to_parquet(paths.downloads / "OA" / filePath.name.replace('.csv', '.parquet'), index=False)
            except Exception as e:
                logger.critical(
                    f"""Raw OA file read Failed {filePath} - {error_detailed(e)}""")

        # normalize and save into OA Folder
        async with asyncio.TaskGroup() as group:
            for file_path in oa_path.iterdir():
                if (file_path.name not in emptyList):
                    group.create_task(cleanseOA(file_path))
    except Exception:
        logger.critical("processOA failed ")


async def pushSilverOA():
    '''
    process the OA files using 'processOA' and push them into 'silver' container using 'runSilver' aysncly
    '''

    await processOA()
    # AG_OA_20221202_INTRDY_2022-12-03_0900.csv

    async with get_blob_service_client() as blob_service_client:
        async with asyncio.TaskGroup() as group:
            for file_path in oa_path.iterdir():

                pipeCode, _, effDate, _ = file_path.name.split('_', 3)
                blob_path = f'Enbridge/PointCapacity/{effDate[:-2]}/{pipeCode}/{file_path.name}'

                group.create_task(runSilver(filePath=file_path,
                                            blob_service_client=blob_service_client,
                                            blob_path=blob_path))


def OCHelper() -> list[str]:
    """
    Read Convert Files to Parquet and report empty files

    :return: Description
    :rtype: list[str]
    """
    emptyList = []

    for filePath in (paths.downloads / 'OC_raw').iterdir():
        try:
            pipeCode, _, EffGasDayTime, cycle_desc = filePath.name.split(
                '_', 3)

            cycle_desc = cycle_desc[:-4]

            try:
                df = pd.read_csv(filePath,
                                 skiprows=1,
                                 usecols=['Station Name', 'Cap', 'Nom', 'Cap2'])
                if (len(df) < 1):
                    emptyList.append(
                        filePath.name.replace('.csv', '.parquet'))
            except Exception as e:
                logger.critical(
                    f"Raw OC file read Failed {filePath} - {error_detailed(e)}")
                continue

            df.assign(PipeCode=pipeCode)\
                .assign(EffGasDate=EffGasDayTime, CycleDesc=cycle_desc, )[
                    ['PipeCode', 'EffGasDate', 'CycleDesc', 'Station Name', 'Cap', 'Nom', 'Cap2']]\
                .to_parquet(paths.downloads / "OC" / filePath.name.replace('.csv', '.parquet'), index=False)
        except Exception as e:
            logger.critical(
                f"""Raw OC file read Failed {filePath} - {error_detailed(e)}""")

    return emptyList


async def processOC():
    '''
    dump segments using 'dumpSegmentConfigs', normalize each raw OC csv's to parquets, update the new segments of 'Enbridge' with appropriate id's and push the changes using 'updateSegmentConfigs' then clean OC files
    '''

    dumpSegmentConfigs()

    try:

        emptyList = OCHelper()

        configs_dfFull = pd.read_parquet(
            dirs.configFiles / 'SegmentConfigs.parquet')
        try:
            configs_df = configs_dfFull[configs_dfFull['ParentPipe']
                                        == 'Enbridge']
        except Exception:
            configs_df = pd.DataFrame(
                columns=['ParentPipe', 'PipeCode', 'GFPipeID', 'StationName'])

        # oc files have duplicates as there is an intersection of stations in segments
        OCdf = pd.read_parquet(ocap_path, columns=['PipeCode', 'Station Name']).rename(columns={'Station Name': 'StationName'})\
            .drop_duplicates(keep='first')

        recordsList = []

        for pipe_code in OCdf['PipeCode'].unique():

            GFPipeID = pipeConfigs_df.loc[
                ((pipeConfigs_df['ParentPipe'] == 'Enbridge') & (
                    pipeConfigs_df['PipeCode'] == pipe_code)),
                'GFPipeID'].item()  # type: ignore

            # Unique station names in config for the pipe code
            configSet = set(
                configs_df[configs_df['PipeCode'] == pipe_code]['StationName'])

            # Unique station names in OC files for the pipe code
            OCset = set(OCdf[OCdf['PipeCode'] == pipe_code]['StationName'])

            # new stations found in data to be inserted into SegmentConfigs table with new Loc id's
            recordsList.extend([{'Loc': f"{key:0>6}", 'ParentPipe': 'Enbridge', 'GFPipeID': GFPipeID, 'PipeCode': pipe_code, 'StationName': StationName}
                               for key, StationName in enumerate((OCset - configSet), start=len(configSet)+1)])

        if (recordsList):
            newdf = pd.DataFrame(recordsList)
            newdf['PartitionKey'] = 'Enbridge'
            newdf['RowKey'] = newdf['ParentPipe'] + \
                newdf['Loc']+newdf['PipeCode']

            # push the new records to SegmentConfigs table and update the parquet dump
            updateSegmentConfigs(newdf)

            pd.concat([configs_dfFull, newdf], ignore_index=True)\
                .to_parquet(dirs.configFiles / 'SegmentConfigs.parquet', index=False)

            Enbridge_configsDF = pd.concat([configs_df, newdf], ignore_index=True)\
                .rename(columns={'StationName': 'Station Name'})[['PipeCode', 'Loc', 'Station Name']]
        else:
            Enbridge_configsDF = configs_df.rename(columns={'StationName': 'Station Name'})[
                ['PipeCode', 'Loc', 'Station Name']]

        async with asyncio.TaskGroup() as group:
            for file_path in ocap_path.iterdir():
                if (file_path.name not in emptyList):
                    group.create_task(
                        cleanseOC(file_path, segmentConfigs=Enbridge_configsDF))

    except Exception as e:
        logger.critical(f"processOC failed - {error_detailed(e)}")


async def pushSilverOC():
    '''cleanse all OC file using 'processOC' and push then into 'silver' container'''
    # logger.error("starting OC processing .............")
    await processOC()
    # logger.error("completed OC processing .............")
    # AG_OC1_20221206_INTRDY_2022-12-07_0900.csv

    async with get_blob_service_client() as blob_service_client:
        async with asyncio.TaskGroup() as group:
            for file_path in ocap_path.iterdir():

                pipeCode, _, effDate, _ = file_path.name.split('_', 3)
                blob_path = f'Enbridge/SegmentCapacity/{effDate[:-2]}/{pipeCode}/{file_path.name}'

                group.create_task(runSilver(filePath=file_path,
                                            blob_service_client=blob_service_client,
                                            blob_path=blob_path))


async def processNN():
    '''
    Converts Raw NN csv's into normalized parquet using 'cleanseNN'
    '''

    # pipeCode, _, effDate = file_path.name.split('_')

    emptyList = []
    try:
        for filePath in (paths.downloads / 'NN_raw').iterdir():
            try:
                # pipeCode, _, EffGasDayTime, _ = filePath.name.split('_', 3)
                pipeCode, _, _ = filePath.name.split('_')

                df = pd.read_csv(filePath, header=0)
                if (len(df) < 1):
                    emptyList.append(filePath.name.replace('.csv', '.parquet'))

                df.assign(PipeCode=pipeCode)\
                    .to_parquet(paths.downloads / "NN" / filePath.name.replace('.csv', '.parquet'), index=False)
            except Exception as e:
                logger.critical(
                    f"""Raw NN file read Failed {filePath} - {error_detailed(e)}""")

        async with asyncio.TaskGroup() as group:
            for file_path in nn_path.iterdir():
                if (file_path.name not in emptyList):
                    group.create_task(cleanseNN(file_path))

    except Exception:
        logger.critical("processNN failed ")


async def pushSilverNN():
    '''
    cleanse all NN files using 'processNN' and push then into 'silver' container
    '''
    # logger.error("starting OC processing .............")
    await processNN()
    # logger.error("completed OC processing .............")
    # AG_OC1_20221206_INTRDY_2022-12-07_0900.csv

    async with get_blob_service_client() as blob_service_client:
        async with asyncio.TaskGroup() as group:
            for file_path in nn_path.iterdir():

                pipeCode, _, effDate = file_path.name.split('_')
                blob_path = f'Enbridge/NoNotice/{effDate[:-10]}/{pipeCode}/{file_path.name}'

                group.create_task(runSilver(filePath=file_path,
                                            blob_service_client=blob_service_client,
                                            blob_path=blob_path))


__all__ = ['pushSilverOA', 'pushSilverOC', 'pushSilverNN']
