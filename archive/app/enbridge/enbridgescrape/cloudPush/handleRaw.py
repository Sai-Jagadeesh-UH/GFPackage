import os
import asyncio

from dotenv import load_dotenv
from contextlib import asynccontextmanager
from pathlib import Path

from azure.storage.blob import StandardBlobTier
from azure.storage.blob.aio import BlobServiceClient

from src.artifacts.detLog import error_detailed
from src.artifacts.dirsFile import dirs
from ..utils import paths, logger


load_dotenv(dirs.root / 'archives/.env')


oa_downloads_path = paths.downloads / 'OA_raw'
ocap_downloads_path = paths.downloads / 'OC_raw'
nn_downloads_path = paths.downloads / 'NN_raw'
metaData_downloads_path = paths.downloads / 'MetaData'


conn_string = os.environ["PROD_STORAGE_CONSTR"]


@asynccontextmanager
async def get_blob_service_client():
    client = BlobServiceClient.from_connection_string(conn_string)
    yield client
    await client.close()


async def runRaw(filePath: Path, blob_path: str, blob_service_client: BlobServiceClient):
    '''
    async blob uploader into 'bronze' container overwrites if exist defaults to COLD Tier

    :param filePath: local file path
    :type filePath: Path
    :param blob_path: path in container
    :type blob_path: str
    :param blob_service_client: blob client
    :type blob_service_client: BlobServiceClient
    '''
    try:
        with open(filePath, mode="rb") as data:
            await blob_service_client.get_blob_client(blob=blob_path, container='bronze')\
                .upload_blob(data=data,
                             overwrite=True,
                             standard_blob_tier=StandardBlobTier.COLD)
    except Exception as e:
        logger.critical(
            f"blob upload failed - {blob_path} - {error_detailed(e)}")


async def pushRawOA():
    '''
    async tasks to push Raw OA files into 'bronze' container using runRaw
    '''
    # AG_OA_20221202_INTRDY_2022-12-03_0900.csv
    # blob_service_client = BlobServiceClient.from_connection_string(conn_string)
    async with get_blob_service_client() as blob_service_client:
        async with asyncio.TaskGroup() as group:
            for file_path in oa_downloads_path.iterdir():

                pipeCode, _, effDate, _ = file_path.name.split('_', 3)
                blob_path = f'Enbridge/PointCapacity/{effDate[:-2]}/{pipeCode}/{file_path.name}'

                group.create_task(runRaw(filePath=file_path,
                                         blob_service_client=blob_service_client,
                                         blob_path=blob_path))

    # await blob_service_client.close()


async def pushRawOC():
    '''
    async tasks to push Raw OC files into 'bronze' container using runRaw
    '''
    # AG_OC1_20221206_INTRDY_2022-12-07_0900.csv
    # blob_service_client = BlobServiceClient.from_connection_string(conn_string)
    async with get_blob_service_client() as blob_service_client:
        async with asyncio.TaskGroup() as group:
            for file_path in ocap_downloads_path.iterdir():

                pipeCode, _, effDate, _ = file_path.name.split('_', 3)
                blob_path = f'Enbridge/SegmentCapacity/{effDate[:-2]}/{pipeCode}/{file_path.name}'

                group.create_task(runRaw(filePath=file_path,
                                         blob_service_client=blob_service_client,
                                         blob_path=blob_path))

    # await blob_service_client.close()


async def pushRawNN():
    '''
    async tasks to push Raw NN files into 'bronze' container using runRaw
    '''
    # TE_NN_20230906.csv

    # blob_service_client = BlobServiceClient.from_connection_string(conn_string)
    async with get_blob_service_client() as blob_service_client:
        async with asyncio.TaskGroup() as group:
            for file_path in nn_downloads_path.iterdir():

                pipeCode, _, effDate = file_path.name.split('_')
                blob_path = f'Enbridge/NoNotice/{effDate[:-4]}/{pipeCode}/{file_path.name}'

                group.create_task(runRaw(filePath=file_path,
                                         blob_service_client=blob_service_client,
                                         blob_path=blob_path))

    # await blob_service_client.close()


async def pushRawMeta():
    '''
    async tasks to push Raw Meta files into 'bronze' container using runRaw
    '''
    # AG_AllPoints.csv

    # blob_service_client = BlobServiceClient.from_connection_string(conn_string)
    async with get_blob_service_client() as blob_service_client:
        async with asyncio.TaskGroup() as group:
            for file_path in metaData_downloads_path.iterdir():

                blob_path = f'Enbridge/Metadata/{file_path.name}'

                group.create_task(runRaw(filePath=file_path,
                                         blob_service_client=blob_service_client,
                                         blob_path=blob_path))

    # await blob_service_client.close()


async def pushRawLogs():
    '''
    async tasks to push Raw Logs files into 'bronze' container
    '''
    # Enbridge_error_20251201.log
    # blob_service_client = BlobServiceClient.from_connection_string(conn_string)

    async with get_blob_service_client() as blob_service_client:
        async with asyncio.TaskGroup() as group:
            for file_path in paths.logs.iterdir():
                if ('Enbridge' in file_path.name and not file_path.name.endswith('.csv')):
                    _, _, effDate = file_path.name.split('_')
                    blob_path = f'Enbridge/logs/{effDate[:-4]}/{file_path.name}'

                    group.create_task(runRaw(filePath=file_path,
                                             blob_service_client=blob_service_client,
                                             blob_path=blob_path))

    # await blob_service_client.close()


__all__ = ['pushRawOA', 'pushRawOC', 'pushRawNN', 'pushRawMeta', 'pushRawLogs']
