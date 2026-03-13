from contextlib import asynccontextmanager
from pathlib import Path
from typing import Callable

from azure.storage.blob import StandardBlobTier
from azure.storage.blob.aio import BlobServiceClient

from .logging import logger
from .settings import settings


@asynccontextmanager
async def get_blob_service_client(conn_string: str | None = None):
    """Async context manager for Azure BlobServiceClient.

    Uses PROD_STORAGE_CONSTR from settings if conn_string not provided.
    """
    cs = conn_string or settings.prod_storage_constr
    client = BlobServiceClient.from_connection_string(cs)
    try:
        yield client
    finally:
        await client.close()


async def upload_blob(
    file_path: Path,
    blob_path: str,
    container: str,
    blob_service_client: BlobServiceClient,
    tier: StandardBlobTier = StandardBlobTier.COLD,
) -> None:
    """Upload a single file to Azure Blob Storage.

    Unified handler for both bronze (raw) and silver (normalized) uploads.
    Overwrites existing blobs.
    """
    try:
        with open(file_path, mode="rb") as data:
            await (
                blob_service_client
                .get_blob_client(blob=blob_path, container=container)
                .upload_blob(data=data, overwrite=True, standard_blob_tier=tier)
            )
    except Exception as e:
        logger.error(f"Blob upload failed: {container}/{blob_path} — {e}")


async def push_directory(
    directory: Path,
    container: str,
    path_resolver: Callable[[Path], str],
    conn_string: str | None = None,
    tier: StandardBlobTier = StandardBlobTier.COLD,
) -> None:
    """Upload all files in a directory to a blob container.

    Args:
        directory: local directory to iterate
        container: blob container name ('bronze' or 'silver')
        path_resolver: function that maps a local file path to its blob path
        conn_string: Azure connection string (defaults to PROD_STORAGE_CONSTR env)
        tier: storage tier for uploaded blobs
    """
    import asyncio

    async with get_blob_service_client(conn_string) as client:
        async with asyncio.TaskGroup() as group:
            for file_path in directory.iterdir():
                if file_path.is_file():
                    blob_path = path_resolver(file_path)
                    group.create_task(
                        upload_blob(file_path, blob_path, container, client, tier)
                    )
