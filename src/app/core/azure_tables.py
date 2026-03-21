from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd
from azure.data.tables import TableServiceClient

from .logging import logger
from .settings import settings


@contextmanager
def get_table(table_name: str, conn_string: str | None = None):
    """Context manager for an Azure Table client.

    Creates the table if it doesn't exist and yields the table client.
    """
    cs = conn_string or settings.prod_storage_constr
    try:
        with TableServiceClient.from_connection_string(conn_str=cs) as client:
            yield client.create_table_if_not_exists(table_name=table_name)
    except Exception as e:
        logger.error(f"Table '{table_name}' access failed: {e}")


class _ConfigCache:
    """In-memory cache for config DataFrames backed by parquet files.

    Tracks the file's mtime — automatically re-reads from disk if the
    file was modified since the last load. Any write (force dump, upsert)
    invalidates the cache so the next read picks up fresh data.
    """

    def __init__(self) -> None:
        self._data: dict[Path, pd.DataFrame] = {}
        self._mtime: dict[Path, float] = {}

    def get(self, path: Path) -> pd.DataFrame | None:
        """Return cached DataFrame if file hasn't changed, else None."""
        if path not in self._data or not path.exists():
            return None
        current_mtime = path.stat().st_mtime
        if current_mtime != self._mtime.get(path):
            # File changed on disk since we cached — invalidate
            self.invalidate(path)
            return None
        return self._data[path]

    def put(self, path: Path, df: pd.DataFrame) -> None:
        """Cache a DataFrame and record the file's current mtime."""
        self._data[path] = df
        if path.exists():
            self._mtime[path] = path.stat().st_mtime

    def invalidate(self, path: Path) -> None:
        """Remove a specific entry from cache."""
        self._data.pop(path, None)
        self._mtime.pop(path, None)

    def clear(self) -> None:
        """Clear all cached entries."""
        self._data.clear()
        self._mtime.clear()


_cache = _ConfigCache()


def dump_pipe_configs(config_dir: Path, force: bool = False) -> pd.DataFrame | None:
    """Download PipeConfigs from Azure Table Storage.

    Cached in-memory with mtime tracking — re-reads from disk only if
    the parquet file was modified since the last load.

    Writes to config_dir/PipeConfigs.parquet. Skips download if file exists
    unless force=True.

    Columns: ParentPipe, PipeName, GFPipeID, PipeCode, MetaCode,
             PointCapCode, SegmentCapCode, StorageCapCode, NoNoticeCode
    """
    parquet_path = config_dir / "PipeConfigs.parquet"

    if not force:
        cached = _cache.get(parquet_path)
        if cached is not None:
            return cached
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            _cache.put(parquet_path, df)
            return df

    # Force download or file doesn't exist
    _cache.invalidate(parquet_path)
    logger.info("Dumping PipeConfigs from Azure Table Storage")
    with get_table(settings.pipe_configs_table) as table_client:
        df = pd.DataFrame(
            table_client.query_entities(
                "",
                select=[
                    "ParentPipe", "PipeName", "GFPipeID", "PipeCode",
                    "MetaCode", "PointCapCode", "SegmentCapCode", "StorageCapCode", "NoNoticeCode",
                ],
            )
        )
        try:
            df["GFPipeID"] = df["GFPipeID"].apply(
                lambda x: x.value if hasattr(x, "value") else x
            )
            df.to_parquet(parquet_path, index=False)
            _cache.put(parquet_path, df)
            return df
        except Exception as e:
            logger.error(f"PipeConfigs dump failed: {e}")
            return None


def dump_Loc_configs(config_dir: Path, force: bool = False) -> pd.DataFrame | None:
    """Download LocConfigs from Azure Table Storage.

    Cached in-memory with mtime tracking — re-reads from disk only if
    the parquet file was modified since the last load.

    Writes to config_dir/LocConfigs.parquet.
    """
    parquet_path = config_dir / "LocConfigs.parquet"

    if not force:
        cached = _cache.get(parquet_path)
        if cached is not None:
            return cached
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            _cache.put(parquet_path, df)
            return df

    # Force download or file doesn't exist
    _cache.invalidate(parquet_path)
    logger.info("Dumping LocConfigs from Azure Table Storage")
    with get_table(settings.Loc_configs_table) as table_client:
        df = pd.DataFrame(table_client.query_entities(""))

    # Unwrap EdmType wrapper objects and datetime subclasses returned by the Azure SDK
    def _unwrap(x):
        if hasattr(x, "value"):
            return x.value
        if isinstance(x, datetime):
            return x.isoformat()
        return x

    for col in df.columns:
        df[col] = df[col].apply(_unwrap)

    df.to_parquet(parquet_path, index=False)
    _cache.put(parquet_path, df)
    return df


def update_Loc_configs(df: pd.DataFrame) -> None:
    """Upsert new rows into the LocConfigs Azure Table.

    Batches in groups of 90 (Azure Table transaction limit is 100).
    Invalidates all cached configs since the backing data changed.
    """
    operations = [("upsert", row) for row in df.to_dict(orient="records")]

    for i in range(0, len(operations), 90):
        with get_table(settings.Loc_configs_table) as table_client:
            try:
                table_client.submit_transaction(
                    operations[i : min(i + 90, len(operations))] # type: ignore
                )
            except Exception as e:
                logger.error(f"LocConfigs upsert failed: {e}")

    # Invalidate all config caches — segment data changed, and any
    # downstream consumers holding references need fresh data
    _cache.clear()
    logger.info("Config cache cleared after LocConfigs upsert")


def invalidate_config_cache() -> None:
    """Manually clear all cached config DataFrames.

    Call this if you know the underlying parquet files or Azure Tables
    have been modified outside of this module.
    """
    _cache.clear()
    logger.info("Config cache manually cleared")
