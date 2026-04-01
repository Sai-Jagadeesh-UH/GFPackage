from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from azure.data.tables import TableServiceClient

from .logging import logger
from .settings import settings


@contextmanager
def get_table(table_name: str, conn_string: str | None = None):
    """Context manager for an Azure Table client.

    Creates the table if it doesn't exist and yields the table client.
    Raises on connection/table errors so callers can handle them.
    """
    cs = conn_string or settings.prod_storage_constr
    with TableServiceClient.from_connection_string(conn_str=cs) as client:
        yield client.create_table_if_not_exists(table_name=table_name)


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
        """Return cached DataFrame if file is from today and hasn't changed, else None."""
        if path not in self._data or not path.exists():
            return None
        stat = path.stat()
        # Stale if file was written before today
        if datetime.fromtimestamp(stat.st_mtime).date() < datetime.now().date():
            self.invalidate(path)
            return None
        if stat.st_mtime != self._mtime.get(path):
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


def _is_stale(path: Path) -> bool:
    """Return True if the file doesn't exist or was last modified before today."""
    if not path.exists():
        return True
    mtime_date = datetime.fromtimestamp(path.stat().st_mtime).date()
    return mtime_date < datetime.now().date()


def dump_pipe_configs(config_dir: Path, force: bool = False) -> pd.DataFrame | None:
    """Download PipeConfigs from Azure Table Storage.

    Re-downloads if the parquet file is from a previous day (daily staleness
    check) or if force=True. In-memory cache with mtime tracking avoids
    repeated disk reads within the same process.

    Writes to config_dir/PipeConfigs.parquet.

    Columns: ParentPipe, PipeName, GFPipeID, PipeCode
    (dataset availability is tracked separately in the daily availability cache)
    """
    parquet_path = config_dir / "PipeConfigs.parquet"

    if not force and not _is_stale(parquet_path):
        cached = _cache.get(parquet_path)
        if cached is not None:
            return cached
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            _cache.put(parquet_path, df)
            return df

    # Force, stale, or file doesn't exist — re-download
    _cache.invalidate(parquet_path)
    logger.info("Dumping PipeConfigs from Azure Table Storage")
    try:
        with get_table(settings.pipe_configs_table) as table_client:
            df = pd.DataFrame(
                table_client.query_entities(
                    "",
                    select=["ParentPipe", "PipeName", "GFPipeID", "PipeCode"],
                )
            )
        df["GFPipeID"] = df["GFPipeID"].apply(
            lambda x: str(int(x.value if hasattr(x, "value") else x)).zfill(3)
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

    if not force and not _is_stale(parquet_path):
        cached = _cache.get(parquet_path)
        if cached is not None:
            return cached
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            _cache.put(parquet_path, df)
            return df

    # Force, stale, or file doesn't exist — re-download
    _cache.invalidate(parquet_path)
    logger.info("Dumping LocConfigs from Azure Table Storage")
    try:
        with get_table(settings.Loc_configs_table) as table_client:
            df = pd.DataFrame(table_client.query_entities(""), dtype=str)
    except Exception as e:
        logger.error(f"LocConfigs dump failed: {e}")
        return None

    # Unwrap EdmType wrapper objects and datetime subclasses returned by the Azure SDK
    def _unwrap(x):
        if hasattr(x, "value"):
            return x.value
        if isinstance(x, datetime):
            return x.isoformat()
        return x

    for col in df.columns:
        df[col] = df[col].apply(_unwrap)

    if "GFPipeID" in df.columns:
        df["GFPipeID"] = df["GFPipeID"].apply(
            lambda x: str(int(x)).zfill(3) if pd.notna(x) else x
        )

    df.to_parquet(parquet_path, index=False)
    _cache.put(parquet_path, df)
    return df



def update_Loc_configs(df: pl.DataFrame) -> None:
    """Upsert new rows into the LocConfigs Azure Table.

    Batches in groups of 90 (Azure Table transaction limit is 100).
    Invalidates all cached configs since the backing data changed.
    """
    # Cast all columns to Utf8 so every value is a plain Python str
    clean = df.with_columns(pl.all().cast(pl.Utf8).fill_null(""))
    operations = [
        ("upsert", row)
        for row in clean.to_dicts()
    ]

    for i in range(0, len(operations), 90):
        batch = operations[i : min(i + 90, len(operations))]
        try:
            with get_table(settings.Loc_configs_table) as table_client:
                table_client.submit_transaction(batch)  # type: ignore
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
